package dash

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/synamedia/linear-packager/internal/config"
	"github.com/synamedia/linear-packager/internal/segment"
	"github.com/synamedia/linear-packager/internal/splice"
	"github.com/synamedia/linear-packager/internal/util"
)

const maxConcurrentRemux = 3

type segEntry struct {
	filename string
	duration float64
	index    int
}

// spliceRecord holds a SCTE-35 event that has been signalled and should appear
// in the next MPD update as an EventStream element.
type spliceRecord struct {
	event       splice.Event
	presentSec  float64 // seconds from Period start when the break starts
}

type rungState struct {
	window  int
	entries []segEntry
}

// Packager converts each incoming TS segment to fragmented MP4, maintains a
// sliding window per rung, and rewrites the DASH MPD after every new segment.
// SCTE-35 events are injected as EventStream elements inside the Period.
type Packager struct {
	cfg          *config.Config
	outDir       string
	rungs        map[string]*rungState
	codecStrings map[string]string // rung → actual RFC 6381 codec string from avcC box
	mu           sync.Mutex        // protects rungs + startTime + spliceEvents + codecStrings
	mpdMu        sync.Mutex        // serialises manifest.mpd writes (Windows rename contention)
	startTime    time.Time
	sem          chan struct{}

	spliceIn     <-chan splice.Event
	spliceEvents []spliceRecord // retained splice events to include in MPD
}

func New(cfg *config.Config) *Packager {
	rungs := make(map[string]*rungState)
	for _, r := range cfg.Transcoder.Ladder {
		rungs[r.Name] = &rungState{window: cfg.Packaging.DASH.WindowSize}
	}
	return &Packager{
		cfg:          cfg,
		outDir:       cfg.Packaging.DASH.OutputDir,
		rungs:        rungs,
		codecStrings: make(map[string]string),
		sem:          make(chan struct{}, maxConcurrentRemux),
	}
}

// SetSpliceQueue wires the packager to the ESAM splice queue.
func (p *Packager) SetSpliceQueue(q *splice.Queue) {
	p.spliceIn = q.Subscribe()
}

// Start creates output directories and begins consuming segments.
func (p *Packager) Start(ctx context.Context, segs <-chan segment.Segment) error {
	if err := p.prepareOutputDirs(); err != nil {
		return err
	}
	// Background goroutine drains the splice channel independently of segments.
	if p.spliceIn != nil {
		go p.drainSplices(ctx)
	}
	go p.run(ctx, segs)
	return nil
}

func (p *Packager) drainSplices(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-p.spliceIn:
			p.mu.Lock()
			elapsed := float64(0)
			if !p.startTime.IsZero() {
				elapsed = time.Since(p.startTime).Seconds()
			}
			// Place the splice point 5 seconds ahead (pre-roll) in the presentation timeline.
			p.spliceEvents = append(p.spliceEvents, spliceRecord{
				event:      e,
				presentSec: elapsed + 5.0,
			})
			// Trim events whose break has fully elapsed from the presentation.
			var keep []spliceRecord
			for _, sr := range p.spliceEvents {
				breakEnd := sr.presentSec + sr.event.Duration.Seconds()
				if elapsed < breakEnd {
					keep = append(keep, sr)
				}
			}
			p.spliceEvents = keep
			p.mu.Unlock()

			slog.Info("dash: splice event queued for MPD",
				"event_id", e.ID,
				"duration_sec", e.Duration.Seconds(),
				"present_sec", elapsed,
			)
		}
	}
}

func (p *Packager) run(ctx context.Context, segs <-chan segment.Segment) {
	for {
		select {
		case <-ctx.Done():
			return
		case seg, ok := <-segs:
			if !ok {
				return
			}
			go p.handleSegment(ctx, seg)
		}
	}
}

func (p *Packager) handleSegment(ctx context.Context, seg segment.Segment) {
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-ctx.Done():
		return
	}

	mp4Name := strings.TrimSuffix(filepath.Base(seg.Path), ".ts") + ".mp4"
	mp4Path := filepath.Join(p.outDir, seg.Rung, mp4Name)

	if err := remuxToFMP4(seg.Path, mp4Path); err != nil {
		slog.Error("dash: remux failed", "segment", filepath.Base(seg.Path), "error", err)
		return
	}

	fmp4Data, err := os.ReadFile(mp4Path)
	if err != nil {
		slog.Error("dash: read fmp4 failed", "rung", seg.Rung, "error", err)
		return
	}
	initBytes, mediaBytes, err := splitFMP4(fmp4Data)
	if err != nil {
		slog.Error("dash: fMP4 split failed", "rung", seg.Rung, "error", err)
		return
	}

	// FFmpeg normalises remuxed timestamps to start at 0 for every segment.
	// DASH players use the MPD timeline to seek, so each fragment's
	// baseMediaDecodeTime must reflect its absolute position in the stream.
	// Use the actual PTS start time from the segment CSV (seg.StartTime) rather
	// than segIdx×segDur so the offset is exact even when segment durations
	// are not perfectly uniform or the packager attached mid-stream.
	if ts := parseTrackTimescales(initBytes); len(ts) > 0 {
		patchTFDT(mediaBytes, seg.StartTime, ts)
	}

	if err := os.WriteFile(mp4Path, mediaBytes, 0o644); err != nil {
		slog.Error("dash: write media segment failed", "rung", seg.Rung, "error", err)
		return
	}

	// Write init.mp4 once per rung and extract the actual codec string from avcC.
	var detectedCodec string
	initPath := filepath.Join(p.outDir, seg.Rung, "init.mp4")
	if _, statErr := os.Stat(initPath); os.IsNotExist(statErr) && len(initBytes) > 0 {
		if err := util.WriteFileAtomic(initPath, initBytes); err != nil {
			slog.Error("dash: write init segment failed", "rung", seg.Rung, "error", err)
			return
		}
		detectedCodec = parseAVC1Codec(initBytes)
		slog.Info("dash: init segment written", "rung", seg.Rung, "codec", detectedCodec)
	}

	p.mu.Lock()
	if detectedCodec != "" {
		p.codecStrings[seg.Rung] = detectedCodec
	}
	if p.startTime.IsZero() {
		p.startTime = time.Now().Add(-time.Duration(float64(time.Second) * seg.StartTime))
	}
	state := p.rungs[seg.Rung]
	state.entries = append(state.entries, segEntry{
		filename: mp4Name,
		duration: seg.Duration,
		index:    seg.Index,
	})
	var toDelete []string
	if len(state.entries) > state.window {
		for _, old := range state.entries[:len(state.entries)-state.window] {
			toDelete = append(toDelete, filepath.Join(p.outDir, seg.Rung, old.filename))
		}
		state.entries = state.entries[len(state.entries)-state.window:]
	}
	p.mu.Unlock()

	for _, path := range toDelete {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			slog.Warn("dash: failed to remove old segment", "path", path, "error", err)
		}
	}

	if err := p.writeMPD(); err != nil {
		slog.Error("dash: MPD write failed", "error", err)
		return
	}
	slog.Debug("dash: segment ready", "rung", seg.Rung, "file", mp4Name)
}

// splitFMP4 parses an in-memory self-contained fMP4 and returns:
//   - initBytes: ftyp + moov boxes only (MSE init segment)
//   - mediaBytes: moof + mdat boxes only (MSE media segment)
//
// All other box types (free, skip, mfra, sidx, …) are discarded.
// FFmpeg sometimes writes a "free" padding box between moov and the first
// moof; including it in the media segment would make the file start with
// "free" instead of "moof", causing both FFprobe and browser MSE to reject it.
func splitFMP4(data []byte) (initBytes, mediaBytes []byte, err error) {
	for off := 0; off+8 <= len(data); {
		size := int(binary.BigEndian.Uint32(data[off : off+4]))
		if size < 8 || off+size > len(data) {
			break
		}
		boxType := string(data[off+4 : off+8])
		switch boxType {
		case "ftyp", "moov":
			initBytes = append(initBytes, data[off:off+size]...)
		case "moof", "mdat":
			mediaBytes = append(mediaBytes, data[off:off+size]...)
		// free, skip, mfra, sidx, etc. — discard
		}
		off += size
	}
	if len(mediaBytes) == 0 {
		return nil, nil, fmt.Errorf("no moof/mdat boxes found in fMP4")
	}
	return initBytes, mediaBytes, nil
}

// parseAVC1Codec scans raw fMP4 bytes for an avcC box and returns the RFC 6381
// codec string "avc1.PPCCLL,mp4a.40.2" built from the actual SPS profile bytes.
// Returns "" when the avcC box cannot be found or is malformed.
func parseAVC1Codec(data []byte) string {
	idx := bytes.Index(data, []byte("avcC"))
	// idx is the offset of the box type field; size field is 4 bytes before it,
	// and the avcC payload starts 4 bytes after (version + profile + constraints + level).
	if idx < 4 || idx+8 > len(data) {
		return ""
	}
	if data[idx+4] != 1 { // configurationVersion must be 1
		return ""
	}
	return fmt.Sprintf("avc1.%02x%02x%02x,mp4a.40.2", data[idx+5], data[idx+6], data[idx+7])
}

// remuxToFMP4 converts an MPEG-TS segment to a self-contained fragmented MP4.
func remuxToFMP4(tsPath, mp4Path string) error {
	// AAC in MPEG-TS uses ADTS framing; MP4 requires ADIF (raw) framing.
	// The aac_adtstoasc BSF strips the ADTS headers during remux.
	// delay_moov defers the moov box until after the first fragment has been
	// processed.  This ensures aac_adtstoasc has seen an audio packet and can
	// write a complete AudioSpecificConfig into the esds box, and it produces
	// proper moof+mdat ordering (no standalone mdat before the first moof).
	cmd := exec.Command("ffmpeg", "-y", "-loglevel", "error",
		"-i", tsPath,
		"-c", "copy",
		"-bsf:a", "aac_adtstoasc",
		"-f", "mp4",
		"-movflags", "frag_keyframe+delay_moov+default_base_moof",
		mp4Path,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s: %w", strings.TrimSpace(string(out)), err)
	}
	return nil
}

func (p *Packager) writeMPD() error {
	p.mpdMu.Lock()
	defer p.mpdMu.Unlock()

	p.mu.Lock()
	startTime := p.startTime
	segDurSec := float64(p.cfg.Packaging.SegmentDuration)
	codecSnap := make(map[string]string, len(p.codecStrings))
	for k, v := range p.codecStrings {
		codecSnap[k] = v
	}
	// Evict splice events whose break has fully elapsed so they disappear from the MPD.
	if !startTime.IsZero() {
		elapsed := time.Since(startTime).Seconds()
		var active []spliceRecord
		for _, sr := range p.spliceEvents {
			if elapsed < sr.presentSec+sr.event.Duration.Seconds() {
				active = append(active, sr)
			}
		}
		p.spliceEvents = active
	}
	spliceSnap := make([]spliceRecord, len(p.spliceEvents))
	copy(spliceSnap, p.spliceEvents)
	p.mu.Unlock()

	minUpdate := p.cfg.Packaging.SegmentDuration
	tsDepth := p.cfg.Packaging.DASH.WindowSize * minUpdate
	segDurMs := p.cfg.Packaging.SegmentDuration * 1000
	presentDelay := minUpdate * 3

	// ── Build period list ─────────────────────────────────────────────────────
	// Period 0 is always a content period starting at 0. Each splice event
	// closes the current content period, opens an ad period, then opens a new
	// content period. EventStream goes inside the content period that precedes
	// each break so players/SSAI know when the break is coming.
	type mpdPeriod struct {
		id        int
		startSec  float64
		endSec    float64       // 0 = open (last period)
		isAd      bool
		spliceEvt *spliceRecord // non-nil: content period has an upcoming break
	}
	var periods []mpdPeriod
	pid := 0
	contentStart := 0.0
	for i := range spliceSnap {
		sr := &spliceSnap[i]
		breakEnd := sr.presentSec + sr.event.Duration.Seconds()
		periods = append(periods, mpdPeriod{id: pid, startSec: contentStart, endSec: sr.presentSec, spliceEvt: sr})
		pid++
		periods = append(periods, mpdPeriod{id: pid, startSec: sr.presentSec, endSec: breakEnd, isAd: true, spliceEvt: sr})
		pid++
		contentStart = breakEnd
	}
	periods = append(periods, mpdPeriod{id: pid, startSec: contentStart}) // final open period

	// ── Write MPD ─────────────────────────────────────────────────────────────
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"` + "\n")
	b.WriteString(`     xmlns:scte35="urn:scte:scte35:2014:xml+bin"` + "\n")
	b.WriteString(`     type="dynamic"` + "\n")
	fmt.Fprintf(&b, `     minimumUpdatePeriod="PT%dS"`+"\n", minUpdate)
	fmt.Fprintf(&b, `     minBufferTime="PT%dS"`+"\n", minUpdate*2)
	fmt.Fprintf(&b, `     suggestedPresentationDelay="PT%dS"`+"\n", presentDelay)
	fmt.Fprintf(&b, `     availabilityStartTime="%s"`+"\n", startTime.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, `     timeShiftBufferDepth="PT%dS">`+"\n", tsDepth)

	for _, period := range periods {
		if period.endSec > 0 {
			fmt.Fprintf(&b, `  <Period id="p%d" start="PT%.3fS" duration="PT%.3fS">`+"\n",
				period.id, period.startSec, period.endSec-period.startSec)
		} else {
			fmt.Fprintf(&b, `  <Period id="p%d" start="PT%.3fS">`+"\n",
				period.id, period.startSec)
		}

		// EventStream: emitted in the content period that precedes each ad break.
		// presentationTime is relative to the Period start (timescale=1 → seconds).
		if !period.isAd && period.spliceEvt != nil {
			sr := period.spliceEvt
			pt := int64(sr.presentSec - period.startSec)
			dur := int64(sr.event.Duration.Seconds())
			b.WriteString(`    <EventStream schemeIdUri="urn:scte:scte35:2014:xml+bin" timescale="1">` + "\n")
			fmt.Fprintf(&b, `      <Event presentationTime="%d" duration="%d" id="%d">`+"\n", pt, dur, sr.event.ID)
			b.WriteString(`        <scte35:Signal>` + "\n")
			fmt.Fprintf(&b, `          <scte35:Binary>%s</scte35:Binary>`+"\n", sr.event.B64)
			b.WriteString(`        </scte35:Signal>` + "\n")
			b.WriteString(`      </Event>` + "\n")
			b.WriteString(`    </EventStream>` + "\n")
		}

		// AdaptationSet — startNumber computed from period start so the player
		// requests the correct segment files for this period's time range.
		startSegNo := int(period.startSec / segDurSec)
		b.WriteString(`    <AdaptationSet id="1" mimeType="video/mp4"` + "\n")
		b.WriteString(`                   segmentAlignment="true" startWithSAP="1">` + "\n")
		for _, r := range p.cfg.Transcoder.Ladder {
			bw := (util.ParseBitrateKbps(r.VideoBitrate) + util.ParseBitrateKbps(r.AudioBitrate)) * 1000
			codecs := codecSnap[r.Name]
			if codecs == "" {
				codecs = util.AVC1Codec(r.Width, r.Height, r.Framerate)
			}
			fmt.Fprintf(&b, `      <Representation id="%s" bandwidth="%d" width="%d" height="%d" codecs="%s">`+"\n",
				r.Name, bw, r.Width, r.Height, codecs)
			fmt.Fprintf(&b, `        <SegmentTemplate media="%s/seg$Number%%05d$.mp4"`+"\n", r.Name)
			fmt.Fprintf(&b, `                         initialization="%s/init.mp4"`+"\n", r.Name)
			fmt.Fprintf(&b, `                         startNumber="%d"`+"\n", startSegNo)
			fmt.Fprintf(&b, `                         duration="%d" timescale="1000"/>`+"\n", segDurMs)
			b.WriteString(`      </Representation>` + "\n")
		}
		b.WriteString(`    </AdaptationSet>` + "\n")
		b.WriteString(`  </Period>` + "\n")
	}

	b.WriteString(`</MPD>` + "\n")

	return util.WriteFileAtomic(filepath.Join(p.outDir, "manifest.mpd"), []byte(b.String()))
}

func (p *Packager) prepareOutputDirs() error {
	for _, r := range p.cfg.Transcoder.Ladder {
		dir := filepath.Join(p.outDir, r.Name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}
	return nil
}

// scanBoxes calls fn(boxType, payload) for every top-level ISO BMFF box in
// data.  payload is the box contents excluding the 8-byte size+type header.
// Modifications to payload write through to the original slice.
func scanBoxes(data []byte, fn func(boxType string, payload []byte)) {
	for off := 0; off+8 <= len(data); {
		size := int(binary.BigEndian.Uint32(data[off : off+4]))
		if size < 8 || off+size > len(data) {
			break
		}
		fn(string(data[off+4:off+8]), data[off+8:off+size])
		off += size
	}
}

// parseTrackTimescales scans the moov box in initBytes and returns a map of
// track_ID → timescale read from tkhd and mdhd respectively.
func parseTrackTimescales(initBytes []byte) map[uint32]uint32 {
	result := make(map[uint32]uint32)
	scanBoxes(initBytes, func(bt string, moovPayload []byte) {
		if bt != "moov" {
			return
		}
		scanBoxes(moovPayload, func(bt2 string, trakPayload []byte) {
			if bt2 != "trak" {
				return
			}
			var trackID, timescale uint32
			scanBoxes(trakPayload, func(bt3 string, p []byte) {
				switch bt3 {
				case "tkhd":
					// FullBox: version(1)+flags(3) then ctime+mtime+track_ID
					// v=0: ctime(4)+mtime(4)+track_ID(4) → track_ID at p[12]
					// v=1: ctime(8)+mtime(8)+track_ID(4) → track_ID at p[24]  ← wait
					// Actually: v=0 payload: version(1)+flags(3)+ctime(4)+mtime(4)+track_ID(4)=p[12]
					//           v=1 payload: version(1)+flags(3)+ctime(8)+mtime(8)+track_ID(4)=p[20]
					if len(p) >= 16 {
						ver := p[0]
						if ver == 0 {
							trackID = binary.BigEndian.Uint32(p[12:16])
						} else if ver == 1 && len(p) >= 24 {
							trackID = binary.BigEndian.Uint32(p[20:24])
						}
					}
				case "mdia":
					scanBoxes(p, func(bt4 string, mp []byte) {
						if bt4 != "mdhd" {
							return
						}
						// v=0: version(1)+flags(3)+ctime(4)+mtime(4)+timescale(4) → mp[12]
						// v=1: version(1)+flags(3)+ctime(8)+mtime(8)+timescale(4) → mp[20]
						if len(mp) >= 16 {
							ver := mp[0]
							if ver == 0 {
								timescale = binary.BigEndian.Uint32(mp[12:16])
							} else if ver == 1 && len(mp) >= 24 {
								timescale = binary.BigEndian.Uint32(mp[20:24])
							}
						}
					})
				}
			})
			if trackID > 0 && timescale > 0 {
				result[trackID] = timescale
			}
		})
	})
	return result
}

// patchTFDT adds segIdx×segDurSec×timescale to every tfdt.baseMediaDecodeTime
// found in mediaBytes.  The patch is applied in-place.
// This corrects FFmpeg's timestamp normalisation: remuxing TS→MP4 subtracts
// the initial PTS so every segment starts at tfdt=0 regardless of its position
// in the stream.  DASH players derive the seek target from the MPD timeline
// (segIdx×segDurSec), so tfdt must match.
func patchTFDT(mediaBytes []byte, startTimeSec float64, timescales map[uint32]uint32) {
	scanBoxes(mediaBytes, func(bt string, moofPayload []byte) {
		if bt != "moof" {
			return
		}
		scanBoxes(moofPayload, func(bt2 string, trafPayload []byte) {
			if bt2 != "traf" {
				return
			}
			var trackID uint32
			var offset uint64
			scanBoxes(trafPayload, func(bt3 string, p []byte) {
				switch bt3 {
				case "tfhd":
					// FullBox: version(1)+flags(3)+track_ID(4)
					if len(p) >= 8 {
						trackID = binary.BigEndian.Uint32(p[4:8])
						if ts, ok := timescales[trackID]; ok && ts > 0 {
							offset = uint64(math.Round(startTimeSec * float64(ts)))
						}
					}
				case "tfdt":
					if offset == 0 || len(p) < 4 {
						return
					}
					// FullBox: version(1)+flags(3)+baseMediaDecodeTime(4 or 8)
					ver := p[0]
					if ver == 1 && len(p) >= 12 {
						bmdt := uint64(p[4])<<56 | uint64(p[5])<<48 | uint64(p[6])<<40 | uint64(p[7])<<32 |
							uint64(p[8])<<24 | uint64(p[9])<<16 | uint64(p[10])<<8 | uint64(p[11])
						bmdt += offset
						p[4] = byte(bmdt >> 56)
						p[5] = byte(bmdt >> 48)
						p[6] = byte(bmdt >> 40)
						p[7] = byte(bmdt >> 32)
						p[8] = byte(bmdt >> 24)
						p[9] = byte(bmdt >> 16)
						p[10] = byte(bmdt >> 8)
						p[11] = byte(bmdt)
					} else if ver == 0 && len(p) >= 8 {
						bmdt := uint32(p[4])<<24 | uint32(p[5])<<16 | uint32(p[6])<<8 | uint32(p[7])
						bmdt += uint32(offset)
						p[4] = byte(bmdt >> 24)
						p[5] = byte(bmdt >> 16)
						p[6] = byte(bmdt >> 8)
						p[7] = byte(bmdt)
					}
				}
			})
		})
	})
}
