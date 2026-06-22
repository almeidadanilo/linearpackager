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
	audioDir     string            // outDir/audio — shared audio track across all rungs
	rungs        map[string]*rungState
	codecStrings map[string]string // rung → actual RFC 6381 codec string from avcC box
	mu           sync.Mutex        // protects all mutable fields below
	mpdMu        sync.Mutex        // serialises manifest.mpd writes (Windows rename contention)
	startTime    time.Time
	sem          chan struct{}

	spliceIn     <-chan splice.Event
	spliceEvents []spliceRecord // retained splice events to include in MPD

	// Audio track — populated only from the first ladder rung.
	audioEntries []segEntry
	audioReady   bool // true once audio/init.mp4 has been written
}

func New(cfg *config.Config) *Packager {
	rungs := make(map[string]*rungState)
	for _, r := range cfg.Transcoder.Ladder {
		rungs[r.Name] = &rungState{window: cfg.Packaging.DASH.WindowSize}
	}
	return &Packager{
		cfg:          cfg,
		outDir:       cfg.Packaging.DASH.OutputDir,
		audioDir:     filepath.Join(cfg.Packaging.DASH.OutputDir, "audio"),
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

	// Only extract audio from the first rung — all rungs carry identical audio.
	isAudioRung := seg.Rung == p.cfg.Transcoder.Ladder[0].Name
	audioPath := ""
	if isAudioRung {
		audioPath = filepath.Join(p.audioDir, mp4Name)
	}

	// audioFragDurUs: use 1.5× segment duration so the entire audio segment
	// always lands in a single moof/mdat fragment.
	audioFragDurUs := p.cfg.Packaging.SegmentDuration * 1_500_000
	if err := remuxToFMP4(seg.Path, mp4Path, audioPath, audioFragDurUs); err != nil {
		slog.Error("dash: remux failed", "segment", filepath.Base(seg.Path), "error", err)
		return
	}

	// ── Process video track ───────────────────────────────────────────────────
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
	if ts := parseTrackTimescales(initBytes); len(ts) > 0 {
		patchTFDT(mediaBytes, seg.StartTime, ts)
	}
	if err := os.WriteFile(mp4Path, mediaBytes, 0o644); err != nil {
		slog.Error("dash: write media segment failed", "rung", seg.Rung, "error", err)
		return
	}

	// Write video init.mp4 once per rung and extract codec string from avcC.
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

	// ── Process audio track (first rung only) ─────────────────────────────────
	var audioToDelete []string
	var audioEntry *segEntry
	if isAudioRung {
		aData, aErr := os.ReadFile(audioPath)
		if aErr != nil {
			slog.Error("dash: read audio fmp4 failed", "error", aErr)
		} else {
			aInit, aMedia, aErr := splitFMP4(aData)
			if aErr != nil {
				slog.Error("dash: audio fMP4 split failed", "error", aErr)
			} else {
				if ats := parseTrackTimescales(aInit); len(ats) > 0 {
					patchTFDT(aMedia, seg.StartTime, ats)
				}
				if aErr = os.WriteFile(audioPath, aMedia, 0o644); aErr != nil {
					slog.Error("dash: write audio segment failed", "error", aErr)
				} else {
					audioEntry = &segEntry{filename: mp4Name, duration: seg.Duration, index: seg.Index}
				}
				// Write audio init.mp4 once.
				audioInitPath := filepath.Join(p.audioDir, "init.mp4")
				if _, statErr := os.Stat(audioInitPath); os.IsNotExist(statErr) && len(aInit) > 0 {
					if aErr = util.WriteFileAtomic(audioInitPath, aInit); aErr != nil {
						slog.Error("dash: write audio init failed", "error", aErr)
					} else {
						slog.Info("dash: audio init segment written")
					}
				}
			}
		}
	}

	// ── Update shared state ───────────────────────────────────────────────────
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
	if audioEntry != nil {
		p.audioEntries = append(p.audioEntries, *audioEntry)
		p.audioReady = true
		if len(p.audioEntries) > p.cfg.Packaging.DASH.WindowSize {
			for _, old := range p.audioEntries[:len(p.audioEntries)-p.cfg.Packaging.DASH.WindowSize] {
				audioToDelete = append(audioToDelete, filepath.Join(p.audioDir, old.filename))
			}
			p.audioEntries = p.audioEntries[len(p.audioEntries)-p.cfg.Packaging.DASH.WindowSize:]
		}
	}
	p.mu.Unlock()

	for _, path := range append(toDelete, audioToDelete...) {
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
	return fmt.Sprintf("avc1.%02x%02x%02x", data[idx+5], data[idx+6], data[idx+7])
}

// remuxToFMP4 converts an MPEG-TS segment to fragmented MP4.
// videoPath receives a video-only fMP4 (frag_keyframe).
// If audioPath is non-empty, an audio-only fMP4 is also written in the same
// FFmpeg invocation using frag_duration so the whole segment lands in one
// fragment regardless of AAC frame boundaries.
// audioFragDurUs is the minimum fragment duration for audio in microseconds;
// set it to at least the segment duration to guarantee a single fragment.
func remuxToFMP4(tsPath, videoPath, audioPath string, audioFragDurUs int) error {
	// Video: strip audio, fragment on keyframes (every segment starts with one).
	// delay_moov defers moov until after the first fragment so avcC/esds boxes
	// are fully populated before they are written.
	args := []string{
		"-y", "-loglevel", "error",
		"-i", tsPath,
		"-map", "0:v", "-c:v", "copy",
		"-f", "mp4", "-movflags", "frag_keyframe+delay_moov+default_base_moof",
		videoPath,
	}
	if audioPath != "" {
		// Audio: strip video, convert ADTS→raw AAC, fragment at segment boundary.
		// frag_keyframe enables fragmentation; -frag_duration (an AVFormatContext
		// option, not a movflag) sets the minimum fragment size so the whole
		// ~6s segment collapses into one moof+mdat.  This avoids the movflag
		// "frag_duration" constant which was only added in newer FFmpeg builds.
		args = append(args,
			"-map", "0:a", "-c:a", "copy", "-bsf:a", "aac_adtstoasc",
			"-f", "mp4",
			"-movflags", "frag_keyframe+delay_moov+default_base_moof",
			"-frag_duration", fmt.Sprintf("%d", audioFragDurUs),
			audioPath,
		)
	}
	cmd := exec.Command("ffmpeg", args...)
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

	codecSnap := make(map[string]string, len(p.codecStrings))
	for k, v := range p.codecStrings {
		codecSnap[k] = v
	}
	audioReady := p.audioReady
	// Evict splice events only after the break has left the timeshift window.
	// Dropping an event the instant the break ends collapses the manifest from
	// 2 periods back to 1 while the player is still inside p1, causing it to
	// lose its current period and stall downloading only manifests.
	minUpdate := p.cfg.Packaging.SegmentDuration
	tsDepth := p.cfg.Packaging.DASH.WindowSize * minUpdate
	presentDelay := minUpdate * 3
	var elapsed float64
	if !startTime.IsZero() {
		elapsed = time.Since(startTime).Seconds()
	}
	if !startTime.IsZero() {
		// Keep the p1 period alive for tsDepth seconds after the break ends so
		// the player has time to exit the ad and buffer content.  The EventStream
		// inside p1 is removed separately (see below) as soon as the declared
		// ad duration elapses, preventing SSAI from treating the lingering signal
		// as a new ad opportunity and looping manifest-only fetches after the ad.
		tail := float64(tsDepth)
		var active []spliceRecord
		for _, sr := range p.spliceEvents {
			if elapsed < sr.presentSec+sr.event.Duration.Seconds()+tail {
				active = append(active, sr)
			}
		}
		p.spliceEvents = active
	}
	spliceSnap := make([]spliceRecord, len(p.spliceEvents))
	copy(spliceSnap, p.spliceEvents)
	p.mu.Unlock()
	segDurMs := p.cfg.Packaging.SegmentDuration * 1000

	segDurSec := float64(p.cfg.Packaging.SegmentDuration)

	// ── Build period list ─────────────────────────────────────────────────────
	// Two-period structure per break:
	//
	//   p(n)   Content  [prevEnd, splicePoint) — closed
	//   p(n+1) Ad/Splice [splicePoint, ∞)      — open; SSAI replaces this with
	//                                             actual ad content and manages
	//                                             the return-to-content itself.
	//
	// SSAI expects the SCTE-35 EventStream in a dedicated second period, not
	// embedded inside the content period.
	type mpdPeriod struct {
		id        int
		startSec  float64
		endSec    float64       // 0 = open (last period)
		isAd      bool
		spliceEvt *spliceRecord // non-nil: period carries an EventStream
	}
	// Snap a presentation time to the nearest segment boundary so that period
	// boundaries always coincide with a segment edge.
	snapToSeg := func(sec float64) float64 {
		return math.Round(sec/segDurSec) * segDurSec
	}
	var periods []mpdPeriod
	pid := 0
	contentStart := 0.0
	for i := range spliceSnap {
		sr := &spliceSnap[i]
		snappedSplice := snapToSeg(sr.presentSec)
		periods = append(periods, mpdPeriod{id: pid, startSec: contentStart, endSec: snappedSplice})
		pid++
		contentStart = snappedSplice
	}
	lastSplice := (*spliceRecord)(nil)
	if len(spliceSnap) > 0 {
		lastSplice = &spliceSnap[len(spliceSnap)-1]
	}
	periods = append(periods, mpdPeriod{id: pid, startSec: contentStart, isAd: lastSplice != nil, spliceEvt: lastSplice})

	// Audio bandwidth from the first rung configuration.
	audioBwBps := util.ParseBitrateKbps(p.cfg.Transcoder.Ladder[0].AudioBitrate) * 1000

	// ── Write MPD ─────────────────────────────────────────────────────────────
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"` + "\n")
	b.WriteString(`     xmlns:scte35="http://www.scte.org/schemas/35/2016"` + "\n")
	b.WriteString(`     profiles="urn:mpeg:dash:profile:isoff-live:2011"` + "\n")
	b.WriteString(`     type="dynamic"` + "\n")
	fmt.Fprintf(&b, `     minimumUpdatePeriod="PT%dS"`+"\n", minUpdate)
	fmt.Fprintf(&b, `     minBufferTime="PT%dS"`+"\n", minUpdate*2)
	fmt.Fprintf(&b, `     suggestedPresentationDelay="PT%dS"`+"\n", presentDelay)
	fmt.Fprintf(&b, `     availabilityStartTime="%s"`+"\n", startTime.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, `     timeShiftBufferDepth="PT%dS">`+"\n", tsDepth)
	fmt.Fprintf(&b, `  <UTCTiming schemeIdUri="urn:mpeg:dash:utc:direct:2014" value="%s"/>`+"\n",
		time.Now().UTC().Format(time.RFC3339))

	for _, period := range periods {
		if period.endSec > 0 {
			fmt.Fprintf(&b, `  <Period id="p%d" start="PT%.3fS" duration="PT%.3fS">`+"\n",
				period.id, period.startSec, period.endSec-period.startSec)
		} else {
			fmt.Fprintf(&b, `  <Period id="p%d" start="PT%.3fS">`+"\n",
				period.id, period.startSec)
		}

		// EventStream: signals an ad opportunity to Iris SSAI.
		// Only emitted while the declared break window is still open.  Once the
		// break duration has elapsed the EventStream is dropped so SSAI does not
		// treat the lingering p1 period as a new ad opportunity.  p1 itself
		// remains for tsDepth more seconds (eviction block above) so the player
		// can smoothly return to content after the ad ends.
		if period.isAd && period.spliceEvt != nil &&
			elapsed < period.spliceEvt.presentSec+period.spliceEvt.event.Duration.Seconds() {
			sr := period.spliceEvt
			pto := int64(math.Round(period.startSec * 1000))
			durMs := int64(sr.event.Duration.Seconds() * 1000)
			fmt.Fprintf(&b, `    <EventStream schemeIdUri="urn:scte:scte35:2014:xml+bin" timescale="1000" presentationTimeOffset="%d">`+"\n", pto)
			fmt.Fprintf(&b, `      <Event presentationTime="%d" duration="%d" id="%d">`+"\n", pto, durMs, sr.event.ID)
			b.WriteString(`        <scte35:Signal>` + "\n")
			fmt.Fprintf(&b, `          <scte35:Binary>%s</scte35:Binary>`+"\n", sr.event.B64)
			b.WriteString(`        </scte35:Signal>` + "\n")
			b.WriteString(`      </Event>` + "\n")
			b.WriteString(`    </EventStream>` + "\n")
		}

		startSegNo := int(math.Round(period.startSec / segDurSec))
		pto := int64(math.Round(period.startSec * 1000))

		// Video AdaptationSet.
		b.WriteString(`    <AdaptationSet id="1" mimeType="video/mp4" contentType="video"` + "\n")
		b.WriteString(`                   segmentAlignment="true" startWithSAP="1">` + "\n")
		b.WriteString(`      <Role schemeIdUri="urn:mpeg:dash:role:2011" value="main"/>` + "\n")
		for _, r := range p.cfg.Transcoder.Ladder {
			bw := util.ParseBitrateKbps(r.VideoBitrate) * 1000
			codecs := codecSnap[r.Name]
			if codecs == "" {
				codecs = util.AVC1Codec(r.Width, r.Height, r.Framerate)
			}
			fmt.Fprintf(&b, `      <Representation id="%s" bandwidth="%d" width="%d" height="%d" codecs="%s">`+"\n",
				r.Name, bw, r.Width, r.Height, codecs)
			fmt.Fprintf(&b, `        <SegmentTemplate media="%s/seg$Number%%05d$.mp4"`+"\n", r.Name)
			fmt.Fprintf(&b, `                         initialization="%s/init.mp4"`+"\n", r.Name)
			fmt.Fprintf(&b, `                         startNumber="%d"`+"\n", startSegNo)
			fmt.Fprintf(&b, `                         presentationTimeOffset="%d"`+"\n", pto)
			fmt.Fprintf(&b, `                         duration="%d" timescale="1000"/>`+"\n", segDurMs)
			b.WriteString(`      </Representation>` + "\n")
		}
		b.WriteString(`    </AdaptationSet>` + "\n")

		// Audio AdaptationSet.
		if audioReady {
			b.WriteString(`    <AdaptationSet id="2" mimeType="audio/mp4" contentType="audio" lang="und"` + "\n")
			b.WriteString(`                   segmentAlignment="true" startWithSAP="1">` + "\n")
			b.WriteString(`      <Role schemeIdUri="urn:mpeg:dash:role:2011" value="main"/>` + "\n")
			fmt.Fprintf(&b, `      <Representation id="audio" bandwidth="%d" codecs="mp4a.40.2" audioSamplingRate="48000">`+"\n", audioBwBps)
			b.WriteString(`        <AudioChannelConfiguration schemeIdUri="urn:mpeg:dash:23003:3:audio_channel_configuration:2011" value="2"/>` + "\n")
			b.WriteString(`        <SegmentTemplate media="audio/seg$Number%05d$.mp4"` + "\n")
			b.WriteString(`                         initialization="audio/init.mp4"` + "\n")
			fmt.Fprintf(&b, `                         startNumber="%d"`+"\n", startSegNo)
			fmt.Fprintf(&b, `                         presentationTimeOffset="%d"`+"\n", pto)
			fmt.Fprintf(&b, `                         duration="%d" timescale="1000"/>`+"\n", segDurMs)
			b.WriteString(`      </Representation>` + "\n")
			b.WriteString(`    </AdaptationSet>` + "\n")
		}

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
		// Remove any stale init.mp4 from a previous run so it is always
		// regenerated with the current demux configuration (video-only).
		// Stale muxed inits cause MSE codec-mismatch errors in the player.
		_ = os.Remove(filepath.Join(dir, "init.mp4"))
	}
	if err := os.MkdirAll(p.audioDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", p.audioDir, err)
	}
	_ = os.Remove(filepath.Join(p.audioDir, "init.mp4"))
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
