package dash

import (
	"context"
	"fmt"
	"log/slog"
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
	cfg       *config.Config
	outDir    string
	rungs     map[string]*rungState
	mu        sync.Mutex // protects rungs + startTime + spliceEvents
	mpdMu     sync.Mutex // serialises manifest.mpd writes (Windows rename contention)
	startTime time.Time
	sem       chan struct{}

	spliceIn     <-chan splice.Event
	spliceEvents []spliceRecord // retained splice events to include in MPD
}

func New(cfg *config.Config) *Packager {
	rungs := make(map[string]*rungState)
	for _, r := range cfg.Transcoder.Ladder {
		rungs[r.Name] = &rungState{window: cfg.Packaging.DASH.WindowSize}
	}
	return &Packager{
		cfg:    cfg,
		outDir: cfg.Packaging.DASH.OutputDir,
		rungs:  rungs,
		sem:    make(chan struct{}, maxConcurrentRemux),
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
			p.spliceEvents = append(p.spliceEvents, spliceRecord{
				event:      e,
				presentSec: elapsed,
			})
			// Trim old splice events that are outside the time-shift buffer.
			maxAge := float64(p.cfg.Packaging.DASH.WindowSize * p.cfg.Packaging.SegmentDuration)
			var keep []spliceRecord
			for _, sr := range p.spliceEvents {
				if elapsed-sr.presentSec <= maxAge {
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

	p.mu.Lock()
	if p.startTime.IsZero() {
		p.startTime = time.Now().Add(-time.Duration(float64(time.Second) * seg.StartTime))
	}
	state := p.rungs[seg.Rung]
	state.entries = append(state.entries, segEntry{
		filename: mp4Name,
		duration: seg.Duration,
		index:    seg.Index,
	})
	if len(state.entries) > state.window {
		state.entries = state.entries[len(state.entries)-state.window:]
	}
	p.mu.Unlock()

	if err := p.writeMPD(); err != nil {
		slog.Error("dash: MPD write failed", "error", err)
		return
	}
	slog.Debug("dash: segment ready", "rung", seg.Rung, "file", mp4Name)
}

// remuxToFMP4 converts an MPEG-TS segment to a self-contained fragmented MP4.
// Each output file carries its own moov box so no separate init segment is
// required (compatible with SegmentList-based DASH).
func remuxToFMP4(tsPath, mp4Path string) error {
	// AAC in MPEG-TS uses ADTS framing; MP4 requires ADIF (raw) framing.
	// The aac_adtstoasc BSF strips the ADTS headers during remux.
	cmd := exec.Command("ffmpeg", "-y", "-loglevel", "error",
		"-i", tsPath,
		"-c", "copy",
		"-bsf:a", "aac_adtstoasc",
		"-f", "mp4",
		"-movflags", "frag_keyframe+empty_moov+default_base_moof",
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
	snapshot := make(map[string][]segEntry, len(p.rungs))
	for name, state := range p.rungs {
		cp := make([]segEntry, len(state.entries))
		copy(cp, state.entries)
		snapshot[name] = cp
	}
	spliceSnap := make([]spliceRecord, len(p.spliceEvents))
	copy(spliceSnap, p.spliceEvents)
	p.mu.Unlock()

	minUpdate := p.cfg.Packaging.SegmentDuration
	tsDepth := p.cfg.Packaging.DASH.WindowSize * minUpdate

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"` + "\n")
	b.WriteString(`     type="dynamic"` + "\n")
	fmt.Fprintf(&b, `     minimumUpdatePeriod="PT%dS"`+"\n", minUpdate)
	fmt.Fprintf(&b, `     minBufferTime="PT%dS"`+"\n", minUpdate*2)
	fmt.Fprintf(&b, `     availabilityStartTime="%s"`+"\n", startTime.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, `     timeShiftBufferDepth="PT%dS">`+"\n", tsDepth)
	b.WriteString(`  <Period start="PT0S" id="1">` + "\n")

	// ── SCTE-35 EventStream ───────────────────────────────────────────────────
	if len(spliceSnap) > 0 {
		b.WriteString(`    <EventStream schemeIdUri="urn:scte:scte35:2013:bin" timescale="1">` + "\n")
		for _, sr := range spliceSnap {
			dur := int64(sr.event.Duration.Seconds())
			pt := int64(sr.presentSec)
			fmt.Fprintf(&b,
				`      <Event presentationTime="%d" duration="%d" id="%d">%s</Event>`+"\n",
				pt, dur, sr.event.ID, sr.event.B64,
			)
		}
		b.WriteString(`    </EventStream>` + "\n")
	}

	// ── AdaptationSet + Representations ──────────────────────────────────────
	b.WriteString(`    <AdaptationSet id="1" contentType="video" mimeType="video/mp4"` + "\n")
	b.WriteString(`                   codecs="avc1.64001f,mp4a.40.2"` + "\n")
	b.WriteString(`                   segmentAlignment="true" startWithSAP="1">` + "\n")

	for _, r := range p.cfg.Transcoder.Ladder {
		bw := (util.ParseBitrateKbps(r.VideoBitrate) + util.ParseBitrateKbps(r.AudioBitrate)) * 1000
		entries := snapshot[r.Name]

		fmt.Fprintf(&b, `      <Representation id="%s" bandwidth="%d" width="%d" height="%d">`+"\n",
			r.Name, bw, r.Width, r.Height)
		segDurMs := p.cfg.Packaging.SegmentDuration * 1000
		fmt.Fprintf(&b, `        <SegmentList duration="%d" timescale="1000">`+"\n", segDurMs)
		for _, e := range entries {
			fmt.Fprintf(&b, `          <SegmentURL media="%s/%s"/>`+"\n", r.Name, e.filename)
		}
		b.WriteString(`        </SegmentList>` + "\n")
		b.WriteString(`      </Representation>` + "\n")
	}

	b.WriteString(`    </AdaptationSet>` + "\n")
	b.WriteString(`  </Period>` + "\n")
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
