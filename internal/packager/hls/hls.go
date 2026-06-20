package hls

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/synamedia/linear-packager/internal/config"
	"github.com/synamedia/linear-packager/internal/segment"
	"github.com/synamedia/linear-packager/internal/splice"
	"github.com/synamedia/linear-packager/internal/util"
)

// ── per-entry state ──────────────────────────────────────────────────────────

type entry struct {
	segRef   string
	duration float64
	index    int
	// SCTE-35 markers — at most one per entry is set
	cueOut      bool
	cueOutDur   float64
	scte35B64   string // base64 SCTE-35 payload; set for cueOut and cueOutCont
	cueIn       bool
	cueOutCont  bool
	contElapsed float64
	contTotal   float64
}

// ── per-rung break tracking ──────────────────────────────────────────────────

type breakState struct {
	totalDur  float64 // total break duration (seconds)
	elapsed   float64 // how much of the break has already been covered
	scte35B64 string  // base64 payload repeated on every CUE-OUT-CONT
}

type rungState struct {
	window   int
	entries  []entry
	maxDur   float64
	inBreak  *breakState // nil when not in a break
	applied  bool        // true when the current pendingEvt has been applied to this rung
}

// ── packager ─────────────────────────────────────────────────────────────────

// minStartSegments is the number of segments each rung must accumulate before
// the master playlist is written. This ensures players always start with a
// healthy live buffer and never hit the live edge cold.
const minStartSegments = 3

// Packager maintains per-rung sliding-window HLS media playlists and a static
// master playlist.  It also injects SCTE-35 CUE-OUT/IN markers when a splice
// event is received from the ESAM queue.
type Packager struct {
	cfg           *config.Config
	outDir        string
	rungs         map[string]*rungState
	relDirs       map[string]string // rung → relative URL prefix from playlist dir to segments dir
	masterWritten bool

	spliceIn    <-chan splice.Event
	pendingEvt  *splice.Event
	pendingLeft int // how many rungs have yet to apply the pending event
}

func New(cfg *config.Config) *Packager {
	rungs := make(map[string]*rungState)
	relDirs := make(map[string]string)

	for _, r := range cfg.Transcoder.Ladder {
		rungs[r.Name] = &rungState{window: cfg.Packaging.HLS.PlaylistWindow}

		playlistDir := filepath.Join(cfg.Packaging.HLS.OutputDir, r.Name)
		segDir := filepath.Join(cfg.Packaging.WorkDir, r.Name)
		rel, err := filepath.Rel(playlistDir, segDir)
		if err != nil {
			rel = segDir
		}
		relDirs[r.Name] = filepath.ToSlash(rel)
	}

	return &Packager{
		cfg:     cfg,
		outDir:  cfg.Packaging.HLS.OutputDir,
		rungs:   rungs,
		relDirs: relDirs,
	}
}

// SetSpliceQueue wires the packager to the ESAM splice queue.
func (p *Packager) SetSpliceQueue(q *splice.Queue) {
	p.spliceIn = q.Subscribe()
}

// Start creates output directories, writes the static master playlist, and
// begins consuming segments in a background goroutine.
func (p *Packager) Start(ctx context.Context, segs <-chan segment.Segment) error {
	if err := p.prepareOutputDirs(); err != nil {
		return err
	}
	go p.run(ctx, segs)
	return nil
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
			// Non-blocking drain of incoming splice events.
			p.drainSplices()

			if err := p.handleSegment(seg); err != nil {
				slog.Error("hls: segment handling failed", "rung", seg.Rung, "error", err)
			}
		}
	}
}

// drainSplices picks up any queued splice event (non-blocking).
// Only one event is queued at a time; a second arrival before the first has
// been applied to all rungs replaces the first.
func (p *Packager) drainSplices() {
	for {
		select {
		case e := <-p.spliceIn:
			p.pendingEvt = &e
			p.pendingLeft = len(p.rungs)
			// Reset applied flags on all rungs
			for _, rs := range p.rungs {
				rs.applied = false
			}
			slog.Info("hls: splice event queued",
				"event_id", e.ID,
				"duration_sec", e.Duration.Seconds(),
			)
		default:
			return
		}
	}
}

func (p *Packager) handleSegment(seg segment.Segment) error {
	state, ok := p.rungs[seg.Rung]
	if !ok {
		return fmt.Errorf("unknown rung %q", seg.Rung)
	}

	relDir := p.relDirs[seg.Rung]
	e := entry{
		segRef:   relDir + "/" + filepath.Base(seg.Path),
		duration: seg.Duration,
		index:    seg.Index,
	}

	// ── SCTE-35 break state machine ──────────────────────────────────────────
	if state.inBreak != nil {
		// We are currently inside an ad break.
		state.inBreak.elapsed += seg.Duration
		if state.inBreak.elapsed >= state.inBreak.totalDur {
			e.cueIn = true
			state.inBreak = nil
		} else {
			e.cueOutCont = true
			e.contElapsed = state.inBreak.elapsed
			e.contTotal = state.inBreak.totalDur
			e.scte35B64 = state.inBreak.scte35B64
		}
	} else if p.pendingEvt != nil && !state.applied && !time.Now().Before(p.pendingEvt.SpliceTime) {
		// There's a pending splice, this rung has not applied it, and the 5s pre-roll has elapsed.
		e.cueOut = true
		e.cueOutDur = p.pendingEvt.Duration.Seconds()
		e.scte35B64 = p.pendingEvt.B64
		state.inBreak = &breakState{
			totalDur:  p.pendingEvt.Duration.Seconds(),
			elapsed:   0,
			scte35B64: p.pendingEvt.B64,
		}
		state.applied = true
		p.pendingLeft--
		if p.pendingLeft <= 0 {
			p.pendingEvt = nil
		}
	}

	// ── sliding window ───────────────────────────────────────────────────────
	if seg.Duration > state.maxDur {
		state.maxDur = seg.Duration
	}
	state.entries = append(state.entries, e)
	if len(state.entries) > state.window {
		state.entries = state.entries[len(state.entries)-state.window:]
	}

	if err := p.writeMediaPlaylist(seg.Rung, state); err != nil {
		return err
	}
	slog.Debug("hls: playlist updated", "rung", seg.Rung, "seq", e.index)

	if !p.masterWritten && p.allRungsReady() {
		if err := p.writeMasterPlaylist(); err != nil {
			return err
		}
		p.masterWritten = true
		slog.Info("hls: master playlist published", "min_segments", minStartSegments)
	}
	return nil
}

// allRungsReady returns true once every rung has accumulated at least
// minStartSegments entries in its sliding window.
func (p *Packager) allRungsReady() bool {
	for _, rs := range p.rungs {
		if len(rs.entries) < minStartSegments {
			return false
		}
	}
	return true
}

func (p *Packager) writeMediaPlaylist(rung string, state *rungState) error {
	targetDur := int(math.Ceil(state.maxDur)) + 1
	if targetDur < p.cfg.Packaging.SegmentDuration+1 {
		targetDur = p.cfg.Packaging.SegmentDuration + 1
	}

	seq := 0
	if len(state.entries) > 0 {
		seq = state.entries[0].index
	}

	var b strings.Builder
	fmt.Fprintln(&b, "#EXTM3U")
	fmt.Fprintln(&b, "#EXT-X-VERSION:3")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", targetDur)
	fmt.Fprintf(&b, "#EXT-X-MEDIA-SEQUENCE:%d\n", seq)

	for _, e := range state.entries {
		// SCTE-35 markers appear BEFORE the #EXTINF of their segment.
		if e.cueOut {
			if e.scte35B64 != "" {
				fmt.Fprintf(&b, "#EXT-OATCLS-SCTE35:%s\n", e.scte35B64)
			}
			fmt.Fprintf(&b, "#EXT-X-CUE-OUT:%.3f\n", e.cueOutDur)
		} else if e.cueOutCont {
			fmt.Fprintf(&b, "#EXT-X-CUE-OUT-CONT:ElapsedTime=%.3f,Duration=%.3f,SCTE35=%s\n",
				e.contElapsed, e.contTotal, e.scte35B64)
		} else if e.cueIn {
			fmt.Fprintln(&b, "#EXT-X-CUE-IN")
		}
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n", e.duration)
		fmt.Fprintln(&b, e.segRef)
	}

	path := filepath.Join(p.outDir, rung, "media.m3u8")
	return util.WriteFileAtomic(path, []byte(b.String()))
}

func (p *Packager) writeMasterPlaylist() error {
	var b strings.Builder
	fmt.Fprintln(&b, "#EXTM3U")
	fmt.Fprintln(&b, "#EXT-X-VERSION:3")

	for _, r := range p.cfg.Transcoder.Ladder {
		bw := (util.ParseBitrateKbps(r.VideoBitrate) + util.ParseBitrateKbps(r.AudioBitrate)) * 1000
		fmt.Fprintln(&b)
		fmt.Fprintf(&b, "#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d,CODECS=\"%s\"\n",
			bw, r.Width, r.Height, util.AVC1Codec(r.Width, r.Height, r.Framerate))
		fmt.Fprintf(&b, "%s/media.m3u8\n", r.Name)
	}

	path := filepath.Join(p.outDir, "master.m3u8")
	return util.WriteFileAtomic(path, []byte(b.String()))
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
