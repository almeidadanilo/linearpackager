package transcoder

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/synamedia/linear-packager/internal/config"
	"github.com/synamedia/linear-packager/internal/input"
	"github.com/synamedia/linear-packager/internal/segment"
)

const (
	segmentChannelBuffer = 64
	pollInterval         = 200 * time.Millisecond
	restartDelay         = 3 * time.Second
)

// Transcoder manages the FFmpeg child process and emits completed segments
// on a channel as they are written to disk.
type Transcoder struct {
	cfg      *config.Config
	src      input.Source
	workDir  string
	segments chan segment.Segment
}

func New(cfg *config.Config, src input.Source) *Transcoder {
	return &Transcoder{
		cfg:      cfg,
		src:      src,
		workDir:  cfg.Packaging.WorkDir,
		segments: make(chan segment.Segment, segmentChannelBuffer),
	}
}

// Segments returns the channel on which completed segments are published.
// The channel is closed when the transcoder shuts down.
func (t *Transcoder) Segments() <-chan segment.Segment {
	return t.segments
}

// Start prepares output directories and launches the FFmpeg pipeline in the
// background. It returns as soon as FFmpeg has been started; use the Segments
// channel to receive output. The transcoder restarts FFmpeg automatically on
// crash until ctx is cancelled.
func (t *Transcoder) Start(ctx context.Context) error {
	if err := t.prepareWorkDirs(); err != nil {
		return fmt.Errorf("preparing work dirs: %w", err)
	}
	go t.supervisorLoop(ctx)
	return nil
}

// supervisorLoop restarts FFmpeg until ctx is cancelled.
func (t *Transcoder) supervisorLoop(ctx context.Context) {
	defer close(t.segments)
	for {
		if ctx.Err() != nil {
			return
		}
		if err := t.runOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("ffmpeg exited unexpectedly, will restart",
				"error", err, "delay", restartDelay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(restartDelay):
			}
		}
	}
}

// runOnce starts FFmpeg, runs the segment watchers, and waits for FFmpeg to exit.
func (t *Transcoder) runOnce(ctx context.Context) error {
	// Remove stale CSV files so watchers start fresh.
	t.cleanCSVFiles()

	args := buildFFmpegArgs(t.cfg, t.src, t.workDir)
	slog.Info("launching ffmpeg", "rungs", len(t.cfg.Transcoder.Ladder))
	slog.Debug("ffmpeg args", "args", strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("creating stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting ffmpeg: %w", err)
	}

	go drainToLog(stderr)

	// Start per-rung segment watchers; they stop when watchCtx is cancelled.
	watchCtx, cancelWatch := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, r := range t.cfg.Transcoder.Ladder {
		wg.Add(1)
		go func(rung config.LadderRung) {
			defer wg.Done()
			t.watchRung(watchCtx, rung)
		}(r)
	}

	exitErr := cmd.Wait()
	cancelWatch()
	wg.Wait()
	return exitErr
}

// watchRung polls the segment CSV for rung r and emits each newly completed
// segment onto t.segments.
func (t *Transcoder) watchRung(ctx context.Context, rung config.LadderRung) {
	csvPath := filepath.Join(t.workDir, rung.Name, "segments.csv")
	seen := 0
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		segs, err := t.readNewSegments(csvPath, rung.Name, seen)
		if err != nil {
			continue // CSV may not exist yet; that's fine
		}
		for _, seg := range segs {
			select {
			case t.segments <- seg:
				slog.Debug("segment ready",
					"rung", seg.Rung,
					"index", seg.Index,
					"duration", fmt.Sprintf("%.3fs", seg.Duration))
				// Prune segment that fell outside the retention window.
				t.pruneSegment(seg.Rung, seg.Index-t.cfg.Packaging.SegmentRetention)
			case <-ctx.Done():
				return
			}
		}
		seen += len(segs)
	}
}

// readNewSegments reads the CSV file starting at line `skip` and returns any
// new segment entries found.
func (t *Transcoder) readNewSegments(csvPath, rungName string, skip int) ([]segment.Segment, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var segs []segment.Segment
	scanner := bufio.NewScanner(f)
	lineIdx := 0
	for scanner.Scan() {
		if lineIdx < skip {
			lineIdx++
			continue
		}
		line := scanner.Text()
		if line == "" {
			lineIdx++
			continue
		}
		seg, err := parseCSVLine(line, rungName, t.workDir, lineIdx)
		if err != nil {
			// A parse error on line N likely means FFmpeg is mid-flush.
			// Stop here — do NOT advance lineIdx so the next poll retries
			// this line once FFmpeg has finished writing it.
			break
		}
		segs = append(segs, seg)
		lineIdx++
	}
	return segs, scanner.Err()
}

// parseCSVLine parses one line of FFmpeg's segment list CSV:
// <filename>,<start_time>,<end_time>
func parseCSVLine(line, rungName, workDir string, index int) (segment.Segment, error) {
	// Guard against partially-written lines (e.g. when FFmpeg flushes the CSV
	// mid-write on a fast no-rate-limit encode on Windows).
	if strings.ContainsRune(line, 0) {
		return segment.Segment{}, fmt.Errorf("line contains null bytes (partial write)")
	}
	parts := strings.SplitN(line, ",", 3)
	if len(parts) != 3 {
		return segment.Segment{}, fmt.Errorf("expected 3 CSV fields, got %d", len(parts))
	}
	start, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return segment.Segment{}, fmt.Errorf("invalid start time %q: %w", parts[1], err)
	}
	end, err := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
	if err != nil {
		return segment.Segment{}, fmt.Errorf("invalid end time %q: %w", parts[2], err)
	}
	name := strings.TrimSpace(parts[0])
	if name == "" || !strings.HasSuffix(strings.ToLower(name), ".ts") {
		return segment.Segment{}, fmt.Errorf("unexpected filename %q", name)
	}
	// FFmpeg writes the full relative path in the CSV; take only the base name
	// and re-join with our canonical work directory to get a reliable path.
	absPath := filepath.Join(workDir, rungName, filepath.Base(name))

	// Verify the file is actually on disk before advertising it downstream.
	if _, err := os.Stat(absPath); err != nil {
		return segment.Segment{}, fmt.Errorf("segment file not accessible: %w", err)
	}
	return segment.Segment{
		Rung:      rungName,
		Path:      absPath,
		Index:     index,
		StartTime: start,
		Duration:  end - start,
	}, nil
}

func (t *Transcoder) prepareWorkDirs() error {
	for _, r := range t.cfg.Transcoder.Ladder {
		dir := filepath.Join(t.workDir, r.Name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}
	return nil
}

func (t *Transcoder) cleanCSVFiles() {
	for _, r := range t.cfg.Transcoder.Ladder {
		path := filepath.Join(t.workDir, r.Name, "segments.csv")
		_ = os.Remove(path)
	}
}

// pruneSegment removes the TS file for the given index from the work directory.
// Called after each new segment is emitted to keep disk usage at retention+1 files.
func (t *Transcoder) pruneSegment(rungName string, index int) {
	if index < 0 {
		return
	}
	path := filepath.Join(t.workDir, rungName, fmt.Sprintf("seg%05d.ts", index))
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		slog.Debug("prune: could not delete segment", "path", path, "error", err)
	}
}

func drainToLog(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			slog.Warn("[ffmpeg]", "msg", line)
		}
	}
}
