package transcoder

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/synamedia/linear-packager/internal/config"
	"github.com/synamedia/linear-packager/internal/input"
)

// buildFFmpegArgs constructs the full argument slice for a single-process
// ABR transcode into per-rung MPEG-TS segments.
//
// Strategy: one filter_complex splits + scales the video stream into N
// outputs, each encoded independently and written to its own directory
// under workDir. Audio is re-encoded per rung from the first input audio
// stream. A CSV segment list is written alongside segments so the watcher
// can detect completions without filesystem events.
func buildFFmpegArgs(cfg *config.Config, src input.Source, workDir string) []string {
	ladder := cfg.Transcoder.Ladder
	n := len(ladder)

	var args []string

	// Global options before inputs
	args = append(args, "-y", "-loglevel", "warning")
	args = append(args, src.FFmpegArgs()...)

	// filter_complex: split video then scale+fps each rung
	var fc strings.Builder
	fmt.Fprintf(&fc, "[0:v]split=%d", n)
	for i := range ladder {
		fmt.Fprintf(&fc, "[vs%d]", i)
	}
	for i, r := range ladder {
		fmt.Fprintf(&fc, ";[vs%d]scale=%d:%d,fps=%d[vr%d]", i, r.Width, r.Height, r.Framerate, i)
	}
	args = append(args, "-filter_complex", fc.String())

	for i, r := range ladder {
		rungDir := filepath.Join(workDir, r.Name)
		gop := cfg.Transcoder.KeyframeInterval * r.Framerate

		args = append(args,
			// stream mapping
			"-map", fmt.Sprintf("[vr%d]", i),
			"-map", "0:a:0",
			// video encode
			"-c:v", cfg.Transcoder.VideoCodec,
			"-preset", cfg.Transcoder.Preset,
			"-b:v", r.VideoBitrate,
			"-maxrate", scaleBitrate(r.VideoBitrate, 1.2),
			"-bufsize", scaleBitrate(r.VideoBitrate, 2.0),
			// GOP / keyframe control — strict so segments align cleanly
			"-g", strconv.Itoa(gop),
			"-keyint_min", strconv.Itoa(gop),
			"-sc_threshold", "0",
			// audio encode
			"-c:a", cfg.Transcoder.AudioCodec,
			"-b:a", r.AudioBitrate,
			// segment muxer
			"-f", "segment",
			"-segment_time", strconv.Itoa(cfg.Packaging.SegmentDuration),
			"-segment_format", "mpegts",
			"-segment_list", filepath.Join(rungDir, "segments.csv"),
			"-segment_list_type", "csv",
			filepath.Join(rungDir, "seg%05d.ts"),
		)
	}

	return args
}

// scaleBitrate multiplies a bitrate string like "4000k" or "4M" by factor.
func scaleBitrate(s string, factor float64) string {
	s = strings.TrimSpace(s)
	lower := strings.ToLower(s)
	switch {
	case strings.HasSuffix(lower, "k"):
		v, err := strconv.ParseFloat(strings.TrimSuffix(lower, "k"), 64)
		if err != nil {
			return s
		}
		return fmt.Sprintf("%dk", int(v*factor))
	case strings.HasSuffix(lower, "m"):
		v, err := strconv.ParseFloat(strings.TrimSuffix(lower, "m"), 64)
		if err != nil {
			return s
		}
		return fmt.Sprintf("%dk", int(v*factor*1000))
	default:
		return s
	}
}
