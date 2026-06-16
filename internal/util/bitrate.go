package util

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ParseBitrateKbps converts strings like "4000k", "4M", or "4000000" to kbps.
func ParseBitrateKbps(s string) int64 {
	s = strings.TrimSpace(strings.ToLower(s))
	switch {
	case strings.HasSuffix(s, "k"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(s, "k"), 64)
		return int64(v)
	case strings.HasSuffix(s, "m"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(s, "m"), 64)
		return int64(v * 1000)
	default:
		v, _ := strconv.ParseInt(s, 10, 64)
		return v / 1000
	}
}

// AVC1Codec returns the RFC 6381 codec string for H.264 High Profile at the
// level appropriate for the given resolution and frame rate.
func AVC1Codec(width, height, fps int) string {
	var level int
	switch {
	case width > 1280 || height > 720:
		if fps > 30 {
			level = 0x2a // 4.2
		} else {
			level = 0x28 // 4.0
		}
	case width > 720 || height > 576:
		level = 0x1f // 3.1
	default:
		level = 0x1e // 3.0
	}
	return fmt.Sprintf("avc1.6400%02x,mp4a.40.2", level)
}

// WriteFileAtomic writes data to path, creating the parent directory if
// needed.  On Windows, os.Rename over an open file fails with "Access is
// denied", so we write directly.  The DASH packager serialises calls with
// mpdMu, and the HLS packager is single-goroutine, so direct writes are safe.
func WriteFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	return os.WriteFile(path, data, 0o644)
}
