package input

import (
	"fmt"

	"github.com/synamedia/linear-packager/internal/config"
)

// Source is a media input that knows how to express itself as FFmpeg arguments.
type Source interface {
	FFmpegArgs() []string
	Validate() error
	String() string
}

// New constructs the appropriate Source from config and validates it.
func New(cfg config.InputConfig) (Source, error) {
	switch cfg.Type {
	case "file":
		src := NewFileSource(cfg.File)
		if err := src.Validate(); err != nil {
			return nil, err
		}
		return src, nil
	case "srt":
		src := NewSRTSource(cfg.SRT)
		if err := src.Validate(); err != nil {
			return nil, err
		}
		return src, nil
	default:
		return nil, fmt.Errorf("unknown input type: %q", cfg.Type)
	}
}
