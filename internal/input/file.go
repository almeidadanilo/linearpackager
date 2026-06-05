package input

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/synamedia/linear-packager/internal/config"
)

type FileSource struct {
	cfg config.FileInput
}

func NewFileSource(cfg config.FileInput) *FileSource {
	return &FileSource{cfg: cfg}
}

func (f *FileSource) Validate() error {
	info, err := os.Stat(f.cfg.Path)
	if err != nil {
		return fmt.Errorf("input file not accessible: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("input path %q is a directory, not a file", f.cfg.Path)
	}
	ext := strings.ToLower(filepath.Ext(f.cfg.Path))
	if ext != ".ts" && ext != ".mp4" {
		return fmt.Errorf("unsupported input extension %q (must be .ts or .mp4)", ext)
	}
	return nil
}

func (f *FileSource) FFmpegArgs() []string {
	var args []string
	if f.cfg.Loop {
		args = append(args, "-stream_loop", "-1")
	}
	if f.cfg.LiveSimulation {
		args = append(args, "-re")
	}
	args = append(args, "-i", f.cfg.Path)
	return args
}

func (f *FileSource) String() string {
	loop := ""
	if f.cfg.Loop {
		loop = " [loop]"
	}
	return fmt.Sprintf("file://%s%s", f.cfg.Path, loop)
}
