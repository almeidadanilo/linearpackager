package input

import (
	"fmt"
	"strings"

	"github.com/synamedia/linear-packager/internal/config"
)

type SRTSource struct {
	cfg config.SRTInput
}

func NewSRTSource(cfg config.SRTInput) *SRTSource {
	return &SRTSource{cfg: cfg}
}

func (s *SRTSource) Validate() error {
	if s.cfg.Host == "" {
		return fmt.Errorf("srt.host is required")
	}
	if s.cfg.Port <= 0 || s.cfg.Port > 65535 {
		return fmt.Errorf("srt.port must be between 1 and 65535, got %d", s.cfg.Port)
	}
	if s.cfg.Mode != "listener" && s.cfg.Mode != "caller" {
		return fmt.Errorf("srt.mode must be \"listener\" or \"caller\", got %q", s.cfg.Mode)
	}
	return nil
}

func (s *SRTSource) FFmpegArgs() []string {
	return []string{"-i", s.url()}
}

func (s *SRTSource) String() string {
	return fmt.Sprintf("srt://%s:%d [mode=%s]", s.cfg.Host, s.cfg.Port, s.cfg.Mode)
}

func (s *SRTSource) url() string {
	var params []string
	params = append(params, fmt.Sprintf("mode=%s", s.cfg.Mode))
	if s.cfg.LatencyMs > 0 {
		// FFmpeg SRT uses microseconds for the latency option
		params = append(params, fmt.Sprintf("latency=%d", s.cfg.LatencyMs*1000))
	}
	if s.cfg.Passphrase != "" {
		params = append(params, fmt.Sprintf("passphrase=%s", s.cfg.Passphrase))
	}
	return fmt.Sprintf("srt://%s:%d?%s", s.cfg.Host, s.cfg.Port, strings.Join(params, "&"))
}
