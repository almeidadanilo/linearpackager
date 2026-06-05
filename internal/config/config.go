package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	Channel    ChannelConfig    `json:"channel"`
	Input      InputConfig      `json:"input"`
	Transcoder TranscoderConfig `json:"transcoder"`
	Packaging  PackagingConfig  `json:"packaging"`
	ESAM       ESAMConfig       `json:"esam"`
	Server     ServerConfig     `json:"server"`
}

type ChannelConfig struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type InputConfig struct {
	Type string    `json:"type"` // "file" or "srt"
	File FileInput `json:"file"`
	SRT  SRTInput  `json:"srt"`
}

type FileInput struct {
	Path           string `json:"path"`
	Loop           bool   `json:"loop"`
	LiveSimulation bool   `json:"live_simulation"` // add -re to read at native rate
}

type SRTInput struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Mode       string `json:"mode"`        // "listener" or "caller"
	LatencyMs  int    `json:"latency_ms"`
	Passphrase string `json:"passphrase"`
}

type TranscoderConfig struct {
	VideoCodec       string       `json:"video_codec"`
	AudioCodec       string       `json:"audio_codec"`
	Preset           string       `json:"preset"`            // libx264 preset: ultrafast..veryslow
	KeyframeInterval int          `json:"keyframe_interval"` // seconds; must divide segment_duration evenly
	Ladder           []LadderRung `json:"ladder"`
}

type LadderRung struct {
	Name         string `json:"name"`
	Width        int    `json:"width"`
	Height       int    `json:"height"`
	VideoBitrate string `json:"video_bitrate"` // e.g. "4000k"
	AudioBitrate string `json:"audio_bitrate"` // e.g. "192k"
	Framerate    int    `json:"framerate"`
}

type PackagingConfig struct {
	SegmentDuration  int        `json:"segment_duration"`  // seconds
	WorkDir          string     `json:"work_dir"`          // where FFmpeg writes raw TS segments
	SegmentRetention int        `json:"segment_retention"` // TS files to keep per rung; 0 = auto
	HLS              HLSConfig  `json:"hls"`
	DASH             DASHConfig `json:"dash"`
}

type HLSConfig struct {
	Enabled        bool   `json:"enabled"`
	PlaylistWindow int    `json:"playlist_window"` // number of segments to keep in playlist
	OutputDir      string `json:"output_dir"`
}

type DASHConfig struct {
	Enabled    bool   `json:"enabled"`
	WindowSize int    `json:"window_size"` // number of segments in MPD window
	OutputDir  string `json:"output_dir"`
}

type ESAMConfig struct {
	Enabled                  bool   `json:"enabled"`
	ListenPort               int    `json:"listen_port"`
	Path                     string `json:"path"`                      // HTTP path, e.g. "/esam/notify"
	AcquisitionPointIdentity string `json:"acquisition_point_identity"` // matches esamid in SOAP payload
}

type ServerConfig struct {
	HTTPPort int    `json:"http_port"` // port that serves HLS/DASH segments
	BaseURL  string `json:"base_url"`  // public base URL for manifest URLs
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Transcoder.VideoCodec == "" {
		c.Transcoder.VideoCodec = "libx264"
	}
	if c.Transcoder.AudioCodec == "" {
		c.Transcoder.AudioCodec = "aac"
	}
	if c.Transcoder.Preset == "" {
		c.Transcoder.Preset = "veryfast"
	}
	if c.Transcoder.KeyframeInterval == 0 {
		c.Transcoder.KeyframeInterval = 2
	}
	if c.Packaging.WorkDir == "" {
		c.Packaging.WorkDir = "./output/segments"
	}
	if c.Packaging.SegmentRetention == 0 {
		// Minimum for linear (no startover): cover the largest window + 2 safety margin.
		maxWindow := c.Packaging.HLS.PlaylistWindow
		if c.Packaging.DASH.WindowSize > maxWindow {
			maxWindow = c.Packaging.DASH.WindowSize
		}
		c.Packaging.SegmentRetention = maxWindow + 2
	}
	if c.Packaging.HLS.PlaylistWindow == 0 {
		c.Packaging.HLS.PlaylistWindow = 5
	}
	if c.Packaging.DASH.WindowSize == 0 {
		c.Packaging.DASH.WindowSize = 5
	}
}

func (c *Config) validate() error {
	if c.Channel.ID == "" {
		return fmt.Errorf("channel.id is required")
	}
	if c.Input.Type != "file" && c.Input.Type != "srt" {
		return fmt.Errorf("input.type must be \"file\" or \"srt\"")
	}
	if c.Input.Type == "file" && c.Input.File.Path == "" {
		return fmt.Errorf("input.file.path is required for file input")
	}
	if c.Input.Type == "srt" {
		if c.Input.SRT.Host == "" {
			return fmt.Errorf("input.srt.host is required")
		}
		if c.Input.SRT.Port <= 0 {
			return fmt.Errorf("input.srt.port is required")
		}
		if c.Input.SRT.Mode != "listener" && c.Input.SRT.Mode != "caller" {
			return fmt.Errorf("input.srt.mode must be \"listener\" or \"caller\"")
		}
	}
	if len(c.Transcoder.Ladder) == 0 {
		return fmt.Errorf("transcoder.ladder must have at least one rung")
	}
	for i, r := range c.Transcoder.Ladder {
		if r.Name == "" {
			return fmt.Errorf("transcoder.ladder[%d].name is required", i)
		}
		if r.Width <= 0 || r.Height <= 0 {
			return fmt.Errorf("transcoder.ladder[%d] (%s): width and height must be positive", i, r.Name)
		}
		if r.VideoBitrate == "" {
			return fmt.Errorf("transcoder.ladder[%d] (%s): video_bitrate is required", i, r.Name)
		}
	}
	if c.Packaging.SegmentDuration <= 0 {
		return fmt.Errorf("packaging.segment_duration must be positive")
	}
	if c.Transcoder.KeyframeInterval > 0 && c.Packaging.SegmentDuration%c.Transcoder.KeyframeInterval != 0 {
		return fmt.Errorf(
			"packaging.segment_duration (%d) must be a multiple of transcoder.keyframe_interval (%d)",
			c.Packaging.SegmentDuration, c.Transcoder.KeyframeInterval,
		)
	}
	if !c.Packaging.HLS.Enabled && !c.Packaging.DASH.Enabled {
		return fmt.Errorf("at least one of packaging.hls or packaging.dash must be enabled")
	}
	minRetention := c.Packaging.HLS.PlaylistWindow
	if c.Packaging.DASH.WindowSize > minRetention {
		minRetention = c.Packaging.DASH.WindowSize
	}
	if c.Packaging.SegmentRetention < minRetention {
		return fmt.Errorf(
			"packaging.segment_retention (%d) must be >= max(hls.playlist_window, dash.window_size) = %d",
			c.Packaging.SegmentRetention, minRetention,
		)
	}
	if c.Packaging.HLS.Enabled && c.Packaging.HLS.OutputDir == "" {
		return fmt.Errorf("packaging.hls.output_dir is required when hls is enabled")
	}
	if c.Packaging.DASH.Enabled && c.Packaging.DASH.OutputDir == "" {
		return fmt.Errorf("packaging.dash.output_dir is required when dash is enabled")
	}
	if c.ESAM.Enabled {
		if c.ESAM.ListenPort <= 0 {
			return fmt.Errorf("esam.listen_port is required when esam is enabled")
		}
		if c.ESAM.Path == "" {
			return fmt.Errorf("esam.path is required when esam is enabled")
		}
	}
	if c.Server.HTTPPort <= 0 {
		return fmt.Errorf("server.http_port is required")
	}
	return nil
}
