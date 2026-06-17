package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HealthInfo is the JSON body returned by GET /health.
type HealthInfo struct {
	Status      string `json:"status"`
	Version     string `json:"version"`
	ChannelID   string `json:"channel_id"`
	ChannelName string `json:"channel_name"`
	UptimeSec   int64  `json:"uptime_seconds"`
	StartedAt   string `json:"started_at"`
	HLSURL      string `json:"hls_url,omitempty"`
	DASHURL     string `json:"dash_url,omitempty"`
	ESAMURL     string `json:"esam_url,omitempty"`
}

func (s *Server) healthHandler(startTime time.Time, version string, releaseNotes string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		base := s.cfg.Server.BaseURL
		info := HealthInfo{
			Status:      "ok",
			Version:     version,
			ChannelID:   s.cfg.Channel.ID,
			ChannelName: s.cfg.Channel.Name,
			UptimeSec:   int64(time.Since(startTime).Seconds()),
			StartedAt:   startTime.UTC().Format(time.RFC3339),
		}
		if s.cfg.Packaging.HLS.Enabled {
			info.HLSURL = base + "/hls/master.m3u8"
		}
		if s.cfg.Packaging.DASH.Enabled {
			info.DASHURL = base + "/dash/manifest.mpd"
		}
		if s.cfg.ESAM.Enabled {
			info.ESAMURL = base + s.cfg.ESAM.Path
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Cache-Control", "no-cache")
		json.NewEncoder(w).Encode(info)
		if releaseNotes != "" {
			fmt.Fprintf(w, "\n%s", releaseNotes)
		}
	}
}
