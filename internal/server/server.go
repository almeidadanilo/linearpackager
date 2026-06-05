package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"time"

	"github.com/synamedia/linear-packager/internal/config"
)

// ESAMRegistrar is implemented by esam.Server to register its HTTP handler
// onto an existing mux without creating a circular import.
type ESAMRegistrar interface {
	Register(mux *http.ServeMux)
}

// Server serves HLS playlists, raw TS segments, DASH segments + MPD, and
// optionally the ESAM inbound endpoint — all on a single HTTP port.
type Server struct {
	cfg      *config.Config
	esamReg  ESAMRegistrar
	version  string
	startAt  time.Time
}

func New(cfg *config.Config, version string) *Server {
	return &Server{cfg: cfg, version: version, startAt: time.Now()}
}

// RegisterESAM wires the ESAM HTTP handler into this server's mux.
// Must be called before Start.
func (s *Server) RegisterESAM(r ESAMRegistrar) {
	s.esamReg = r
}

// Start registers routes and begins serving. It blocks until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	if s.cfg.Packaging.HLS.Enabled {
		hlsDir := filepath.Clean(s.cfg.Packaging.HLS.OutputDir)
		mux.Handle("/hls/", withHeaders(http.StripPrefix("/hls/", http.FileServer(http.Dir(hlsDir)))))
	}

	// Raw TS segments are referenced by HLS playlists via relative URLs.
	segDir := filepath.Clean(s.cfg.Packaging.WorkDir)
	mux.Handle("/segments/", withHeaders(http.StripPrefix("/segments/", http.FileServer(http.Dir(segDir)))))

	if s.cfg.Packaging.DASH.Enabled {
		dashDir := filepath.Clean(s.cfg.Packaging.DASH.OutputDir)
		mux.Handle("/dash/", withHeaders(http.StripPrefix("/dash/", http.FileServer(http.Dir(dashDir)))))
	}

	if s.esamReg != nil {
		s.esamReg.Register(mux)
	}

	mux.HandleFunc("/health", s.healthHandler(s.startAt, s.version))

	addr := fmt.Sprintf(":%d", s.cfg.Server.HTTPPort)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutCtx)
	}()

	base := s.cfg.Server.BaseURL
	if s.cfg.Packaging.HLS.Enabled {
		slog.Info("HLS master playlist", "url", base+"/hls/master.m3u8")
	}
	if s.cfg.Packaging.DASH.Enabled {
		slog.Info("DASH manifest", "url", base+"/dash/manifest.mpd")
	}
	if s.cfg.ESAM.Enabled {
		slog.Info("ESAM endpoint", "url", fmt.Sprintf("%s%s", base, s.cfg.ESAM.Path))
	}
	slog.Info("HTTP server listening", "addr", addr)

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// withHeaders adds CORS and no-cache headers for browser / player compatibility.
func withHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Cache-Control", "no-cache, no-store")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}
