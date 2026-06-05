package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/synamedia/linear-packager/internal/config"
	"github.com/synamedia/linear-packager/internal/esam"
	"github.com/synamedia/linear-packager/internal/fanout"
	"github.com/synamedia/linear-packager/internal/input"
	"github.com/synamedia/linear-packager/internal/packager/dash"
	"github.com/synamedia/linear-packager/internal/packager/hls"
	"github.com/synamedia/linear-packager/internal/server"
	"github.com/synamedia/linear-packager/internal/splice"
	"github.com/synamedia/linear-packager/internal/transcoder"
)

func main() {
	configPath := flag.String("config", "config.json", "path to channel config file")
	debug := flag.Bool("debug", false, "enable debug logging")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	slog.Info("channel loaded",
		"id", cfg.Channel.ID,
		"rungs", len(cfg.Transcoder.Ladder),
		"segment_dur", cfg.Packaging.SegmentDuration,
	)
	for _, r := range cfg.Transcoder.Ladder {
		slog.Info("  rung",
			"name", r.Name,
			"res", fmt.Sprintf("%dx%d", r.Width, r.Height),
			"vbr", r.VideoBitrate,
			"fps", r.Framerate,
		)
	}

	src, err := input.New(cfg.Input)
	if err != nil {
		slog.Error("invalid input", "error", err)
		os.Exit(1)
	}
	slog.Info("input ready", "source", src.String())

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// ── Shared splice event queue (ESAM → packagers) ─────────────────────────
	spliceQ := &splice.Queue{}

	// ── Transcoder ────────────────────────────────────────────────────────────
	tc := transcoder.New(cfg, src)
	if err := tc.Start(ctx); err != nil {
		slog.Error("failed to start transcoder", "error", err)
		os.Exit(1)
	}

	// ── Fan-out segment channel ────────────────────────────────────────────────
	n := 0
	if cfg.Packaging.HLS.Enabled {
		n++
	}
	if cfg.Packaging.DASH.Enabled {
		n++
	}
	consumers := fanout.FanOut(ctx, tc.Segments(), n)

	// ── HLS packager ──────────────────────────────────────────────────────────
	i := 0
	if cfg.Packaging.HLS.Enabled {
		hlsPkg := hls.New(cfg)
		hlsPkg.SetSpliceQueue(spliceQ)
		if err := hlsPkg.Start(ctx, consumers[i]); err != nil {
			slog.Error("failed to start HLS packager", "error", err)
			os.Exit(1)
		}
		slog.Info("HLS packager started", "output", cfg.Packaging.HLS.OutputDir)
		i++
	}

	// ── DASH packager ─────────────────────────────────────────────────────────
	if cfg.Packaging.DASH.Enabled {
		dashPkg := dash.New(cfg)
		dashPkg.SetSpliceQueue(spliceQ)
		if err := dashPkg.Start(ctx, consumers[i]); err != nil {
			slog.Error("failed to start DASH packager", "error", err)
			os.Exit(1)
		}
		slog.Info("DASH packager started", "output", cfg.Packaging.DASH.OutputDir)
	}

	// ── HTTP server (HLS + DASH + ESAM endpoint) ──────────────────────────────
	srv := server.New(cfg, Version)
	if cfg.ESAM.Enabled {
		esamSrv := esam.New(&cfg.ESAM, spliceQ)
		srv.RegisterESAM(esamSrv)
	}
	go func() {
		if err := srv.Start(ctx); err != nil {
			slog.Error("HTTP server error", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown complete")
}
