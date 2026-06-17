package esam

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/synamedia/linear-packager/internal/config"
	"github.com/synamedia/linear-packager/internal/scte35"
	"github.com/synamedia/linear-packager/internal/splice"
)

// Server exposes the inbound ESAM HTTP endpoint.  External systems POST a
// CableLabs SignalProcessingNotification XML body to the configured path.
// Each valid signal is encoded as a SCTE-35 binary and emitted on the queue.
type Server struct {
	cfg   *config.ESAMConfig
	queue *splice.Queue
}

func New(cfg *config.ESAMConfig, q *splice.Queue) *Server {
	return &Server{cfg: cfg, queue: q}
}

// Start registers the ESAM route on the provided mux and returns immediately.
// The caller is responsible for actually running the HTTP server.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc(s.cfg.Path, s.handle)
	slog.Info("ESAM endpoint registered",
		"port", s.cfg.ListenPort,
		"path", s.cfg.Path,
	)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	ev, err := ParseNotification(body)
	if err != nil {
		slog.Warn("esam: invalid payload", "error", err)
		http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
		return
	}

	// Always use received_at + 5s as the splice point; ignore the UTCPoint value.
	const preRollSec = 5.0
	ev.SpliceTime = ev.ReceivedAt.Add(preRollSec * time.Second)

	// Encode the SCTE-35 binary with a timed splice 5 seconds from now.
	ptsTime := uint64(preRollSec * 90000)
	bin, err := scte35.EncodeSpliceInsert(
		ev.ID,
		ev.OutOfNetwork,
		ev.Duration.Seconds(),
		ev.UniqueProgramID,
		ptsTime,
	)
	if err != nil {
		slog.Error("esam: scte35 encode failed", "error", err)
		http.Error(w, "scte35 encode error", http.StatusInternalServerError)
		return
	}
	ev.Binary = bin
	ev.Hex = scte35.HexEncode(bin)
	ev.B64 = scte35.Base64Encode(bin)

	slog.Info("esam: splice signal received",
		"event_id", ev.ID,
		"splice_time", ev.SpliceTime.Format(time.RFC3339),
		"duration_sec", ev.Duration.Seconds(),
		"out_of_network", ev.OutOfNetwork,
		"scte35_hex", ev.Hex,
	)

	s.queue.Emit(*ev)

	// Respond with a minimal ESAM status acknowledgement.
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w,
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ns5:StatusCode classCode="0" detail="OK" `+
			`xmlns:ns5="urn:cablelabs:iptvservices:esam:xsd:common:1"/>`,
	)
}
