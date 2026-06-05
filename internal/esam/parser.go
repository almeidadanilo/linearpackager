package esam

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/synamedia/linear-packager/internal/splice"
)

// ParseNotification parses a CableLabs ESAM SignalProcessingNotification body
// and returns a SpliceEvent.  The parser is namespace-agnostic — it matches
// elements by local name only, which tolerates prefix variations.
func ParseNotification(data []byte) (*splice.Event, error) {
	var ev splice.Event
	ev.ReceivedAt = time.Now()

	dec := xml.NewDecoder(bytes.NewReader(data))
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("xml: %w", err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}

		switch se.Name.Local {
		case "SignalProcessingNotification":
			// (acquisitionPointIdentity is informational; not stored)

		case "UTCPoint":
			for _, a := range se.Attr {
				if a.Name.Local == "utcPoint" {
					t, err := time.Parse(time.RFC3339, a.Value)
					if err != nil {
						// Try without timezone
						t, err = time.Parse("2006-01-02T15:04:05", a.Value)
					}
					if err == nil {
						ev.SpliceTime = t
					}
				}
			}

		case "SpliceInsert":
			for _, a := range se.Attr {
				switch a.Name.Local {
				case "spliceEventID":
					id, err := strconv.ParseUint(a.Value, 10, 32)
					if err != nil {
						return nil, fmt.Errorf("spliceEventID %q: %w", a.Value, err)
					}
					ev.ID = uint32(id)

				case "outOfNetworkIndicator":
					ev.OutOfNetwork = strings.EqualFold(a.Value, "true")

				case "uniqueProgramID":
					id, _ := strconv.ParseUint(a.Value, 10, 16)
					ev.UniqueProgramID = uint16(id)

				case "duration":
					d, err := parseISO8601Duration(a.Value)
					if err != nil {
						return nil, fmt.Errorf("duration %q: %w", a.Value, err)
					}
					ev.Duration = d
				}
			}
		}
	}

	if ev.ID == 0 {
		return nil, fmt.Errorf("missing or zero spliceEventID in payload")
	}
	return &ev, nil
}

// parseISO8601Duration parses a subset of ISO 8601 duration: PT[nH][nM][nS].
// Examples: "PT30S" → 30s, "PT1M30S" → 90s, "PT2M" → 120s.
func parseISO8601Duration(s string) (time.Duration, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if !strings.HasPrefix(s, "PT") {
		return 0, fmt.Errorf("expected PT prefix, got %q", s)
	}
	s = s[2:] // strip "PT"

	var total time.Duration

	if idx := strings.Index(s, "H"); idx != -1 {
		v, err := strconv.ParseFloat(s[:idx], 64)
		if err != nil {
			return 0, fmt.Errorf("hours: %w", err)
		}
		total += time.Duration(v * float64(time.Hour))
		s = s[idx+1:]
	}
	if idx := strings.Index(s, "M"); idx != -1 {
		v, err := strconv.ParseFloat(s[:idx], 64)
		if err != nil {
			return 0, fmt.Errorf("minutes: %w", err)
		}
		total += time.Duration(v * float64(time.Minute))
		s = s[idx+1:]
	}
	if idx := strings.Index(s, "S"); idx != -1 {
		v, err := strconv.ParseFloat(s[:idx], 64)
		if err != nil {
			return 0, fmt.Errorf("seconds: %w", err)
		}
		total += time.Duration(v * float64(time.Second))
	}

	if total <= 0 {
		return 0, fmt.Errorf("parsed duration is zero from %q", s)
	}
	return total, nil
}
