package segment

// Segment represents a completed, on-disk MPEG-TS segment for one ABR rung.
type Segment struct {
	Rung          string
	Path          string  // absolute path to .ts file
	Index         int     // 0-based sequence number within this rung
	StartTime     float64 // seconds from stream epoch
	Duration      float64 // seconds
	Discontinuity bool    // true when a SCTE-35 splice boundary applies
}
