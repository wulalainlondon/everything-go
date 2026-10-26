package recovery

import "errors"

var ErrClock = errors.New("automatic recovery clock provenance unavailable")

// ClockAnchor is private durable provenance, never a public boot identifier.
// ElapsedMS is a same-boot continuous clock including system sleep.
type ClockAnchor struct {
	Boot      string `json:"boot,omitempty"`
	Kind      string `json:"kind,omitempty"`
	WallMS    int64  `json:"wall_ms,omitempty"`
	ElapsedMS int64  `json:"elapsed_ms,omitempty"`
}

func (a ClockAnchor) Valid() bool {
	return a.Boot != "" && a.Kind != "" && a.WallMS > 0 && a.ElapsedMS > 0
}

// Time returns a same-boot common continuous coordinate, never a per-run wall
// offset. Wall jumps cannot shorten lower-bound waits or shared rate spacing.
func (a ClockAnchor) Time(sample ClockAnchor) (int64, error) {
	if !a.Valid() || !sample.Valid() || a.Boot != sample.Boot || a.Kind != sample.Kind || sample.ElapsedMS < a.ElapsedMS {
		return 0, ErrClock
	}
	return sample.ElapsedMS, nil
}

func (a ClockAnchor) Domain() string {
	if !a.Valid() {
		return "unqualified_wall_fixture"
	}
	return Hash([]byte(a.Boot + "\x00" + a.Kind))
}

// PublicWall projects a private continuous timestamp for legacy UTC fields.
// It is display provenance only and never enters scheduling comparisons.
func (a ClockAnchor) PublicWall(v int64) int64 {
	if v == 0 || !a.Valid() {
		return v
	}
	delta := v - a.ElapsedMS
	if delta < 0 || delta > (1<<63-1)-a.WallMS {
		return 0
	}
	return a.WallMS + delta
}
