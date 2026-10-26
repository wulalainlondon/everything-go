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

// Time retains the original admission epoch across Bridge restart. Wall jumps
// forward may shorten a budget; backwards jumps cannot extend elapsed time.
func (a ClockAnchor) Time(sample ClockAnchor) (int64, error) {
	if !a.Valid() || !sample.Valid() || a.Boot != sample.Boot || a.Kind != sample.Kind || sample.ElapsedMS < a.ElapsedMS {
		return 0, ErrClock
	}
	delta := sample.ElapsedMS - a.ElapsedMS
	if delta > (1<<63-1)-a.WallMS {
		return 0, ErrClock
	}
	return max(sample.WallMS, a.WallMS+delta), nil
}
