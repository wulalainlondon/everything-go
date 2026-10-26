//go:build !darwin

package recovery

// No wall-only fallback on platforms without a qualified continuous clock.
func SystemClock() (ClockAnchor, error) { return ClockAnchor{}, ErrClock }
