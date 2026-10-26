//go:build darwin

package recovery

import (
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

var bootClock struct {
	sync.Once
	boot string
	err  error
}

// CLOCK_MONOTONIC_RAW is Apple's continuous clock (including sleep). The boot
// UUID is read once per process; subsequent samples are one local clock syscall.
func SystemClock() (ClockAnchor, error) {
	bootClock.Do(func() {
		bootClock.boot, bootClock.err = unix.Sysctl("kern.bootsessionuuid")
		bootClock.boot = strings.TrimSpace(bootClock.boot)
	})
	if bootClock.err != nil || bootClock.boot == "" {
		return ClockAnchor{}, ErrClock
	}
	var ts unix.Timespec
	if unix.ClockGettime(unix.CLOCK_MONOTONIC_RAW, &ts) != nil {
		return ClockAnchor{}, ErrClock
	}
	a := ClockAnchor{Boot: bootClock.boot, Kind: "darwin_continuous_v1", WallMS: time.Now().UnixMilli(), ElapsedMS: ts.Nano() / int64(time.Millisecond)}
	if !a.Valid() {
		return ClockAnchor{}, ErrClock
	}
	return a, nil
}
