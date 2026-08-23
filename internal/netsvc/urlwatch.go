package netsvc

import (
	"context"
	"os"
	"strings"
	"time"
)

// WatchURLFile follows an externally managed tunnel URL file. Empty or removed
// files clear the URL; each value change is emitted exactly once.
func WatchURLFile(ctx context.Context, name string, interval time.Duration, onURL func(string)) {
	if interval <= 0 {
		interval = time.Second
	}
	last := "\x00"
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	check := func() {
		data, _ := os.ReadFile(name)
		current := strings.TrimSpace(string(data))
		if current != last {
			last = current
			onURL(current)
		}
	}
	check()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
}
