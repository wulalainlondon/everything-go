//go:build !windows

package lifecycle

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestPriorFIFORefusesWithoutReadingOrBlocking(t *testing.T) {
	j := New(t.TempDir())
	if err := syscall.Mkfifo(j.path, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- j.Add(Event{Kind: "transport_connected", Owner: "provider_connection", Reason: "connected"})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("FIFO blocked lifecycle")
	}
	info, err := os.Lstat(j.path)
	if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatal("original FIFO changed")
	}
}
