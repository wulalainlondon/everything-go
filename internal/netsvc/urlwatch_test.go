package netsvc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWatchURLFileReportsChangesAndClear(t *testing.T) {
	name := filepath.Join(t.TempDir(), "tunnel.txt")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := make(chan string, 4)
	go WatchURLFile(ctx, name, 5*time.Millisecond, func(value string) { updates <- value })
	wantURLUpdate(t, updates, "")
	if err := os.WriteFile(name, []byte("https://one.example\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wantURLUpdate(t, updates, "https://one.example")
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
	wantURLUpdate(t, updates, "")
}

func wantURLUpdate(t *testing.T, updates <-chan string, want string) {
	t.Helper()
	select {
	case got := <-updates:
		if got != want {
			t.Fatalf("update = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %q", want)
	}
}
