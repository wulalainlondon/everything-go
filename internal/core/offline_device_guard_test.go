package core

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestOfflineDeviceGuardDoesNotReplaceActiveUser(t *testing.T) {
	h, _ := newTestHub(t)
	user := newDeviceClient(h, "paired", 4)
	h.registerLatest(user)
	qa := newDeviceClient(h, "paired", 4)
	qa.offlineDeviceGuard = true
	if h.reserveOfflineDevice(qa) {
		t.Fatal("guard accepted an active identity")
	}
	h.registerLatest(qa)
	if !h.isCurrent(user) || !user.live() {
		t.Fatal("guarded QA evicted the active user")
	}
}

func TestOfflineDeviceGuardConcurrentReservationHasOneWinner(t *testing.T) {
	h, _ := newTestHub(t)
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := newDeviceClient(h, "paired", 4)
			c.offlineDeviceGuard = true
			if h.reserveOfflineDevice(c) {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := accepted.Load(); got != 1 {
		t.Fatalf("concurrent QA connections accepted: %d", got)
	}
}
