package session

import (
	"sync"
	"testing"
	"time"
)

// newIdle builds a bare Idle session for state-machine tests without going
// through the registry.
func newIdle(id string) *Session {
	return &Session{ID: id, CreatedAt: nowSeconds(), state: Idle}
}

func TestStateStartsIdle(t *testing.T) {
	s := newIdle("s1")
	if s.State() != Idle {
		t.Fatalf("new session should be Idle, got %s", s.State())
	}
	if s.IsStreaming() {
		t.Fatal("idle session must not report streaming")
	}
}

// A turn moves Idle→Streaming for its duration and back to Idle on EndTurn.
func TestTurnStreamingThenIdle(t *testing.T) {
	s := newIdle("s1")
	inTurn := make(chan State, 1)
	if !s.Submit(func() { inTurn <- s.State() }) {
		t.Fatal("Submit on idle session should succeed")
	}
	if got := <-inTurn; got != Streaming {
		t.Fatalf("state during turn should be Streaming, got %s", got)
	}
	s.EndTurn()
	// EndTurn is synchronous about state, so this is race-free.
	if s.State() != Idle {
		t.Fatalf("after EndTurn state should be Idle, got %s", s.State())
	}
}

// The core guarantee: a second turn for the same session does not begin until
// the first one ends.
func TestSubmitSerializesTurns(t *testing.T) {
	s := newIdle("s1")
	started := make(chan int, 2)

	s.Submit(func() { started <- 1 }) // worker runs this, then waits for EndTurn
	s.Submit(func() { started <- 2 }) // must stay queued until turn 1 ends

	if got := <-started; got != 1 {
		t.Fatalf("first turn should run first, got %d", got)
	}
	select {
	case n := <-started:
		t.Fatalf("turn %d started before turn 1 ended — turns not serialized", n)
	case <-time.After(60 * time.Millisecond):
		// good: turn 2 is still queued
	}
	if s.QueueLen() != 1 {
		t.Fatalf("one turn should be queued, got QueueLen=%d", s.QueueLen())
	}

	s.EndTurn() // release turn 1
	if got := <-started; got != 2 {
		t.Fatalf("turn 2 should run after turn 1 ended, got %d", got)
	}
	s.EndTurn()
}

func TestMarkStoppingTransition(t *testing.T) {
	s := newIdle("s1")
	gate := make(chan struct{})
	s.Submit(func() { <-gate }) // hold the worker inside the turn fn
	// Let the worker enter the turn.
	waitFor(t, func() bool { return s.State() == Streaming })
	s.MarkStopping()
	if s.State() != Stopping {
		t.Fatalf("after MarkStopping state should be Stopping, got %s", s.State())
	}
	close(gate)
	s.EndTurn()
	if s.State() != Idle {
		t.Fatalf("after EndTurn from Stopping state should be Idle, got %s", s.State())
	}
}

func TestSubmitAfterCloseRejected(t *testing.T) {
	s := newIdle("s1")
	s.Close()
	if s.State() != Closed {
		t.Fatalf("after Close state should be Closed, got %s", s.State())
	}
	if s.Submit(func() {}) {
		t.Fatal("Submit on a closed session must return false")
	}
}

func TestCloseReleasesInFlightTurn(t *testing.T) {
	s := newIdle("s1")
	ran := make(chan struct{})
	s.Submit(func() { close(ran) })
	<-ran // turn is in flight, worker now waiting on turnDone
	// Close must not deadlock even with a turn waiting for its terminal event.
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close blocked while a turn was in flight")
	}
}

func TestEndTurnIdempotent(t *testing.T) {
	s := newIdle("s1")
	// EndTurn with no in-flight turn is a no-op and must not panic.
	s.EndTurn()
	s.EndTurn()
	if s.State() != Idle {
		t.Fatalf("state should remain Idle, got %s", s.State())
	}
}

func TestSettersAndSnapshot(t *testing.T) {
	s := newIdle("s1")
	s.SetName("renamed")
	s.SetEffort("high")
	s.SetResumeID("uuid-1")
	s.ApplyConfig("codex", "gpt", "danger")
	// Empty values must not overwrite.
	s.ApplyConfig("", "", "")

	snap := s.Snapshot()
	if snap.Name != "renamed" || snap.Effort != "high" || snap.ResumeID != "uuid-1" {
		t.Fatalf("setters not reflected: %+v", snap)
	}
	if snap.Backend != "codex" || snap.Model != "gpt" || snap.Sandbox != "danger" {
		t.Fatalf("ApplyConfig not reflected (or empties overwrote): %+v", snap)
	}
	if s.Name() != "renamed" || s.Backend() != "codex" || s.ResumeID() != "uuid-1" {
		t.Fatal("single-field getters disagree with snapshot")
	}
}

// Concurrent setters + readers must be race-free (run under -race).
func TestConcurrentAccessRaceFree(t *testing.T) {
	s := newIdle("s1")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); s.SetName("n"); s.ApplyConfig("b", "m", "s") }()
		go func() { defer wg.Done(); _ = s.Snapshot(); _ = s.IsStreaming(); _ = s.Name() }()
	}
	wg.Wait()
}

// Submit racing Close must never panic with send-on-closed-channel, and must
// never deadlock. Run under -race to exercise the window between Submit's state
// check and its channel send.
func TestSubmitCloseConcurrentNoPanic(t *testing.T) {
	for iter := 0; iter < 100; iter++ {
		s := newIdle("s1")
		var wg sync.WaitGroup
		for j := 0; j < 6; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := 0; k < 20; k++ {
					// A turn that ends itself immediately keeps the worker moving.
					s.Submit(func() { s.EndTurn() })
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Close()
		}()
		wg.Wait()
		// Post-close, Submit must be rejected, not panic.
		if s.Submit(func() {}) {
			t.Fatal("Submit after Close should be rejected")
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within 1s")
}
