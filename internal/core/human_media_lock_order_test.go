package core

import (
	"context"
	"database/sql"
	"everything-go/internal/session"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHumanMediaContinuationNeverHoldsSourceWhileWaitingForOriginalQueueWriter(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	defer s.Close()
	config := session.ConfigurationFrom(s.SettingsSnapshot())
	started, begin, actorDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	if !s.SubmitNamed("r_text_actor", func() {
		close(started)
		<-begin
		actorDone <- h.migrateMediaQueuedConfiguration(s, "r_text_actor", queuedPayload{Configuration: &config})
		s.EndTurn()
	}) {
		t.Fatal("actor unavailable")
	}
	<-started
	// Prepare B behind this already-active text actor, not ahead of the fixture
	// actor: an unready reserved head must correctly block a later actor.
	qaR4ReservePrepare(t, h, s, c, "r_lock_successor", "")
	held, release := make(chan struct{}), make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- h.messageQueue.TaskJournal().Transaction(context.Background(), func(*sql.Tx) error { close(held); <-release; return nil })
	}()
	<-held
	close(begin)
	// Observe this isolated actor actually waiting in its queue-ownership read
	// before testing the reverse lock ordering. No product test hook is installed.
	deadline := time.Now().Add(time.Second)
	waiting := false
	truncated := false
	for time.Now().Before(deadline) {
		stack, complete := humanMediaCompleteStackSnapshot()
		if !complete {
			truncated = true
			break
		}
		if strings.Contains(string(stack), "mediaContinuationOwned") {
			waiting = true
			break
		}
		runtime.Gosched()
	}
	if !waiting {
		close(release)
		<-writerDone
		if truncated {
			t.Fatal("complete actor stack observation exceeded bounded 16 MiB capture")
		}
		t.Fatal("actor never reached original queue ownership read")
	}
	// The writer's final source CAS must remain possible even while the actor is
	// waiting for its SQLite connection. This is the original lock ordering.
	sourceDone := make(chan error, 1)
	go func() {
		snapshot := s.SettingsSnapshot()
		sourceDone <- s.CommitInputSource(snapshot, func() error { return nil })
	}()
	var blocked bool
	select {
	case err := <-sourceDone:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		blocked = true
	}
	close(release)
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-actorDone:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("actor did not recover after writer")
	}
	if blocked {
		t.Fatal("media actor held Session.mu while borrowing the original queue connection")
	}
}

// A full-package run retains other fixture goroutines. A truncated all-stack
// snapshot cannot prove that this actor has not reached its ownership read.
func humanMediaCompleteStackSnapshot() ([]byte, bool) {
	for size := 256 * 1024; size <= 16*1024*1024; size *= 2 {
		stack := make([]byte, size)
		n := runtime.Stack(stack, true)
		if n < len(stack) {
			return stack[:n], true
		}
	}
	return nil, false
}

func TestHumanMediaContinuationOriginalQueueOwnershipWithStackPressure(t *testing.T) {
	const count = 1500
	release := make(chan struct{})
	var ready, finished sync.WaitGroup
	ready.Add(count)
	finished.Add(count)
	for i := 0; i < count; i++ {
		go func() { defer finished.Done(); ready.Done(); <-release }()
	}
	ready.Wait()
	t.Cleanup(func() { close(release); finished.Wait() })
	// Run the original actor/one-second/lock-order/recovery assertions unchanged.
	TestHumanMediaContinuationNeverHoldsSourceWhileWaitingForOriginalQueueWriter(t)
}
