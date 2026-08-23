package session

import (
	"log"
	"time"
)

// State is the explicit session lifecycle. Transitions:
//
//	Idle      --BeginTurn-->  Streaming
//	Streaming --EndTurn---->  Idle
//	Streaming --markStop--->  Stopping --EndTurn--> Idle
//	any       --Close------>  Closed   (terminal)
//
// State is owned here and exposed read-only; the connection core observes the
// executor's terminal events (done/stopped/error) and calls EndTurn, while the
// per-session worker calls beginTurn. No backend mutates state directly.
type State int

const (
	Idle State = iota
	Streaming
	Stopping
	Closed
)

func (st State) String() string {
	switch st {
	case Idle:
		return "idle"
	case Streaming:
		return "streaming"
	case Stopping:
		return "stopping"
	case Closed:
		return "closed"
	default:
		return "unknown"
	}
}

// mailboxSize bounds queued turns per session before Submit blocks. The app
// almost never pipelines turns for one session; this is headroom, not a design
// point.
const mailboxSize = 64

// turnWatchdog is a backstop, NOT a turn time limit: if a turn never produces a
// terminal event and is never stopped/cleared (an executor bug), it releases
// the worker so the session doesn't wedge forever. Real turns finish via
// EndTurn long before this fires.
const turnWatchdog = 2 * time.Hour

// State returns the current lifecycle state.
func (s *Session) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// QueueLen reports how many turns are waiting behind the in-flight one. Surfaced
// as status_result.queued_commands.
func (s *Session) QueueLen() int {
	s.mu.Lock()
	mb := s.mailbox
	s.mu.Unlock()
	if mb == nil {
		return 0
	}
	return len(mb)
}

// Submit enqueues a turn for serial execution. The worker runs fn, then waits
// for the turn's terminal event (EndTurn) before pulling the next one, so two
// turns for the same session never overlap. Returns false if the session is
// closed.
func (s *Session) Submit(fn func()) bool {
	s.mu.Lock()
	if s.state == Closed {
		s.mu.Unlock()
		return false
	}
	if !s.workerUp {
		s.workerUp = true
		s.mailbox = make(chan func(), mailboxSize)
		s.quit = make(chan struct{})
		go s.runWorker(s.mailbox, s.quit)
	}
	mb, quit := s.mailbox, s.quit
	s.mu.Unlock()

	// The mailbox is never closed, so this send can't panic. If Close races us
	// after the state check above, quit fires and we report the session closed
	// instead of blocking on a worker that has already exited.
	select {
	case mb <- fn:
		return true
	case <-quit:
		return false
	}
}

// runWorker is the per-session actor loop: one turn at a time, in submission
// order. It owns the Idle→Streaming transition and blocks until the turn ends.
// It stops when quit is closed (by Close), not by the mailbox closing.
func (s *Session) runWorker(mailbox chan func(), quit chan struct{}) {
	for {
		select {
		case <-quit:
			return
		case fn := <-mailbox:
			done := s.beginTurn()
			fn() // typically executor.Send: returns quickly, then streams async
			select {
			case <-done:
				// terminal event arrived (EndTurn) — pull the next turn
			case <-quit:
				return
			case <-time.After(turnWatchdog):
				log.Printf("[%s] turn watchdog fired after %s — releasing queue", s.ID, turnWatchdog)
				s.EndTurn()
			}
		}
	}
}

// beginTurn moves Idle→Streaming and arms a fresh completion signal for the
// worker. Returns the channel the worker waits on. No-op intent if already
// closed (returns an already-fired channel so the worker won't block).
func (s *Session) beginTurn() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == Closed {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	s.state = Streaming
	s.lastActivity = nowSeconds()
	s.turnDone = make(chan struct{})
	return s.turnDone
}

// EndTurn marks the in-flight turn complete and releases the worker. Called by
// the connection core when the executor emits a terminal event (done/stopped/
// error) and by stop/clear/kill paths that forcibly cancel a turn. Idempotent.
func (s *Session) EndTurn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == Streaming || s.state == Stopping {
		s.state = Idle
	}
	if s.turnDone != nil {
		close(s.turnDone)
		s.turnDone = nil
	}
	s.lastActivity = nowSeconds()
}

// MarkStopping records that a stop was requested for the in-flight turn. Purely
// observational (the actual interrupt is the executor's job); EndTurn follows
// when the turn unwinds.
func (s *Session) MarkStopping() {
	s.mu.Lock()
	if s.state == Streaming {
		s.state = Stopping
	}
	s.mu.Unlock()
}

// Close moves the session to the terminal state and shuts the turn worker down.
// Any queued turns are dropped; an in-flight turn's wait is released. Safe to
// call more than once.
func (s *Session) Close() {
	s.mu.Lock()
	if s.state == Closed {
		s.mu.Unlock()
		return
	}
	s.state = Closed
	if s.turnDone != nil {
		close(s.turnDone)
		s.turnDone = nil
	}
	// Stop the worker by signaling quit; never close the mailbox (a concurrent
	// Submit may still be selecting on it). Queued fns are simply dropped.
	if s.quit != nil {
		close(s.quit)
		s.quit = nil
	}
	s.mailbox = nil
	s.workerUp = false
	s.mu.Unlock()
}
