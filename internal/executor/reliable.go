package executor

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

const defaultTerminalTimeout = 2 * time.Hour

type turnKey struct {
	sessionID string
	reqID     string
}

// TerminalSink wraps the real outbound sink and tracks terminal turn events.
// Reliable executors use it to guarantee a started turn eventually emits one of
// done/error/stopped, even when the backend returns an error, panics, or wedges.
type TerminalSink struct {
	delegate Sink
	timeout  time.Duration

	mu       sync.Mutex
	inflight map[turnKey]chan struct{}
	bySess   map[string]map[turnKey]bool
}

func NewTerminalSink(delegate Sink) *TerminalSink {
	return NewTerminalSinkWithTimeout(delegate, defaultTerminalTimeout)
}

func NewTerminalSinkWithTimeout(delegate Sink, timeout time.Duration) *TerminalSink {
	return &TerminalSink{
		delegate: delegate,
		timeout:  timeout,
		inflight: make(map[turnKey]chan struct{}),
		bySess:   make(map[string]map[turnKey]bool),
	}
}

func (s *TerminalSink) Emit(event any) {
	s.delegate.Emit(event)
	s.observeTerminal(event)
}

func (s *TerminalSink) Begin(sessionID, reqID string) turnKey {
	k := turnKey{sessionID: sessionID, reqID: reqID}
	done := make(chan struct{})
	s.mu.Lock()
	s.inflight[k] = done
	if s.bySess[sessionID] == nil {
		s.bySess[sessionID] = map[turnKey]bool{}
	}
	s.bySess[sessionID][k] = true
	s.mu.Unlock()

	if s.timeout > 0 {
		go func() {
			select {
			case <-done:
			case <-time.After(s.timeout):
				if s.complete(k) {
					s.delegate.Emit(backend.NewError(sessionID, "", backend.ErrTimeout, "executor turn timed out without a terminal event"))
				}
			}
		}()
	}
	return k
}

func (s *TerminalSink) Done(k turnKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight[k] == nil
}

func (s *TerminalSink) complete(k turnKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := s.inflight[k]
	if ch == nil {
		return false
	}
	delete(s.inflight, k)
	if m := s.bySess[k.sessionID]; m != nil {
		delete(m, k)
		if len(m) == 0 {
			delete(s.bySess, k.sessionID)
		}
	}
	close(ch)
	return true
}

func (s *TerminalSink) completeSession(sessionID string) {
	s.mu.Lock()
	keys := make([]turnKey, 0, len(s.bySess[sessionID]))
	for k := range s.bySess[sessionID] {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	for _, k := range keys {
		s.complete(k)
	}
}

func (s *TerminalSink) observeTerminal(event any) {
	switch e := event.(type) {
	case protocol.Done:
		s.complete(turnKey{sessionID: e.SessionID, reqID: e.RequestID})
	case protocol.Stopped:
		if e.RequestID != "" {
			s.complete(turnKey{sessionID: e.SessionID, reqID: e.RequestID})
		} else {
			s.completeSession(e.SessionID)
		}
	case protocol.Error:
		if e.SessionID == "" {
			return
		}
		if e.RequestID != "" {
			s.complete(turnKey{sessionID: e.SessionID, reqID: e.RequestID})
		} else {
			s.completeSession(e.SessionID)
		}
	}
}

// sendReliable enforces the Executor adapter contract for one turn. It lives
// below Mux.Send so optional capability detection still sees the raw backend.
func sendReliable(ctx context.Context, inner Executor, sink *TerminalSink, s *session.Session, reqID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) (err error) {
	k := sink.Begin(s.ID, reqID)
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("executor panic: %v", rec)
			log.Printf("[%s] %v", s.ID, err)
			if !sink.Done(k) {
				sink.Emit(backend.NewError(s.ID, reqID, backend.ErrPanic, err.Error()))
			}
		}
	}()
	err = inner.Send(ctx, s, reqID, content, images, files)
	if err != nil && !sink.Done(k) {
		sink.Emit(backend.NewError(s.ID, reqID, backend.ErrSend, err.Error()))
	}
	return err
}
