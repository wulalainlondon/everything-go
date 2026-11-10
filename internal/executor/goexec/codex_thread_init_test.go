package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

type initProgressSink struct {
	capSink
	slow chan struct{}
}

func (s *initProgressSink) Emit(e any) {
	s.capSink.Emit(e)
	if p, ok := e.(backend.TurnProgress); ok && p.Stage == "starting_thread" {
		select {
		case s.slow <- struct{}{}:
		default:
		}
	}
}

func initFixture(t *testing.T) (*Codex, *session.Session, *rpcCaptureWriter, *initProgressSink) {
	t.Helper()
	sink := &initProgressSink{slow: make(chan struct{}, 4)}
	c := NewCodex(sink, "unused-fake-only")
	c.appServerMode = "daemon"
	c.dataDir = t.TempDir()
	c.threadInitSlowAfter, c.threadInitTimeout = 5*time.Millisecond, time.Second
	c.health.probe = func(string) codexProbeResult { return codexProbeResult{Reads: 1} }
	w := &rpcCaptureWriter{writes: make(chan []byte, 16)}
	c.rpc.setWriter(w)
	s := session.NewRegistry().Create("init", "New Session", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "")
	t.Cleanup(s.CloseAndWait)
	return c, s, w, sink
}

func initFrame(t *testing.T, w *rpcCaptureWriter) int {
	t.Helper()
	select {
	case data := <-w.writes:
		var frame struct {
			ID     int
			Method string
		}
		if json.Unmarshal(data, &frame) != nil || frame.Method != "thread/start" {
			t.Fatalf("unexpected bytes: %s", data)
		}
		return frame.ID
	case <-time.After(time.Second):
		t.Fatal("no original initialization request")
	}
	return 0
}
func initReply(c *Codex, id int, result string) {
	c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":%s}`, id, result)))
}
func initResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("init waiter did not finish")
	}
	return nil
}
func noInitReplay(t *testing.T, w *rpcCaptureWriter) {
	t.Helper()
	select {
	case data := <-w.writes:
		t.Fatalf("unexpected replay or model turn: %s", data)
	default:
	}
}

func TestThreadInitSlowACKRetainsOriginalRPCAndPinsReceipt(t *testing.T) {
	c, s, w, sink := initFixture(t)
	done := make(chan error, 1)
	go func() { done <- c.ensureThreadWithContext(context.Background(), s, c.state(s.ID), "r_original", 0) }()
	id := initFrame(t, w)
	select {
	case <-sink.slow:
	case <-time.After(time.Second):
		t.Fatal("missing slow initialization progress")
	}
	if c.rpc.pendingCount() != 1 || s.ResumeID() != "" {
		t.Fatal("original waiter lost or adopted without ACK")
	}
	noInitReplay(t, w)
	initReply(c, id+100, `{"thread":{"id":"unrelated"}}`)
	if s.ResumeID() != "" {
		t.Fatal("unrelated RPC adopted")
	}
	initReply(c, id, `{"thread":{"id":"exact-original"}}`)
	if err := initResult(t, done); err != nil {
		t.Fatal(err)
	}
	if s.ResumeID() != "exact-original" || c.rpc.pendingCount() != 0 {
		t.Fatal("exact ACK was lost")
	}
	c.threadInitMu.Lock()
	record, err := c.loadThreadInit(s.ID)
	c.threadInitMu.Unlock()
	if err != nil || record.RPCID != id || record.RequestID != "r_original" || record.TransportEpoch != c.rpc.transportEpoch.Load() || record.Status != "acknowledged" {
		t.Fatalf("missing exact receipt %+v %v", record, err)
	}
	if err := c.ensureThread(s, c.state(s.ID)); err != nil {
		t.Fatal(err)
	}
	noInitReplay(t, w)
}

func TestThreadInitHardTimeoutBlocksConcurrentQueueReconnectAndReopen(t *testing.T) {
	c, s, w, _ := initFixture(t)
	c.threadInitTimeout = 25 * time.Millisecond
	done := make(chan error, 2)
	go func() { done <- c.ensureThreadWithContext(context.Background(), s, c.state(s.ID), "r_original", 0) }()
	id := initFrame(t, w)
	go func() { done <- c.ensureThreadWithContext(context.Background(), s, c.state(s.ID), "r_next", 0) }()
	if err := initResult(t, done); codexThreadInitCode(err) != "thread_initialization_timeout" {
		t.Fatalf("first result %v", err)
	}
	if err := initResult(t, done); codexThreadInitCode(err) != "thread_initialization_unknown" {
		t.Fatalf("second result %v", err)
	}
	initReply(c, id, `{"thread":{"id":"too-late"}}`)
	if s.ResumeID() != "" {
		t.Fatal("late ACK adopted after original terminal")
	}
	c.rpc.setWriter(w)
	if err := c.QueuedSubmissionReady(context.Background(), s, "r_queue"); codexThreadInitCode(err) != "thread_initialization_unknown" {
		t.Fatalf("queue admitted %v", err)
	}
	reopened := NewCodex(&capSink{}, "unused")
	reopened.dataDir = c.dataDir
	reopened.rpc.setWriter(w)
	if err := reopened.ensureThread(s, reopened.state(s.ID)); codexThreadInitCode(err) != "thread_initialization_unknown" {
		t.Fatalf("reopen admitted %v", err)
	}
	noInitReplay(t, w)
}

func TestThreadInitACKAfterSourceBoundaryNeverBindsOrStartsTurn(t *testing.T) {
	for _, change := range []string{"cancel", "close", "config", "source", "transport"} {
		t.Run(change, func(t *testing.T) {
			c, s, w, _ := initFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- c.ensureThreadWithContext(ctx, s, c.state(s.ID), "r_original", 0) }()
			id := initFrame(t, w)
			switch change {
			case "cancel":
				cancel()
			case "close":
				s.Close()
			case "config":
				s.SetConfigRevision(9)
			case "source":
				s.SetResumeID("another-thread")
			case "transport":
				c.rpc.setWriter(w)
			}
			initReply(c, id, `{"thread":{"id":"old-ack"}}`)
			if err := initResult(t, done); err == nil {
				t.Fatal("changed source adopted ACK")
			}
			if s.ResumeID() == "old-ack" {
				t.Fatal("stale ACK bound to session")
			}
			if err := c.threadInitReady(s.Snapshot()); err == nil {
				t.Fatal("uncertain initialization reopened")
			}
			noInitReplay(t, w)
		})
	}
}

func TestThreadInitMalformedACKAndReceiptFailClosed(t *testing.T) {
	for _, reply := range []string{`{}`, `{"thread":{"id":""}}`, `{"thread":42}`} {
		t.Run(reply, func(t *testing.T) {
			c, s, w, _ := initFixture(t)
			done := make(chan error, 1)
			go func() { done <- c.ensureThread(s, c.state(s.ID)) }()
			id := initFrame(t, w)
			initReply(c, id, reply)
			if err := initResult(t, done); codexThreadInitCode(err) != "thread_initialization_unknown" {
				t.Fatal(err)
			}
			if err := c.ensureThread(s, c.state(s.ID)); err == nil {
				t.Fatal("invalid ACK allowed second create")
			}
			noInitReplay(t, w)
		})
	}
	c, s, w, _ := initFixture(t)
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- c.ensureThreadWithContext(ctx, s, c.state(s.ID), "r_original", 0) }()
	initFrame(t, w)
	cancel()
	initResult(t, done)
	path := c.threadInitPath(s.ID)
	if err := os.WriteFile(path, []byte(`{broken`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.QueuedSubmissionReady(context.Background(), s, "r_next"); err == nil {
		t.Fatal("corrupt receipt admitted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := c.QueuedSubmissionReady(context.Background(), s, "r_next"); err == nil {
		t.Fatal("missing observed receipt admitted")
	}
	noInitReplay(t, w)
}

func TestThreadInitDefiniteRPCRejectionIsNotTimeoutOrProcessDeath(t *testing.T) {
	c, s, w, _ := initFixture(t)
	done := make(chan error, 1)
	go func() { done <- c.ensureThread(s, c.state(s.ID)) }()
	id := initFrame(t, w)
	c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"error":{"code":-32602,"message":"invalid configuration"}}`, id)))
	err := initResult(t, done)
	var response *rpcResponseError
	if codexThreadInitCode(err) != "thread_initialization_failed" || !errors.As(err, &response) {
		t.Fatal(err)
	}
	if err := c.threadInitReady(s.Snapshot()); err != nil {
		t.Fatal("definite rejection mistaken for unknown", err)
	}
	noInitReplay(t, w)
}

func TestThreadInitConflictingRPCEnvelopeCannotProveRejection(t *testing.T) {
	for _, result := range []string{`null`, `{"thread":{"id":"possibly-created"}}`} {
		t.Run(result, func(t *testing.T) {
			c, s, w, _ := initFixture(t)
			done := make(chan error, 1)
			go func() { done <- c.ensureThreadWithContext(context.Background(), s, c.state(s.ID), "r_original", 0) }()
			id := initFrame(t, w)
			c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":%s,"error":{"code":-32602,"message":"conflicting evidence"}}`, id, result)))
			if err := initResult(t, done); codexThreadInitCode(err) != "thread_initialization_unknown" {
				t.Fatal("conflicting frame was not unknown", err)
			}
			if s.ResumeID() != "" || c.threadInitReady(s.Snapshot()) == nil {
				t.Fatal("conflicting ACK adopted or unlocked another create")
			}
			noInitReplay(t, w)
		})
	}
}
