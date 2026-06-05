package core

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"everything-go/internal/executor"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// fakeExec is a scriptable Executor for hub behavior tests. Send delegates to an
// injectable onSend so a test decides what a "turn" emits (a full turn with
// done, or a long-running turn that emits nothing until stopped).
type fakeExec struct {
	sink   executor.Sink
	onSend func(s *session.Session, reqID, content string)
}

func (f *fakeExec) Send(_ context.Context, s *session.Session, reqID, content string, _ []protocol.InboundImage, _ []protocol.InboundFile) error {
	if f.onSend != nil {
		f.onSend(s, reqID, content)
	}
	return nil
}

func (f *fakeExec) Stop(_ context.Context, s *session.Session) error {
	f.sink.Emit(protocol.NewStopped(s.ID, ""))
	return nil
}

func (f *fakeExec) Clear(_ context.Context, s *session.Session) error {
	f.sink.Emit(protocol.NewSessionWarning(s.ID, "Session history cleared."))
	return nil
}

func (f *fakeExec) Close(_ context.Context, s *session.Session) error { return nil }

func newTestHub(t *testing.T) (*Hub, *fakeExec) {
	t.Helper()
	reg := session.NewRegistry()
	pairing := governance.NewPairing(filepath.Join(t.TempDir(), "pairing.json"))
	h := NewHub(reg, Config{InstanceID: "i1", InstanceName: "test"}, pairing)
	fe := &fakeExec{sink: h}
	h.SetExecutor(fe)
	return h, fe
}

// newTestClient registers a client whose send channel we drain directly. The
// buffer is large enough that enqueue never overflows (so the nil conn is never
// touched).
func newTestClient(h *Hub) *Client {
	c := &Client{hub: h, send: make(chan []byte, 1024), quit: make(chan struct{}), clientID: "test-client"}
	h.addClient(c)
	return c
}

func route(h *Hub, c *Client, frame string) {
	in, err := protocol.ParseInbound([]byte(frame))
	if err != nil {
		panic(err)
	}
	h.route(context.Background(), c, in)
}

// waitForType drains the client's send channel until it sees an event of the
// given type, returning it. Fails the test on timeout.
func waitForType(t *testing.T, c *Client, typ string) map[string]any {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case data := <-c.send:
			var m map[string]any
			if err := json.Unmarshal(data, &m); err != nil {
				t.Fatalf("bad event JSON: %v", err)
			}
			if m["type"] == typ {
				return m
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event type %q", typ)
		}
	}
}

func waitState(t *testing.T, s *session.Session, want session.State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.State() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("session state = %s, want %s", s.State(), want)
}

// Regression: a background goroutine (sendHistory/sendUsage/…) that captured a
// client and enqueues after the client disconnected must never panic. The real
// 4G app triggered this: it requested history then immediately switched bridges,
// and the late sendHistory enqueue hit a closed send channel → process crash.
// The fix is to never close send and gate on quit instead.
func TestEnqueueAfterShutdownDoesNotPanic(t *testing.T) {
	h, _ := newTestHub(t)
	c := &Client{hub: h, send: make(chan []byte, 2), quit: make(chan struct{}), clientID: "bg"}

	c.shutdown()
	c.shutdown() // idempotent

	// These would panic ("send on closed channel") under the old close(send) model.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			c.enqueueEvent(protocol.NewPong())
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueue after shutdown blocked")
	}
}

// pending_interactions_list must return a valid empty array (never null) even
// when the wired executor can't answer interactions (the hub's fakeExec).
func TestPendingInteractionsListEmptyWhenUnsupported(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	route(h, c, `{"type":"pending_interactions_list"}`)
	ev := waitForType(t, c, "pending_interactions_list")
	arr, ok := ev["interactions"].([]any)
	if !ok {
		t.Fatalf("interactions must be an array, got %T", ev["interactions"])
	}
	if len(arr) != 0 {
		t.Fatalf("interactions should be empty, got %v", arr)
	}
}

// The Phase 5 read commands the app polls on connect must return valid empty
// lists (arrays, never null) so the app's z.array schemas accept them, instead
// of being left unhandled.
func TestPhase5ReadStubsReturnEmptyArrays(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)

	cases := []struct{ send, want, field string }{
		{`{"type":"list_instances"}`, "instances_list", "instances"},
		{`{"type":"get_inbox"}`, "inbox_list", "items"},
		{`{"type":"feed_list_request"}`, "feed_list", "items"},
	}
	for _, tc := range cases {
		route(h, c, tc.send)
		ev := waitForType(t, c, tc.want)
		arr, ok := ev[tc.field].([]any)
		if !ok {
			t.Fatalf("%s.%s must be an array (not null), got %T: %v", tc.want, tc.field, ev[tc.field], ev[tc.field])
		}
		if len(arr) != 0 {
			t.Fatalf("%s.%s should be empty, got %v", tc.want, tc.field, arr)
		}
	}
}

func TestNewSessionBroadcastsSessionsList(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)

	route(h, c, `{"type":"new_session","session_id":"s1","name":"Fresh","backend":"claude"}`)
	waitForType(t, c, "session_created")
	ev := waitForType(t, c, "sessions_list")
	sessions, _ := ev["sessions"].([]any)
	if len(sessions) != 1 {
		t.Fatalf("new_session should broadcast one-session sessions_list, got %d", len(sessions))
	}
	ss, _ := sessions[0].(map[string]any)
	if ss["id"] != "s1" {
		t.Fatalf("sessions_list id = %v, want s1", ss["id"])
	}
}

func TestRequestHistoryMissingSessionReturnsEmptySnapshot(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)

	route(h, c, `{"type":"request_history","session_id":"missing","mode":"snapshot"}`)
	ev := waitForType(t, c, "history_snapshot")
	if ev["session_id"] != "missing" {
		t.Fatalf("session_id = %v, want missing", ev["session_id"])
	}
	msgs, ok := ev["messages"].([]any)
	if !ok || len(msgs) != 0 {
		t.Fatalf("missing session history should be empty array, got %#v", ev["messages"])
	}
}

func TestRenameSessionBroadcastsToAllClients(t *testing.T) {
	h, _ := newTestHub(t)
	c1 := newTestClient(h)
	c2 := newTestClient(h)

	route(h, c1, `{"type":"new_session","session_id":"s1","name":"Old","backend":"claude"}`)
	waitForType(t, c1, "session_created")

	route(h, c1, `{"type":"rename_session","session_id":"s1","name":"New"}`)
	for _, c := range []*Client{c1, c2} {
		ev := waitForType(t, c, "session_renamed")
		if ev["session_id"] != "s1" || ev["name"] != "New" {
			t.Fatalf("bad rename event: %v", ev)
		}
	}
}

func TestSetSessionMetaBroadcastsAndUpdatesSummaries(t *testing.T) {
	h, _ := newTestHub(t)
	c1 := newTestClient(h)
	c2 := newTestClient(h)

	route(h, c1, `{"type":"new_session","session_id":"s1","name":"One","backend":"claude"}`)
	waitForType(t, c1, "session_created")

	route(h, c1, `{"type":"set_session_meta","session_id":"s1","pinned":true,"hidden":true}`)
	for _, c := range []*Client{c1, c2} {
		ev := waitForType(t, c, "session_meta_updated")
		if ev["session_id"] != "s1" || ev["pinned"] != true || ev["hidden"] != true {
			t.Fatalf("bad meta event: %v", ev)
		}
	}

	route(h, c1, `{"type":"request_sessions_list"}`)
	ev := waitForType(t, c1, "sessions_list")
	sessions, _ := ev["sessions"].([]any)
	if len(sessions) != 1 {
		t.Fatalf("sessions_list len = %d, want 1", len(sessions))
	}
	ss, _ := sessions[0].(map[string]any)
	if ss["pinned"] != true || ss["hidden"] != true {
		t.Fatalf("sessions_list should include updated meta, got %v", ss)
	}
}

func TestSetSessionMetaIgnoresUnknownSession(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)

	route(h, c, `{"type":"set_session_meta","session_id":"missing","hidden":true}`)
	select {
	case data := <-c.send:
		t.Fatalf("unknown session meta should be silent, got %s", string(data))
	case <-time.After(80 * time.Millisecond):
	}
}

// A full turn: session_created, then the streamed events, then done — and the
// session returns to Idle (the Hub drives EndTurn off the done event).
func TestMessageStreamsAndEndsTurn(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	fe.onSend = func(s *session.Session, reqID, content string) {
		h.Emit(protocol.NewTextChunk(s.ID, reqID, "hello "+content))
		h.Emit(protocol.NewDone(s.ID, reqID))
	}

	route(h, c, `{"type":"new_session","session_id":"s1","backend":"claude"}`)
	waitForType(t, c, "session_created")
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r1","content":"world"}`)

	chunk := waitForType(t, c, "text_chunk")
	if chunk["content"] != "hello world" {
		t.Fatalf("unexpected chunk: %v", chunk)
	}
	waitForType(t, c, "done")

	s, _ := h.registry.Get("s1")
	waitState(t, s, session.Idle)
}

// stop on a long-running turn (one that emits nothing until interrupted) must
// emit stopped and return the session to Idle.
func TestStopEndsTurn(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	fe.onSend = func(s *session.Session, reqID, content string) {
		h.Emit(protocol.NewTextChunk(s.ID, reqID, "thinking..."))
		// no done — the turn stays in flight until stop
	}

	route(h, c, `{"type":"new_session","session_id":"s1","backend":"claude"}`)
	waitForType(t, c, "session_created")
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r1","content":"go"}`)
	waitForType(t, c, "text_chunk")

	s, _ := h.registry.Get("s1")
	waitState(t, s, session.Streaming)

	route(h, c, `{"type":"stop","session_id":"s1"}`)
	waitForType(t, c, "stopped")
	waitState(t, s, session.Idle)
}

func TestClearWarnsAndEndsTurn(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	fe.onSend = func(s *session.Session, reqID, content string) {
		h.Emit(protocol.NewTextChunk(s.ID, reqID, "..."))
	}

	route(h, c, `{"type":"new_session","session_id":"s1","backend":"claude"}`)
	waitForType(t, c, "session_created")
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r1","content":"go"}`)
	waitForType(t, c, "text_chunk")

	s, _ := h.registry.Get("s1")
	waitState(t, s, session.Streaming)

	route(h, c, `{"type":"clear_session","session_id":"s1"}`)
	waitForType(t, c, "session_warning")
	waitState(t, s, session.Idle) // router calls EndTurn after Clear
}

func TestCloseRemovesSession(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	route(h, c, `{"type":"new_session","session_id":"s1","backend":"claude"}`)
	waitForType(t, c, "session_created")

	s, _ := h.registry.Get("s1")
	route(h, c, `{"type":"close_session","session_id":"s1"}`)
	waitForType(t, c, "session_closed")

	if _, ok := h.registry.Get("s1"); ok {
		t.Fatal("session should be removed from the registry after close")
	}
	// The worker is shut down: further turns are rejected.
	if s.Submit(func() {}) {
		t.Fatal("Submit on a closed session must fail")
	}
}

// Events emitted while no client is connected are buffered and replayed to the
// next client after its sessions_list (the offline-recovery path).
func TestReconnectReplaysOfflineEvents(t *testing.T) {
	h, _ := newTestHub(t)
	// No clients connected: this event must be buffered, not dropped.
	h.Emit(protocol.NewTextChunk("s1", "r1", "missed while offline"))

	c := newTestClient(h)
	route(h, c, `{"type":"hello","device_id":"d1"}`)

	// hello replies with hello_ack + sessions_list, then replays the buffered chunk.
	waitForType(t, c, "hello_ack")
	waitForType(t, c, "sessions_list")
	replayed := waitForType(t, c, "text_chunk")
	if replayed["content"] != "missed while offline" {
		t.Fatalf("offline event not replayed correctly: %v", replayed)
	}
}

// Two messages for the same session must not interleave: the second turn only
// starts after the first emits done.
func TestPerSessionTurnsSerialize(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)

	starts := make(chan string, 2)
	release := make(chan struct{})
	fe.onSend = func(s *session.Session, reqID, content string) {
		starts <- reqID
		<-release // hold the turn open until the test releases it
		h.Emit(protocol.NewDone(s.ID, reqID))
	}

	route(h, c, `{"type":"new_session","session_id":"s1","backend":"claude"}`)
	waitForType(t, c, "session_created")
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r1","content":"a"}`)
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r2","content":"b"}`)

	if got := <-starts; got != "r1" {
		t.Fatalf("first turn should be r1, got %s", got)
	}
	select {
	case got := <-starts:
		t.Fatalf("second turn %s started before first finished", got)
	case <-time.After(60 * time.Millisecond):
	}
	close(release) // let r1 finish → r2 should start
	if got := <-starts; got != "r2" {
		t.Fatalf("second turn should be r2, got %s", got)
	}
}
