package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"sync/atomic"
	"testing"
	"time"
)

type observedAsyncExec struct{ *asyncInteractionFixture }

func (e *observedAsyncExec) QueuedSubmissionReady(context.Context, *session.Session, string) error {
	return backend.ErrNativeSubmissionBusy
}
func TestObservedAsyncAnswerPromotesOriginalOnce(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s1", "observed", t.TempDir(), backend.Codex, "", "", "native-thread")
	defer s.CloseAndWait()
	h.SetExecutor(&observedAsyncExec{&asyncInteractionFixture{fakeExec: fe}})
	c := sharedReadClient(t, h, "owner")
	var calls atomic.Int32
	fe.onSteer = func(_ *session.Session, _, _ string) (backend.SteerResult, error) {
		calls.Add(1)
		return backend.SteerResult{StrictACK: true, TurnID: "native-turn", RequestID: "observed-request"}, nil
	}
	h.Emit(backend.ObservedTurn{SessionID: s.ID, RequestID: "observed-request", Phase: "running"})
	input := `{"type":"user_input_response","request_id":"ui_async_fixture","answers":{"fixture":"ready"}}`
	route(h, c, input)
	waitForType(t, c, "message_ack")
	if r := waitForType(t, c, "queue_action_result"); r["status"] != "accepted" {
		t.Fatal(r)
	}
	expectState(t, h, "ui_async_fixture", messagequeue.Steered)
	route(h, c, input)
	waitForType(t, c, "message_ack")
	if calls.Load() != 1 || s.IsStreaming() {
		t.Fatal("duplicate or worker claimed", calls.Load())
	}
}
func TestObservedAsyncTerminalRaceNeverStartsFallback(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s1", "observed", t.TempDir(), backend.Codex, "", "", "native-thread")
	defer s.CloseAndWait()
	h.SetExecutor(&asyncInteractionFixture{fakeExec: fe})
	c := sharedReadClient(t, h, "owner")
	var sends atomic.Int32
	fe.onSend = func(*session.Session, string, string) { sends.Add(1) }
	h.Emit(backend.ObservedTurn{SessionID: s.ID, RequestID: "observed", Phase: "running"})
	fe.onSteer = func(_ *session.Session, _, _ string) (backend.SteerResult, error) {
		h.Emit(backend.ObservedTurn{SessionID: s.ID, RequestID: "observed", Phase: "completed"})
		return backend.SteerResult{}, backend.ErrNoActiveTurn
	}
	route(h, c, `{"type":"user_input_response","request_id":"ui_async_fixture","answers":{"fixture":"ready"}}`)
	waitForType(t, c, "message_ack")
	if r := waitForType(t, c, "queue_action_result"); r["status"] != "rejected" {
		t.Fatal(r)
	}
	expectState(t, h, "ui_async_fixture", messagequeue.Failed)
	if h.queuedSubmissionReady(s, "ui_async_fixture") {
		t.Fatal("steer-only admitted new turn")
	}
	time.Sleep(20 * time.Millisecond)
	if sends.Load() != 0 {
		t.Fatal("new turn fallback")
	}
	fe.onSend = func(s *session.Session, id, _ string) {
		if id != "followup" {
			t.Error("wrong new request", id)
		}
		sends.Add(1)
		fe.sink.Emit(backend.CompletedAnswer{SessionID: s.ID, RequestID: id, Text: "ok"})
		fe.sink.Emit(protocol.NewDone(s.ID, id))
	}
	enqueueTestMessage(t, h, c, "followup")
	deadline := time.Now().Add(time.Second)
	for sends.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if sends.Load() != 1 {
		t.Fatal("rejected supplement blocked FIFO")
	}
}
func TestControllerSteersObservedNativeWithoutNewWorker(t *testing.T) {
	h, fe, caller, target := controllerFixture(t)
	ctx := context.Background()
	h.dispatches.SetGrant(ctx, caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true, Steer: true}, 0)
	var sends, steers atomic.Int32
	fe.onSend = func(*session.Session, string, string) { sends.Add(1) }
	fe.onSteer = func(*session.Session, string, string) (backend.SteerResult, error) {
		steers.Add(1)
		return backend.SteerResult{StrictACK: true, TurnID: "observed-turn", RequestID: "native-external"}, nil
	}
	h.Emit(backend.ObservedTurn{SessionID: target.ID, RequestID: "native-external", Phase: "running"})
	value, e := h.ControlSession(ctx, caller, backend.SessionControlRequest{Action: "dispatch_to_session", Mode: "steer", SessionID: target.ID, ExpectedThreadID: target.ResumeID(), Content: "supplement"})
	if e != nil {
		t.Fatal(e)
	}
	receipt := value.(sessiondispatch.Record)
	deadline := time.Now().Add(time.Second)
	for steers.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if steers.Load() != 1 || sends.Load() != 0 {
		t.Fatal(receipt, steers.Load(), sends.Load())
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		row, found, _ := h.messageQueue.Get(target.ID, receipt.RequestID)
		if found && row.State == messagequeue.Steered {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("strict ACK was not persisted")
}

func TestSteerOnlyRestoreDoesNotBlockNewInputOrReplay(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s1", "restore", t.TempDir(), backend.Codex, "", "", "thread")
	defer s.CloseAndWait()
	c := sharedReadClient(t, h, "owner")
	raw, _ := json.Marshal(queuedPayload{Content: "original supplement", SteerOnly: true})
	_, _, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: "original-steer", Payload: raw})
	if e != nil {
		t.Fatal(e)
	}
	h.resumeQueuedMessages()
	expectState(t, h, "original-steer", messagequeue.Failed)
	ids := make(chan string, 1)
	fe.onSend = func(s *session.Session, id, _ string) { ids <- id; fe.sink.Emit(protocol.NewDone(s.ID, id)) }
	enqueueTestMessage(t, h, c, "followup")
	select {
	case id := <-ids:
		if id != "followup" {
			t.Fatal("replayed supplement", id)
		}
	case <-time.After(time.Second):
		t.Fatal("restore blocked next input")
	}
}
