package core

import (
	"context"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"testing"
)

type queuedReasonExec struct {
	*fakeExec
	readiness error
}

func (e *queuedReasonExec) QueuedSubmissionReady(context.Context, *session.Session, string) error {
	return e.readiness
}
func TestQueuedWaitReasonDistinguishesTypedNativeBusyAndUnverified(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want string
	}{{"typedbusy", backend.ErrNativeSubmissionBusy, "等待上一個對話回合結束；訊息仍在排隊。"}, {"unverified", errors.New("synthetic private provider detail must not leak"), "正在確認對話狀態；訊息仍在排隊。"}, {"untypedbusy", errors.New("native_submission_busy"), "正在確認對話狀態；訊息仍在排隊。"}} {
		t.Run(tt.name, func(t *testing.T) {
			h, fe := newTestHub(t)
			s := h.registry.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "original")
			defer s.CloseAndWait()
			e := &queuedReasonExec{fakeExec: fe, readiness: tt.err}
			h.SetExecutor(e)
			_, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: "original", Payload: []byte(`{"content":"synthetic"}`)})
			if err != nil {
				t.Fatal(err)
			}
			h.updateRuntime(s.ID, "queued", "original", 1, "", "")
			before := h.runtimes.Snapshot("fixture", []string{s.ID})[0]
			if h.queuedSubmissionReady(s, "original") {
				t.Fatal("unverified native admitted")
			}
			after := h.runtimes.Snapshot("fixture", []string{s.ID})[0]
			if after.Phase != "queued" || after.Stage != "queued" || after.StageMessage != tt.want || after.ActiveRequestID != "original" || after.ActiveStartedAt != before.ActiveStartedAt || after.QueueLength != 1 {
				t.Fatalf("queue status fabricated or missing: %+v", after)
			}
			revision := after.Revision
			h.queuedSubmissionReady(s, "original")
			after = h.runtimes.Snapshot("fixture", []string{s.ID})[0]
			if after.Revision != revision {
				t.Fatal("same wait reason churned revision")
			}
			e.readiness = nil
			if !h.queuedSubmissionReady(s, "original") {
				t.Fatal("ready head remained blocked")
			}
			after = h.runtimes.Snapshot("fixture", []string{s.ID})[0]
			if after.StageMessage != "" || after.Phase != "queued" {
				t.Fatal("readiness pretended native start or retained wait", after)
			}
		})
	}
}
func TestQueuedWaitReasonCannotRebindDifferentRequestOrTerminal(t *testing.T) {
	for _, phase := range []string{"queued", "running", "waiting", "stopping", "completed", "failed", "interrupted", "closed"} {
		t.Run(phase, func(t *testing.T) {
			h, fe := newTestHub(t)
			s := h.registry.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "original")
			defer s.CloseAndWait()
			h.SetExecutor(&queuedReasonExec{fakeExec: fe, readiness: backend.ErrNativeSubmissionBusy})
			_, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: "old", Payload: []byte(`{"content":"synthetic"}`)})
			if err != nil {
				t.Fatal(err)
			}
			terminal := ""
			if phase == "completed" || phase == "failed" || phase == "interrupted" || phase == "closed" {
				terminal = phase
			}
			h.updateRuntime(s.ID, phase, "new", 0, terminal, "")
			before := h.runtimes.Snapshot("fixture", []string{s.ID})[0]
			h.queuedSubmissionReady(s, "old")
			after := h.runtimes.Snapshot("fixture", []string{s.ID})[0]
			if after.Revision != before.Revision || after.Phase != before.Phase || after.ActiveRequestID != before.ActiveRequestID || after.StageMessage != before.StageMessage {
				t.Fatal("late readiness rebound real turn", before, after)
			}
		})
	}
}

func TestQueuedWaitReasonCannotReviveCancelledDurableHead(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "original")
	defer s.CloseAndWait()
	h.SetExecutor(&queuedReasonExec{fakeExec: fe, readiness: backend.ErrNativeSubmissionBusy})
	_, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: "cancelled", Payload: []byte(`{"content":"synthetic"}`)})
	if err != nil {
		t.Fatal(err)
	}
	h.updateRuntime(s.ID, "queued", "cancelled", 1, "", "")
	_, _, err = h.messageQueue.Transition(s.ID, "cancelled", []messagequeue.State{messagequeue.Queued}, messagequeue.Cancelled, "cancelled", "", "")
	if err != nil {
		t.Fatal(err)
	}
	before := h.runtimes.Snapshot("fixture", []string{s.ID})[0]
	if h.queuedSubmissionReady(s, "cancelled") {
		t.Fatal("native busy must stay not ready")
	}
	after := h.runtimes.Snapshot("fixture", []string{s.ID})[0]
	if after.Revision != before.Revision || after.StageMessage != "" {
		t.Fatal("late busy response revived cancelled head", after)
	}
}
