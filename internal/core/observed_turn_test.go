package core

import (
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
)

func TestObservedTurnProjectsRunningListChatAndReconnect(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("s1", "Ghostty", t.TempDir(), "codex", "", "", "")
	h.Emit(protocol.NewDone("s1", "old-request"))
	phone := attachmentClient(h, "phone")
	h.Emit(backend.ObservedTurn{SessionID: "s1", RequestID: "external-current", Phase: "running"})
	h.Emit(backend.NewTextChunk("s1", "external-current", "live"))
	view := h.runtimeSnapshot("reconnected-phone").Items[0]
	if view.Phase != "running" || view.Stage != "composing" || view.ActiveRequestID != "external-current" || view.CompletedAt != 0 || view.LastTerminalStatus != "" {
		t.Fatalf("new external turn retained old terminal state: %+v", view)
	}
	if !h.sessionSummaries()[0].IsStreaming || s.IsStreaming() {
		t.Fatal("presentation and execution ownership were conflated")
	}
	_ = waitForType(t, phone, "session_runtime")
	h.Emit(backend.ObservedTurn{SessionID: "s1", RequestID: "external-current", Phase: "completed"})
	_ = waitForType(t, phone, "done")
	view = h.runtimeSnapshot("reconnected-phone").Items[0]
	if view.Phase != "completed" || h.sessionSummaries()[0].IsStreaming {
		t.Fatalf("terminal projection: %+v", view)
	}
	revision := view.Revision
	h.Emit(backend.ObservedTurn{SessionID: "s1", RequestID: "external-current", Phase: "running"})
	h.Emit(backend.ObservedTurn{SessionID: "s1", RequestID: "external-current", Phase: "completed"})
	if h.runtimeSnapshot("phone").Items[0].Revision != revision {
		t.Fatal("duplicate terminal added unread/revision")
	}
}

func TestObservedTerminalCannotReleaseBridgeQueue(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("s1", "Ghostty", t.TempDir(), "codex", "", "", "")
	h.Emit(backend.ObservedTurn{SessionID: "s1", RequestID: "external", Phase: "running"})
	started := make(chan struct{})
	s.SubmitNamed("owned", func() { close(started) })
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker not started")
	}
	defer s.EndTurn()
	h.updateRuntime("s1", "running", "owned", 0, "", "")
	h.Emit(backend.ObservedTurn{SessionID: "s1", RequestID: "external", Phase: "completed"})
	h.Emit(backend.ObservedTurn{SessionID: "s1", RequestID: "external", Phase: "running"})
	if !s.IsStreaming() || s.ActiveQueuedID() != "owned" {
		t.Fatal("observer released Bridge worker")
	}
	if view := h.runtimeSnapshot("phone").Items[0]; view.Phase != "running" || view.ActiveRequestID != "owned" {
		t.Fatalf("observer replaced owned runtime: %+v", view)
	}
}
