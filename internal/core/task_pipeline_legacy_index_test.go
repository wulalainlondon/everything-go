package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	p "everything-go/internal/taskpipeline"
	"testing"
	"time"
)

// A real original steer ACK has two request identities. The derived index must
// retain the dispatch command, never adopt its already-running native turn.
func TestPipelineLegacySteeredIndexRetainsCommandAndResolvesOriginal(t *testing.T) {
	h, fe, caller, target := controllerFixture(t)
	if _, e := h.dispatches.SetGrant(context.Background(), caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true, Steer: true}, 0); e != nil {
		t.Fatal(e)
	}
	active := "r_original_active_native"
	turn := "original-active-turn"
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	fe.onSend = func(s *session.Session, id, text string) {
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: id, ThreadID: s.ResumeID(), TurnID: turn})
		close(started)
		<-release
		h.Emit(protocol.NewDone(s.ID, id))
	}
	fe.onSteer = func(*session.Session, string, string) (backend.SteerResult, error) {
		return backend.SteerResult{StrictACK: true, RequestID: active, TurnID: turn}, nil
	}
	client := sharedReadClient(t, h, "original-steer-viewer")
	if !h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: target.ID, RequestID: active, MessagePurpose: "instruction", Content: "synthetic original active"}) {
		t.Fatal("original active input refused")
	}
	<-started
	value, e := h.ControlSession(context.Background(), caller, backend.SessionControlRequest{Action: "dispatch_to_session", SessionID: target.ID, ExpectedThreadID: target.ResumeID(), ExpectedConfigRevision: target.SettingsSnapshot().ConfigRevision, Content: "synthetic explicit supplement", Mode: "steer"})
	if e != nil {
		t.Fatal(e)
	}
	original := value.(sessiondispatch.Record)
	deadline := time.Now().Add(time.Second)
	for {
		entry, found, _ := h.messageQueue.Get(target.ID, original.RequestID)
		if found && entry.State == messagequeue.Steered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("original steer not acknowledged")
		}
		time.Sleep(time.Millisecond)
	}
	h.reconcileSessionDispatches(context.Background())
	original, _, e = h.dispatches.Get(context.Background(), original.ID)
	if e != nil || original.ExecutionRequestID != active || original.RequestID == active {
		t.Fatal("original split tuple absent", original, e)
	}
	c := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + caller.Parent.ID, SourceSessionID: caller.Parent.ID, SourceResumeID: caller.Parent.ResumeID(), SourceConfigRevision: caller.Parent.SettingsSnapshot().ConfigRevision, SourceConfigKnown: true, BindingKind: "native_tool"}
	index, e := h.controllerIndexGet(context.Background(), c, apiID(h.cfg.InstanceID, "controller", original.ID))
	if e != nil {
		t.Fatal(e)
	}
	// This exact request comes from the original canonical read index, not a name.
	loc := unifiedLocator{Kind: "task", Authority: h.cfg.InstanceID, TaskID: index.TaskID, SessionID: index.SessionID, RequestID: index.RequestID, ThreadID: target.ResumeID(), Revision: 0, RevisionKnown: true}
	resolved, e := h.resolveUnified(context.Background(), c, caller.Parent.ID, loc)
	if e != nil {
		t.Fatal("derived legacy index cannot resolve its own original command", e)
	}
	if resolved.Locator.RequestID != original.RequestID || index.RequestID != original.RequestID {
		t.Fatal("steer active native request was substituted for original command", index, resolved.Locator)
	}
	if _, e = h.unifiedTaskDTO(context.Background(), c, caller.Parent.ID, resolved); e != nil {
		t.Fatal(e)
	}
	wrong := loc
	wrong.RequestID = active
	if _, e = h.resolveUnified(context.Background(), c, caller.Parent.ID, wrong); e == nil {
		t.Fatal("original active turn adopted as dispatch command")
	}
	fact, e := h.pipelineFact(context.Background(), c, caller.Parent.ID, p.Locator{Kind: "controller", Authority: h.cfg.InstanceID, NativeID: original.ID, RoundID: original.RequestID, Session: target.ID, Request: original.RequestID, Thread: target.ResumeID(), Revision: 0})
	if e != nil {
		t.Fatal(e)
	}
	if p.PredicateMet(p.Predicate{Kind: "success_sealed"}, fact) || fact.NativeStart == "observed" {
		b, _ := json.Marshal(fact)
		t.Fatal("existing active native turn used as new command start/success", string(b))
	}
}
