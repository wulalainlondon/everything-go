package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/delegation"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	p "everything-go/internal/taskpipeline"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type pipelineSourceFixture struct {
	h                          *Hub
	parent, child, destination *session.Session
	first                      delegation.Record
	manager                    p.Identity
	source                     p.Locator
	call                       func(string, string, any, string) *httptest.ResponseRecorder
}

func sourceWriterFixture(t *testing.T) pipelineSourceFixture {
	t.Helper()
	h, parent, _, first, call := initializedUnifiedDelegation(t, true)
	child, _ := h.registry.Get(first.ChildSessionID)
	destination := h.registry.Create("source-bearing-destination", "synthetic", h.cfg.RootDir, "codex", "m", "read-only", "destination-native-thread")
	t.Cleanup(destination.CloseAndWait)
	grant, _ := h.dispatches.Grant(context.Background(), parent.ID)
	_, e := h.dispatches.SetGrant(context.Background(), parent.ID, sessiondispatch.Grant{Enabled: true, Local: true, Sessions: []string{h.cfg.InstanceID + ":" + child.ID, h.cfg.InstanceID + ":" + destination.ID}}, grant.Revision)
	if e != nil {
		t.Fatal(e)
	}
	original := h.exec.(*extensionExec)
	original.onSend = func(s *session.Session, request, text string) {
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: request, ThreadID: s.ResumeID(), TurnID: "source-bearing-" + request})
		h.Emit(protocol.NewDone(s.ID, request))
	}
	return pipelineSourceFixture{h, parent, child, destination, first, pipelineIdentity(h.unifiedIdentity(parent.ID)), p.Locator{Kind: "delegation", Authority: h.cfg.InstanceID, NativeID: first.ID, RoundID: first.ID, Session: child.ID, Request: first.ChildRequestID, Thread: child.ResumeID(), Revision: 0}, call}
}
func (f pipelineSourceFixture) ready(t *testing.T, kind, key string) (p.Record, p.Issuer, p.Continuation) {
	t.Helper()
	h := f.h
	target := f.destination
	if kind == "delegation_continue" {
		target = f.child
	}
	observer := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:phone", NamespaceGeneration: jarvisTokenGeneration("initialized-owned-paired-token"), BindingKind: "paired_human"}
	fact, e := h.pipelineFact(context.Background(), observer, f.parent.ID, f.source)
	if e != nil || !p.PredicateMet(p.Predicate{Kind: "success_sealed"}, fact) {
		t.Fatal("original source fixture is not proven success", fact, e)
	}
	action := &p.Action{Kind: kind, Target: pipelineIdentity(h.unifiedIdentity(target.ID)), Instruction: "bounded original source continuation", Disclosure: "references_only"}
	action.InstructionHash = taskapi.RawDigest([]byte(action.Instruction))
	if kind == "delegation_continue" {
		action.OriginalTask = &f.source
	}
	watch := p.Watch{Source: f.source, Destination: f.manager, Predicate: p.Predicate{Kind: "success_sealed"}, Mode: "resume_authorized", CatchUp: true, Action: action, Authorization: p.Authorization{Confirmed: true, SourceRef: "synthetic-original-user", Budget: p.Budget{DeadlineMS: time.Now().Add(time.Hour).UnixMilli(), MaxEffects: 1, MaxModelTurns: 1, MaxNoProgress: 1}}}
	response := decodePipeline(t, f.call("POST", "task-api", pipelineRequest(t, "watch_create", key, map[string]any{"manager": f.manager, "watch": watch}), "phone").Body.Bytes())
	if !response.OK {
		t.Fatal(response.Error)
	}
	raw, _ := json.Marshal(response.Result)
	var receipt p.Receipt
	json.Unmarshal(raw, &receipt)
	issuer, record, e := h.messageQueue.PipelineStore().Issuer(context.Background(), receipt.Record.ID)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if e = h.messageQueue.PipelineStore().Observe(ctx, record.ID, record.Revision, []p.Fact{fact}, func(ctx context.Context) error { return h.pipelineEffectCurrent(ctx, issuer, record) }); e != nil {
		t.Fatal("source-bearing observation failed", e)
	}
	pending, e := h.messageQueue.PipelineStore().Pending(ctx, 10)
	if e != nil {
		t.Fatal(e)
	}
	for _, c := range pending {
		if c.RecordID == record.ID {
			return record, issuer, c
		}
	}
	t.Fatal("source-bearing ready claim missing")
	return record, issuer, p.Continuation{}
}

// Both source-bearing writer boundaries must use their original SQL transaction.
func TestPipelineSourceBearingWatchOriginalWriterTransactionContext(t *testing.T) {
	for _, kind := range []string{"session_resume", "delegation_continue"} {
		t.Run(kind, func(t *testing.T) {
			f := sourceWriterFixture(t)
			record, _, _ := f.ready(t, kind, "r_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			f.h.reconcileTaskPipelines(ctx)
			if ctx.Err() != nil {
				t.Fatal("original writer source disclosure reborrowed its held SQL connection", ctx.Err())
			}
			pending, e := f.h.messageQueue.PipelineStore().Pending(context.Background(), 10)
			if e != nil || len(pending) != 1 || pending[0].RecordID != record.ID || pending[0].OriginalReceipt == nil {
				t.Fatal("source-bearing original effect not admitted", pending, e)
			}
		})
	}
}

// Two original writers concurrently acquire MQ before delegation, never ABBA.
func TestPipelineSourceBearingConcurrentOriginalWritersNoReverseLockOrder(t *testing.T) {
	f := sourceWriterFixture(t)
	f.ready(t, "session_resume", "r_11111111111111111111111111111111")
	_, b, cb := f.ready(t, "delegation_continue", "r_22222222222222222222222222222222")
	rb, _, _, _ := f.h.messageQueue.PipelineStore().Get(context.Background(), b.Scope, cb.RecordID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); f.h.reconcileTaskPipelines(ctx) }()
	go func() { defer wg.Done(); f.h.submitPipelineDelegation(ctx, b, rb, cb) }()
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatal("MQ/delegation writers deadlocked", ctx.Err())
	}
	pending, e := f.h.messageQueue.PipelineStore().Pending(context.Background(), 10)
	if e != nil || len(pending) != 2 {
		t.Fatal(pending, e)
	}
	for _, c := range pending {
		if c.OriginalReceipt == nil {
			t.Fatal("missing original admission", c)
		}
	}
}

// A changed grant or quarantined original native proof after issue but before
// the actual original writer must roll back with zero canonical admission.
func TestPipelineSourceBearingFinalWritersRejectLateRevocationAndNativeConflict(t *testing.T) {
	for _, kind := range []string{"session_resume", "delegation_continue"} {
		for _, change := range []string{"grant", "native_conflict"} {
			t.Run(kind+"/"+change, func(t *testing.T) {
				f := sourceWriterFixture(t)
				h := f.h
				record, issuer, c := f.ready(t, kind, "r_33333333333333333333333333333333")
				check := func(ctx context.Context) error { return h.pipelineEffectCurrent(ctx, issuer, record) }
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				var e error
				c, e = h.messageQueue.PipelineStore().MarkIssued(ctx, c, check)
				if e != nil {
					t.Fatal(e)
				}
				text, e := h.pipelineInputSnapshot(ctx, issuer, record, c)
				if e != nil {
					t.Fatal(e)
				}
				if change == "grant" {
					grant, _ := h.dispatches.Grant(ctx, f.parent.ID)
					_, e = h.dispatches.SetGrant(ctx, f.parent.ID, sessiondispatch.Grant{Enabled: false}, grant.Revision)
					if e != nil {
						t.Fatal(e)
					}
				} else {
					if e = h.messageQueue.RecordNativeAcceptance(f.child.ID, f.first.ChildRequestID, f.child.ResumeID(), "foreign-conflicting-native-turn"); e == nil {
						t.Fatal("fixture did not quarantine conflict")
					}
				}
				if kind == "delegation_continue" {
					if _, e = h.continuePipelineDelegation(ctx, f.parent, issuer, record, c, check); e == nil {
						t.Fatal("revoked source created continuation")
					}
					if _, found, _ := h.delegations.ByOrigin(ctx, f.parent.ID, c.RequestID, c.ID); found {
						t.Fatal("rejected continuation left original record")
					}
				} else {
					client := &Client{hub: h, pipelineAdmission: &p.Admission{ID: c.ID, Fence: c.Fence, Check: check}, deviceID: "automation_service", clientSurface: "task_pipeline", send: make(chan []byte, 8), urgent: make(chan []byte, 1), quit: make(chan struct{})}
					if h.enqueueChatMessageExpected(client, clientproto.Command{Kind: "message", SessionID: f.destination.ID, RequestID: c.RequestID, Content: text, MessagePurpose: "control"}, &dispatchTargetExpectation{ThreadID: c.Action.Target.Thread, Revision: c.Action.Target.Revision}) {
						t.Fatal("revoked source queued original input")
					}
					if _, found, _ := h.messageQueue.GetContext(ctx, f.destination.ID, c.RequestID); found {
						t.Fatal("rejected source left MQ receipt")
					}
				}
				if ctx.Err() != nil {
					t.Fatal("final rejection self-deadlocked", ctx.Err())
				}
			})
		}
	}
}
