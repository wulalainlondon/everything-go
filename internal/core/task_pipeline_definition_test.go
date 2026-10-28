package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	p "everything-go/internal/taskpipeline"
	"testing"
	"time"
)

// The first actor really admits A via the original retained writer; its new
// native round finishes only after the user has changed unissued C.
func definitionCarryFixture(t *testing.T, finish ...bool) (pipelineSourceFixture, p.Record, p.Issuer, p.Continuation) {
	t.Helper()
	f := sourceWriterFixture(t)
	h := f.h
	ctx := context.Background()
	store := h.messageQueue.PipelineStore()
	observer := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:phone", NamespaceGeneration: jarvisTokenGeneration("initialized-owned-paired-token"), BindingKind: "paired_human"}
	fact, e := h.pipelineFact(ctx, observer, f.parent.ID, f.source)
	if e != nil {
		t.Fatal(e)
	}
	a := &p.Action{Kind: "delegation_continue", Target: pipelineIdentity(h.unifiedIdentity(f.child.ID)), OriginalTask: &f.source, Instruction: "unchanged original A", Disclosure: "references_only"}
	a.InstructionHash = taskapi.RawDigest([]byte(a.Instruction))
	c := &p.Action{Kind: "session_resume", Target: pipelineIdentity(h.unifiedIdentity(f.destination.ID)), Instruction: "old unissued C", Disclosure: "references_only"}
	c.InstructionHash = taskapi.RawDigest([]byte(c.Instruction))
	def := p.Pipeline{Name: "original unchanged admitted A", Authorization: p.Authorization{Confirmed: true, SourceRef: "synthetic explicit user", Budget: p.Budget{DeadlineMS: time.Now().Add(time.Hour).UnixMilli(), MaxEffects: 2, MaxModelTurns: 2, MaxNoProgress: 1}}, Nodes: []p.Step{{ID: "A", Title: "A", Required: true, Source: &f.source, Predicate: p.Predicate{Kind: "success_sealed"}, Action: a}, {ID: "C", Title: "C", Required: true, Source: &f.source, Predicate: p.Predicate{Kind: "success_sealed"}, Action: c}}, Edges: []p.Edge{{ID: "AC", From: []string{"A"}, To: "C", Condition: "on_success", Join: "all_success", Output: "references_only"}}}
	response := decodePipeline(t, f.call("POST", "task-api", pipelineRequest(t, "pipeline_define", "r_19919919919919919919919919919919", map[string]any{"manager": f.manager, "pipeline": def}), "phone").Body.Bytes())
	if !response.OK {
		t.Fatal(response.Error)
	}
	raw, _ := json.Marshal(response.Result)
	var receipt p.Receipt
	json.Unmarshal(raw, &receipt)
	response = decodePipeline(t, f.call("POST", "task-api", pipelineRequest(t, "pipeline_enable", "r_19919919919919919919919919919920", map[string]any{"manager": f.manager, "id": receipt.Record.ID, "expected_revision": receipt.Record.Revision}), "phone").Body.Bytes())
	if !response.OK {
		t.Fatal(response.Error)
	}
	issuer, r, e := store.Issuer(ctx, receipt.Record.ID)
	if e != nil {
		t.Fatal(e)
	}
	if e = store.Observe(ctx, r.ID, r.Revision, []p.Fact{fact}, func(ctx context.Context) error { return h.pipelineEffectCurrent(ctx, issuer, r) }); e != nil {
		t.Fatal(e)
	}
	h.exec.(*extensionExec).onSend = func(s *session.Session, request, _ string) {
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: request, ThreadID: s.ResumeID(), TurnID: "carry-" + request})
		h.Emit(backend.NativeTaskStarted{SessionID: s.ID, RequestID: request, Provider: "codex", ThreadID: s.ResumeID(), Token: "carry-" + request, TokenKind: "turn_id", ProofKind: "live_turn_start_in_progress", ObservedAt: time.Now()})
	}
	h.reconcileTaskPipelines(ctx)
	pending, e := store.Pending(ctx, 32)
	if e != nil {
		t.Fatal(e)
	}
	var original p.Continuation
	for _, one := range pending {
		if one.RecordID == r.ID && one.NodeID == "A" {
			original = one
		}
	}
	if original.OriginalReceipt == nil {
		t.Fatal("A did not cross actual retained admission", pending)
	}
	if e = h.reconcileDelegations(ctx); e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok, _ := h.messageQueue.NativeStart(f.child.ID, original.OriginalReceipt.Request); ok {
			break
		}
		time.Sleep(time.Millisecond)
	}
	current, _, _, _ := store.Get(ctx, issuer.Scope, r.ID)
	raw, _ = json.Marshal(current)
	var next p.Record
	json.Unmarshal(raw, &next)
	next.Pipeline.Nodes[1].Action.Instruction = "new explicitly authorized unissued C"
	next.Pipeline.Nodes[1].Action.InstructionHash = taskapi.RawDigest([]byte(next.Pipeline.Nodes[1].Action.Instruction))
	// The existing ledger's CAS writer supports definition versions. The current
	// closed HTTP pipeline_define has no id/CAS update fields; this integration
	// test intentionally exercises the original store writer, not a fake route.
	updated, e := store.Mutate(ctx, issuer.Scope, issuer, "r_19919919919919919919919919919921", "pipeline_define", "synthetic-new-definition-hash", r.ID, current.Revision, &next, nil, nil, func(ctx context.Context) error { return h.pipelineEffectCurrent(ctx, issuer, current) })
	if e != nil {
		t.Fatal(e)
	}
	r = *updated.Record

	issuer, r, e = store.Issuer(ctx, r.ID)
	if e != nil {
		t.Fatal(e)
	}
	h.reconcileTaskPipelines(ctx)
	now, _, _, _ := store.Get(ctx, issuer.Scope, r.ID)
	for _, v := range now.Steps {
		if v.ID == "C" && v.ContinuationID != "" {
			t.Fatal("C claimed before actual native A final")
		}
	}
	if len(finish) > 0 && !finish[0] {
		return f, r, issuer, original
	}
	bound, found, e := h.delegations.ByID(ctx, original.OriginalReceipt.NativeID)
	if e != nil || !found {
		t.Fatal(e)
	}
	bound.Result = "actual original A round2 result"
	correctionSeal(t, h, bound, false)
	h.Emit(protocol.NewDone(f.child.ID, bound.ChildRequestID))
	if e = h.reconcileDelegations(ctx); e != nil {
		t.Fatal(e)
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entry, ok, _ := h.messageQueue.GetContext(ctx, f.child.ID, bound.ChildRequestID)
		if ok && entry.State == messagequeue.Completed && f.child.State() == session.Idle {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// Complete only the old frozen outbox. The new definition remains untouched
	// until a fresh coordinator observation supplies exact canonical input facts.
	h.reconcilePipelineOriginal(ctx, issuer, r, original)
	preserved, ok, e := store.ExactContinuation(ctx, original.ID)
	if e != nil || !ok || preserved.State != "checkpointed" {
		t.Fatal("original completion unproven", preserved, e)
	}
	return f, r, issuer, preserved
}

func TestPipelineDefinitionUnchangedAdmittedActionFreshCarryOriginalWriter(t *testing.T) {
	f, r, issuer, original := definitionCarryFixture(t)
	h := f.h
	store := h.messageQueue.PipelineStore()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	h.exec.(*extensionExec).onSend = func(*session.Session, string, string) {}
	h.reconcileTaskPipelines(ctx)
	current, _, _, e := store.Get(ctx, issuer.Scope, r.ID)
	if e != nil {
		t.Fatal(e)
	}
	var carried, admitted bool
	for _, v := range current.Steps {
		if v.ID == "A" {
			carried = v.Reason == "carried_original_checkpoint" && v.ContinuationID == original.ID
		}
		if v.ID == "C" {
			c, ok, e := store.ExactContinuation(ctx, v.ContinuationID)
			admitted = ok && e == nil && c.OriginalReceipt != nil && c.DefinitionRevision == r.DefinitionRevision
		}
	}
	if !carried || !admitted || ctx.Err() != nil {
		t.Fatal("actual current frontier did not carry and admit C", current.Steps, ctx.Err())
	}
	rounds, e := h.delegations.Rounds(ctx, f.first.ID)
	if e != nil || len(rounds) != 2 {
		t.Fatal("original A was repeated", rounds, e)
	}
}

func TestPipelineDefinitionCarriedActionFinalWriterRejectsLateNativeConflict(t *testing.T) {
	f, r, issuer, original := definitionCarryFixture(t)
	h := f.h
	store := h.messageQueue.PipelineStore()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	facts, e := h.pipelineCurrentFacts(ctx, issuer, r)
	if e != nil {
		t.Fatal(e)
	}
	current, _, _, _ := store.Get(ctx, issuer.Scope, r.ID)
	if e = store.Observe(ctx, r.ID, current.Revision, facts, func(ctx context.Context) error { return h.pipelineIssuerCurrent(ctx, issuer, r) }); e != nil {
		t.Fatal(e)
	}
	issuer, r, e = store.Issuer(ctx, r.ID)
	if e != nil {
		t.Fatal(e)
	}
	pending, e := store.Pending(ctx, 32)
	if e != nil {
		t.Fatal(e)
	}
	var next p.Continuation
	for _, c := range pending {
		if c.NodeID == "C" && c.RecordID == r.ID {
			next = c
		}
	}
	if next.ID == "" {
		t.Fatal("C missing")
	}
	check := func(ctx context.Context) error { return h.pipelineEffectCurrent(ctx, issuer, r) }
	next, e = store.MarkIssued(ctx, next, check)
	if e != nil {
		t.Fatal(e)
	}
	text, e := h.pipelineInputSnapshot(ctx, issuer, r, next)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.messageQueue.RecordNativeAcceptance(f.child.ID, original.OriginalReceipt.Request, f.child.ResumeID(), "foreign-turn-after-carry"); e == nil {
		t.Fatal("did not quarantine native conflict")
	}
	client := &Client{hub: h, pipelineAdmission: &p.Admission{ID: next.ID, Fence: next.Fence, Check: check}, deviceID: "automation_service", clientSurface: "task_pipeline", send: make(chan []byte, 8), urgent: make(chan []byte, 1), quit: make(chan struct{})}
	if h.enqueueChatMessageExpected(client, clientproto.Command{Kind: "message", SessionID: f.destination.ID, RequestID: next.RequestID, Content: text, MessagePurpose: "control"}, &dispatchTargetExpectation{ThreadID: next.Action.Target.Thread, Revision: next.Action.Target.Revision}) {
		t.Fatal("late conflicted A issued C")
	}
	if _, found, _ := h.messageQueue.GetContext(ctx, f.destination.ID, next.RequestID); found {
		t.Fatal("late rejection left canonical C admission")
	}
	if ctx.Err() != nil {
		t.Fatal("carried final reader reborrowed held MQ connection", ctx.Err())
	}
}

func TestPipelineDefinitionOriginalFinalReaderRequiresExactPipelineReceiptAndSeal(t *testing.T) {
	for _, kind := range []string{"valid", "reopen", "non_pipeline_R1", "child", "request", "round", "source", "intent_hash", "target_ABA", "source_ABA", "authority", "no_seal", "no_consumed_input", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			f, _, _, c := definitionCarryFixture(t, kind != "unknown")
			h := f.h
			ctx := context.Background()
			d, found, e := h.delegations.ByID(ctx, c.OriginalReceipt.NativeID)
			if e != nil || !found {
				t.Fatal(e)
			}
			request := d.ChildRequestID
			alterMQ := func(query string, args ...any) {
				t.Helper()
				if e := h.messageQueue.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error { _, e := tx.ExecContext(ctx, query, args...); return e }); e != nil {
					t.Fatal(e)
				}
			}
			switch kind {
			case "reopen":
				if e = h.messageQueue.Close(); e != nil {
					t.Fatal(e)
				}
				if e = h.delegations.Close(); e != nil {
					t.Fatal(e)
				}
				h.messageQueue, e = messagequeue.Open(h.cfg.DataDir)
				if e != nil {
					t.Fatal(e)
				}
				h.delegations, e = delegation.Open(h.cfg.DataDir)
				if e != nil {
					t.Fatal(e)
				}
			case "non_pipeline_R1":
				d = f.first
				request = d.ChildRequestID
			case "child":
				d.ChildSessionID = "foreign-child"
			case "request":
				request = "foreign-request"
			case "round":
				d.ID = "foreign-round"
			case "source":
				d.ParentSessionID = "foreign-parent"
			case "intent_hash":
				d.IntentHash = "foreign-hash"
			case "target_ABA":
				f.child.SetResumeID("foreign-child-thread")
			case "source_ABA":
				f.parent.SetResumeID("foreign-parent-thread")
			case "authority":
				if e = h.delegations.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
					_, e := tx.ExecContext(ctx, `UPDATE delegation_rounds SET authority='foreign-authority' WHERE id=?`, d.ID)
					return e
				}); e != nil {
					t.Fatal(e)
				}
			case "no_seal":
				alterMQ(`DELETE FROM task_api_seals WHERE session_id=? AND request_id=?`, d.ChildSessionID, request)
			case "no_consumed_input":
				alterMQ(`DELETE FROM task_pipeline_native_inputs WHERE session_id=? AND request_id=?`, d.ChildSessionID, request)
			case "unknown":
				if e = h.messageQueue.PipelineStore().SetOutcome(ctx, c, "unknown", "synthetic lost original receipt lookup", c.OriginalReceipt); e != nil {
					t.Fatal(e)
				}
			}
			seal, e := h.pipelineDelegationFinal(ctx, d, request)
			if kind == "valid" || kind == "reopen" {
				if e != nil || seal.SessionID != d.ChildSessionID || seal.RequestID != request || taskapi.RawDigest([]byte(seal.Text)) != seal.Hash {
					t.Fatal("exact original final unreadable", seal, e)
				}
			} else if e == nil {
				t.Fatal("unbound/unknown original final was accepted", kind, seal)
			}
		})
	}
}
