package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	p "everything-go/internal/taskpipeline"
	"testing"
	"time"
)

// Original paired HTTP, real ledger/coordinator and original delegation writer.
// The fake provider does not finish A's new retained round. C must have neither
// a durable claim nor an ordinary writer admission while A only has R1 readiness.
func TestPipelineActionReadinessDoesNotAdmitDownstreamOriginalWriter(t *testing.T) {
	f := sourceWriterFixture(t)
	h := f.h
	h.exec.(*extensionExec).onSend = func(_ *session.Session, _ string, _ string) {}
	observer := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:phone", NamespaceGeneration: jarvisTokenGeneration("initialized-owned-paired-token"), BindingKind: "paired_human"}
	fact, e := h.pipelineFact(context.Background(), observer, f.parent.ID, f.source)
	if e != nil {
		t.Fatal(e)
	}
	a := &p.Action{Kind: "delegation_continue", Target: pipelineIdentity(h.unifiedIdentity(f.child.ID)), OriginalTask: &f.source, Instruction: "synthetic retained action A", Disclosure: "references_only"}
	a.InstructionHash = taskapi.RawDigest([]byte(a.Instruction))
	c := &p.Action{Kind: "session_resume", Target: pipelineIdentity(h.unifiedIdentity(f.destination.ID)), Instruction: "synthetic downstream action C", Disclosure: "references_only"}
	c.InstructionHash = taskapi.RawDigest([]byte(c.Instruction))
	definition := p.Pipeline{Name: "actual original writer action boundary", Authorization: p.Authorization{Confirmed: true, SourceRef: "synthetic-original-user", Budget: p.Budget{DeadlineMS: time.Now().Add(time.Hour).UnixMilli(), MaxEffects: 2, MaxModelTurns: 2, MaxNoProgress: 1}}, Nodes: []p.Step{{ID: "B", Title: "sealed source", Required: true, Source: &f.source, Predicate: p.Predicate{Kind: "success_sealed"}}, {ID: "A", Title: "readiness and action", Required: true, Source: &f.source, Predicate: p.Predicate{Kind: "success_sealed"}, Action: a}, {ID: "C", Title: "readiness and downstream", Required: true, Source: &f.source, Predicate: p.Predicate{Kind: "success_sealed"}, Action: c}}, Edges: []p.Edge{{ID: "BA", From: []string{"B"}, To: "A", Condition: "on_success", Join: "all_success", Output: "references_only"}, {ID: "AC", From: []string{"A"}, To: "C", Condition: "on_success", Join: "all_success", Output: "references_only"}}}
	response := decodePipeline(t, f.call("POST", "task-api", pipelineRequest(t, "pipeline_define", "r_19819819819819819819819819819819", map[string]any{"manager": f.manager, "pipeline": definition}), "phone").Body.Bytes())
	if !response.OK {
		t.Fatal(response.Error)
	}
	raw, _ := json.Marshal(response.Result)
	var receipt p.Receipt
	json.Unmarshal(raw, &receipt)
	response = decodePipeline(t, f.call("POST", "task-api", pipelineRequest(t, "pipeline_enable", "r_19819819819819819819819819819820", map[string]any{"manager": f.manager, "id": receipt.Record.ID, "expected_revision": receipt.Record.Revision}), "phone").Body.Bytes())
	if !response.OK {
		t.Fatal(response.Error)
	}
	issuer, record, e := h.messageQueue.PipelineStore().Issuer(context.Background(), receipt.Record.ID)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.messageQueue.PipelineStore().Observe(context.Background(), record.ID, record.Revision, []p.Fact{fact}, func(ctx context.Context) error { return h.pipelineEffectCurrent(ctx, issuer, record) }); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	h.reconcileTaskPipelines(ctx)
	current, _, _, e := h.messageQueue.PipelineStore().Get(ctx, issuer.Scope, record.ID)
	if e != nil {
		t.Fatal(e)
	}
	var actionReceipt bool
	for _, state := range current.Steps {
		if state.ID == "C" && state.ContinuationID != "" {
			t.Fatal("C claimed before A actual final", state)
		}
		if state.ID == "A" {
			original, found, err := h.messageQueue.PipelineStore().ExactContinuation(ctx, state.ContinuationID)
			if err != nil || !found {
				t.Fatal(original, err)
			}
			actionReceipt = original.OriginalReceipt != nil
		}
	}
	if !actionReceipt {
		t.Fatal("original A writer was not admitted", current.Steps)
	}
	queued, e := h.messageQueue.SnapshotContext(ctx, f.destination.ID)
	if e != nil {
		t.Fatal(e)
	}
	if len(queued.Items) != 0 {
		t.Fatal("downstream ordinary admission before actual A final", queued)
	}
}
