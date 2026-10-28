package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/pipeline-v7"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	ti "everything-go/internal/taskinteraction"
	"everything-go/internal/taskpipeline"
	"strings"
	"testing"
	"time"
)

func pipelineRequest(t *testing.T, op, key string, input any) map[string]any {
	t.Helper()
	v := map[string]any{"type": "task_api_pipeline_request", "api_version": contract.Version, "correlation_id": "synthetic-" + op, "operation": op, "input": input}
	if key != "" {
		v["idempotency_key"] = key
	}
	b, _ := json.Marshal(v)
	if e := contract.Validate(b); e != nil {
		t.Fatal(e, string(b))
	}
	return v
}
func decodePipeline(t *testing.T, raw []byte) taskapi.Response {
	t.Helper()
	if e := contract.ValidateResponse(raw); e != nil {
		t.Fatal(e, string(raw))
	}
	var r taskapi.Response
	if e := json.Unmarshal(raw, &r); e != nil {
		t.Fatal(e)
	}
	return r
}
func TestPipelineRealPairedHTTPKnownZeroOriginalOperationAndNoAuthorityInjection(t *testing.T) {
	h, call := jarvisFixture(t)
	var setupErr error
	h.dispatches, setupErr = sessiondispatch.Open(t.TempDir())
	if setupErr != nil {
		t.Fatal(setupErr)
	}
	t.Cleanup(func() { h.dispatches.Close() })
	s := h.registry.Create("synthetic-manager", "Synthetic", h.cfg.RootDir, "codex", "m", "read-only", "native-manager")
	manager := pipelineIdentity(h.unifiedIdentity(s.ID))
	src := h.registry.Create("synthetic-source", "Source", h.cfg.RootDir, "codex", "m", "read-only", "native-source")
	_, e := h.dispatches.SetGrant(context.Background(), s.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	if e != nil {
		t.Fatal(e)
	}
	d := sessiondispatch.Record{ID: "scd_synthetic", ParentID: s.ID, ParentThreadID: s.ResumeID(), ParentConfigKnown: true, ParentConfigRevision: 0, OriginRequestID: "r_user", ToolCallID: "actual-synthetic", InstanceID: h.cfg.InstanceID, SessionID: src.ID, ThreadID: src.ResumeID(), RequestID: "r_source", IntentHash: "originalhash", State: "queued"}
	if _, e = h.dispatches.Create(context.Background(), d); e != nil {
		t.Fatal(e)
	}
	loc := taskpipeline.Locator{Kind: "controller", Authority: h.cfg.InstanceID, NativeID: d.ID, RoundID: d.RequestID, Session: src.ID, Request: d.RequestID, Thread: src.ResumeID(), Revision: 0}
	watch := taskpipeline.Watch{Source: loc, Destination: manager, Predicate: taskpipeline.Predicate{Kind: "success_sealed"}, Mode: "notify_only", Authorization: taskpipeline.Authorization{SourceRef: "human-original", Budget: taskpipeline.Budget{DeadlineMS: time.Now().Add(time.Hour).UnixMilli(), MaxNoProgress: 1}}}
	in := map[string]any{"manager": manager, "watch": watch}
	request := pipelineRequest(t, "watch_create", "original-once", in)
	w := call("POST", "task-api", request, "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	r := decodePipeline(t, w.Body.Bytes())
	if !r.OK {
		t.Fatal(r.Error)
	}
	var receipt taskpipeline.Receipt
	raw, _ := json.Marshal(r.Result)
	json.Unmarshal(raw, &receipt)
	if receipt.Record == nil || receipt.Record.Manager.Revision != 0 || receipt.State != "applied" {
		t.Fatal(receipt)
	}
	// Unknown transport recovery reads the exact immutable original receipt.
	hash := unifiedHash(taskapi.Request{Operation: "watch_create", Input: mustPipelineJSON(in)})
	lookup := pipelineRequest(t, "operation_get", "", map[string]any{"manager": manager, "original_intent_id": "original-once", "original_operation": "watch_create", "expected_input_hash": hash})
	w = call("POST", "task-api", lookup, "phone")
	r = decodePipeline(t, w.Body.Bytes())
	if !r.OK {
		t.Fatal(r.Error)
	}
	if !strings.Contains(w.Body.String(), receipt.Record.ID) {
		t.Fatal(w.Body.String())
	}
	request["automation_principal"] = "spoof"
	w = call("POST", "task-api", request, "phone")
	r = decodePipeline(t, w.Body.Bytes())
	if r.OK || r.Error.Code != "invalid_argument" {
		t.Fatal(r)
	}
	g, _ := h.messageQueue.PipelineStore().Guard(context.Background(), s.ID)
	if g.Ready {
		t.Fatal("context/maintenance ignored live watch", g)
	}
	if !strings.Contains(strings.Join(h.contextBlockers(s), ","), "pipeline_outstanding_or_unknown") {
		t.Fatal(h.contextBlockers(s))
	}
}
func mustPipelineJSON(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
func TestPipelineLegacyIndexIsReadOnlyAndRetainsOriginalControllerID(t *testing.T) {
	h, parent, child, request := fixtureInteractions(t)
	_ = request
	records, e := h.dispatches.List(context.Background(), parent.ID)
	if e != nil || len(records) != 1 {
		t.Fatal(records, e)
	}
	d := records[0]
	before, e := h.dispatches.TaskJournal().All(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	caller := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + parent.ID, SourceSessionID: parent.ID, SourceResumeID: parent.ResumeID(), SourceConfigRevision: parent.SettingsSnapshot().ConfigRevision, SourceConfigKnown: true, BindingKind: "native_tool"}
	id := apiID(h.cfg.InstanceID, "controller", d.ID)
	r, e := h.apiRecord(context.Background(), caller, id)
	if e != nil {
		t.Fatal(e)
	}
	if r.NativeID != d.ID || r.SessionID != child.ID || r.Hash != d.IntentHash || r.ReceiptID != "" {
		t.Fatal(r)
	}
	after, _ := h.dispatches.TaskJournal().All(context.Background())
	if len(before) != len(after) {
		t.Fatal("read fabricated admission journal")
	}
	caller.SourceSessionID = "foreign"
	caller.StableScopeID = "session:foreign"
	if _, e = h.apiRecord(context.Background(), caller, id); e == nil {
		t.Fatal("foreign index access")
	}
}
func TestPipelineActualWriterSingleAdmissionUnknownRecoveryAndSendRevocation(t *testing.T) {
	h, _ := jarvisFixture(t)
	s := h.registry.Create("synthetic-owner", "Synthetic", h.cfg.RootDir, "codex", "m", "read-only", "native-owner")
	release := s.HoldQueue()
	defer release()
	scope := taskpipeline.Scope{Principal: "device:phone", Generation: jarvisTokenGeneration("jarvis-token"), Manager: pipelineIdentity(h.unifiedIdentity(s.ID))}
	issuer := taskpipeline.Issuer{Scope: scope, Kind: "paired_human", SourceGeneration: s.Snapshot().SourceRevision, Targets: map[string]uint64{s.ID: s.Snapshot().SourceRevision}}
	instruction := "Synthetic original writer only"
	action := taskpipeline.Action{Kind: "session_resume", Target: scope.Manager, Instruction: instruction, InstructionHash: taskapi.RawDigest([]byte(instruction)), Disclosure: "references_only"}
	record := taskpipeline.Record{Kind: "mission", Mission: &taskpipeline.Mission{Goal: "fixture", Completion: "original proof", Steps: []taskpipeline.Step{{ID: "one", Title: "One", Required: true, Action: &action}}, Authorization: taskpipeline.Authorization{Confirmed: true, SourceRef: "original-user", Budget: taskpipeline.Budget{DeadlineMS: time.Now().Add(time.Hour).UnixMilli(), MaxEffects: 1, MaxModelTurns: 1, MaxNoProgress: 1}}}}
	receipt, e := h.messageQueue.PipelineStore().Mutate(context.Background(), scope, issuer, "original-create", "mission_create", "hash", "", 0, &record, nil, nil, func(context.Context) error { return nil })
	if e != nil {
		t.Fatal(e)
	}
	h.reconcileTaskPipelines(context.Background())
	snapshot, e := h.messageQueue.Snapshot(s.ID)
	if e != nil {
		t.Fatal(e)
	}
	if len(snapshot.Items) != 1 {
		pending, _ := h.messageQueue.PipelineStore().Pending(context.Background(), 20)
		t.Fatalf("original writer not admitted: %v pending=%+v", snapshot, pending)
	}
	request := snapshot.Items[0].RequestID
	if !strings.HasPrefix(request, "pauto_") {
		t.Fatal(request)
	}
	h.reconcileTaskPipelines(context.Background())
	snapshot, _ = h.messageQueue.Snapshot(s.ID)
	if len(snapshot.Items) != 1 {
		t.Fatal("duplicate effect", snapshot)
	}
	r, _, _, _ := h.messageQueue.PipelineStore().Get(context.Background(), scope, receipt.Record.ID)
	_, e = h.messageQueue.PipelineStore().Mutate(context.Background(), scope, issuer, "cancel", "mission_cancel", "hash2", r.ID, r.Revision, nil, nil, nil, func(context.Context) error { return nil })
	if e != nil {
		t.Fatal(e)
	}
	payload := queuedPayload{PipelineID: strings.TrimPrefix(request, "pauto_")}
	if e = h.pipelineSendGuard(context.Background(), payload.PipelineID, s.ID, request); e != nil {
		t.Fatal("pause/cancel rewrote already accepted original version", e)
	}
	s.CloseAndWait()
}

var _ = session.Idle

func TestPipelineM0ContinuedLogicalIDAndLegacyBlankBindingStayExact(t *testing.T) {
	h, parent, _ := continuityFixture(t)
	_, first := continuityDelegate(t, h, parent, "m0-first", "first")
	second := continuityContinue(t, h, parent, "m0-next", continuityInput(first, parent, h, "second"))
	binding, e := h.interactionBinding(context.Background(), second.ChildSessionID, second.ChildRequestID)
	if e != nil {
		t.Fatal(e)
	}
	if binding.LogicalID != first.ID || binding.RelationID != second.ID || binding.ChildRequest != second.ChildRequestID {
		t.Fatal(binding)
	}
	legacy := binding
	legacy.LogicalID = ""
	record := ti.Record{Binding: legacy}
	if e = h.validInteraction(context.Background(), record); e != nil {
		t.Fatal("original legacy blank no longer readable", e)
	}
	if record.Binding.LogicalID != "" {
		t.Fatal("historical binding mutated")
	}
	wrong := record
	wrong.Binding.RelationID = first.ID
	if e = h.validInteraction(context.Background(), wrong); e == nil {
		t.Fatal("foreign round accepted")
	}
	wrong = record
	wrong.Binding.ChildRevision++
	if e = h.validInteraction(context.Background(), wrong); e == nil {
		t.Fatal("wrong config accepted")
	}
	wrong = record
	wrong.Binding.ParentThread += "foreign"
	if e = h.validInteraction(context.Background(), wrong); e == nil {
		t.Fatal("foreign parent thread accepted")
	}
}
