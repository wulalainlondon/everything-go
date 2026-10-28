package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	p "everything-go/internal/taskpipeline"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The real ExecuteTask provider verifier retains the active original native
// request, ingress tuple, grant and single-MQ transaction, including ACK replay.
func TestPipelineNativeAckOriginalReceiptReadbackAndNoCallerImpersonation(t *testing.T) {
	h, _, c, _, _ := extensionFormal(t)
	parent := h.registry.Create("synthetic-pipeline-native", "pipeline native", h.cfg.RootDir, backend.Codex, "m", "read-only", "synthetic-pipeline-thread")
	_, err := h.dispatches.SetGrant(context.Background(), parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.CloseAndWait()
	ctx := context.Background()
	manager := pipelineIdentity(h.unifiedIdentity(parent.ID))
	if e := h.pairing.Claim("jarvis-token", "phone"); e != nil {
		t.Fatal(e)
	}
	scope := p.Scope{Principal: "device:phone", Generation: jarvisTokenGeneration("jarvis-token"), Manager: manager}
	grant, e := h.dispatches.Grant(ctx, parent.ID)
	if e != nil {
		t.Fatal(e)
	}
	issuer := p.Issuer{Scope: scope, Kind: "paired_human", SourceGeneration: parent.Snapshot().SourceRevision, GrantRevision: grant.Revision, Targets: map[string]uint64{parent.ID: parent.Snapshot().SourceRevision}}
	action := &p.Action{Kind: "session_resume", Target: manager, Instruction: "original synthetic native scope only", Disclosure: "references_only"}
	action.InstructionHash = taskapi.RawDigest([]byte(action.Instruction))
	r := p.Record{Kind: "mission", Manager: manager, Mission: &p.Mission{Goal: "prove actual gateway ACK", Completion: "exact server proof", Steps: []p.Step{{ID: "one", Title: "one", Required: true, Predicate: p.Predicate{Kind: "success_sealed"}, Action: action}}, Authorization: p.Authorization{Confirmed: true, SourceRef: "synthetic original authorization", Budget: p.Budget{DeadlineMS: time.Now().Add(time.Hour).UnixMilli(), MaxEffects: 1, MaxModelTurns: 1, MaxNoProgress: 1}}}}
	store := h.messageQueue.PipelineStore()
	receipt, e := store.Mutate(ctx, scope, issuer, "create", "mission_create", "hash", "", 0, &r, nil, nil, func(context.Context) error { return nil })
	if e != nil {
		t.Fatal(e)
	}
	if e = store.Observe(ctx, receipt.Record.ID, receipt.Record.Revision, nil, func(context.Context) error { return nil }); e != nil {
		t.Fatal(e)
	}
	pending, e := store.Pending(ctx, 10)
	if e != nil || len(pending) != 1 {
		t.Fatal(pending, e)
	}
	cont, e := store.MarkIssued(ctx, pending[0], func(context.Context) error { return nil })
	if e != nil {
		t.Fatal(e)
	}
	// This original actor is already executing the isolated fixture request.
	// Adopt its exact request through the same production MQ admission transaction.
	c.Session = parent
	c.RequestID = cont.RequestID
	c.ThreadID = parent.ResumeID()
	c.TurnID = "synthetic-pipeline-native-turn"
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	if !parent.SubmitNamed(c.RequestID, func() { close(started); <-release }) {
		t.Fatal("fixture actor start refused")
	}
	<-started
	c.Validate = func() bool {
		return parent.ActiveQueuedID() == c.RequestID && h.nativeCorrelationKnown(parent, c.ThreadID, c.RequestID, c.TurnID)
	}
	cfg := session.ConfigurationFrom(parent.SettingsSnapshot())
	payload, _ := json.Marshal(queuedPayload{Content: action.Instruction, Configuration: &cfg, PipelineID: cont.ID})
	if _, _, e = h.messageQueue.Enqueue(messagequeue.Entry{SessionID: parent.ID, RequestID: c.RequestID, Content: action.Instruction, Payload: payload, Pipeline: &p.Admission{ID: cont.ID, Fence: cont.Fence, Check: func(context.Context) error { return nil }}}); e != nil {
		t.Fatal(e)
	}
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: parent.ID, RequestID: c.RequestID, ThreadID: c.ThreadID, TurnID: c.TurnID})
	current, _, _, _ := store.Get(ctx, scope, receipt.Record.ID)
	input := map[string]any{"manager": manager, "id": cont.ID, "expected_revision": current.Revision, "checkpoint": p.Checkpoint{Revision: current.Revision, StepID: "one", Processed: []string{}, ProgressHash: "not a native completion", Outcome: "progress", Blockers: []string{}, RequestID: c.RequestID}}
	reqMap := pipelineRequest(t, "continuation_ack", "r_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", input)
	raw, _ := json.Marshal(reqMap)
	var req taskapi.Request
	json.Unmarshal(raw, &req)
	c.CallID = "original-ack-call"
	response := h.ExecuteTask(ctx, c, raw)
	if !response.OK {
		t.Fatal(response.Error)
	}
	received := response.Result.(p.Receipt)
	if received.Provenance == nil || received.Provenance.ToolCallID != c.CallID || received.Provenance.NativeTurnID != c.TurnID || received.Provenance.RequestID != c.RequestID || received.Provenance.Source != manager {
		t.Fatal(received)
	}
	// This is the same original operation, not a second ACK effect.
	response = h.ExecuteTask(ctx, c, raw)
	if !response.OK {
		t.Fatal(response.Error)
	}
	read := pipelineRequest(t, "continuation_receipt_get", "", map[string]any{"manager": manager, "id": cont.ID, "original_intent_id": req.IdempotencyKey, "original_operation": req.Operation, "expected_input_hash": unifiedHash(req)})
	readRaw, _ := json.Marshal(read)
	c.CallID = "original-read-call"
	response = h.ExecuteTask(ctx, c, readRaw)
	if !response.OK {
		t.Fatal(response.Error)
	}
	recovered := response.Result.(p.Receipt)
	if recovered.Provenance.ToolCallID != "original-ack-call" || recovered.InputHash != unifiedHash(req) || len(recovered.Continuation.NativeOperations) != 1 {
		t.Fatal(recovered)
	}
	// The actual authenticated paired HTTP parent reads the SAME persisted
	// native ACK, without an unpublished storage prefix or forged provider caller.
	httpReq := httptest.NewRequest("POST", "/api/jarvis/v1/task-api", strings.NewReader(string(readRaw)))
	httpReq.Header.Set("Authorization", "Bearer jarvis-token")
	httpReq.Header.Set("X-Bridge-Device-ID", "phone")
	httpRes := httptest.NewRecorder()
	h.ServeJarvisAPI(httpRes, httpReq)
	paired := decodePipeline(t, httpRes.Body.Bytes())
	if !paired.OK {
		t.Fatal(httpRes.Code, paired.Error)
	}
	var viaHTTP p.Receipt
	data, _ := json.Marshal(paired.Result)
	json.Unmarshal(data, &viaHTTP)
	if viaHTTP.InputHash != unifiedHash(req) || viaHTTP.Provenance == nil || viaHTTP.Provenance.ToolCallID != "original-ack-call" {
		t.Fatal(viaHTTP)
	}
	c.ThreadID = "foreign-source"
	if response = h.ExecuteTask(ctx, c, readRaw); response.OK {
		t.Fatal("foreign source recovered native proof")
	}
}

// Real production cross-target chain: paired parent definition -> original
// retained delegation writer -> actual child actor/provider tuple -> native ACK
// -> original paired parent HTTP receipt. No request/round IDs are conflated.
func TestPipelinePairedRootDelegationChildNativeAckOriginalReadback(t *testing.T) {
	h, parent, _, first, call := initializedUnifiedDelegation(t)
	ctx := context.Background()
	child, _ := h.registry.Get(first.ChildSessionID)
	manager := pipelineIdentity(h.unifiedIdentity(parent.ID))
	existing, e := h.dispatches.Grant(ctx, parent.ID)
	if e != nil {
		t.Fatal(e)
	}
	allowed, e := h.dispatches.SetGrant(ctx, parent.ID, sessiondispatch.Grant{Enabled: true, Local: true, Sessions: []string{h.cfg.InstanceID + ":" + child.ID}}, existing.Revision)
	if e != nil {
		t.Fatal(e)
	}
	if !allowed.Allows(h.cfg.InstanceID, h.cfg.InstanceID, child.ID) || allowed.Allows(h.cfg.InstanceID, h.cfg.InstanceID, "unrelated-child") {
		t.Fatal("isolated exact-child authority fixture invalid", allowed)
	}
	target := pipelineIdentity(h.unifiedIdentity(child.ID))
	childRequest := make(chan string, 1)
	release := make(chan struct{})
	defer close(release)
	// Keep the original provider's immutable R1 correlation oracle; replacing
	// the executor would correctly make the old initialized proof unavailable.
	original := h.exec.(*extensionExec)
	original.onSend = func(s *session.Session, req, content string) {
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: req, ThreadID: s.ResumeID(), TurnID: "pipeline-child-turn"})
		childRequest <- req
		<-release
		h.Emit(protocol.NewDone(s.ID, req))
	}
	action := &p.Action{Kind: "delegation_continue", Target: target, OriginalTask: &p.Locator{Kind: "delegation", Authority: h.cfg.InstanceID, NativeID: first.ID, RoundID: first.ID, Session: child.ID, Request: first.ChildRequestID, Thread: child.ResumeID(), Revision: target.Revision}, Instruction: "original bounded child ACK only", Disclosure: "references_only"}
	action.InstructionHash = taskapi.RawDigest([]byte(action.Instruction))
	mission := p.Mission{Goal: "cross-target actual native receipt", Completion: "separate actual proof", Steps: []p.Step{{ID: "handoff", Title: "handoff", Required: true, Predicate: p.Predicate{Kind: "success_sealed"}, Action: action}}, Authorization: p.Authorization{Confirmed: true, SourceRef: "synthetic-original-human", Budget: p.Budget{DeadlineMS: time.Now().Add(time.Hour).UnixMilli(), MaxEffects: 1, MaxModelTurns: 1, MaxNoProgress: 1}}}
	response := decodePipeline(t, call("POST", "task-api", pipelineRequest(t, "mission_create", "r_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", map[string]any{"manager": manager, "mission": mission}), "phone").Body.Bytes())
	if !response.OK {
		t.Fatal(response.Error)
	}
	raw, _ := json.Marshal(response.Result)
	var created p.Receipt
	json.Unmarshal(raw, &created)
	h.reconcileTaskPipelines(ctx)
	if e := h.reconcileDelegations(ctx); e != nil {
		t.Fatal(e)
	}
	var actual string
	select {
	case actual = <-childRequest:
	case <-time.After(3 * time.Second):
		pending, _ := h.messageQueue.PipelineStore().Pending(ctx, 10)
		t.Fatal("original child writer not started", pending)
	}
	scope := p.Scope{Principal: "device:phone", Generation: jarvisTokenGeneration("initialized-owned-paired-token"), Manager: manager}
	current, _, found, e := h.messageQueue.PipelineStore().Get(ctx, scope, created.Record.ID)
	if e != nil || !found {
		t.Fatal(current, found, e)
	}
	pending, e := h.messageQueue.PipelineStore().Pending(ctx, 10)
	if e != nil || len(pending) != 1 {
		t.Fatal(pending, e)
	}
	cont := pending[0]
	if cont.RequestID == actual || cont.OriginalReceipt == nil || cont.OriginalReceipt.Request != actual || cont.OriginalReceipt.NativeID == first.ID {
		t.Fatal("management/actual child request conflated", cont, actual)
	}
	caller := backend.TaskCaller{Session: child, RequestID: actual, ThreadID: child.ResumeID(), TurnID: "pipeline-child-turn", CallID: "pipeline-original-child-ack", ProviderVersion: "0.162.1", Validate: func() bool { return h.nativeCorrelationKnown(child, child.ResumeID(), actual, "pipeline-child-turn") }}
	ackInput := map[string]any{"manager": target, "id": cont.ID, "expected_revision": current.Revision, "checkpoint": p.Checkpoint{Revision: current.Revision, StepID: "handoff", Processed: []string{}, ProgressHash: "processing only", Outcome: "progress", Blockers: []string{}, RequestID: actual}}
	request := pipelineRequest(t, "continuation_ack", "r_cccccccccccccccccccccccccccccccc", ackInput)
	raw, _ = json.Marshal(request)
	var typed taskapi.Request
	json.Unmarshal(raw, &typed)
	response = h.ExecuteTask(ctx, caller, raw)
	if !response.OK {
		t.Fatal("actual child native ACK failed", response.Error)
	}
	parentRead := pipelineRequest(t, "continuation_receipt_get", "", map[string]any{"manager": manager, "id": cont.ID, "original_intent_id": typed.IdempotencyKey, "original_operation": "continuation_ack", "expected_input_hash": unifiedHash(typed)})
	response = decodePipeline(t, call("POST", "task-api", parentRead, "phone").Body.Bytes())
	if !response.OK {
		t.Fatal("original parent HTTP read failed", response.Error)
	}
	raw, _ = json.Marshal(response.Result)
	var received p.Receipt
	json.Unmarshal(raw, &received)
	if received.Provenance == nil || received.Provenance.Source != target || received.Provenance.RequestID != actual || received.Record.Manager != manager || received.Provenance.ToolCallID != caller.CallID {
		t.Fatal(received)
	}
	ackInput["manager"] = manager
	wrong := pipelineRequest(t, "continuation_ack", "r_dddddddddddddddddddddddddddddddd", ackInput)
	raw, _ = json.Marshal(wrong)
	if response = h.ExecuteTask(ctx, caller, raw); response.OK {
		t.Fatal("child impersonated root manager")
	}
}
