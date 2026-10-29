package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/controller-results-v9"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	cr "everything-go/internal/controllerresults"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	"everything-go/internal/taskpipeline"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func resultsRequest(op, key string, input any) map[string]any {
	v := map[string]any{"type": "task_api_controller_results_request", "api_version": contract.Version, "correlation_id": "synthetic-crd-" + op, "operation": op, "input": input}
	if key != "" {
		v["idempotency_key"] = key
	}
	return v
}
func resultsDecode(t *testing.T, b []byte) taskapi.Response {
	t.Helper()
	if e := contract.ValidateResponse(b); e != nil {
		t.Fatal(e, string(b))
	}
	var r taskapi.Response
	if e := json.Unmarshal(b, &r); e != nil {
		t.Fatal(e)
	}
	return r
}
func seedControllerResult(t *testing.T, h *Hub, parent, target *session.Session, id, execution string, steer bool) sessiondispatch.Record {
	t.Helper()
	ctx := context.Background()
	cfg := session.ConfigurationFrom(target.SettingsSnapshot())
	payload, _ := json.Marshal(queuedPayload{Content: "synthetic-original-command", Configuration: &cfg})
	_, inserted, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: target.ID, RequestID: execution, Payload: payload})
	if e != nil {
		t.Fatal(e)
	}
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: target.ID, RequestID: execution, ThreadID: target.ResumeID(), TurnID: "native-" + execution})
	if inserted {
		if _, _, e = h.messageQueue.Transition(target.ID, execution, []messagequeue.State{messagequeue.Queued}, messagequeue.Running, "", "", ""); e != nil {
			t.Fatal(e)
		}
		h.Emit(backend.NativeTaskStarted{SessionID: target.ID, RequestID: execution, Provider: "codex", ThreadID: target.ResumeID(), Token: "native-" + execution, TokenKind: "turn_id", ProofKind: "live_turn_start_in_progress", ObservedAt: time.Now()})
	}
	_, _, e = h.messageQueue.Transition(target.ID, execution, []messagequeue.State{messagequeue.Running}, messagequeue.Completed, "", "", "")
	if e != nil {
		t.Fatal(e)
	}
	request := execution
	mode := "queue"
	if steer {
		request = "command-" + id
		mode = "steer"
		if _, _, e = h.messageQueue.Enqueue(messagequeue.Entry{SessionID: target.ID, RequestID: request, Payload: []byte("supplement")}); e != nil {
			t.Fatal(e)
		}
		if _, _, e = h.messageQueue.Transition(target.ID, request, []messagequeue.State{messagequeue.Queued}, messagequeue.Steered, "", execution, "native-"+execution, true); e != nil {
			t.Fatal(e)
		}
	}
	body := "同文字合成final <>&繁體中文"
	hash := jarvisTextHash(body)
	anchor, _ := json.Marshal(map[string]any{"instance_id": h.cfg.InstanceID, "session_id": target.ID, "request_id": execution, "source_message_id": "msg-" + execution, "execution": map[string]any{"backend": "codex", "native_thread_id": target.ResumeID(), "native_turn_id": "native-" + execution}})
	if _, e = h.messageQueue.SealTask(ctx, messagequeue.TaskSeal{ID: "seal_" + hash, SessionID: target.ID, RequestID: execution, Hash: hash, Text: body, Anchor: anchor}); e != nil {
		t.Fatal(e)
	}
	r := sessiondispatch.Record{ID: id, ParentID: parent.ID, ParentThreadID: parent.ResumeID(), ParentConfigKnown: true, ParentConfigRevision: parent.SettingsSnapshot().ConfigRevision, OriginRequestID: "origin-" + id, ToolCallID: "call-" + id, InstanceID: h.cfg.InstanceID, SessionID: target.ID, ThreadID: target.ResumeID(), ConfigRevision: target.SettingsSnapshot().ConfigRevision, RequestID: request, ExecutionRequestID: execution, ExecutionTurnID: "native-" + execution, Mode: mode, State: "completed", Result: body, IntentHash: taskapi.RawDigest([]byte(id)), DeliveryState: "pending"}
	if _, e = h.dispatches.Create(ctx, r); e != nil {
		t.Fatal(e)
	}
	return r
}
func TestControllerResultsProductionBusyPersistThenOneOriginalBatch(t *testing.T) {
	controllerResultsBusyBatchCoverage(t, 26)
}
func TestControllerResultsHistorical24Queue48SteerIncludingSteerOnly(t *testing.T) {
	controllerResultsBusyBatchCoverage(t, 24)
}
func controllerResultsBusyBatchCoverage(t *testing.T, base int) {
	h, call, parent, target := unifiedFixtureSessions(t)
	fe := h.exec.(*scopeFixtureExecutor).fakeExec
	useNativeCorrelationFixture(h, fe, target.ResumeID(), "execution-0", "native-execution-0")
	for i := 0; i < 72; i++ {
		seedControllerResult(t, h, parent, target, fmt.Sprint("d", i), fmt.Sprint("execution-", i%26), i >= base)
	}
	var starts atomic.Int32
	fe.onSend = func(s *session.Session, id, text string) {
		if s.ID == parent.ID {
			starts.Add(1)
			if !strings.HasPrefix(id, "screturn_batch_") {
				t.Error("v1 fanout", id)
			}
			if strings.Contains(text, "同文字") {
				t.Error("body interpolation")
			}
		}
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: id, ThreadID: s.ResumeID(), TurnID: "batch-native"})
		body := "synthetic parent processing final"
		anchor, _ := json.Marshal(map[string]any{"instance_id": h.cfg.InstanceID, "session_id": s.ID, "request_id": id, "source_message_id": "parent-final", "execution": map[string]any{"backend": "codex", "native_thread_id": s.ResumeID(), "native_turn_id": "batch-native"}})
		if _, e := h.messageQueue.SealTask(context.Background(), messagequeue.TaskSeal{ID: "seal_" + jarvisTextHash(body), SessionID: s.ID, RequestID: id, Hash: jarvisTextHash(body), Text: body, Anchor: anchor}); e != nil {
			t.Error(e)
		}
		fe.sink.Emit(protocol.NewDone(s.ID, id))
	}
	h.Emit(backend.ObservedTurn{SessionID: parent.ID, RequestID: "busy-native", Phase: "running"})
	h.reconcileSessionDispatches(context.Background())
	manager := cr.Identity{h.cfg.InstanceID, parent.ID, parent.ResumeID(), 0}
	req := resultsRequest("read_dispatch_inbox", "", map[string]any{"manager": manager, "cursor": "", "limit": 50})
	resp := resultsDecode(t, call("POST", "task-api", req, "phone").Body.Bytes())
	if !resp.OK {
		t.Fatal(resp.Error)
	}
	raw, _ := json.Marshal(resp.Result)
	var page cr.Page
	json.Unmarshal(raw, &page)
	if len(page.Groups) != 26 || page.Unread != 26 || starts.Load() != 0 {
		t.Fatal(page, starts.Load())
	}
	for _, g := range page.Groups {
		if g.State != "final" || g.MemberCount < 2 {
			t.Fatal(g)
		}
	}
	enable := resultsRequest("set_dispatch_return_control", "r_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", map[string]any{"manager": manager, "expected_revision": 0, "mode": "resume_authorized", "paused": false, "deadline_ms": time.Now().Add(time.Hour).UnixMilli(), "max_batches": 1, "max_no_progress": 1})
	if r := resultsDecode(t, call("POST", "task-api", enable, "phone").Body.Bytes()); !r.OK {
		t.Fatal(r.Error)
	}
	h.reconcileSessionDispatches(context.Background())
	if starts.Load() != 0 {
		t.Fatal("busy woke")
	}
	h.Emit(backend.ObservedTurn{SessionID: parent.ID, RequestID: "busy-native", Phase: "completed"})
	h.reconcileSessionDispatches(context.Background())
	deadline := time.Now().Add(2 * time.Second)
	for starts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if starts.Load() != 1 {
		t.Fatal(starts.Load())
	}
	h.reconcileSessionDispatches(context.Background())
	if starts.Load() != 1 {
		t.Fatal("replayed")
	}
	for time.Now().Before(deadline) {
		h.reconcileSessionDispatches(context.Background())
		r, _, _ := h.dispatches.Get(context.Background(), "d0")
		if r.DeliveryState == "delivered" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < 72; i++ {
		r, found, e := h.dispatches.Get(context.Background(), fmt.Sprint("d", i))
		if e != nil || !found || r.DeliveryState != "delivered" {
			status, _ := h.dispatches.Results().Status(context.Background(), cr.Scope{Parent: manager, Generation: parent.Snapshot().SourceRevision})
			for _, b := range status.Batches {
				entry, found, err := h.messageQueue.Get(parent.ID, b.Request)
				_, sealErr := h.jarvisResultSeal(context.Background(), parent, b.Request, "")
				t.Log("batch", b.State, "mq", entry.State, found, err, "payloadmatch", h.controllerResultEntryMatches(cr.Scope{Parent: manager}, b, entry), "seal", sealErr, "actualSyntheticPayload", string(entry.Payload), "intentHash", entry.PayloadHash)
			}
			t.Fatal("coverage missing", i, r, e)
		}
	}
	for i := 0; i < 72; i++ {
		original, _, _ := h.dispatches.Get(context.Background(), fmt.Sprint("d", i))
		state, anchor := h.apiDelivery(context.Background(), taskapi.IntentRecord{Path: "controller", NativeID: original.ID, SessionID: original.SessionID, RequestID: original.RequestID})
		if state != "delivered" || anchor == nil {
			t.Fatalf("actual TaskAPI member %d delivery=%s anchor=%v", i, state, anchor)
		}
	}
	pageResult := resultsDecode(t, call("POST", "task-api", req, "phone").Body.Bytes())
	b, _ := json.Marshal(pageResult.Result)
	json.Unmarshal(b, &page)
	for _, g := range page.Groups {
		if !g.NativeConsumed || g.Acknowledged || g.MissionApplied {
			t.Fatal("delivery confused with processing", g)
		}
	}
}
func TestControllerResultsActualHTTPWireAndACL(t *testing.T) {
	h, call, parent, target := unifiedFixtureSessions(t)
	fe := h.exec.(*scopeFixtureExecutor).fakeExec
	useNativeCorrelationFixture(h, fe, target.ResumeID(), "original-execution", "native-original-execution")
	record := seedControllerResult(t, h, parent, target, "dispatch-original", "original-execution", false)
	seedControllerResult(t, h, parent, target, "dispatch-alias", "original-execution", true)
	h.reconcileSessionDispatches(context.Background())
	manager := cr.Identity{h.cfg.InstanceID, parent.ID, parent.ResumeID(), 0}
	entries := []any{}
	directory := os.Getenv("CONTROLLER_RESULTS_WIRE_OUT")
	invoke := func(name, op, key string, input any) taskapi.Response {
		request := resultsRequest(op, key, input)
		raw, _ := json.Marshal(request)
		if e := contract.Validate(raw); e != nil {
			t.Fatal(e)
		}
		w := call("POST", "task-api", request, "phone")
		r := resultsDecode(t, w.Body.Bytes())
		if directory != "" {
			requestPath := name + ".request.json"
			responsePath := name + ".response.json"
			if e := os.WriteFile(filepath.Join(directory, requestPath), raw, 0600); e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(filepath.Join(directory, responsePath), w.Body.Bytes(), 0600); e != nil {
				t.Fatal(e)
			}
			entries = append(entries, map[string]any{"name": name, "operation": op, "request_path": requestPath, "request_sha256": taskapi.RawDigest(raw), "request_bytes": len(raw), "response_path": responsePath, "response_sha256": taskapi.RawDigest(w.Body.Bytes()), "response_bytes": w.Body.Len(), "http_status": w.Code, "ok": r.OK, "expected_schema_valid": true})
		}
		return r
	}
	if r := invoke("capabilities", "capabilities", "", map[string]any{}); !r.OK {
		t.Fatal(r.Error)
	}
	r := invoke("inbox", "read_dispatch_inbox", "", map[string]any{"manager": manager, "cursor": "", "limit": 20})
	if !r.OK {
		t.Fatal(r.Error)
	}
	b, _ := json.Marshal(r.Result)
	var p cr.Page
	json.Unmarshal(b, &p)
	if len(p.Groups) != 1 || p.Groups[0].MemberCount != 2 {
		t.Fatal(p)
	}
	g := p.Groups[0]
	if r = invoke("members", "read_dispatch_group", "", map[string]any{"manager": manager, "group_id": g.ID, "cursor": "", "limit": 20}); !r.OK {
		t.Fatal(r.Error)
	}
	if r = invoke("full-original-final", "read_dispatch_final", "", map[string]any{"manager": manager, "group_id": g.ID, "seal_id": g.Final.SealID, "expected_hash": g.Final.Hash, "offset": 0, "limit": 49152}); !r.OK {
		t.Fatal(r.Error)
	}
	if r = invoke("paired-no-native-ack", "ack_dispatch_results", "r_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", map[string]any{"manager": manager, "read_receipt_id": p.ReadReceipt.ID, "versions": p.ReadReceipt.Versions}); r.OK {
		t.Fatal("paired ACK")
	}
	seenInput := map[string]any{"manager": manager, "read_receipt_id": p.ReadReceipt.ID, "versions": p.ReadReceipt.Versions}
	if r = invoke("human-seen", "mark_dispatch_results_seen", "r_cccccccccccccccccccccccccccccccc", seenInput); !r.OK {
		t.Fatal(r.Error)
	}
	hash := unifiedHash(taskapi.Request{Operation: "mark_dispatch_results_seen", Input: mustPipelineJSON(seenInput)})
	if r = invoke("seen-original-lookup", "operation_get", "", map[string]any{"manager": manager, "original_intent_id": "r_cccccccccccccccccccccccccccccccc", "expected_input_hash": hash}); !r.OK {
		t.Fatal(r.Error)
	}
	invoke("events", "dispatch_result_events", "", map[string]any{"manager": manager, "after_sequence": 0, "limit": 100})
	invoke("status", "get_dispatch_return_status", "", map[string]any{"manager": manager})
	controlInput := map[string]any{"manager": manager, "expected_revision": 0, "mode": "notify_only", "paused": true, "deadline_ms": 0, "max_batches": 0, "max_no_progress": 0}
	if r = invoke("control-pause", "set_dispatch_return_control", "r_11111111111111111111111111111111", controlInput); !r.OK {
		t.Fatal(r.Error)
	}
	controlHash := unifiedHash(taskapi.Request{Operation: "set_dispatch_return_control", Input: mustPipelineJSON(controlInput)})
	if r = invoke("control-original-lookup", "operation_get", "", map[string]any{"manager": manager, "original_intent_id": "r_11111111111111111111111111111111", "expected_input_hash": controlHash}); !r.OK {
		t.Fatal(r.Error)
	}

	wrong := manager
	wrong.Thread = "foreign"
	if r = invoke("wrong-parent-generation", "read_dispatch_inbox", "", map[string]any{"manager": wrong, "cursor": "", "limit": 20}); r.OK {
		t.Fatal("foreign")
	}
	grant, _ := h.dispatches.Grant(context.Background(), parent.ID)
	h.dispatches.SetGrant(context.Background(), parent.ID, sessiondispatch.Grant{}, grant.Revision)
	if r = invoke("grant-revoked", "read_dispatch_inbox", "", map[string]any{"manager": manager, "cursor": "", "limit": 20}); r.OK {
		t.Fatal("revoked")
	}
	if record.RequestID != "original-execution" {
		t.Fatal(record)
	}
	if directory != "" {
		manifest := map[string]any{"schema": 1, "source_qualified": false, "schema_hash": contract.Hash(), "effects": "isolated synthetic provider/SQLite/paired HTTP only; no live/native/model", "entries": entries}
		if e := os.WriteFile(filepath.Join(directory, "manifest.json"), mustPipelineJSON(manifest), 0600); e != nil {
			t.Fatal(e)
		}
	}
}

func TestControllerResultsActualNativeReadACKOriginalReceipt(t *testing.T) {
	h, call, parent, target := unifiedFixtureSessions(t)
	fe := h.exec.(*scopeFixtureExecutor).fakeExec
	useNativeCorrelationFixture(h, fe, target.ResumeID(), "producer", "native-producer")
	seedControllerResult(t, h, parent, target, "primary", "producer", false)
	h.reconcileSessionDispatches(context.Background())
	ctx := context.Background()
	request := "original-parent-active"
	turn := "parent-native-turn"
	cfg := session.ConfigurationFrom(parent.SettingsSnapshot())
	payload, _ := json.Marshal(queuedPayload{Content: "bounded synthetic parent", Configuration: &cfg})
	if _, _, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: parent.ID, RequestID: request, Payload: payload}); e != nil {
		t.Fatal(e)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if !parent.SubmitNamed(request, func() { close(started); <-release }) {
		t.Fatal("actor")
	}
	<-started
	defer close(release)
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: parent.ID, RequestID: request, ThreadID: parent.ResumeID(), TurnID: turn})
	caller := backend.TaskCaller{Session: parent, RequestID: request, ThreadID: parent.ResumeID(), TurnID: turn, CallID: "read-original-inbox", ProviderVersion: "0.162.1"}
	caller.Validate = func() bool { return h.nativeCorrelationKnown(parent, caller.ThreadID, caller.RequestID, caller.TurnID) }
	manager := cr.Identity{h.cfg.InstanceID, parent.ID, parent.ResumeID(), parent.SettingsSnapshot().ConfigRevision}
	raw := mustPipelineJSON(resultsRequest("read_dispatch_inbox", "", map[string]any{"manager": manager, "cursor": "", "limit": 20}))
	r := h.ExecuteTask(ctx, caller, raw)
	if !r.OK {
		t.Fatal(r.Error)
	}
	b, _ := json.Marshal(r.Result)
	var p cr.Page
	json.Unmarshal(b, &p)
	if len(p.Groups) != 1 || !p.ReadReceipt.Native || !p.Groups[0].Unread {
		t.Fatal(p)
	}
	input := map[string]any{"manager": manager, "read_receipt_id": p.ReadReceipt.ID, "versions": p.ReadReceipt.Versions}
	caller.CallID = "actual-native-ack-call"
	raw = mustPipelineJSON(resultsRequest("ack_dispatch_results", "r_dddddddddddddddddddddddddddddddd", input))
	r = h.ExecuteTask(ctx, caller, raw)
	if !r.OK {
		t.Fatal(r.Error)
	}
	wire, _ := json.Marshal(r)
	if e := contract.ValidateResponse(wire); e != nil {
		t.Fatal(e, string(wire))
	}
	receipt := r.Result.(cr.Receipt)
	if receipt.NativeProvenance == nil || receipt.NativeProvenance.CallID != caller.CallID || receipt.NativeProvenance.Request != request || receipt.NativeProvenance.Turn != turn || receipt.NativeProvenance.Source != manager {
		t.Fatal(receipt)
	}
	if directory := os.Getenv("CONTROLLER_RESULTS_NATIVE_OUT"); directory != "" {
		requestBytes := append([]byte{}, raw...)
		if e := os.WriteFile(filepath.Join(directory, "native-ack.request.json"), requestBytes, 0600); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(filepath.Join(directory, "native-ack.response.json"), wire, 0600); e != nil {
			t.Fatal(e)
		}
	}
	parentRead := resultsRequest("operation_get", "", map[string]any{"manager": manager, "original_intent_id": receipt.Intent, "expected_input_hash": receipt.InputHash})
	parentResponse := call("POST", "task-api", parentRead, "phone").Body.Bytes()
	look := resultsDecode(t, parentResponse)
	if directory := os.Getenv("CONTROLLER_RESULTS_NATIVE_OUT"); directory != "" {
		if e := os.WriteFile(filepath.Join(directory, "parent-original-lookup.request.json"), mustPipelineJSON(parentRead), 0600); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(filepath.Join(directory, "parent-original-lookup.response.json"), parentResponse, 0600); e != nil {
			t.Fatal(e)
		}
	}

	gotRaw, _ := json.Marshal(look.Result)
	var got cr.Receipt
	_ = json.Unmarshal(gotRaw, &got)
	if !look.OK || cr.Hash(got) != cr.Hash(receipt) {
		t.Fatal(look)
	}
	// A wrong native turn cannot borrow the saved read receipt or original key.
	caller.TurnID = "foreign"
	r = h.ExecuteTask(ctx, caller, raw)
	if r.OK {
		t.Fatal("foreign native ack")
	}
}

func TestControllerResultsSnapshotSubscribeCommitAndRetire(t *testing.T) {
	h, _, parent, target := unifiedFixtureSessions(t)
	fe := h.exec.(*scopeFixtureExecutor).fakeExec
	useNativeCorrelationFixture(h, fe, target.ResumeID(), "before", "native-before")
	seedControllerResult(t, h, parent, target, "before", "before", false)
	h.reconcileSessionDispatches(context.Background())
	c := sharedReadClient(t, h, "passive-phone")
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	defer close(c.quit)
	manager := cr.Identity{h.cfg.InstanceID, parent.ID, parent.ResumeID(), 0}
	request := mustPipelineJSON(resultsRequest("read_dispatch_inbox", "", map[string]any{"manager": manager, "cursor": "", "limit": 20}))
	caller, e := h.taskHuman(c)
	if e != nil {
		t.Fatal(e)
	}
	response, handled := h.executeControllerResults(context.Background(), caller, request)
	if !handled || !response.OK {
		t.Fatal(response)
	}
	// Exact race: a commit occurs AFTER the returned snapshot and BEFORE subscribe.
	second := seedControllerResult(t, h, parent, target, "between", "between", false)
	scope, ok := h.observeControllerResult(context.Background(), second)
	if !ok {
		t.Fatal("observation")
	}
	h.subscribeControllerResults(context.Background(), c, request, response)
	h.publishControllerResultInvalidation(context.Background(), scope)
	event := waitForType(t, c, "controller_results_invalidated")
	raw, _ := json.Marshal(event)
	if strings.Contains(string(raw), "同文字") || strings.Contains(string(raw), "group_id") {
		t.Fatal("body/membership broadcast", string(raw))
	}
	var page cr.Page
	b, _ := json.Marshal(response.Result)
	json.Unmarshal(b, &page)
	if event["high_water"].(float64) <= float64(page.HighWater) {
		t.Fatal("snapshot-subscribe event lost", event)
	}
	grant, _ := h.dispatches.Grant(context.Background(), parent.ID)
	h.dispatches.SetGrant(context.Background(), parent.ID, sessiondispatch.Grant{}, grant.Revision)
	third := seedControllerResult(t, h, parent, target, "revoked", "revoked", false)
	h.observeControllerResult(context.Background(), third)
	h.publishControllerResultInvalidation(context.Background(), scope)
	retired := waitForType(t, c, "controller_results_retired")
	rawRetired, _ := json.Marshal(retired)
	if strings.Contains(string(rawRetired), "high_water") || strings.Contains(string(rawRetired), "group_id") || retired["scope_hash"] != scope.Key() {
		t.Fatal(retired)
	}
	if _, exists := c.controllerResultSubscriptions.Load(scope.Key()); exists {
		t.Fatal("revoked subscription retained")
	}
}

func TestControllerResultsOriginalMQAcceptedBeforeOutboxACKAndUnknown(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint(unknown), func(t *testing.T) {
			h, _, parent, target := unifiedFixtureSessions(t)
			fe := h.exec.(*scopeFixtureExecutor).fakeExec
			useNativeCorrelationFixture(h, fe, target.ResumeID(), "producer", "native-producer")
			seedControllerResult(t, h, parent, target, "primary", "producer", false)
			h.reconcileSessionDispatches(context.Background())
			g, _ := h.dispatches.Grant(context.Background(), parent.ID)
			scope := cr.Scope{Parent: cr.Identity{h.cfg.InstanceID, parent.ID, parent.ResumeID(), 0}, Generation: parent.Snapshot().SourceRevision, GrantRevision: g.Revision}
			st := h.dispatches.Results()
			ctx := context.Background()
			if _, e := st.SetControl(ctx, scope, 0, cr.Control{Mode: "resume_authorized", Deadline: time.Now().Add(time.Hour).UnixMilli(), MaxBatches: 1, MaxNoProgress: 1}, "enable", "hash", func() error { return nil }); e != nil {
				t.Fatal(e)
			}
			batch, _, e := st.Freeze(ctx, scope, func() error { return nil })
			if e != nil || batch.ID == "" {
				t.Fatal(batch, e)
			}
			var starts atomic.Int32
			fe.onSend = func(s *session.Session, id, text string) {
				starts.Add(1)
				emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: id, ThreadID: s.ResumeID(), TurnID: "original-batch-native"})
				fe.sink.Emit(protocol.NewDone(s.ID, id))
			}
			h.Emit(backend.ObservedTurn{SessionID: parent.ID, RequestID: "previous", Phase: "completed"})
			if unknown {
				if e = st.SetBatchState(ctx, batch.ID, "unknown", ""); e != nil {
					t.Fatal(e)
				}
			} else {
				// Actual original MQ writer accepts. The simulated crash omits only the
				// dispatch-DB local acknowledgment; no new request or alternate writer.
				client := &Client{hub: h, deviceID: "controller-result-v2", controllerResultAdmission: &messagequeue.ControllerResultAdmission{Batch: batch.ID, Scope: scope.Key(), Session: parent.ID, Request: batch.Request}, send: make(chan []byte, 8), quit: make(chan struct{}), ctx: ctx}
				if !h.enqueueChatMessageExpected(client, clientproto.Command{Kind: "message", SessionID: parent.ID, RequestID: batch.Request, Content: batch.Payload, MessagePurpose: "result_return"}, &dispatchTargetExpectation{ThreadID: parent.ResumeID(), Revision: 0}) {
					t.Fatal("original writer")
				}
				deadline := time.Now().Add(time.Second)
				for starts.Load() == 0 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
			}
			// Reopen the ORIGINAL dispatch DB, with the MQ receipt kept independently.
			h.dispatches.Close()
			reopened, e := sessiondispatch.Open(h.cfg.DataDir)
			if e != nil {
				t.Fatal(e)
			}
			h.dispatches = reopened
			defer reopened.Close()
			h.reconcileControllerResultBatch(ctx, scope)
			h.reconcileControllerResultBatch(ctx, scope)
			status, e := reopened.Results().Status(ctx, scope)
			if e != nil || len(status.Batches) != 1 || status.Batches[0].Request != batch.Request || status.Batches[0].PayloadHash != batch.PayloadHash {
				t.Fatal(status, e)
			}
			expected := int32(1)
			if unknown {
				expected = 0
			}
			if starts.Load() != expected {
				t.Fatal("replayed unknown/original", starts.Load())
			}
		})
	}
}

func TestControllerResultsMissionOriginalCheckpointAndSoleWakeOwner(t *testing.T) {
	h, call, parent, target := unifiedFixtureSessions(t)
	fe := h.exec.(*scopeFixtureExecutor).fakeExec
	useNativeCorrelationFixture(h, fe, target.ResumeID(), "mission-producer", "native-mission-producer")
	seedControllerResult(t, h, parent, target, "mission-primary", "mission-producer", false)
	h.reconcileSessionDispatches(context.Background())
	ctx := context.Background()
	manager := pipelineIdentity(h.unifiedIdentity(parent.ID))
	source := taskpipeline.Locator{Kind: "controller", Authority: h.cfg.InstanceID, NativeID: "mission-primary", RoundID: "mission-producer", Session: target.ID, Request: "mission-producer", Thread: target.ResumeID(), Revision: 0}
	action := &taskpipeline.Action{Kind: "session_resume", Target: manager, Instruction: "Process the exact authorized controller result then checkpoint; no new work", Disclosure: "references_only"}
	action.InstructionHash = taskapi.RawDigest([]byte(action.Instruction))
	mission := taskpipeline.Mission{Goal: "Synthetic original mission result processing", Completion: "Exact original checkpoint, not ACK alone", Steps: []taskpipeline.Step{{ID: "required-result", Title: "original result", Required: true, Source: &source, Predicate: taskpipeline.Predicate{Kind: "success_sealed"}}, {ID: "later-human-step", Title: "Remaining authorized work", Required: true, Predicate: taskpipeline.Predicate{Kind: "success_sealed"}}}, RecoveryAction: action, Authorization: taskpipeline.Authorization{Confirmed: true, SourceRef: "synthetic-explicit-human", Budget: taskpipeline.Budget{DeadlineMS: time.Now().Add(time.Hour).UnixMilli(), MaxEffects: 1, MaxModelTurns: 1, MaxNoProgress: 1}}}
	r := decodePipeline(t, call("POST", "task-api", pipelineRequest(t, "mission_create", "r_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", map[string]any{"manager": manager, "mission": mission}), "phone").Body.Bytes())
	if !r.OK {
		t.Fatal(r.Error)
	}
	b, _ := json.Marshal(r.Result)
	var created taskpipeline.Receipt
	json.Unmarshal(b, &created)
	store := h.messageQueue.PipelineStore()
	issuer, record, e := store.Issuer(ctx, created.Record.ID)
	if e != nil {
		t.Fatal(e)
	}
	ready := make(chan string, 1)
	release := make(chan struct{})
	var starts atomic.Int32
	fe.onSend = func(s *session.Session, id, text string) {
		if s.ID != parent.ID {
			t.Error("unexpected actor")
		}
		starts.Add(1)
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: id, ThreadID: s.ResumeID(), TurnID: "mission-processing-native"})
		ready <- id
		<-release
		fe.sink.Emit(protocol.NewDone(s.ID, id))
	}
	defer func() { close(release); parent.CloseAndWait() }()
	h.reconcileTaskPipelines(ctx)
	var request string
	select {
	case request = <-ready:
	case <-time.After(2 * time.Second):
		pending, _ := store.Pending(ctx, 10)
		t.Fatal("original recovery was not admitted", pending)
	}
	grant, grantErr := h.dispatches.Grant(ctx, parent.ID)
	if grantErr != nil {
		t.Fatal(grantErr)
	}
	scope := cr.Scope{Parent: cr.Identity{manager.Authority, manager.Session, manager.Thread, manager.Revision}, Generation: parent.Snapshot().SourceRevision, GrantRevision: grant.Revision}
	h.reconcileControllerResults(ctx)
	status, _ := h.dispatches.Results().Status(ctx, scope)
	if len(status.Batches) != 0 || starts.Load() != 1 {
		t.Fatal("second wake owner", status, starts.Load())
	}
	page, e := h.dispatches.Results().Read(ctx, scope, cr.ReadProof{Principal: "paired"}, "", 20)
	if e != nil || len(page.Groups) != 1 || page.Groups[0].MissionApplied {
		t.Fatal("uncheckpointed source/receipt is not application", page, e)
	}
	current, _, _, e := store.Get(ctx, issuer.Scope, record.ID)
	if e != nil {
		t.Fatal(e)
	}
	events, _, _, e := store.Events(ctx, issuer.Scope, record.ID, 0, 100)
	if e != nil {
		t.Fatal(e)
	}
	processed := []string{}
	for _, event := range events {
		if event.State == "mission_applied" && event.Fact.Source.Request == source.Request {
			processed = append(processed, event.ID)
		}
	}
	if len(processed) != 1 {
		t.Fatal("exact source event", events)
	}
	cont, _, _, found, e := store.ExecutionContext(ctx, parent.ID, request)
	if e != nil || !found {
		t.Fatal(cont, e)
	}
	caller := backend.TaskCaller{Session: parent, RequestID: request, ThreadID: parent.ResumeID(), TurnID: "mission-processing-native", CallID: "mission-checkpoint-actual", ProviderVersion: "0.162.1"}
	caller.Validate = func() bool { return h.nativeCorrelationKnown(parent, caller.ThreadID, caller.RequestID, caller.TurnID) }
	input := map[string]any{"manager": manager, "id": cont.ID, "expected_revision": current.Revision, "checkpoint": taskpipeline.Checkpoint{Revision: current.Revision, StepID: "required-result", Processed: processed, ProgressHash: taskpipeline.ProgressHash(current), Outcome: "progress", Blockers: []string{}, RequestID: request}}
	native := h.ExecuteTask(ctx, caller, mustPipelineJSON(pipelineRequest(t, "continuation_ack", "r_ffffffffffffffffffffffffffffffff", input)))
	if !native.OK {
		t.Fatal(native.Error)
	}
	h.reconcileControllerResultApplications(ctx, scope)
	after, e := h.dispatches.Results().Read(ctx, scope, cr.ReadProof{Principal: "paired"}, "", 20)
	if e != nil || !after.Groups[0].MissionApplied || len(after.Groups[0].MissionApplications) != 1 || after.Groups[0].MissionApplications[0].ProcessingRequest != request || after.Groups[0].Acknowledged {
		t.Fatal(after, e)
	}
	// Explicit controller pause restricts this recovery evidence at the original
	// mission writer. Default (revision zero) notify-only did not block the mission.
	control, err := h.dispatches.Results().SetControl(ctx, scope, 0, cr.Control{Mode: "notify_only", Paused: true}, "explicit-pause", "pause", func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if control.Control.Revision != 1 {
		t.Fatal(control)
	}
	if h.controllerResultMissionFence(ctx, manager, issuer.SourceGeneration, []taskpipeline.Fact{events[0].Fact}) == nil {
		t.Fatal("explicit pause did not fence exact controller evidence")
	}
}
