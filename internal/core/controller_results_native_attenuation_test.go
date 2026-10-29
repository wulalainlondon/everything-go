package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/controller-results-v9"
	"everything-go/internal/backend"
	cr "everything-go/internal/controllerresults"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Real authenticated paired producer -> exact original native actor -> original
// MQ acceptance fence -> dispatch SQLite control writer -> both original lookups.
func TestControllerResultsNativeAttenuationOriginalHumanPolicy(t *testing.T) {
	h, call, parent, target := unifiedFixtureSessions(t)
	useNativeCorrelationFixture(h, h.exec.(*scopeFixtureExecutor).fakeExec, target.ResumeID(), "producer", "native-producer")
	seedControllerResult(t, h, parent, target, "pause-source", "producer", false)
	h.reconcileSessionDispatches(context.Background())
	ctx := context.Background()
	manager := cr.Identity{h.cfg.InstanceID, parent.ID, parent.ResumeID(), parent.SettingsSnapshot().ConfigRevision}
	deadline := time.Now().Add(time.Hour).UnixMilli()
	policy := map[string]any{"manager": manager, "expected_revision": 0, "mode": "resume_authorized", "paused": false, "deadline_ms": deadline, "max_batches": 2, "max_no_progress": 2}
	humanKey := "r_11111111111111111111111111111111"
	human := resultsDecode(t, call("POST", "task-api", resultsRequest("set_dispatch_return_control", humanKey, policy), "phone").Body.Bytes())
	if !human.OK {
		t.Fatal(human.Error)
	}
	humanRaw, _ := json.Marshal(human.Result)
	var humanReceipt cr.Receipt
	json.Unmarshal(humanRaw, &humanReceipt)
	request, turn := "original-parent-pause-request", "original-parent-pause-turn"
	cfg := session.ConfigurationFrom(parent.SettingsSnapshot())
	payload, _ := json.Marshal(queuedPayload{Content: "synthetic original parent", Configuration: &cfg})
	if _, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: parent.ID, RequestID: request, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	if !parent.SubmitNamed(request, func() { close(started); <-release }) {
		t.Fatal("actor")
	}
	<-started
	defer close(release)
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: parent.ID, RequestID: request, ThreadID: parent.ResumeID(), TurnID: turn})
	caller := backend.TaskCaller{Session: parent, RequestID: request, ThreadID: parent.ResumeID(), TurnID: turn, CallID: "native-pause-original", ProviderVersion: "0.162.1"}
	caller.Validate = func() bool { return h.nativeCorrelationKnown(parent, caller.ThreadID, caller.RequestID, caller.TurnID) }
	exports := map[string]any{}
	for _, op := range []string{"capabilities", "get_dispatch_return_status"} {
		input := map[string]any{}
		if op != "capabilities" {
			input["manager"] = manager
		}
		q := resultsRequest(op, "", input)
		r := h.ExecuteTask(ctx, caller, mustPipelineJSON(q))
		if !r.OK {
			t.Fatal(r.Error)
		}
		exports[op+".request.json"] = q
		exports[op+".response.json"] = r
	}
	nativePolicy := map[string]any{"manager": manager, "expected_revision": 1, "mode": "notify_only", "paused": true, "deadline_ms": deadline, "max_batches": 2, "max_no_progress": 2}
	for _, tc := range []struct {
		name, key string
		value     any
	}{{"resume", "mode", "resume_authorized"}, {"unpause", "paused", false}, {"deadline", "deadline_ms", deadline + 1}, {"budget", "max_batches", 3}, {"no_progress", "max_no_progress", 3}, {"create", "expected_revision", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			input := map[string]any{}
			for k, v := range nativePolicy {
				input[k] = v
			}
			input[tc.key] = tc.value
			key := "denied-" + tc.name
			q := resultsRequest("set_dispatch_return_control", key, input)
			res := h.ExecuteTask(ctx, caller, mustPipelineJSON(q))
			if res.OK {
				t.Fatal("native elevated policy", res)
			}
			var original taskapi.Request
			json.Unmarshal(mustPipelineJSON(q), &original)
			lookup := resultsDecode(t, call("POST", "task-api", resultsRequest("operation_get", "", map[string]any{"manager": manager, "original_intent_id": key, "expected_input_hash": unifiedHash(original)}), "phone").Body.Bytes())
			if lookup.OK {
				t.Fatal("denied operation persisted", lookup)
			}
		})
	}
	key := "r_22222222222222222222222222222222"
	q := resultsRequest("set_dispatch_return_control", key, nativePolicy)
	res := h.ExecuteTask(ctx, caller, mustPipelineJSON(q))
	if !res.OK {
		t.Fatal(res.Error)
	}
	exports["native-pause.request.json"] = q
	exports["native-pause.response.json"] = res
	wire, _ := json.Marshal(res)
	if err := contract.ValidateResponse(wire); err != nil {
		t.Fatal(err)
	}
	receipt := res.Result.(cr.Receipt)
	c := receipt.Control
	p := receipt.NativeProvenance
	if receipt.Intent != key || c == nil || c.SourceIntent != humanKey || c.Revision != 2 || c.Mode != "notify_only" || !c.Paused || c.Deadline != deadline || c.MaxBatches != 2 || c.MaxNoProgress != 2 || p == nil || p.Source != manager || p.Request != request || p.Turn != turn || p.CallID != caller.CallID || p.ReadReceipt != "" {
		t.Fatal(receipt)
	}
	for _, want := range []cr.Receipt{humanReceipt, receipt} {
		got := resultsDecode(t, call("POST", "task-api", resultsRequest("operation_get", "", map[string]any{"manager": manager, "original_intent_id": want.Intent, "expected_input_hash": want.InputHash}), "phone").Body.Bytes())
		raw, _ := json.Marshal(got.Result)
		var r cr.Receipt
		json.Unmarshal(raw, &r)
		if !got.OK || cr.Hash(r) != cr.Hash(want) {
			t.Fatal("original lookup changed", got, want)
		}
	}
	if out := os.Getenv("CONTROLLER_RESULTS_ATTENUATION_OUT"); out != "" {
		for name, v := range exports {
			if e := os.WriteFile(filepath.Join(out, name), mustPipelineJSON(v), 0600); e != nil {
				t.Fatal(e)
			}
		}
	}
	page := resultsDecode(t, call("POST", "task-api", resultsRequest("read_dispatch_inbox", "", map[string]any{"manager": manager, "cursor": "", "limit": 20}), "phone").Body.Bytes())
	pageRaw, _ := json.Marshal(page.Result)
	var after cr.Page
	json.Unmarshal(pageRaw, &after)
	if !page.OK || len(after.Groups) != 1 || after.Groups[0].NativeRead || after.Groups[0].Acknowledged || after.Groups[0].HumanSeen || after.Groups[0].NativeConsumed || after.Groups[0].MissionApplied {
		t.Fatal("pause is not read/ACK/seen/consumed/mission", after)
	}
	// A different native turn cannot borrow the paused policy or original key.
	caller.TurnID = "foreign"
	if h.ExecuteTask(ctx, caller, mustPipelineJSON(q)).OK {
		t.Fatal("foreign pause")
	}
}
