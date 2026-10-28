package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	"strings"
	"testing"
)

func post201IndexedController(t *testing.T) (*Hub, taskapi.VerifiedContext, taskapi.IntentRecord) {
	t.Helper()
	h, _, parent, target := controllerFixture(t)
	ctx := context.Background()
	if _, e := h.dispatches.SetGrant(ctx, parent.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0); e != nil {
		t.Fatal(e)
	}
	d := sessiondispatch.Record{ID: "scd_adjacent", ParentID: parent.Parent.ID, ParentThreadID: parent.Parent.ResumeID(), ParentConfigKnown: true, ParentConfigRevision: 0, OriginRequestID: "r_original_source", ParentExecutionRequestID: "r_original_source", TurnID: "original-source-turn", ToolCallID: "original-call", InstanceID: h.cfg.InstanceID, SessionID: target.ID, ThreadID: target.ResumeID(), ConfigRevision: 0, RequestID: "scjob_scd_adjacent", Mode: "queue", Content: "bounded original", IntentHash: "original-hash", State: "prepared", DeliveryState: "pending"}
	if made, e := h.dispatches.Create(ctx, d); !made || e != nil {
		t.Fatal(made, e)
	}
	client := sharedReadClient(t, h, "post201-adjacent")
	client.pinTaskSchema(contract.MediaHash())
	bound, e := h.taskHuman(client)
	if e != nil {
		t.Fatal(e)
	}
	caller, e := bound.Check(ctx)
	if e != nil {
		t.Fatal(e)
	}
	r, e := h.controllerIndexRecord(ctx, d)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.checkAPIRecord(ctx, caller, r); e != nil {
		t.Fatal(e)
	}
	return h, caller, r
}
func TestPost201ProjectionKeepsPrivateOriginAndOriginalIdentity(t *testing.T) {
	h, c, r := post201IndexedController(t)
	ctx := context.Background()
	var private apiMetadata
	if e := json.Unmarshal(r.Metadata, &private); e != nil {
		t.Fatal(e)
	}
	if private.Caller.BindingKind != "controller_origin" || private.Transport != "original_controller_read_index" {
		t.Fatal("private original authorization changed")
	}
	task, e := h.apiTask(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	source := task["source"].(map[string]any)
	if source["binding_kind"] != "legacy_unknown" || source["provenance"] != "legacy" {
		t.Fatal("derived read index falsely advertised as a verified invocation", source)
	}
	id := source["identity"].(map[string]any)
	if id["session_id"] != private.Caller.SourceSessionID || id["request_id"] != private.Caller.SourceRequestID || id["native_thread_id"] != private.SourceThread || id["config_revision"] != uint64(0) {
		t.Fatal("source tuple changed", id)
	}
	if len(task["receipt_ids"].([]string)) != 0 || task["task_id"] != r.TaskID || task["task_ref"].(map[string]string)["native_record_id"] != r.NativeID {
		t.Fatal("original index/receipt absence changed")
	}
	raw, _ := json.Marshal(task)
	for _, newContract := range []func() (*contract.Contract, error){contract.New, contract.NewMedia} {
		v, e := newContract()
		if e != nil {
			t.Fatal(e)
		}
		if e = v.Validate(raw, "Task"); e != nil {
			t.Fatal("strict Task", e)
		}
	}
	if _, e = h.dispatches.SetGrant(ctx, private.Caller.SourceSessionID, sessiondispatch.Grant{Enabled: false}, 1); e != nil {
		t.Fatal(e)
	}
	if e = h.checkAPIRecord(ctx, c, r); e == nil {
		t.Fatal("public legacy projection bypassed actual controller grant")
	}
}
func TestPost201ActualTaskStrictSchemaMatrix(t *testing.T) {
	h, _, r := post201IndexedController(t)
	task, e := h.apiTask(context.Background(), r)
	if e != nil {
		t.Fatal(e)
	}
	base, _ := json.Marshal(task)
	cases := []struct {
		name          string
		change        func(map[string]any)
		native, media bool
	}{
		{"ordinary-legacy", func(map[string]any) {}, true, true},
		{"invalid-controller-origin", func(m map[string]any) { m["source"].(map[string]any)["binding_kind"] = "controller_origin" }, false, false},
		{"empty-receipt-id", func(m map[string]any) { m["receipt_ids"] = []string{""} }, false, false},
		{"absent-optional-old-identities", func(m map[string]any) {
			s := m["source"].(map[string]any)["identity"].(map[string]any)
			delete(s, "request_id")
			delete(s, "native_thread_id")
			delete(s, "config_revision")
		}, true, true},
		{"present-empty-old-thread-invalid", func(m map[string]any) {
			m["source"].(map[string]any)["identity"].(map[string]any)["native_thread_id"] = ""
		}, false, false},
		{"unicode4096", func(m map[string]any) { m["goal"] = strings.Repeat("💡", 4096) }, true, true},
		{"unicode4097", func(m map[string]any) { m["goal"] = strings.Repeat("💡", 4097) }, false, false},
		{"empty-media-goal", func(m map[string]any) { m["goal"] = "" }, false, true},
		{"typed-unknown", func(m map[string]any) { m["axes"].(map[string]any)["execution"] = "unknown" }, true, true},
		{"untyped-status", func(m map[string]any) { m["axes"].(map[string]any)["delivery"] = "pending_parent" }, false, false},
		{"unsafe-revision", func(m map[string]any) { m["revision"] = uint64(9007199254740992) }, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m map[string]any
			json.Unmarshal(base, &m)
			tc.change(m)
			raw, _ := json.Marshal(m)
			for _, which := range []struct {
				new      func() (*contract.Contract, error)
				expected bool
			}{{contract.New, tc.native}, {contract.NewMedia, tc.media}} {
				v, e := which.new()
				if e != nil {
					t.Fatal(e)
				}
				if passed := v.Validate(raw, "Task") == nil; passed != which.expected {
					t.Fatalf("actual strict schema pass=%v expected=%v", passed, which.expected)
				}
			}
		})
	}
}
