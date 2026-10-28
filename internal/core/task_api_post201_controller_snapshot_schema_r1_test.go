package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"os"
	"path/filepath"
	"testing"
)

// Existing canonical controller store, paired caller, real projection/freeze and
// real ca3 response validator. No provider/model/socket or live store is used.
func TestPost201DerivedControllerSnapshotMustConform(t *testing.T) {
	h, _, controller, target := controllerFixture(t)
	ctx := context.Background()
	if _, e := h.dispatches.SetGrant(ctx, controller.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0); e != nil {
		t.Fatal(e)
	}
	d := sessiondispatch.Record{ID: "scd_synthetic_original", ParentID: controller.Parent.ID, ParentExecutionRequestID: "r_synthetic_parent", OriginRequestID: "r_synthetic_parent", ParentThreadID: controller.Parent.ResumeID(), ParentConfigRevision: controller.Parent.SettingsSnapshot().ConfigRevision, ParentConfigKnown: true, TurnID: "synthetic-source-turn", ToolCallID: "synthetic-call", InstanceID: h.cfg.InstanceID, SessionID: target.ID, ThreadID: target.ResumeID(), ConfigRevision: target.SettingsSnapshot().ConfigRevision, RequestID: "scjob_scd_synthetic_original", ExecutionRequestID: "scjob_scd_synthetic_original", IntentHash: "synthetic-intent", Mode: "queue", Content: "Synthetic original controller command", State: "failed", Error: "Synthetic terminal refusal", DeliveryState: "not_requested"}
	if made, e := h.dispatches.Create(ctx, d); e != nil || !made {
		t.Fatal(made, e)
	}
	client := sharedReadClient(t, h, "synthetic-media-reader")
	if !client.pinTaskSchema(contract.MediaHash()) {
		t.Fatal("media negotiation")
	}
	bound, e := h.taskHuman(client)
	if e != nil {
		t.Fatal(e)
	}
	caller, e := bound.Check(ctx)
	if e != nil {
		t.Fatal(e)
	}
	input := map[string]any{"session_id": controller.Parent.ID, "views": []string{"inbox", "self", "results", "source_children", "manager_arranging"}, "limit": 100, "include_relation_coverage": true}
	rawInput, _ := json.Marshal(input)
	command := taskapi.AuthorizedCommand{Caller: caller, Request: taskapi.Request{Operation: "snapshot", Input: rawInput}, Revalidate: func(context.Context) error { return nil }}
	page, e := h.Read(ctx, command)
	if e != nil {
		t.Fatal("gateway", e)
	}
	op := "snapshot"
	raw, e := json.Marshal(taskapi.Response{Type: "task_api_response", Version: contract.Version, CorrelationID: "synthetic-derived-controller-schema", Operation: &op, OK: true, Result: page})
	if e != nil {
		t.Fatal(e)
	}
	v, e := contract.NewMedia()
	if e != nil {
		t.Fatal(e)
	}
	validation := v.Validate(raw, "Response")
	if validation != nil {
		if root, ok := validation.(*jsonschema.ValidationError); ok {
			var walk func(*jsonschema.ValidationError)
			walk = func(node *jsonschema.ValidationError) {
				if len(node.Causes) == 0 {
					t.Logf("synthetic schema location=%v keyword=%v", node.InstanceLocation, node.ErrorKind.KeywordPath())
				}
				for _, child := range node.Causes {
					walk(child)
				}
			}
			walk(root)
		}
	}
	request, _ := json.Marshal(taskapi.Request{Type: "task_api_request", Version: contract.Version, CorrelationID: "synthetic-derived-controller-schema", Operation: "snapshot", Input: rawInput})
	response := h.taskService.Execute(ctx, bound, request)
	t.Logf("media_hash=%s frame_bytes=%d public_ok=%v public_error=%v", contract.MediaHash(), len(raw), response.OK, response.Error)
	if out := os.Getenv("POST201_SCHEMA_EXPORT"); out != "" {
		if e := os.MkdirAll(out, 0700); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(filepath.Join(out, "synthetic-snapshot-gateway.json"), raw, 0600); e != nil {
			t.Fatal(e)
		}
		public, _ := json.Marshal(response)
		if e := os.WriteFile(filepath.Join(out, "synthetic-snapshot-service.json"), public, 0600); e != nil {
			t.Fatal(e)
		}
	}
	if validation != nil || !response.OK {
		t.Fatal("legitimate original controller index makes entire authenticated media snapshot unreadable; validator guards retained")
	}
}
