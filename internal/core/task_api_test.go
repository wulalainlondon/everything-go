package core

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"github.com/coder/websocket"
)

func TestTaskAPIAuthenticatedWSCanonicalStore(t *testing.T) {
	root := t.TempDir()
	pair := governance.NewPairing(filepath.Join(root, "pairing.json"))
	if err := pair.Claim("isolated-fixture-token", "fixture-device"); err != nil {
		t.Fatal(err)
	}
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", RootDir: root, DataDir: root}, pair, 0)
	defer h.messageQueue.Close()
	defer h.delegations.Close()
	defer h.dispatches.Close()
	target := h.registry.Create("s_fixturetarget", "Target", root, backend.Codex, "gpt-6.1-sol", "read-only", "")
	target.SetResumeID("fixture-thread")
	started := make(chan string, 4)
	release := make(chan struct{})
	h.SetExecutor(&scopeFixtureExecutor{fakeExec: &fakeExec{sink: h, onSend: func(s *session.Session, req string, _ string) {
		started <- req
		<-release
		h.Emit(protocol.NewDone(s.ID, req))
	}}})
	conn, ctx, cleanup := dialWS(t, h)
	defer cleanup()
	send := func(value any) {
		raw, _ := json.Marshal(value)
		if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
			t.Fatal(err)
		}
	}
	receive := func(kind string) map[string]any {
		for {
			value := readEvent(t, ctx, conn)
			if value["type"] == kind {
				return value
			}
		}
	}
	send(map[string]any{"type": "hello", "device_id": "fixture-device", "auth_token": "isolated-fixture-token", "protocol_version": 3})
	receive("hello_ack")
	call := func(op string, input any, key string) map[string]any {
		frame := map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "fixture-correlation", "operation": op, "input": input}
		if key != "" {
			frame["idempotency_key"] = key
		}
		send(frame)
		result := receive("task_api_response")
		raw, _ := json.Marshal(result)
		compiler, _ := contract.New()
		if err := compiler.Validate(raw, "Response"); err != nil {
			t.Fatalf("invalid response %s: %v", raw, err)
		}
		return result
	}
	caps := call("capabilities", map[string]any{}, "")
	if caps["ok"] != true {
		t.Fatal(caps)
	}
	input := map[string]any{"goal": "isolated task", "instruction": "fixture instruction", "scope": map[string]any{"workspace_roots": []string{root}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "acceptance": []any{}, "dependencies": []any{}, "workspace": map[string]any{"cwd": root, "input_artifact_ids": []any{}}, "existing_target": map[string]any{"instance_id": "i1", "session_id": target.ID, "expected_native_thread_id": "fixture-thread", "expected_config_revision": target.SettingsSnapshot().ConfigRevision}}
	first := call("create_dispatch", input, "fixture-key-0001")
	if first["ok"] != true {
		t.Fatal(first)
	}
	req := <-started
	receipt := first["result"].(map[string]any)
	again := call("create_dispatch", input, "fixture-key-0001")
	if again["ok"] != true || again["result"].(map[string]any)["receipt_id"] != receipt["receipt_id"] {
		t.Fatal(again)
	}
	changed := map[string]any{}
	for k, v := range input {
		changed[k] = v
	}
	changed["instruction"] = "different payload"
	conflict := call("create_dispatch", changed, "fixture-key-0001")
	if conflict["error"].(map[string]any)["code"] != "idempotency_conflict" {
		t.Fatal(conflict)
	}
	lookup := call("get", map[string]any{"original_receipt": receipt["locator"]}, "")
	if lookup["ok"] != true {
		t.Fatal(lookup)
	}
	task := call("get", map[string]any{"task_id": receipt["task_id"]}, "")
	if task["result"].(map[string]any)["axes"].(map[string]any)["execution"] != "handoff" {
		t.Fatal(task)
	}
	h.Emit(backend.NativeTaskAccepted{SessionID: target.ID, RequestID: req, ThreadID: "fixture-thread", TurnID: "native-fixture-turn"})
	task = call("get", map[string]any{"task_id": receipt["task_id"]}, "")
	if task["result"].(map[string]any)["axes"].(map[string]any)["execution"] != "native_consumed" {
		t.Fatal(task)
	}
	snapshot := call("snapshot", map[string]any{"session_id": target.ID, "views": []string{"self"}, "limit": 1}, "")
	if snapshot["ok"] != true {
		t.Fatal(snapshot)
	}
	cursor := snapshot["result"].(map[string]any)["events_cursor"]
	close(release)
	time.Sleep(10 * time.Millisecond)
	events := call("events", map[string]any{"session_id": target.ID, "cursor": cursor, "limit": 100, "subscribe": false}, "")
	if events["ok"] != true {
		t.Fatal(events)
	}
	unknown := call("unregistered-op", map[string]any{}, "")
	if unknown["operation"] != "unregistered-op" || unknown["error"].(map[string]any)["code"] != "unsupported" {
		t.Fatal(unknown)
	}
	_ = context.Background()
}

// This permission enforcer exists only in the isolated fixture. Production
// executors without per-command scope support must reject before enqueue.
type scopeFixtureExecutor struct{ *fakeExec }

func (*scopeFixtureExecutor) ValidateExistingTaskScope(*session.Session, taskapi.ChildScope, string) error {
	return nil
}
func TestTaskAPIExistingScopeUnsupportedHasZeroEffect(t *testing.T) {
	root := t.TempDir()
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", RootDir: root, DataDir: root}, governance.NewPairing(filepath.Join(root, "pair.json")), 0)
	defer h.messageQueue.Close()
	defer h.delegations.Close()
	defer h.dispatches.Close()
	target := h.registry.Create("s_scope", "Scope", root, backend.Codex, "gpt-6.1-sol", "danger-full-access", "")
	target.SetResumeID("thread")
	input := map[string]any{"existing_target": map[string]any{"instance_id": "i1", "session_id": target.ID, "expected_native_thread_id": "thread", "expected_config_revision": 0}, "scope": map[string]any{"workspace_roots": []string{root}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "workspace": map[string]any{"cwd": root, "input_artifact_ids": []string{}}}
	raw, _ := json.Marshal(input)
	_, err := h.Authorize(context.Background(), taskapi.VerifiedContext{Authority: "i1", StableScopeID: "device:fixture", BindingKind: "paired_human"}, taskapi.Request{Operation: "create_dispatch", IdempotencyKey: "fixture-scope", Input: raw})
	if failure, ok := err.(*taskapi.APIError); !ok || failure.Code != "unsupported" {
		t.Fatalf("got %v", err)
	}
	snap, _ := h.messageQueue.Snapshot(target.ID)
	if len(snap.Items) != 0 {
		t.Fatal("unsupported scope created an effect")
	}
}
