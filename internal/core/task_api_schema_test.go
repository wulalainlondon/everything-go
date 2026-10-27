package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"github.com/coder/websocket"
)

func taskSchemaConnection(t *testing.T, h *Hub, hash string) (*Client, func(string, any, string) map[string]any, func(string)) {
	t.Helper()
	conn, ctx, cleanup := dialWS(t, h)
	t.Cleanup(cleanup)
	send := func(v any) {
		t.Helper()
		raw, _ := json.Marshal(v)
		if e := conn.Write(ctx, websocket.MessageText, raw); e != nil {
			t.Fatal(e)
		}
	}
	receive := func(kind string) map[string]any {
		t.Helper()
		for {
			v := readEvent(t, ctx, conn)
			if v["type"] == kind {
				return v
			}
		}
	}
	clientID := ""
	hello := func(hash string) {
		v := map[string]any{"type": "hello", "auth_token": "media-token", "device_id": "media-phone", "protocol_version": 3}
		if hash != "" {
			v["task_api_schema_hash"] = hash
		}
		send(v)
		for {
			ack := readEvent(t, ctx, conn)
			if ack["type"] == "hello_ack" {
				clientID, _ = ack["client_id"].(string)
				break
			}
			if ack["type"] == "error" {
				if clientID == "" || ack["code"] != "task_api_schema_locked" {
					t.Fatal("unexpected hello refusal", ack)
				}
				break
			}
		}
	}
	hello(hash)
	var client *Client
	h.mu.RLock()
	for c := range h.clients {
		if c.clientID == clientID && c.live() {
			client = c
		}
	}
	h.mu.RUnlock()
	if client == nil {
		t.Fatal("no original authenticated client")
	}
	call := func(op string, input any, key string) map[string]any {
		t.Helper()
		v := map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "schema-case", "operation": op, "input": input}
		if key != "" {
			v["idempotency_key"] = key
		}
		send(v)
		out := receive("task_api_response")
		var validator *contract.Contract
		var e error
		if hash == contract.MediaHash() {
			validator, e = contract.NewMedia()
		} else {
			validator, e = contract.New()
		}
		if e != nil {
			t.Fatal(e)
		}
		raw, _ := json.Marshal(out)
		if e = validator.Validate(raw, "Response"); e != nil {
			t.Fatalf("selected contract response invalid %s %v", raw, e)
		}
		return out
	}
	return client, call, hello
}
func TestTaskSchemaNegotiationKeepsLegacyTextAndGatesPureMedia(t *testing.T) {
	for _, hash := range []string{"", contract.Hash(), contract.MediaHash(), strings.Repeat("f", 64)} {
		t.Run(hash, func(t *testing.T) {
			h, s, _, exec := mediaIntakeFixture(t, backend.Codex)
			_, call, _ := taskSchemaConnection(t, h, hash)
			caps := call("capabilities", map[string]any{}, "")
			if hash == strings.Repeat("f", 64) {
				if caps["ok"] != false || caps["error"].(map[string]any)["code"] != "unsupported" {
					t.Fatal("unknown hash aliased", caps)
				}
				refused := call("intake", map[string]any{"action": "submit", "submission_request_id": "r_" + strings.Repeat("e", 32), "target": map[string]any{"instance_id": "i1", "session_id": s.ID, "expected_native_thread_id": s.ResumeID(), "expected_config_revision": 0}, "goal": "must not dispatch"}, "r_"+strings.Repeat("e", 32))
				if refused["ok"] != false || refused["error"].(map[string]any)["code"] != "unsupported" {
					t.Fatal("unknown contract dispatched", refused)
				}
				if snapshot, e := h.messageQueue.Snapshot(s.ID); e != nil || len(snapshot.Items) != 0 {
					t.Fatal("unknown contract wrote queue", e)
				}
				select {
				case <-exec.starts:
					t.Fatal("unknown hash wrote")
				default:
				}
				return
			}
			expected := hash
			if expected == "" {
				expected = contract.Hash()
			}
			if caps["ok"] != true || caps["result"].(map[string]any)["schema_hash"] != expected {
				t.Fatal("not exact negotiated contract", caps)
			}
			target := map[string]any{"instance_id": "i1", "session_id": s.ID, "expected_native_thread_id": s.ResumeID(), "expected_config_revision": 0}
			pure := map[string]any{"action": "submit", "submission_request_id": "r_" + strings.Repeat("a", 32), "target": target, "goal": "", "images": []any{map[string]any{"data": "AQID", "media_type": "image/png"}}}
			out := call("intake", pure, pure["submission_request_id"].(string))
			if (out["ok"] == true) != (hash == contract.MediaHash()) {
				t.Fatal("pure media crossed wrong grammar", out)
			}
			text := map[string]any{"action": "submit", "submission_request_id": "r_" + strings.Repeat("b", 32), "target": target, "goal": "original legacy text"}
			if out = call("intake", text, text["submission_request_id"].(string)); out["ok"] != true {
				t.Fatal("legacy text no longer writable", out)
			}
		})
	}
}
func TestTaskSchemaNegotiationRetiresChangedConnectionAndFreshReconnectWorks(t *testing.T) {
	h, s, _, _ := mediaIntakeFixture(t, backend.Codex)
	client, call, hello := taskSchemaConnection(t, h, contract.MediaHash())
	original, e := h.taskHuman(client)
	if e != nil {
		t.Fatal(e)
	}
	hello(contract.Hash())
	if !client.taskSchemaRetired.Load() {
		t.Fatal("connection negotiation revived")
	}
	if _, e = original.Check(context.Background()); e == nil {
		t.Fatal("old slow binding survives renegotiation")
	}
	hello(contract.MediaHash())
	if !client.taskSchemaRetired.Load() {
		t.Fatal("schema ABA revived original connection")
	}
	if _, e = original.Check(context.Background()); e == nil {
		t.Fatal("schema ABA revived original bound caller")
	}
	if out := call("capabilities", map[string]any{}, ""); out["ok"] != false {
		t.Fatal("retired connection task API survived", out)
	}
	_, fresh, _ := taskSchemaConnection(t, h, "")
	out := fresh("capabilities", map[string]any{}, "")
	if out["ok"] != true || out["result"].(map[string]any)["schema_hash"] != contract.Hash() {
		t.Fatal("fresh original contract reconnect failed", out)
	}
	if snapshot, e := h.messageQueue.Snapshot(s.ID); e != nil || len(snapshot.Items) != 0 {
		t.Fatal("negotiation wrote task", e)
	}
}
func TestTaskSchemaLegacyReadNewMediaOmitsOnlyOptionalGoal(t *testing.T) {
	h, s, media, _ := mediaIntakeFixture(t, backend.Codex)
	id := "r_" + strings.Repeat("c", 32)
	out := media("intake", map[string]any{"action": "submit", "submission_request_id": id, "target": map[string]any{"instance_id": "i1", "session_id": s.ID, "expected_native_thread_id": s.ResumeID(), "expected_config_revision": 0}, "goal": "", "images": []any{map[string]any{"data": "AQID", "media_type": "image/png"}}}, id)
	if out["ok"] != true {
		t.Fatal(out)
	}
	receipt := out["result"].(map[string]any)
	newView := media("get", map[string]any{"task_id": receipt["task_id"]}, "")
	if newView["result"].(map[string]any)["goal"] != "" {
		t.Fatal("new view lost exact empty goal", newView)
	}
	before, _, _ := h.messageQueue.Get(s.ID, id)
	_, legacy, _ := taskSchemaConnection(t, h, "")
	oldView := legacy("get", map[string]any{"task_id": receipt["task_id"]}, "")
	if oldView["ok"] != true {
		t.Fatal(oldView)
	}
	task := oldView["result"].(map[string]any)
	if _, exists := task["goal"]; exists {
		t.Fatal("legacy got invalid or synthesized goal", task)
	}
	if task["task_id"] != receipt["task_id"] || task["source"] == nil || task["axes"] == nil {
		t.Fatal("task dropped or unbound", task)
	}
	after, _, _ := h.messageQueue.Get(s.ID, id)
	if string(before.Payload) != string(after.Payload) || before.PayloadHash != after.PayloadHash {
		t.Fatal("view rewrote canonical input")
	}
}

// Optional engineering harness exercises exact installed legacy SDK source over
// this real isolated WebSocket Hub. No live Bridge/account/provider is touched.
func TestTaskSchemaNegotiationActualLegacySDK(t *testing.T) {
	manifest := os.Getenv("OWNED_TASK_SCHEMA_SDK_SET")
	if manifest == "" {
		t.Skip("owner exact old-device SDK harness not configured")
	}
	raw, e := os.ReadFile(manifest)
	if e != nil {
		t.Fatal(e)
	}
	var fixtures []struct{ Device, Source, Bundle, BundleSHA, Runner, RunnerSHA string }
	if json.Unmarshal(raw, &fixtures) != nil || len(fixtures) != 2 {
		t.Fatal("exact Note20/iPad SDK fixtures required")
	}
	for _, fixture := range fixtures {
		t.Run(fixture.Device, func(t *testing.T) {
			for p, want := range map[string]string{fixture.Bundle: fixture.BundleSHA, fixture.Runner: fixture.RunnerSHA} {
				raw, e := os.ReadFile(p)
				sum := sha256.Sum256(raw)
				if e != nil || hex.EncodeToString(sum[:]) != want {
					t.Fatal("original SDK/harness bytes changed")
				}
			}
			h, s, media, _ := mediaIntakeFixture(t, backend.Codex)
			id := "r_" + strings.Repeat("d", 32)
			out := media("intake", map[string]any{"action": "submit", "submission_request_id": id, "target": map[string]any{"instance_id": "i1", "session_id": s.ID, "expected_native_thread_id": s.ResumeID(), "expected_config_revision": 0}, "goal": "", "images": []any{map[string]any{"data": "AQID", "media_type": "image/png"}}}, id)
			if out["ok"] != true {
				t.Fatal(out)
			}
			taskID := out["result"].(map[string]any)["task_id"].(string)
			server := httptest.NewServer(http.HandlerFunc(h.ServeWS))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "node", fixture.Runner, fixture.Bundle, "ws"+strings.TrimPrefix(server.URL, "http"), s.ID, taskID)
			result, e := command.CombinedOutput()
			if e != nil {
				t.Fatalf("actual %s SDK failed: %v %s", fixture.Device, e, result)
			}
			var proof struct {
				SchemaHash, RequestID                 string
				OldTextMutation, ReadMediaWithoutGoal bool
			}
			if json.Unmarshal(result, &proof) != nil || proof.SchemaHash != contract.Hash() || !proof.OldTextMutation || !proof.ReadMediaWithoutGoal {
				t.Fatal("legacy SDK scope proof missing", string(result))
			}
			entry, found, e := h.messageQueue.Get(s.ID, proof.RequestID)
			if e != nil || !found || entry.Content != "original legacy SDK text" {
				t.Fatal("actual SDK did not admit original text", entry, e)
			}
			t.Logf("exact %s source %s original schema %s: SDK mutation+read passed", fixture.Device, fixture.Source, proof.SchemaHash)
		})
	}
}
