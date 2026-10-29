package core

import (
	"encoding/json"
	contract "everything-go/contracts/task-api/controller-results-v9"
	"github.com/coder/websocket"
	"testing"
)

// Exercise the real authenticated wire handshake consumed by installed mobile,
// then the advertised rc9 endpoint. An isolated API test alone misses this edge.
func TestControllerResultsPairedHelloAdvertisesUsableV2Only(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		paired, missingQueue, missingDispatch bool
	}{
		{name: "paired", paired: true}, {name: "enrollment"},
		{name: "no_queue", paired: true, missingQueue: true}, {name: "no_dispatch", paired: true, missingDispatch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newControlTestHub(t, t.TempDir())
			if h.messageQueue == nil || h.dispatches == nil {
				t.Fatal("fixture stores missing")
			}
			if tc.missingQueue {
				q := h.messageQueue
				h.messageQueue = nil
				defer func() { h.messageQueue = q }()
			}
			if tc.missingDispatch {
				d := h.dispatches
				h.dispatches = nil
				defer func() { h.dispatches = d }()
			}
			if tc.paired {
				if e := h.pairing.Claim("fixture-credential", "fixture-s10"); e != nil {
					t.Fatal(e)
				}
			}
			conn, ctx, cleanup := dialWS(t, h)
			defer cleanup()
			if e := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","device_id":"fixture-s10","auth_token":"fixture-credential","protocol_version":3}`)); e != nil {
				t.Fatal(e)
			}
			ack := readEvent(t, ctx, conn)
			if ack["type"] != "hello_ack" {
				t.Fatal(ack)
			}
			caps := map[string]bool{}
			for _, v := range ack["capabilities"].([]any) {
				caps[v.(string)] = true
			}
			want := tc.paired && !tc.missingQueue && !tc.missingDispatch
			if caps["session_controller_results_v2"] != want {
				t.Fatalf("v2 mobile handshake eligibility=%v want %v", caps["session_controller_results_v2"], want)
			}
			if caps["session_controller_v1"] != (tc.paired && !tc.missingDispatch) {
				t.Fatal("original controller v1 changed")
			}
			if !want {
				return
			}
			raw, _ := json.Marshal(map[string]any{"type": "task_api_controller_results_request", "api_version": contract.Version, "correlation_id": "hello-capability-fixture", "operation": "capabilities", "input": map[string]any{}})
			if e := conn.Write(ctx, websocket.MessageText, raw); e != nil {
				t.Fatal(e)
			}
			for {
				event := readEvent(t, ctx, conn)
				if event["type"] != "task_api_controller_results_response" {
					continue
				}
				if event["ok"] != true {
					t.Fatal("advertised endpoint unavailable", event)
				}
				result := event["result"].(map[string]any)
				if result["role"] != "paired_human" || result["schema_hash"] != contract.Hash() || result["features"].(map[string]any)["session_controller_results_v2"] != true {
					t.Fatal("wrong advertised scope", result)
				}
				break
			}
		})
	}
}
