package core

import (
	"encoding/json"
	"os"
	"testing"
)

func TestHumanSourceProductionWireGolden(t *testing.T) {
	binding := map[string]any{"device": "device", "authority": "auth", "session": "s", "thread": "t", "config": 1, "epoch": 0, "generation": 1}
	raw, e := marshalEvent(map[string]any{"type": "queue_input_result", "session_id": "s", "request_id": "golden_source", "instance_id": "auth", "result": map[string]any{"status": "source", "binding": binding, "compatible_bindings": []any{binding}, "limit": 64}}, "auth")
	if e != nil {
		t.Fatal(e)
	}
	var v map[string]any
	if e = json.Unmarshal(raw, &v); e != nil {
		t.Fatal(e)
	}
	if v["session_id"] != "sk1:auth:s" || v["instance_id"] != "auth" {
		t.Fatalf("production scope changed: %s", raw)
	}
	if path := os.Getenv("BRIDGE_QMU_WIRE_FIXTURE"); path != "" {
		if e = os.WriteFile(path, append(raw, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
	t.Logf("actual production wire: %s", raw)
}
