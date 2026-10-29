package core

import (
	"encoding/json"
	contract "everything-go/contracts/task-api/controller-results-v9"
	base "everything-go/contracts/task-api/v1"
	"everything-go/internal/taskapi"
	"os"
	"path/filepath"
	"testing"
)

func TestControllerResultsIntentActualGoGolden(t *testing.T) {
	inputs := []string{`{"manager":{"authority_instance_id":"本機<>&\u2028\u2029","session_id":"原對話","thread_id":"native","config_revision":0},"10":"ten","2":"two","\ue000":"private-use","💡":"astral"}`, `{"nested":{"negative":-3,"maximum":9007199254740991,"normal":1000},"text":"繁體中文 <>&\u2028\u2029","zero":0}`}
	vectors := []any{}
	for _, input := range inputs {
		parsed, e := base.Parse([]byte(input))
		if e != nil {
			t.Fatal(e)
		}
		if e = base.ValidateWireNumbers(parsed); e != nil {
			t.Fatal(e)
		}
		req := taskapi.Request{Operation: "ack_dispatch_results", Input: json.RawMessage(input)}
		actual := unifiedHash(req)
		if len(actual) != 64 {
			t.Fatal(actual)
		}
		var decoded any
		json.Unmarshal(req.Input, &decoded)
		vectors = append(vectors, map[string]any{"operation": req.Operation, "input": decoded, "expected_hash": actual})
	}
	bad := []byte(`{"x":9007199254740992}`)
	v, e := base.Parse(bad)
	if e == nil && base.ValidateWireNumbers(v) == nil {
		t.Fatal("unsafe integer accepted")
	}
	if directory := os.Getenv("CONTROLLER_RESULTS_WIRE_OUT"); directory != "" {
		path := filepath.Join(directory, "HASH_GOLDEN_RC9_R1.json")
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		if e = json.NewEncoder(f).Encode(map[string]any{"status": "WIP_ACTUAL_GO_FUNCTION_NOT_FINAL_SOURCE", "schema_hash": contract.Hash(), "function": "core.unifiedHash (actual rc9 mutation path)", "algorithm": "SHA256(encoding/json Marshal struct{operation,input}); excludes correlation/idempotency key", "vectors": vectors, "unsafe_integer": "9007199254740992 rejected by production pre-hash wire validator"}); e != nil {
			t.Fatal(e)
		}
	}
}
