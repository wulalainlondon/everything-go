package core

import (
	"encoding/base64"
	"encoding/json"
	unified "everything-go/contracts/task-api/unified-v6"
	"everything-go/internal/taskapi"
	"os"
	"path/filepath"
	"testing"
)

func TestTaskUnifiedActualIntentHashGolden(t *testing.T) {
	input := map[string]any{"manager": map[string]any{"authority_instance_id": "authority-fixture", "session_id": "s_fixture", "thread_id": "thread-fixture", "config_revision": 0}, "original_request": map[string]any{"2": "second", "10": "tenth", "\ue000": "private-use-key", "💡": "astral-key", "nested": map[string]any{"text": "<>&\u2028\u2029繁體中文", "config_revision": 0, "safe": 9007199254740991}}}
	raw, _ := json.Marshal(input)
	r := taskapi.Request{Type: "task_api_unified_request", Version: unified.Version, CorrelationID: "first-correlation", Operation: "dispatch", IdempotencyKey: "original-key", Input: raw}
	hash := unifiedHash(r)
	r.CorrelationID = "different"
	r.IdempotencyKey = "different"
	if unifiedHash(r) != hash {
		t.Fatal("transport identity contaminated hash")
	}
	canonical, _ := json.Marshal(struct {
		Operation string `json:"operation"`
		Input     any    `json:"input"`
	}{r.Operation, input})
	fixture := map[string]any{"source_function": "internal/core/task_unified.go:unifiedHash", "schema_hash": unified.Hash(), "scope": "hash and strict-number engineering only; no provider/API effect", "vectors": []any{map[string]any{"name": "go-utf8-map-html-unicode-zero-safe", "operation": r.Operation, "input": input, "expected_hash": hash, "canonical_bytes_base64": base64.StdEncoding.EncodeToString(canonical)}, map[string]any{"name": "nested-float-refused", "operation": "dispatch", "input": map[string]any{"original_request": map[string]any{"nested": map[string]any{"value": 1.5}}}, "validation": "refused"}, map[string]any{"name": "nested-unsafe-int-refused", "operation": "dispatch", "input": map[string]any{"original_request": map[string]any{"nested": map[string]any{"value": int64(9007199254740992)}}}, "validation": "refused"}}}
	for _, value := range []string{`{"nested":{"value":1.5}}`, `{"nested":{"value":9007199254740992}}`, `{"x":1,"x":2}`} {
		request := `{"type":"task_api_unified_request","api_version":"1.0.0-rc.6","correlation_id":"reject","idempotency_key":"original","operation":"dispatch","input":{"original_request":` + value + `}}`
		if unified.Validate([]byte(request)) == nil {
			t.Fatal("accepted noncanonical or duplicate", request)
		}
	}
	if out := os.Getenv("TASK_UNIFIED_FIXTURE_DIR"); out != "" {
		if e := os.MkdirAll(out, 0700); e != nil {
			t.Fatal(e)
		}
		b, _ := json.MarshalIndent(fixture, "", "  ")
		name := os.Getenv("TASK_UNIFIED_HASH_FILENAME")
		if name == "" {
			name = "HASH_GOLDEN_R1.json"
		}
		if filepath.Base(name) != name {
			t.Fatal("invalid output name")
		}
		file, e := os.OpenFile(filepath.Join(out, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			t.Fatal(e)
		}
		defer file.Close()
		if _, e = file.Write(append(b, '\n')); e != nil {
			t.Fatal(e)
		}
	}
	t.Logf("actual unifiedHash unicode canonical SHA256=%s", hash)
}
