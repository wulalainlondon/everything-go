package goexec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"everything-go/internal/history"
)

func TestGeneratedImageHistoryRetainsOriginalRequestAcrossCacheAndRestart(t *testing.T) {
	root, data := t.TempDir(), t.TempDir()
	thread := "00000000-0000-0000-0000-000000008180"
	dir := filepath.Join(t.TempDir(), ".codex", "generated_images", thread)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	image := filepath.Join(dir, "original.png")
	if err := os.WriteFile(image, []byte("synthetic"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "rollout-2026-10-25T00-00-00-"+thread+".jsonl")
	rows := []map[string]any{
		{"type": "event_msg", "payload": map[string]any{"type": "task_started", "turn_id": "original-turn"}},
		{"type": "response_item", "payload": map[string]any{"type": "custom_tool_call", "call_id": "original-tool", "name": "exec", "input": "synthetic image generation"}},
		{"type": "response_item", "payload": map[string]any{"type": "custom_tool_call_output", "call_id": "original-tool", "output": []map[string]any{{"type": "input_image", "image_url": "data:image/png;base64,DO_NOT_FORWARD"}, {"type": "input_text", "text": "Generated images saved as " + image}}}},
		{"type": "response_item", "payload": map[string]any{"type": "message", "role": "assistant", "phase": "final_answer", "content": "Generated"}},
		{"type": "event_msg", "payload": map[string]any{"type": "task_complete", "turn_id": "original-turn"}},
	}
	writeJSONL(t, path, rows)
	makeExecutor := func() *Codex { c := NewCodex(&capSink{}, "codex"); c.sessionsRoot = root; c.SetDataDir(data); return c }
	c := makeExecutor()
	if err := c.rememberTurnRequest(thread, "original-turn", "original-request"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if i == 2 {
			c = makeExecutor()
		}
		result, err := c.LoadHistory(thread, history.Opts{Mode: "snapshot", Limit: 20})
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(result.Messages)
		if strings.Contains(string(raw), "DO_NOT_FORWARD") {
			t.Fatal("inline base64 escaped into history")
		}
		found := 0
		for _, m := range result.Messages {
			paths, _ := m["generated_image_paths"].([]string)
			if len(paths) == 0 {
				if generic, ok := m["generated_image_paths"].([]any); ok {
					for _, p := range generic {
						paths = append(paths, p.(string))
					}
				}
			}
			if len(paths) > 0 {
				found++
				if len(paths) != 1 || paths[0] != image || m["request_id"] != "original-request" || m["source_turn_id"] != "original-turn" {
					t.Fatalf("identity lost: %+v", m)
				}
			}
		}
		if found != 1 {
			t.Fatalf("want one native image mention, got %d", found)
		}
	}
}

func TestGeneratedImageUnknownOrMissingPathNeverForwardsBase64(t *testing.T) {
	for _, raw := range []json.RawMessage{
		json.RawMessage(`[{"type":"inputImage","imageUrl":"data:image/png;base64,DO_NOT_FORWARD"}]`),
		json.RawMessage(`[{"type":"input_image","image_url":"data:image/png;base64,DO_NOT_FORWARD"},{"type":"input_text","text":"/missing/.codex/generated_images/thread/missing.png"}]`),
	} {
		if len(codexGeneratedImagePaths(raw)) != 0 {
			t.Fatal("missing path accepted")
		}
		if out := codexResponseOutputText(raw); strings.Contains(out, "DO_NOT_FORWARD") || !strings.Contains(out, "no saved file path") {
			t.Fatal(out)
		}
	}
}
