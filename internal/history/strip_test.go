package history

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestStripThinkingBlocks(t *testing.T) {
	messages := []map[string]any{
		{ // untouched: no blocks
			"role": "user", "content": "hi",
		},
		{ // untouched: text-only blocks — must keep the SAME map (no copy)
			"role": "assistant", "content": "a",
			"blocks": []map[string]any{{"type": "text", "text": "a"}},
		},
		{ // thinking stripped, text kept
			"role": "assistant", "content": "b",
			"blocks": []map[string]any{
				{"type": "thinking", "thinking": "hmm"},
				{"type": "text", "text": "b"},
			},
		},
		{ // thinking-only + empty content → dropped entirely
			"role": "assistant", "content": "",
			"blocks": []map[string]any{{"type": "thinking", "thinking": "only"}},
		},
	}

	out := StripThinkingBlocks(messages)
	if len(out) != 3 {
		t.Fatalf("want 3 messages, got %d", len(out))
	}
	if !reflect.DeepEqual(out[0], messages[0]) || !reflect.DeepEqual(out[1], messages[1]) {
		t.Fatalf("untouched messages changed")
	}
	// Original map with thinking must NOT be mutated (shared cache).
	if len(messages[2]["blocks"].([]map[string]any)) != 2 {
		t.Fatalf("cached message mutated")
	}
	kept, ok := out[2]["blocks"].([]any)
	if !ok || len(kept) != 1 {
		t.Fatalf("want 1 kept block, got %#v", out[2]["blocks"])
	}
	if bm := kept[0].(map[string]any); bm["type"] != "text" {
		t.Fatalf("kept block is %v, want text", bm["type"])
	}
}

func TestStripThinkingBlocksJSONRoundTrip(t *testing.T) {
	// Blocks that went through the SQLite cache come back as []any.
	raw := `[{"role":"assistant","content":"","blocks":[{"type":"thinking","thinking":"x"}]},
	         {"role":"assistant","content":"c","blocks":[{"type":"thinking","thinking":"y"},{"type":"text","text":"c"}]}]`
	var messages []map[string]any
	if err := json.Unmarshal([]byte(raw), &messages); err != nil {
		t.Fatal(err)
	}
	out := StripThinkingBlocks(messages)
	if len(out) != 1 {
		t.Fatalf("want 1 message, got %d", len(out))
	}
	if got := out[0]["content"]; got != "c" {
		t.Fatalf("wrong survivor: %v", got)
	}
}
