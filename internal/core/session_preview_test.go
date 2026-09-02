package core

import (
	"encoding/json"
	"os"
	"testing"

	"everything-go/internal/protocol"
)

func TestSessionPreviewSharedVectors(t *testing.T) {
	data, err := os.ReadFile("../../../contracts/v3/normalization_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Previews []struct {
			Phase             string `json:"phase"`
			User              string `json:"user"`
			Assistant         string `json:"assistant"`
			TerminalAssistant string `json:"terminal_assistant"`
			ExpectedRole      string `json:"expected_role"`
			Expected          string `json:"expected"`
		} `json:"previews"`
	}
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, vector := range vectors.Previews {
		messages := []protocol.RecentMessage{{Role: "user", Text: vector.User}}
		if vector.Assistant != "" {
			messages = append(messages, protocol.RecentMessage{Role: "assistant", Text: vector.Assistant})
		}
		if vector.TerminalAssistant != "" && vector.TerminalAssistant != vector.Assistant {
			messages = append(messages, protocol.RecentMessage{Role: "assistant", Text: vector.TerminalAssistant})
		}
		text, role := selectSessionPreview(messages, vector.Phase)
		if text != vector.Expected || role != vector.ExpectedRole {
			t.Fatalf("phase=%s got=(%s,%s) want=(%s,%s)", vector.Phase, role, text, vector.ExpectedRole, vector.Expected)
		}
	}
}

func TestTruncateGraphemesDoesNotSplitEmoji(t *testing.T) {
	if got := truncateGraphemes("A👨‍👩‍👧‍👦B", 2); got != "A👨‍👩‍👧‍👦" {
		t.Fatalf("got %q", got)
	}
}
