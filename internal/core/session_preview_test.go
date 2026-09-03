package core

import (
	"encoding/json"
	"os"
	"testing"

	"everything-go/internal/protocol"
	"everything-go/internal/search"
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

func TestPersistedPreviewBeatsOlderSearchProjection(t *testing.T) {
	messages := []protocol.RecentMessage{{Role: "assistant", Text: "stale indexed result"}}
	indexed := &search.SessionPreview{LastTS: 100, LastAssistantTS: 100}
	text, role, at := resolveSessionPreview("fresh durable result", "assistant", 200_000, messages, indexed, "idle")
	if text != "fresh durable result" || role != "assistant" || at != 200_000 {
		t.Fatalf("older index replaced durable projection: (%q,%q,%d)", text, role, at)
	}
}

func TestNewerExternalCLIProjectionSupersedesPersistedPreview(t *testing.T) {
	messages := []protocol.RecentMessage{{Role: "assistant", Text: "new external result"}}
	indexed := &search.SessionPreview{LastTS: 300, LastAssistantTS: 300}
	text, role, at := resolveSessionPreview("old durable result", "assistant", 200_000, messages, indexed, "idle")
	if text != "new external result" || role != "assistant" || at != 300_000 {
		t.Fatalf("newer external result was ignored: (%q,%q,%d)", text, role, at)
	}
}
