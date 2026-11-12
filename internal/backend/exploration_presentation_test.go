package backend

import (
	"context"
	"strings"
	"testing"
)

func TestExplorationSameTurnAndExactProjection(t *testing.T) {
	raw := "參數 e\u0301 與條件\n原始問題"
	ctx := WithVisualPresentation(context.Background(), "explore-v1")
	effective := PresentationContent(ctx, raw)
	if !ValidVisualPresentation("explore-v1") || !strings.HasPrefix(effective, "[Averything presentation context: conversation-exploration.v1]\n") || !strings.HasSuffix(effective, raw) {
		t.Fatal("new mode did not preserve original turn")
	}
	if OriginalPresentationContent(effective) != raw {
		t.Fatal("search/display projection rewrote original")
	}
	end := strings.Index(effective, "[/Averything presentation context]\n\n")
	if end < 0 || end > 12000 {
		t.Fatalf("new prompt exceeded exact existing projection grammar: %d", end)
	}
	a, b := PresentationHashes(ctx, raw)
	if len(a) != 64 || len(b) != 64 || a == b {
		t.Fatal("raw/effective hashes lost")
	}
	for _, term := range []string{"selected_sum", "explicit round", "only the user's explicit confirmation", "These are your source declarations", "conversation-exploration-response.v1"} {
		if !strings.Contains(effective, term) {
			t.Fatalf("missing contract rule %q", term)
		}
	}
	for _, command := range []string{"/compact", " /status"} {
		if PresentationContent(ctx, command) != command {
			t.Fatal("slash command changed")
		}
	}
	if PresentationContent(context.Background(), raw) != raw {
		t.Fatal("unknown client upgraded")
	}
	plain := PresentationContent(WithVisualPresentation(context.Background(), "text-v1"), raw)
	if strings.Contains(plain, "conversation-exploration.v1") {
		t.Fatal("text preference upgraded")
	}
}
