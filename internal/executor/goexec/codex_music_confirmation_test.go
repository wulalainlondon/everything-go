package goexec

import (
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"strings"
	"testing"
)

func TestMusicConfirmationCatalogDistinguishesExactMetadataAndDisplay(t *testing.T) {
	c := &Codex{}
	c.SetSessionControlProvider(mediaCatalogProvider{})
	s := session.NewRegistry().Create("s_catalog", "catalog", t.TempDir(), backend.Codex, "", "", "")
	params := map[string]any{}
	c.applyVoiceMediaTools(s, params)
	for _, tool := range params["dynamicTools"].([]map[string]any)[0]["tools"].([]map[string]any) {
		props := tool["inputSchema"].(map[string]any)["properties"].(map[string]any)
		name := tool["name"].(string)
		if name == "play_music" || name == "play_youtube" {
			label, ok := props["display_label"].(map[string]any)
			if !ok || !strings.Contains(label["description"].(string), "Never a native title") {
				t.Fatal("label is not display-only", tool)
			}
			if !strings.Contains(props["title"].(map[string]any)["description"].(string), "exact song title only") {
				t.Fatal("ambiguous exact title", tool)
			}
		} else if props["display_label"] != nil {
			t.Fatal("label exposed as control expectation", tool)
		}
	}
}
