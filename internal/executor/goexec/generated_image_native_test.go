package goexec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// Forensic regression probe only. The current provider schema carries dynamic
// tool output in item.contentItems, with inputImage/inputText discriminators.
// No provider, original conversation, microphone or external effect is started.
func TestInvestigationBotGeneratedImageNativeContract(t *testing.T) {
	for _, tt := range []struct{ name, field, imageType, textType string }{
		{"legacy_output_snake_control", "output", "input_image", "input_text"},
		{"native_contentItems_camel", "contentItems", "inputImage", "inputText"},
		{"contentItems_snake_isolates_field", "contentItems", "input_image", "input_text"},
		{"output_camel_isolates_discriminator", "output", "inputImage", "inputText"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sink := &capSink{}
			c := NewCodex(sink, "codex")
			reg := session.NewRegistry()
			dir := filepath.Join(t.TempDir(), ".codex", "generated_images", "original-shape-fixture")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "fixture.png")
			if err := os.WriteFile(path, []byte("synthetic image file; no real image generation"), 0600); err != nil {
				t.Fatal(err)
			}
			s := reg.Create("image-probe", "codex", "/tmp", "codex", "", "", "")
			st := c.state(s.ID)
			st.threadID = "fixture-thread"
			st.reqID = "fixture-request"
			st.currentTurnID = "fixture-turn"
			st.turnActive = true
			c.threadToSession["fixture-thread"] = s
			imageKey := "image_url"
			if tt.imageType == "inputImage" {
				imageKey = "imageUrl"
			}
			parts := []map[string]any{{"type": tt.imageType, imageKey: "data:image/png;base64,SYNTHETIC_PROBE"}, {"type": tt.textType, "text": "Generated images are saved as " + path}}
			item := map[string]any{"id": "fixture-tool", "type": "dynamicToolCall", "tool": "exec", "arguments": "generatedImage(result)", "status": "completed", "success": true}
			params := map[string]any{"threadId": "fixture-thread", "turnId": "fixture-turn", "itemId": "fixture-tool", "item": item}
			if tt.field == "contentItems" {
				item[tt.field] = parts
			} else {
				params[tt.field] = parts
			}
			raw, _ := json.Marshal(map[string]any{"method": "item/completed", "params": params})
			c.dispatch(raw)
			count := 0
			for _, e := range sink.events {
				if media, ok := e.(protocol.Media); ok {
					count++
					if media.Path != path || media.RequestID != "fixture-request" {
						t.Fatalf("foreign media: %+v", media)
					}
				}
			}
			if count != 1 {
				t.Fatalf("expected generated image as media; got %d (native field=%s type=%s)", count, tt.field, tt.imageType)
			}
		})
	}
}
