package goexec

import (
	"encoding/json"
	"everything-go/internal/history"
	"testing"
)

func TestClaudeHistoryGalleryImagesOnlyDirectHumanBlocks(t *testing.T) {
	png := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl6GQAAAABJRU5ErkJggg=="
	image := map[string]any{"type": "image", "source": map[string]string{"type": "base64", "media_type": "image/png", "data": png}}
	content, _ := json.Marshal([]any{image, image})
	if images := claudeHistoryInputImages(content); len(images) != 2 || images[0]["data"] != png {
		t.Fatal("intentional duplicate inputs lost")
	}
	for _, raw := range []string{`[{"type":"image","source":{"type":"url","url":"https://foreign/image.png"}}]`, `[{"type":"tool_result","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + png + `"}}]}]`} {
		if len(claudeHistoryInputImages(json.RawMessage(raw))) > 0 {
			t.Fatal("foreign/tool image admitted")
		}
	}
	row, _ := json.Marshal(map[string]any{"type": "user", "message": map[string]any{"content": []any{image}}})
	rows, _, e := parseClaudeHistoryLines([]history.TailLine{{LineNo: 1, Data: row}}, "thread", 1000, false)
	if e != nil || len(rows) != 1 || len(rows[0]["images"].([]map[string]string)) != 1 {
		t.Fatal("image-only native user row lost", e)
	}
}
