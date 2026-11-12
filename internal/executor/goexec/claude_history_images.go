package goexec

import "encoding/json"

// Only direct human input image blocks are eligible. Nested tool_result images,
// URLs and assistant tool inspection are deliberately excluded.
func claudeHistoryInputImages(raw json.RawMessage) []map[string]string {
	var blocks []struct {
		Type   string `json:"type"`
		Source struct {
			Type string `json:"type"`
			MIME string `json:"media_type"`
			Data string `json:"data"`
		} `json:"source"`
	}
	if json.Unmarshal(raw, &blocks) != nil {
		return nil
	}
	parts := []map[string]string{}
	for _, b := range blocks {
		if b.Type == "image" && b.Source.Type == "base64" {
			parts = append(parts, map[string]string{"type": "input_image", "image_url": "data:" + b.Source.MIME + ";base64," + b.Source.Data})
		}
	}
	converted, _ := json.Marshal(parts)
	_, images := codexHistoryInputImages(converted)
	return images
}
