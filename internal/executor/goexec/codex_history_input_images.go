package goexec

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
)

const codexHistoryInputImageCount = 8
const codexHistoryInputImageBytes = 20 << 20

var codexInputImageOpen = regexp.MustCompile(`^<image name=\[Image #[1-9][0-9]*\] path="[^"\r\n<>]+">$`)

// Only inline bytes supplied by the original native user item are projected.
// A local path/URL is never fetched, and unverified wrappers remain visible.
func codexHistoryInputImages(raw json.RawMessage) (string, []map[string]string) {
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
		URL  string `json:"image_url"`
	}
	if json.Unmarshal(raw, &parts) != nil {
		return extractCodexText(raw), nil
	}
	var images []map[string]string
	projected := make(map[int]bool)
	total := 0
	for i, part := range parts {
		if part.Type != "input_image" || len(images) >= codexHistoryInputImageCount {
			continue
		}
		media, data, ok := strings.Cut(part.URL, ";base64,")
		if !ok {
			continue
		}
		media = strings.TrimPrefix(media, "data:")
		if !strings.HasPrefix(part.URL, "data:") || (media != "image/png" && media != "image/jpeg" && media != "image/webp") || len(data) > base64.StdEncoding.EncodedLen(codexHistoryInputImageBytes) {
			continue
		}
		decoded, err := base64.StdEncoding.Strict().DecodeString(data)
		if err != nil || len(decoded) == 0 || len(decoded) > codexHistoryInputImageBytes-total || base64.StdEncoding.EncodeToString(decoded) != data || http.DetectContentType(decoded) != media {
			continue
		}
		total += len(decoded)
		images = append(images, map[string]string{"data": data, "media_type": media})
		// Strip only the exact native wrapper triple around this proven byte item.
		if i > 0 && i+1 < len(parts) && parts[i-1].Type == "input_text" && codexInputImageOpen.MatchString(parts[i-1].Text) && parts[i+1].Type == "input_text" && parts[i+1].Text == "</image>" {
			projected[i-1], projected[i+1] = true, true
		}
	}
	if len(images) == 0 {
		return extractCodexText(raw), nil
	}
	var text []string
	for i, part := range parts {
		if !projected[i] && strings.TrimSpace(part.Text) != "" {
			text = append(text, part.Text)
		}
	}
	return cleanCodexText(strings.Join(text, "\n")), images
}
