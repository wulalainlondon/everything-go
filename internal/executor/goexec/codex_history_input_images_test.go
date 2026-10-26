package goexec

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/history"
	"os"
	"strings"
	"testing"
)

func TestCodexActual485UserImageProjection(t *testing.T) {
	raw, err := os.ReadFile("testdata/codex-user-image485.json")
	if err != nil {
		t.Fatal(err)
	}
	lines := []history.TailLine{
		{LineNo: 62, Data: []byte(`{"type":"turn_context","payload":{"turn_id":"01a17648-a673-7331-a7a9-fa18f54152ea"}}`)},
		{LineNo: 63, Data: raw},
	}
	messages, _, err := parseCodexHistoryLines(lines, "01a174dc-5f28-7051-83ce-67932520f119", false)
	if err != nil || len(messages) != 1 {
		t.Fatal(messages, err)
	}
	m := messages[0]
	images, ok := m["images"].([]map[string]string)
	if !ok || len(images) != 1 || images[0]["media_type"] != "image/png" {
		t.Fatal("original bytes not projected")
	}
	decoded, err := base64.StdEncoding.DecodeString(images[0]["data"])
	sum := sha256.Sum256(decoded)
	if err != nil || len(decoded) != 5044 || hex.EncodeToString(sum[:]) != "8bcebebedec43854b38a24660715a59ce71a5681401ead06bcc150913141c326" {
		t.Fatal("original bytes changed")
	}
	content, _ := m["content"].(string)
	if content == "" || strings.Contains(content, "<image") || strings.Contains(content, "</image>") || strings.Contains(content, "codex-input-images") || m["source_turn_id"] != "01a17648-a673-7331-a7a9-fa18f54152ea" || m["source_message_id"] != "codex:01a174dc-5f28-7051-83ce-67932520f119:line:63" {
		t.Fatal(m)
	}
	// Optional report output is confined to the owner-provided report path and
	// contains only this already-authorized immutable QA fixture projection.
	if dest := os.Getenv("JARVIS_IMAGE_PROJECTION_REPORT"); dest != "" {
		b, _ := json.MarshalIndent(m, "", "  ")
		if err := os.WriteFile(dest, append(b, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCodexUserImageProjectionRejectsUnprovenBytesWithoutStrippingHumanText(t *testing.T) {
	raw, _ := os.ReadFile("testdata/codex-user-image485.json")
	var row codexHistoryRow
	_ = json.Unmarshal(raw, &row)
	var payload struct {
		Content []map[string]any `json:"content"`
	}
	_ = json.Unmarshal(row.Payload, &payload)
	for _, kind := range []string{"remote", "bad_base64", "wrong_mime", "not_image", "oversized", "noncanonical", "literal_wrapper"} {
		t.Run(kind, func(t *testing.T) {
			b, _ := json.Marshal(payload.Content)
			var parts []map[string]any
			_ = json.Unmarshal(b, &parts)
			switch kind {
			case "remote":
				parts[2]["image_url"] = "https://example.invalid/image.png"
			case "bad_base64":
				parts[2]["image_url"] = "data:image/png;base64,%%%"
			case "wrong_mime":
				parts[2]["image_url"] = strings.Replace(parts[2]["image_url"].(string), "image/png", "image/jpeg", 1)
			case "not_image":
				parts[2]["image_url"] = "data:image/png;base64," + base64.StdEncoding.EncodeToString([]byte("plain text"))
			case "oversized":
				parts[2]["image_url"] = "data:image/png;base64," + strings.Repeat("A", base64.StdEncoding.EncodedLen(codexHistoryInputImageBytes)+4)
			case "noncanonical":
				parts[2]["image_url"] = parts[2]["image_url"].(string) + "\n"
			case "literal_wrapper":
				parts[1]["text"] = "Human quote: " + parts[1]["text"].(string)
			}
			b, _ = json.Marshal(parts)
			text, images := codexHistoryInputImages(b)
			if kind == "literal_wrapper" {
				if len(images) != 1 || !strings.Contains(text, "Human quote:") || !strings.Contains(text, "</image>") {
					t.Fatal("literal human text stripped")
				}
				return
			}
			if len(images) != 0 || !strings.Contains(text, "<image") || !strings.Contains(text, "</image>") {
				t.Fatal("unverified bytes projected or human wrapper stripped")
			}
		})
	}
}

func TestCodexUserImageOnlyAndCountBudget(t *testing.T) {
	raw, _ := os.ReadFile("testdata/codex-user-image485.json")
	var row codexHistoryRow
	_ = json.Unmarshal(raw, &row)
	var payload struct {
		Content []json.RawMessage `json:"content"`
	}
	_ = json.Unmarshal(row.Payload, &payload)
	images := make([]json.RawMessage, 9)
	for i := range images {
		images[i] = payload.Content[2]
	}
	b, _ := json.Marshal(images)
	text, got := codexHistoryInputImages(b)
	if text != "" || len(got) != 8 {
		t.Fatal("count budget", len(got))
	}
	messages, _, err := parseCodexHistoryLines([]history.TailLine{{LineNo: 1, Data: []byte(`{"type":"response_item","payload":{"type":"message","role":"user","content":` + string(b) + `}}`)}}, "thread", false)
	if err != nil || len(messages) != 1 || len(messages[0]["images"].([]map[string]string)) != 8 {
		t.Fatal("image-only user dropped", messages, err)
	}
}
