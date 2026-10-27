package goexec

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func TestMediaOnlyCodexInputPreservesImageBytesWithoutSyntheticText(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.SetDataDir(t.TempDir())
	s := session.NewRegistry().Create("s_media", "Media", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "")
	st := c.state(s.ID)
	data := []byte("original image bytes")
	image := backend.ImageAttachment{Data: base64.StdEncoding.EncodeToString(data), MediaType: "image/png"}
	for _, text := range []string{"", " \n\t"} {
		t.Run(text, func(t *testing.T) {
			input := c.codexInput(s, "r_original", "r_original", text, []backend.ImageAttachment{image}, nil, st)
			defer c.cleanupTempImages(st, "r_original")
			index := 0
			if text != "" {
				if len(input) != 2 || input[0]["type"] != "text" || input[0]["text"] != text {
					t.Fatal("original whitespace changed", input)
				}
				index = 1
			} else if len(input) != 1 {
				t.Fatal("pure image gained text input", input)
			}
			if input[index]["type"] != "localImage" {
				t.Fatal("native image type lost", input)
			}
			raw, e := os.ReadFile(input[index]["path"].(string))
			if e != nil || !bytes.Equal(raw, data) {
				t.Fatal("image bytes lost", e)
			}
		})
	}
}
func TestMediaOnlyClaudeInputContainsOnlyOriginalImageBlocks(t *testing.T) {
	image := backend.ImageAttachment{Data: "AQID", MediaType: "image/png"}
	raw := userMessageJSON("", []backend.ImageAttachment{image}, nil)
	var v map[string]any
	if json.Unmarshal(raw, &v) != nil {
		t.Fatal("invalid native frame")
	}
	blocks := v["message"].(map[string]any)["content"].([]any)
	if len(blocks) != 1 {
		t.Fatal("pure media gained text", string(raw))
	}
	b := blocks[0].(map[string]any)
	source := b["source"].(map[string]any)
	if b["type"] != "image" || source["data"] != image.Data || source["media_type"] != image.MediaType {
		t.Fatal("native image changed", string(raw))
	}
}
