package goexec

import (
	"encoding/json"
	"testing"

	"everything-go/internal/protocol"
)

// TestUserMessageJSONAttachments pins the stream-json content-block order/shape
// (images → files → text) matching claude_cli.py.
func TestUserMessageJSONAttachments(t *testing.T) {
	raw := userMessageJSON("hello",
		[]protocol.InboundImage{{Data: "AAAA", MediaType: "image/png"}},
		[]protocol.InboundFile{
			{Name: "a.go", Content: "package x", MediaType: "text/plain"},
			{Name: "doc.pdf", Content: "JVBER", MediaType: "application/pdf"},
		},
	)
	var f struct {
		Type    string `json:"type"`
		Message struct {
			Role    string           `json:"role"`
			Content []map[string]any `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if f.Type != "user" || f.Message.Role != "user" {
		t.Fatalf("frame wrong: %+v", f)
	}
	c := f.Message.Content
	if len(c) != 4 {
		t.Fatalf("want 4 blocks (image, txt, pdf, text), got %d: %v", len(c), c)
	}
	// image first
	if c[0]["type"] != "image" {
		t.Errorf("block0 = %v, want image", c[0]["type"])
	}
	if src, _ := c[0]["source"].(map[string]any); src["media_type"] != "image/png" || src["data"] != "AAAA" {
		t.Errorf("image source wrong: %v", c[0]["source"])
	}
	// text file fenced
	if c[1]["type"] != "text" || c[1]["text"] != "[File: a.go]\n```go\npackage x\n```" {
		t.Errorf("txt file block wrong: %q", c[1]["text"])
	}
	// pdf as document
	if c[2]["type"] != "document" {
		t.Errorf("pdf block = %v, want document", c[2]["type"])
	}
	// content text last
	if c[3]["type"] != "text" || c[3]["text"] != "hello" {
		t.Errorf("text block wrong: %v", c[3])
	}
}

// TestUserMessageJSONTextOnly: no attachments → single text block.
func TestUserMessageJSONTextOnly(t *testing.T) {
	raw := userMessageJSON("hi", nil, nil)
	var f struct {
		Message struct {
			Content []map[string]any `json:"content"`
		} `json:"message"`
	}
	_ = json.Unmarshal(raw, &f)
	if len(f.Message.Content) != 1 || f.Message.Content[0]["text"] != "hi" {
		t.Fatalf("text-only wrong: %v", f.Message.Content)
	}
}
