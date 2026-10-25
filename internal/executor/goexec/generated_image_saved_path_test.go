package goexec

import (
	"encoding/json"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeImageGenerationSavedPathActualProviderShape(t *testing.T) {
	for _, kind := range []string{"completed", "failed", "foreign-thread", "missing", "unsupported-extension"} {
		t.Run(kind, func(t *testing.T) {
			sink := &capSink{}
			c := NewCodex(sink, "disabled")
			s := session.NewRegistry().Create("native-image", "Image", "/tmp", "codex", "", "", "original-thread")
			st := c.state(s.ID)
			st.threadID = "original-thread"
			st.reqID = "original-request"
			st.currentTurnID = "original-turn"
			st.turnActive = true
			c.threadToSession[st.threadID] = s
			thread := "original-thread"
			if kind == "foreign-thread" {
				thread = "foreign-thread"
			}
			dir := filepath.Join(t.TempDir(), ".codex", "generated_images", thread)
			os.MkdirAll(dir, 0700)
			path := filepath.Join(dir, "original.png")
			if kind == "unsupported-extension" {
				path = filepath.Join(dir, "original.txt")
			}
			if kind != "missing" {
				os.WriteFile(path, []byte("image fixture"), 0600)
			}
			item := map[string]any{"type": "imageGeneration", "id": "exec-original-image", "status": "completed", "savedPath": path, "result": "BASE64_MUST_NOT_FORWARD", "failure": nil, "revisedPrompt": "original prompt"}
			if kind == "failed" {
				item["status"] = "failed"
				item["failure"] = map[string]any{"message": "failed"}
			}
			params := map[string]any{"threadId": "original-thread", "turnId": "original-turn", "item": item}
			raw, _ := json.Marshal(map[string]any{"method": "item/completed", "params": params})
			c.dispatch(raw)
			media := 0
			for _, event := range sink.events {
				if m, ok := event.(protocol.Media); ok {
					media++
					if m.SessionID != s.ID || m.RequestID != "original-request" || m.Path != path {
						t.Fatal(m)
					}
				}
				if r, ok := event.(protocol.ToolResult); ok && strings.Contains(r.Output, "BASE64_MUST_NOT_FORWARD") {
					t.Fatal("base64 leaked")
				}
			}
			if kind == "completed" && media != 1 {
				t.Fatalf("actual native imageGeneration ignored: %d", media)
			}
			if kind != "completed" && media != 0 {
				t.Fatalf("invalid native image accepted: %s", kind)
			}
		})
	}
}
