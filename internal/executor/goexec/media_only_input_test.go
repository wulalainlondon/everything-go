package goexec

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/coder/websocket"
	"image"
	"image/png"
	"os"
	"path/filepath"
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
			input, inputErr := c.codexInput(s, "r_original", "r_original", text, []backend.ImageAttachment{image}, nil, st)
			if inputErr != nil {
				t.Fatal(inputErr)
			}
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

func mediaPNG(t *testing.T) backend.ImageAttachment {
	t.Helper()
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, 1, 1))); err != nil {
		t.Fatal(err)
	}
	return backend.ImageAttachment{Data: base64.StdEncoding.EncodeToString(b.Bytes()), MediaType: "image/png"}
}

func TestMediaOnlyCodexStagingIOFailureStopsSendBeforeTurnWriter(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	c.appServerMode = "daemon"
	c.remoteConn = &websocket.Conn{} // Only fake in-memory RPC; never connects a daemon.
	st.turnActive = false
	st.currentTurnID = ""
	st.reqID = ""
	if err := os.WriteFile(filepath.Join(c.dataDir, "codex-input-images"), []byte("cache blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	w.reply = func(method string, _ json.RawMessage) (any, error) {
		if method != "thread/read" {
			t.Fatalf("unexpected native writer %s", method)
		}
		return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}, nil
	}
	err := c.Send(context.Background(), s, "r_original_media", "", []backend.ImageAttachment{mediaPNG(t)}, nil)
	var staging *imageInputError
	if !errors.As(err, &staging) {
		t.Fatalf("missing typed local error: %v", err)
	}
	if st.turnActive || st.turnStartPending || c.activeThreadOwner["root"] != nil {
		t.Fatal("failed staging retained an active native claim")
	}
	if len(w.methods) != 1 || w.methods[0] != "thread/read" {
		t.Fatalf("staging reached a native turn writer: %v", w.methods)
	}
	if len(st.tempImages) != 0 {
		t.Fatal("failed staging leaked media lease")
	}
}

func TestMediaOnlyCodexPartialSteerStagingRollsBackOnlyNewImages(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	original, err := c.codexInput(s, "request-1", "request-1", "", []backend.ImageAttachment{mediaPNG(t)}, nil, st)
	if err != nil {
		t.Fatal(err)
	}
	originalPath := original[0]["path"].(string)
	originalBytes, err := os.ReadFile(originalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer c.cleanupTempImages(st, "request-1")
	_, err = c.steerActiveTurn(s, "r_new_steer", "", []backend.ImageAttachment{mediaPNG(t), {Data: "not-base64!", MediaType: "image/png"}}, nil)
	var staging *imageInputError
	if !errors.Is(err, backend.ErrSteerRejected) || !errors.As(err, &staging) {
		t.Fatalf("not a definite local rejection: %v", err)
	}
	if len(w.methods) != 0 {
		t.Fatalf("partial batch reached native writer: %v", w.methods)
	}
	if !st.turnActive || st.currentTurnID != "turn-1" || st.reqID != "request-1" {
		t.Fatal("rejected steer changed original active turn")
	}
	paths := st.tempImages["request-1"]
	if len(paths) != 1 || paths[0] != originalPath {
		t.Fatalf("original lease changed or partial staging leaked: %v", paths)
	}
	retained, err := os.ReadFile(originalPath)
	if err != nil || !bytes.Equal(retained, originalBytes) {
		t.Fatal("rejected steer removed original accepted image", err)
	}
	entries, err := os.ReadDir(filepath.Dir(originalPath))
	if err != nil || len(entries) != 1 {
		t.Fatalf("partial batch files leaked: %v %v", entries, err)
	}
}
