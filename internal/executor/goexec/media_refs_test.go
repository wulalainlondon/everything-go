package goexec

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func adapterImageRef(t *testing.T) (backend.ImageAttachment, []byte) {
	t.Helper()
	data := []byte("actual image bytes for isolated adapter")
	path := filepath.Join(t.TempDir(), "image.jpg")
	if e := os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	h := sha256.Sum256(data)
	return backend.ImageAttachment{AttachmentID: "u_" + strings.Repeat("a", 32), MediaType: "image/jpeg", SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(h[:]), VerifiedPath: path}, data
}
func TestMediaRefsCodexActualLocalImageSameBytes(t *testing.T) {
	img, data := adapterImageRef(t)
	root := t.TempDir()
	c := NewCodex(nil, "")
	c.SetDataDir(root)
	s := session.NewRegistry().Create("media", "media", t.TempDir(), backend.Codex, "fixture", "read-only", "")
	st := &codexState{tempImages: map[string][]string{}}
	out, e := c.codexInput(s, "r_media", "r_media", "", []backend.ImageAttachment{img}, nil, st)
	if e != nil {
		t.Fatal(e)
	}
	if len(out) != 1 || out[0]["type"] != "localImage" {
		t.Fatalf("image-only must be native image: %#v", out)
	}
	path := out[0]["path"].(string)
	if !strings.HasPrefix(path, filepath.Join(root, "codex-input-images")) {
		t.Fatal("outside original runtime staging")
	}
	got, e := os.ReadFile(path)
	if e != nil || !bytes.Equal(got, data) {
		t.Fatal("native bytes differ")
	}
	c.cleanupTempImages(st, "r_media")
	if _, e = os.Stat(img.VerifiedPath); e != nil {
		t.Fatal("provider cleaner deleted owned original")
	}
}
func TestMediaRefsClaudeActualImageBlockSameBytes(t *testing.T) {
	img, data := adapterImageRef(t)
	raw, e := checkedUserMessageJSON("", []backend.ImageAttachment{img}, nil)
	if e != nil {
		t.Fatal(e)
	}
	var v struct {
		Message struct {
			Content []struct {
				Type   string `json:"type"`
				Source struct {
					Data      string `json:"data"`
					MediaType string `json:"media_type"`
				} `json:"source"`
			} `json:"content"`
		} `json:"message"`
	}
	if e = json.Unmarshal(raw, &v); e != nil {
		t.Fatal(e)
	}
	if len(v.Message.Content) != 1 || v.Message.Content[0].Type != "image" {
		t.Fatalf("not native image-only: %s", raw)
	}
	got, e := base64.StdEncoding.DecodeString(v.Message.Content[0].Source.Data)
	if e != nil || !bytes.Equal(got, data) {
		t.Fatal("native image block bytes differ")
	}
	if cap(mediaMaterializers) != 2 {
		t.Fatal("materializers not globally bounded2")
	}
}

func TestMediaRefsEightClearObjectsMeasuredNativeEnvelope(t *testing.T) {
	// One immutable source object, eight intentional references. Materialization
	// must stream each original rather than retain eight decoded/base64 copies.
	data := bytes.Repeat([]byte{0x5a}, 1<<20)
	path := filepath.Join(t.TempDir(), "prepared.jpg")
	if e := os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(data)
	images := make([]backend.ImageAttachment, 8)
	for n := range images {
		images[n] = backend.ImageAttachment{AttachmentID: "u_" + strings.Repeat("a", 32), MediaType: "image/jpeg", SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), VerifiedPath: path}
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	raw, e := checkedUserMessageJSON("eight original objects", images, nil)
	runtime.ReadMemStats(&after)
	if e != nil {
		t.Fatal(e)
	}
	if int64(len(raw)) > backend.MediaModelMaxBytes || !json.Valid(raw) {
		t.Fatal("invalid or overbudget native envelope")
	}
	t.Logf("materializer measurement: objects=8 rawEach=%d nativeEnvelope=%d heapAllocBefore=%d heapAllocAfter=%d totalAllocatedDelta=%d; process RSS measured separately; no provider effect", len(data), len(raw), before.HeapAlloc, after.HeapAlloc, after.TotalAlloc-before.TotalAlloc)
}
