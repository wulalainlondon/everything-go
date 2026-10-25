package core

import (
	"os"
	"path/filepath"
	"testing"

	"everything-go/internal/session"
)

func TestNativeHistoryImageRecoveryUsesOriginalJournalAndPerDeviceReplay(t *testing.T) {
	h, _ := newTestHub(t)
	thread := "original-thread"
	s := h.registry.Create("history-images", "Images", "/tmp", "codex", "", "", thread)
	dir := filepath.Join(t.TempDir(), ".codex", "generated_images", thread)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "fixture.png")
	if err := os.WriteFile(p, []byte("png"), 0600); err != nil {
		t.Fatal(err)
	}
	m := map[string]any{"source": "codex", "role": "assistant", "source_session_id": thread, "request_id": "original-request", "generated_image_paths": []string{p}}
	desktop := attachmentClient(h, "desktop-history")
	phone := attachmentClient(h, "phone-history")
	recover := func(msg map[string]any) { h.recoverHistoryGeneratedImages(s, []string{thread}, []map[string]any{msg}) }
	recover(m)
	_, desktopBatch := attachmentFromBatch(t, desktop)
	event, phoneBatch := attachmentFromBatch(t, phone)
	if event["session_id"] != s.ID || event["request_id"] != "original-request" {
		t.Fatal(event)
	}
	ackAttachment(t, h, desktop, desktopBatch)
	if len(h.attachments.Pending("phone-history", s.ID, 10)) != 1 {
		t.Fatal("desktop consumed phone attachment")
	}
	ackAttachment(t, h, phone, phoneBatch)
	cached := map[string]any{}
	for k, v := range m {
		cached[k] = v
	}
	cached["generated_image_paths"] = []any{p}
	recover(cached)
	recover(m)
	assertNoOutbound(t, phone)
	for _, change := range []map[string]any{{"request_id": ""}, {"source_session_id": "foreign-thread"}, {"source": "claude"}, {"role": "user"}, {"generated_image_paths": []string{filepath.Join(dir, "missing.png")}}} {
		copy := map[string]any{}
		for k, v := range m {
			copy[k] = v
		}
		for k, v := range change {
			copy[k] = v
		}
		recover(copy)
	}
	if len(h.attachments.Pending("fresh-device", s.ID, 10)) != 1 {
		t.Fatal("unbound or foreign image admitted")
	}
}

// A generic non-Codex provider cannot use this internal recovery projection.
func TestGeneratedImageRecoveryRejectsOtherBackend(t *testing.T) {
	h, _ := newTestHub(t)
	reg := session.NewRegistry()
	s := reg.Create("other", "Other", "/tmp", "claude", "", "", "thread")
	h.recoverHistoryGeneratedImages(s, []string{"thread"}, nil)
	if len(h.attachments.Pending("phone", s.ID, 10)) != 0 {
		t.Fatal("foreign backend admitted")
	}
}
