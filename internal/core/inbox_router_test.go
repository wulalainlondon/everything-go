package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"everything-go/internal/inbox"
)

// push_file inlines the file, acks the sender, and broadcasts file_push to every
// connected client; a target device's file_push_ack then drains the inbox.
func TestPushFileBroadcastAndAck(t *testing.T) {
	h, _ := newTestHub(t)
	dir := t.TempDir()
	h.SetInbox(inbox.New(dir))

	src := filepath.Join(dir, "report.txt")
	if err := os.WriteFile(src, []byte("daily digest"), 0o644); err != nil {
		t.Fatal(err)
	}

	sender := newDeviceClient(h, "sender", 1024)
	h.addClient(sender)
	h.registerLatest(sender)
	phoneA := newDeviceClient(h, "phoneA", 1024)
	h.addClient(phoneA)
	h.registerLatest(phoneA)

	h.handlePushFile(sender, src)

	ack := waitForType(t, sender, "push_ack")
	fileID, _ := ack["file_id"].(string)
	if fileID == "" {
		t.Fatal("push_ack missing file_id")
	}
	// Both the sender and the target receive the broadcast (the sender is still
	// in h.clients, so Emit reaches it — parity with Python's broadcast_json).
	if fp := waitForType(t, sender, "file_push"); fp["file_id"] != fileID {
		t.Fatal("sender file_push file_id mismatch")
	}
	fpA := waitForType(t, phoneA, "file_push")
	if fpA["file_id"] != fileID {
		t.Fatal("phoneA file_push file_id mismatch")
	}
	if fpA["data"] == nil || fpA["data"].(string) == "" {
		t.Fatal("file_push must carry inline base64 data")
	}

	// phoneA is the only target (sender excluded) → its ack drains the entry.
	h.handleFilePushAck(fileID, "phoneA")
	if p := h.inbox.Pending("phoneA"); len(p) != 0 {
		t.Fatalf("entry should be gone after the sole target acked, got %d", len(p))
	}
}

// A reconnecting device receives its un-acked pushes as file_push frames on hello.
func TestPendingReplayedOnHello(t *testing.T) {
	h, _ := newTestHub(t)
	dir := t.TempDir()
	h.SetInbox(inbox.New(dir))

	src := filepath.Join(dir, "build.apk")
	if err := os.WriteFile(src, []byte("PK\x03\x04 apk"), 0o644); err != nil {
		t.Fatal(err)
	}

	sender := newDeviceClient(h, "sender", 1024)
	h.addClient(sender)
	h.registerLatest(sender)
	// phoneA is targeted but not currently connected when the push happens.
	h.handlePushFile(sender, src)
	ack := waitForType(t, sender, "push_ack")
	fileID := ack["file_id"].(string)

	// Wait, with no phoneA connected the push targets only currently-connected
	// devices (none but sender). So nothing is pending for phoneA — instead test
	// the realistic case: phoneA WAS connected, then reconnects.
	_ = fileID

	// Reconnect phoneA via hello and assert the replay.
	phoneA := newDeviceClient(h, "", 1024) // device set by the hello handler
	h.addClient(phoneA)
	// Manually seed a targeted entry so the replay path is exercised regardless
	// of who was connected at push time.
	if _, err := h.inbox.Push(src, "sender", []string{"phoneA"}); err != nil {
		t.Fatal(err)
	}
	route(h, phoneA, `{"type":"hello","device_id":"phoneA","device_name":"S10"}`)

	fp := waitForType(t, phoneA, "file_push")
	if fp["filename"] != "build.apk" {
		t.Fatalf("hello replay file_push wrong filename: %v", fp["filename"])
	}
}

// get_inbox returns the pending items (with pushed_at) for the asking device.
func TestGetInbox(t *testing.T) {
	h, _ := newTestHub(t)
	dir := t.TempDir()
	h.SetInbox(inbox.New(dir))
	src := filepath.Join(dir, "note.md")
	if err := os.WriteFile(src, []byte("# note"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.Push(src, "sender", []string{"phoneA"}); err != nil {
		t.Fatal(err)
	}

	phoneA := newDeviceClient(h, "phoneA", 1024)
	h.addClient(phoneA)
	h.registerLatest(phoneA)
	route(h, phoneA, `{"type":"get_inbox"}`)

	raw := <-phoneA.send
	var msg map[string]any
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatal(err)
	}
	if msg["type"] != "inbox_list" {
		t.Fatalf("expected inbox_list, got %v", msg["type"])
	}
	items, _ := msg["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 inbox item, got %d", len(items))
	}
	it := items[0].(map[string]any)
	if it["pushed_at"] == nil {
		t.Fatal("inbox_list items must include pushed_at")
	}
}
