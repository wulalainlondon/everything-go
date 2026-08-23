package core

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/session"
)

func TestAttachmentUploadRoundTrip(t *testing.T) {
	dataDir := t.TempDir()
	reg := session.NewRegistry()
	reg.Create("s1", "Video QA", dataDir, "codex", "", "danger-full-access", "")
	h := NewHub(
		reg,
		Config{InstanceID: "i1", InstanceName: "test", DataDir: dataDir},
		governance.NewPairing(filepath.Join(dataDir, "pairing.json")),
		0,
	)
	c := newTestClient(h)
	c.deviceID = "note20"
	c.uploads = newAttachmentUploads(c, dataDir)
	payload := append([]byte("\x00\x00\x00\x18ftypmp42"), []byte("video bytes for upload")...)

	c.uploads.init("s1", "req1", "Screen Recording.mp4", "video/mp4", int64(len(payload)))
	ready := waitForType(t, c, "attachment_upload_ready")
	id, _ := ready["upload_id"].(string)
	if len(id) != attachmentIDBytes {
		t.Fatalf("upload ID length=%d, want %d", len(id), attachmentIDBytes)
	}
	frame := append([]byte(attachmentMagicV1+id), payload...)
	if !c.uploads.writeFrame(frame) {
		t.Fatal("binary frame was not recognized")
	}
	c.uploads.finish(id)
	complete := waitForType(t, c, "attachment_upload_complete")
	remote, _ := complete["remote_path"].(string)
	got, err := os.ReadFile(remote)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch: %q", got)
	}
	wantHash := sha256.Sum256(payload)
	if complete["sha256"] != hex.EncodeToString(wantHash[:]) {
		t.Fatalf("sha256 mismatch: %v", complete["sha256"])
	}

	content, files, err := h.resolveUploadedVideos(
		"s1",
		"inspect this",
		[]backend.FileAttachment{{Name: "Screen Recording.mp4", RemotePath: remote, MediaType: "video/mp4"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 || !strings.Contains(content, remote) {
		t.Fatalf("resolved content/files invalid: %q %#v", content, files)
	}
	manifestRaw, err := os.ReadFile(filepath.Join(filepath.Dir(remote), "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest uploadedVideoManifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SessionID != "s1" || manifest.SizeBytes != int64(len(payload)) {
		t.Fatalf("bad manifest: %#v", manifest)
	}
}

func TestAttachmentUploadResumesAfterDisconnectWithOffsetAck(t *testing.T) {
	dataDir := t.TempDir()
	reg := session.NewRegistry()
	reg.Create("s1", "Video QA", dataDir, "codex", "", "danger-full-access", "")
	h := NewHub(
		reg,
		Config{InstanceID: "i1", InstanceName: "test", DataDir: dataDir},
		governance.NewPairing(filepath.Join(dataDir, "pairing.json")),
		0,
	)
	payload := append([]byte("\x00\x00\x00\x18ftypmp42"), []byte(strings.Repeat("resumable-video-", 4096))...)
	split := len(payload) / 2

	first := newTestClient(h)
	first.deviceID = "note20"
	first.uploads = newAttachmentUploads(first, dataDir)
	first.uploads.init("s1", "stable-request", "recording.mp4", "video/mp4", int64(len(payload)))
	ready := waitForType(t, first, "attachment_upload_ready")
	id, _ := ready["upload_id"].(string)
	if eventInt64(ready["protocol_version"]) != 2 || eventInt64(ready["received_bytes"]) != 0 {
		t.Fatalf("unexpected initial ready: %#v", ready)
	}

	frame := makeV2AttachmentFrame(id, 0, payload[:split])
	if !first.uploads.writeFrame(frame) {
		t.Fatal("CBV2 frame was not recognized")
	}
	ack := waitForType(t, first, "attachment_upload_ack")
	if eventInt64(ack["received_bytes"]) != int64(split) {
		t.Fatalf("ack offset=%v, want %d", ack["received_bytes"], split)
	}
	first.uploads.close()

	second := newTestClient(h)
	second.deviceID = "note20"
	second.uploads = newAttachmentUploads(second, dataDir)
	second.uploads.init("s1", "stable-request", "recording.mp4", "video/mp4", int64(len(payload)))
	resumed := waitForType(t, second, "attachment_upload_ready")
	if resumed["upload_id"] != id || eventInt64(resumed["received_bytes"]) != int64(split) {
		t.Fatalf("resume mismatch: %#v", resumed)
	}

	// A duplicate/reordered chunk must not be appended twice; the server returns
	// its authoritative cumulative offset so the client can recover.
	second.uploads.writeFrame(makeV2AttachmentFrame(id, 0, payload[:split]))
	duplicateAck := waitForType(t, second, "attachment_upload_ack")
	if eventInt64(duplicateAck["received_bytes"]) != int64(split) {
		t.Fatalf("duplicate ack offset=%v, want %d", duplicateAck["received_bytes"], split)
	}

	second.uploads.writeFrame(makeV2AttachmentFrame(id, int64(split), payload[split:]))
	finalAck := waitForType(t, second, "attachment_upload_ack")
	if eventInt64(finalAck["received_bytes"]) != int64(len(payload)) {
		t.Fatalf("final ack offset=%v, want %d", finalAck["received_bytes"], len(payload))
	}
	second.uploads.finish(id)
	complete := waitForType(t, second, "attachment_upload_complete")
	remote, _ := complete["remote_path"].(string)
	got, err := os.ReadFile(remote)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatal("resumed payload mismatch")
	}
}

func TestAttachmentUploadInitIsIdempotentOnSameConnection(t *testing.T) {
	dataDir := t.TempDir()
	reg := session.NewRegistry()
	reg.Create("s1", "Video QA", dataDir, "codex", "", "", "")
	h := NewHub(
		reg,
		Config{DataDir: dataDir},
		governance.NewPairing(filepath.Join(dataDir, "pairing.json")),
		0,
	)
	c := newTestClient(h)
	c.deviceID = "note20"
	c.uploads = newAttachmentUploads(c, dataDir)
	c.uploads.init("s1", "req", "recording.mp4", "video/mp4", 100)
	first := waitForType(t, c, "attachment_upload_ready")
	c.uploads.init("s1", "req", "recording.mp4", "video/mp4", 100)
	second := waitForType(t, c, "attachment_upload_ready")
	if first["upload_id"] != second["upload_id"] || len(c.uploads.active) != 1 {
		t.Fatalf("init was not idempotent: first=%#v second=%#v", first, second)
	}
}

func TestAttachmentUploadCleansStalePartialState(t *testing.T) {
	dataDir := t.TempDir()
	staleDir := filepath.Join(dataDir, "uploads", "old-session", "u_stale")
	if err := os.MkdirAll(staleDir, 0o700); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(staleDir, "upload-state.json")
	if err := os.WriteFile(statePath, []byte(`{"version":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	staleTime := time.Now().Add(-staleUploadMaxAge - time.Hour)
	if err := os.Chtimes(statePath, staleTime, staleTime); err != nil {
		t.Fatal(err)
	}

	reg := session.NewRegistry()
	reg.Create("s1", "Video QA", dataDir, "codex", "", "", "")
	h := NewHub(
		reg,
		Config{DataDir: dataDir},
		governance.NewPairing(filepath.Join(dataDir, "pairing.json")),
		0,
	)
	c := newTestClient(h)
	c.deviceID = "note20"
	c.uploads = newAttachmentUploads(c, dataDir)
	c.uploads.init("s1", "fresh", "recording.mp4", "video/mp4", 100)
	_ = waitForType(t, c, "attachment_upload_ready")

	if _, err := os.Stat(staleDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale upload dir still exists: %v", err)
	}
}

func makeV2AttachmentFrame(id string, offset int64, payload []byte) []byte {
	header := make([]byte, len(attachmentMagicV2)+attachmentIDBytes+attachmentOffsetBytes)
	copy(header, attachmentMagicV2)
	copy(header[len(attachmentMagicV2):], id)
	binary.BigEndian.PutUint64(header[len(header)-attachmentOffsetBytes:], uint64(offset))
	return append(header, payload...)
}

func eventInt64(value any) int64 {
	switch n := value.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	default:
		return -1
	}
}

func TestAttachmentUploadRejectsUnsupportedFile(t *testing.T) {
	dataDir := t.TempDir()
	reg := session.NewRegistry()
	reg.Create("s1", "Video QA", dataDir, "codex", "", "", "")
	h := NewHub(
		reg,
		Config{DataDir: dataDir},
		governance.NewPairing(filepath.Join(dataDir, "pairing.json")),
		0,
	)
	c := newTestClient(h)
	c.uploads = newAttachmentUploads(c, dataDir)
	c.uploads.init("s1", "req", "payload.exe", "application/octet-stream", 10)
	event := waitForType(t, c, "attachment_upload_error")
	if event["message"] != "Unsupported video format" {
		t.Fatalf("unexpected error: %v", event)
	}
}
