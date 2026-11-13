package core

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

type recordingFixtureExecutor struct{ *mediaIntakeExecutor }

func (e *recordingFixtureExecutor) SupportsAudioInput(context.Context, *session.Session) bool {
	return true
}

func TestRecordingDescriptorBoundedAndNoImplicitAudioSupport(t *testing.T) {
	item := protocol.MediaBatchItem{UploadKind: "file", Name: "note.wav", MediaType: "audio/wav", SizeBytes: 44}
	if !validMediaDescriptor(item) {
		t.Fatal("bounded recording rejected")
	}
	item.SizeBytes = backend.MediaAudioMaxBytes + 1
	if validMediaDescriptor(item) {
		t.Fatal("oversize accepted")
	}
	item.SizeBytes = 44
	item.MediaType = "audio/unknown"
	if validMediaDescriptor(item) {
		t.Fatal("unknown codec accepted")
	}
	h, s, _, _ := mediaFixture(t)
	if h.supportsAudioInput(s) {
		t.Fatal("executor without native audio support accepted")
	}
	if _, _, e := h.resolveUploadedVideos(s.ID, "", []backend.FileAttachment{{Name: "fake.wav", MediaType: "audio/wav", Content: "pretend"}}); e == nil {
		t.Fatal("inline forged audio accepted")
	}
}

func TestRecordingProbeValidatesFiniteAudioAndRejectsNonAudio(t *testing.T) {
	if _, e := exec.LookPath("ffprobe"); e != nil {
		t.Skip("ffprobe unavailable; real container validation unverified")
	}
	b := make([]byte, 44+32000)
	copy(b, "RIFF")
	binary.LittleEndian.PutUint32(b[4:], uint32(len(b)-8))
	copy(b[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(b[16:], 16)
	binary.LittleEndian.PutUint16(b[20:], 1)
	binary.LittleEndian.PutUint16(b[22:], 1)
	binary.LittleEndian.PutUint32(b[24:], 16000)
	binary.LittleEndian.PutUint32(b[28:], 32000)
	binary.LittleEndian.PutUint16(b[32:], 2)
	binary.LittleEndian.PutUint16(b[34:], 16)
	copy(b[36:], "data")
	binary.LittleEndian.PutUint32(b[40:], 32000)
	path := filepath.Join(t.TempDir(), "one-second.wav")
	if e := os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	ms, e := probeUploadedAudio(path, "audio/wav")
	if e != nil || ms != 1000 {
		t.Fatalf("real WAV probe = %d %v", ms, e)
	}
	if _, err := probeUploadedAudio(path, "audio/mp3"); err == nil {
		t.Fatal("WAV bytes mislabeled as MP3 accepted")
	}
	if e := os.WriteFile(path, []byte("not audio"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = probeUploadedAudio(path, "audio/wav"); e == nil {
		t.Fatal("invalid bytes accepted")
	}
}

func TestRecordingTypedUploadRetainsOriginalReceiptAndNativeAudioReference(t *testing.T) {
	if _, e := exec.LookPath("ffprobe"); e != nil {
		t.Skip("real recording container validation unavailable")
	}
	h, s, c, _ := mediaFixture(t)
	h.SetExecutor(&recordingFixtureExecutor{h.exec.(*mediaIntakeExecutor)})
	data := make([]byte, 44+32000)
	copy(data, "RIFF")
	binary.LittleEndian.PutUint32(data[4:], uint32(len(data)-8))
	copy(data[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(data[16:], 16)
	binary.LittleEndian.PutUint16(data[20:], 1)
	binary.LittleEndian.PutUint16(data[22:], 1)
	binary.LittleEndian.PutUint32(data[24:], 16000)
	binary.LittleEndian.PutUint32(data[28:], 32000)
	binary.LittleEndian.PutUint16(data[32:], 2)
	binary.LittleEndian.PutUint16(data[34:], 16)
	copy(data[36:], "data")
	binary.LittleEndian.PutUint32(data[40:], 32000)
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	revision := s.SettingsSnapshot().ConfigRevision
	item := protocol.MediaBatchItem{ItemID: "recording-1", ItemRevision: 1, UploadRequestID: "vu_synthetic_audio", UploadKind: "file", Name: "voice.wav", MediaType: "audio/wav", SizeBytes: int64(len(data)), SHA256: digest}
	cmd := clientproto.Command{Kind: "media_batch_prepare", RequestID: "prepare-recording", MediaBatchID: "r_synthetic_audio", MediaBatchRevision: 1, SessionID: s.ID, ExpectedNativeThreadID: s.ResumeID(), ExpectedConfigRevision: &revision, MediaItems: []protocol.MediaBatchItem{item}}
	h.handleMediaBatch(c, cmd)
	prepared := waitForType(t, c, "media_batch_receipt")
	if prepared["state"] != "prepared" {
		t.Fatal(prepared)
	}
	body := prepared["body_hash"].(string)
	c.uploads.initCommand(clientproto.Command{SessionID: s.ID, UploadRequestID: item.UploadRequestID, UploadKind: "file", Name: item.Name, MediaType: item.MediaType, SizeBytes: item.SizeBytes, MediaBatchID: cmd.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: body, ItemID: item.ItemID, ItemRevision: 1, SHA256: digest})
	id := waitForType(t, c, "attachment_upload_ready")["upload_id"].(string)
	c.uploads.writeFrame(makeV2AttachmentFrame(id, 0, data))
	waitForType(t, c, "attachment_upload_ack")
	c.uploads.finish(id)
	complete := waitForType(t, c, "attachment_upload_complete")
	if complete["sha256"] != digest || complete["duration_ms"] != float64(1000) {
		t.Fatal(complete)
	}
	text, files, err := h.resolveUploadedVideos(s.ID, "summarize", []backend.FileAttachment{{AttachmentID: id, Name: item.Name, MediaType: item.MediaType}})
	if err != nil || text != "summarize" || len(files) != 1 || files[0].VerifiedAudioPath == "" || files[0].SHA256 != digest {
		t.Fatalf("audio replaced by text or lost: %q %#v %v", text, files, err)
	}
	if _, _, err = h.resolveUploadedVideos("foreign-session", "", []backend.FileAttachment{{AttachmentID: id, MediaType: item.MediaType}}); err == nil {
		t.Fatal("foreign session acquired recording")
	}
	h.handleMediaBatch(c, cmd)
	again := waitForType(t, c, "media_batch_receipt")
	if again["body_hash"] != body || again["state"] != "prepared" {
		t.Fatal("original receipt not retained", again)
	}
}
