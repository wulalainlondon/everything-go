package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"image"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"github.com/coder/websocket"
)

func mediaFixture(t *testing.T) (*Hub, *session.Session, *Client, chan mediaIntakeInvocation) {
	t.Helper()
	root := t.TempDir()
	pair := governance.NewPairing(filepath.Join(root, "pair.json"))
	if e := pair.Claim("mua-token", "phone-a"); e != nil {
		t.Fatal(e)
	}
	h := NewHub(session.NewRegistry(), Config{InstanceID: "mua", RootDir: root, DataDir: root}, pair, 0)
	s := h.registry.Create("s_mua", "Media", root, backend.Codex, "fixture-model", "read-only", "")
	c := newTestClient(h)
	c.deviceID = "phone-a"
	h.registerLatest(c)
	c.ctx = context.Background()
	c.readIdentity.Store(&pairedReadIdentity{token: "mua-token", deviceID: "phone-a"})
	c.pinTaskSchema(contract.MediaHash())
	c.uploads = newAttachmentUploads(c, root)
	starts := make(chan mediaIntakeInvocation, 10)
	release := make(chan struct{})
	h.SetExecutor(&mediaIntakeExecutor{fakeExec: &fakeExec{sink: h}, starts: starts, release: release})
	t.Cleanup(func() {
		close(release)
		c.uploads.close()
		s.CloseAndWait()
		h.messageQueue.Close()
		h.delegations.Close()
		h.dispatches.Close()
	})
	return h, s, c, starts
}
func mediaPNG(t *testing.T) []byte {
	t.Helper()
	var b bytes.Buffer
	if e := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, 7, 9))); e != nil {
		t.Fatal(e)
	}
	return b.Bytes()
}
func mediaPrepareItem(t *testing.T, h *Hub, s *session.Session, c *Client, id string, data []byte) (clientproto.Command, protocol.MediaBatchItem, string) {
	t.Helper()
	hash := sha256.Sum256(data)
	item := protocol.MediaBatchItem{ItemID: "item-1", ItemRevision: 1, UploadRequestID: "vu_" + id, UploadKind: "image", Name: "圖片.png", MediaType: "image/png", SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}
	rev := s.SettingsSnapshot().ConfigRevision
	cmd := clientproto.Command{Kind: "media_batch_prepare", RequestID: "prepare-" + id, MediaBatchID: id, MediaBatchRevision: 1, SessionID: s.ID, ExpectedConfigRevision: &rev, MediaItems: []protocol.MediaBatchItem{item}}
	h.handleMediaBatch(c, cmd)
	receipt := waitForType(t, c, "media_batch_receipt")
	if receipt["state"] != "prepared" {
		t.Fatal(receipt)
	}
	return cmd, item, receipt["body_hash"].(string)
}
func mediaUploadItem(t *testing.T, h *Hub, s *session.Session, c *Client, cmd clientproto.Command, item protocol.MediaBatchItem, body string, data []byte) string {
	t.Helper()
	c.uploads.initCommand(clientproto.Command{SessionID: s.ID, UploadRequestID: item.UploadRequestID, UploadKind: item.UploadKind, Name: item.Name, MediaType: item.MediaType, SizeBytes: item.SizeBytes, MediaBatchID: cmd.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: body, ItemID: item.ItemID, ItemRevision: item.ItemRevision, SHA256: item.SHA256})
	ready := waitForType(t, c, "attachment_upload_ready")
	id := ready["upload_id"].(string)
	c.uploads.writeFrame(makeV2AttachmentFrame(id, 0, data))
	ack := waitForType(t, c, "attachment_upload_ack")
	if ack["item_id"] != item.ItemID || ack["received_bytes"] != float64(len(data)) {
		t.Fatal(ack)
	}
	c.uploads.finish(id)
	complete := waitForType(t, c, "attachment_upload_complete")
	if complete["sha256"] != item.SHA256 || complete["media_batch_id"] != cmd.MediaBatchID || complete["width"] != float64(7) || complete["height"] != float64(9) {
		t.Fatal(complete)
	}
	return id
}
func TestMediaUnificationTypedOrdinaryAdmissionNativeImageRef(t *testing.T) {
	h, s, c, starts := mediaFixture(t)
	data := mediaPNG(t)
	cmd, item, body := mediaPrepareItem(t, h, s, c, "r_"+strings.Repeat("a", 32), data)
	id := mediaUploadItem(t, h, s, c, cmd, item, body, data)
	raw, _ := json.Marshal(map[string]any{"type": "message", "request_id": cmd.MediaBatchID, "session_id": s.ID, "media_batch_id": cmd.MediaBatchID, "content": "", "images": []any{map[string]any{"attachment_id": id, "media_type": item.MediaType, "sha256": item.SHA256, "size_bytes": item.SizeBytes}}})
	route(h, c, string(raw))
	waitForType(t, c, "message_ack")
	select {
	case got := <-starts:
		if got.content != "" || len(got.images) != 1 || got.images[0].Data != "" || got.images[0].VerifiedPath == "" {
			t.Fatalf("not exact image-only ref: %#v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("native adapter not reached")
	}
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel-after-admit", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID, UploadRequestID: item.UploadRequestID})
	if got := waitForType(t, c, "media_batch_receipt"); got["state"] != "admitted" {
		t.Fatal(got)
	}
}
func TestMediaUnificationCancelBeforeInitAndFinishPins(t *testing.T) {
	for _, phase := range []string{"before-init", "after-finish"} {
		t.Run(phase, func(t *testing.T) {
			h, s, c, _ := mediaFixture(t)
			data := mediaPNG(t)
			cmd, item, body := mediaPrepareItem(t, h, s, c, "r_"+strings.Repeat("b", 32), data)
			var object string
			if phase == "after-finish" {
				object = mediaUploadItem(t, h, s, c, cmd, item, body, data)
			}
			cancel := clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel-original", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID, UploadRequestID: item.UploadRequestID}
			h.handleMediaBatch(c, cancel)
			got := waitForType(t, c, "media_batch_receipt")
			if got["state"] != "cancelled_before_admission" || got["known_effect"] != "none" {
				t.Fatal(got)
			}
			h.handleMediaBatch(c, cancel)
			if got = waitForType(t, c, "media_batch_receipt"); got["cancel_request_id"] != "cancel-original" {
				t.Fatal(got)
			}
			c.uploads.initCommand(clientproto.Command{SessionID: s.ID, UploadRequestID: item.UploadRequestID, UploadKind: item.UploadKind, Name: item.Name, MediaType: item.MediaType, SizeBytes: item.SizeBytes, MediaBatchID: cmd.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: body, ItemID: item.ItemID, ItemRevision: 1, SHA256: item.SHA256})
			waitForType(t, c, "attachment_upload_error")
			if object != "" {
				m, e := h.unifiedUpload(s.ID, object)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = os.Stat(m.Path); e != nil {
					t.Fatal("cancel deleted owned object")
				}
			}
			reopened := openMediaLifecycle(h.cfg.DataDir)
			if reopened.err != nil {
				t.Fatal(reopened.err)
			}
			if len(reopened.data.Batches) != 1 {
				t.Fatal("cancel tombstone lost")
			}
		})
	}
}
func TestMediaUnificationCancelBeforePrepareCannotResurrect(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel-early", SessionID: s.ID, MediaBatchID: "r_early", UploadRequestID: "vu_original"})
	if got := waitForType(t, c, "media_batch_receipt"); got["state"] != "cancelled_before_admission" {
		t.Fatal(got)
	}
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_prepare", RequestID: "prepare-late", SessionID: s.ID, MediaBatchID: "r_early"})
	if got := waitForType(t, c, "media_batch_receipt"); got["state"] != "cancelled_before_admission" {
		t.Fatal(got)
	}
	if b := h.media.data.Batches[mediaKey("phone-a", jarvisTokenGeneration("mua-token"), "r_early")]; b == nil || b.State != "cancelled_before_admission" {
		t.Fatal("early fence lost")
	}
}
func TestMediaUnificationSourceAndOwnerFences(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	data := mediaPNG(t)
	cmd, item, body := mediaPrepareItem(t, h, s, c, "r_fences", data)
	other := sharedReadClient(t, h, "phone-b")
	other.ctx = context.Background()
	other.uploads = newAttachmentUploads(other, h.cfg.DataDir)
	other.uploads.initCommand(clientproto.Command{SessionID: s.ID, MediaBatchID: cmd.MediaBatchID, UploadRequestID: item.UploadRequestID})
	waitForType(t, other, "attachment_upload_error")
	s.SetResumeID("different-thread")
	c.uploads.initCommand(clientproto.Command{SessionID: s.ID, UploadRequestID: item.UploadRequestID, UploadKind: item.UploadKind, Name: item.Name, MediaType: item.MediaType, SizeBytes: item.SizeBytes, MediaBatchID: cmd.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: body, ItemID: item.ItemID, ItemRevision: 1, SHA256: item.SHA256})
	waitForType(t, c, "attachment_upload_error")
	if len(c.uploads.active) != 0 {
		t.Fatal("changed source acquired upload")
	}
}
func TestMediaUnificationCrossDeviceHistoryReadNotMutation(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	data := mediaPNG(t)
	cmd, item, body := mediaPrepareItem(t, h, s, c, "r_history", data)
	id := mediaUploadItem(t, h, s, c, cmd, item, body, data)
	actor, e := h.mediaActor(c)
	if e != nil {
		t.Fatal(e)
	}
	images, e := h.resolveMediaImages(s.ID, []backend.ImageAttachment{{AttachmentID: id, MediaType: item.MediaType, SHA256: item.SHA256, SizeBytes: item.SizeBytes}})
	if e != nil {
		t.Fatal(e)
	}
	done, e := h.media.begin()
	if e != nil {
		t.Fatal(e)
	}
	b, e := h.mediaBeginAdmission(actor, s.ID, cmd.MediaBatchID, cmd.MediaBatchID, "", images, nil)
	if e == nil {
		e = h.mediaAdmitted(b)
	}
	done()
	if e != nil {
		t.Fatal(e)
	}
	other := sharedReadClient(t, h, "phone-b")
	_ = other
	req := httptest.NewRequest("GET", "/api/jarvis/v1/media/attachment?session_id="+s.ID+"&attachment_id="+id+"&sha256="+item.SHA256, nil)
	req.Header.Set("Authorization", "Bearer qa-read-phone-b")
	req.Header.Set("X-Bridge-Device-ID", "phone-b")
	out := httptest.NewRecorder()
	h.ServeJarvisAPI(out, req)
	if out.Code != http.StatusOK || !bytes.Equal(out.Body.Bytes(), data) {
		t.Fatalf("scoped second device read failed %d %s", out.Code, out.Body.String())
	}
	if len(h.media.data.Readers) != 0 {
		t.Fatal("terminal reader not released")
	}
	req.Header.Set("Authorization", "Bearer revoked")
	out = httptest.NewRecorder()
	h.ServeJarvisAPI(out, req)
	if out.Code != 403 {
		t.Fatal("revoked reader accepted")
	}
	m, _ := h.unifiedUpload(s.ID, id)
	if !h.TypedMediaPathPrivate(m.Path) {
		t.Fatal("raw private upload exposed")
	}
}
func TestMediaUnificationGCGraceAndConcurrentPins(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	data := mediaPNG(t)
	cmd, item, body := mediaPrepareItem(t, h, s, c, "r_gc", data)
	id := mediaUploadItem(t, h, s, c, cmd, item, body, data)
	m, _ := h.unifiedUpload(s.ID, id)
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel-gc", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID, UploadRequestID: item.UploadRequestID})
	waitForType(t, c, "media_batch_receipt")
	if e := h.collectMediaGarbage(time.Now()); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(m.Path); e != nil {
		t.Fatal("before24h deletion")
	}
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() { defer wg.Done(); _ = h.collectMediaGarbage(time.Now().Add(25 * time.Hour)) }()
	}
	wg.Wait()
	if _, e := os.Stat(m.Path); !os.IsNotExist(e) {
		t.Fatal("cancelled unreferenced object not collected")
	}
	if len(h.media.data.GC) != 1 {
		t.Fatal("durable tombstone missing")
	}
}
func TestMediaUnificationNewVideoAllEntrancesAndHeader(t *testing.T) {
	for _, kind := range []string{"video", "file"} {
		t.Run(kind, func(t *testing.T) {
			_, s, c, _ := mediaFixture(t)
			c.uploads.initKind(s.ID, "fresh-large", "movie.mov", "video/quicktime", mediaVideoMaxBytes+1, kind)
			if e := waitForType(t, c, "attachment_upload_error"); !strings.Contains(e["message"].(string), "1 GiB") {
				t.Fatal(e)
			}
		})
	}
	if mediaPrefixKind([]byte("\x00\x00\x00\x18ftypmp42")) != "video" || mediaPrefixKind([]byte("RIFF0000AVI ")) != "video" || mediaPrefixKind([]byte{0x1a, 0x45, 0xdf, 0xa3}) != "video" {
		t.Fatal("video corpus disguise detection")
	}
	_, s, c, _ := mediaFixture(t)
	c.uploads.initKind(s.ID, "disguised", "payload.bin", "application/octet-stream", mediaVideoMaxBytes+1, "file")
	ready := waitForType(t, c, "attachment_upload_ready")
	c.uploads.writeFrame(makeV2AttachmentFrame(ready["upload_id"].(string), 0, []byte("\x00\x00\x00\x18ftypmp42")))
	waitForType(t, c, "attachment_upload_error")
}
func TestMediaUnificationRealWSOldHelloNewRefsIntake(t *testing.T) {
	h, s, _, starts := mediaFixture(t)
	s.SetResumeID("native-original-thread")
	h.latestMu.Lock()
	delete(h.latestByDevice, "phone-a")
	h.latestMu.Unlock()
	conn, ctx, close := dialWS(t, h)
	defer close()
	send := func(v any) {
		raw, _ := json.Marshal(v)
		if e := conn.Write(ctx, websocket.MessageText, raw); e != nil {
			t.Fatal(e)
		}
	}
	receive := func(kind string) map[string]any {
		for {
			v := readEvent(t, ctx, conn)
			if v["type"] == kind {
				return v
			}
		}
	}
	send(map[string]any{"type": "hello", "auth_token": "mua-token", "device_id": "phone-a", "task_api_schema_hash": contract.MediaHash()})
	receive("hello_ack")
	send(map[string]any{"type": "media_batch_capabilities", "session_id": s.ID, "request_id": "caps"})
	cap := receive("media_batch_receipt")
	if cap["task_api_schema_hash"] != contract.MediaRefsHash() || cap["available"] != true {
		t.Fatal(cap)
	}
	target := cap["target"].(map[string]any)
	data := mediaPNG(t)
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	request := "r_" + strings.Repeat("c", 32)
	item := map[string]any{"item_id": "first", "item_revision": 1, "upload_request_id": "vu_first", "upload_kind": "image", "name": "clear.png", "media_type": "image/png", "size_bytes": len(data), "sha256": hash}
	send(map[string]any{"type": "media_batch_prepare", "request_id": "prep", "session_id": s.ID, "media_batch_id": request, "media_batch_revision": 1, "expected_native_thread_id": target["expected_native_thread_id"], "expected_config_revision": target["expected_config_revision"], "content": "", "items": []any{item}})
	receipt := receive("media_batch_receipt")
	if receipt["state"] != "prepared" {
		t.Fatal(receipt)
	}
	send(map[string]any{"type": "attachment_upload_init", "session_id": s.ID, "media_batch_id": request, "media_batch_revision": 1, "media_body_hash": receipt["body_hash"], "item_id": "first", "item_revision": 1, "upload_request_id": "vu_first", "upload_kind": "image", "name": "clear.png", "media_type": "image/png", "size_bytes": len(data), "sha256": hash, "upload_protocol": 2})
	ready := receive("attachment_upload_ready")
	id := ready["upload_id"].(string)
	if e := conn.Write(ctx, websocket.MessageBinary, makeV2AttachmentFrame(id, 0, data)); e != nil {
		t.Fatal(e)
	}
	receive("attachment_upload_ack")
	send(map[string]any{"type": "attachment_upload_finish", "upload_id": id})
	receive("attachment_upload_complete")
	input := map[string]any{"action": "submit", "submission_request_id": request, "target": target, "goal": "", "media_batch_id": request, "images": []any{map[string]any{"attachment_id": id, "media_type": "image/png", "size_bytes": len(data), "sha256": hash}}}
	send(map[string]any{"type": "media_task_api_request", "schema_hash": contract.MediaRefsHash(), "request": map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "native-media", "operation": "intake", "idempotency_key": request, "input": input}})
	out := receive("task_api_response")
	if out["ok"] != true {
		t.Fatal(out)
	}
	select {
	case got := <-starts:
		if len(got.images) != 1 || got.images[0].VerifiedPath == "" || got.content != "" {
			t.Fatal(got)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("native Send not reached")
	}
	send(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "old-still-valid", "operation": "capabilities", "input": map[string]any{}})
	old := receive("task_api_response")
	if old["ok"] != true || old["result"].(map[string]any)["schema_hash"] != contract.MediaHash() {
		t.Fatal("old immutable hello selector changed", old)
	}
}

// Explicit opt-in loopback-only fixture for the App serializer. It uses a fresh
// temporary pairing/store and a fake executor; no live service or provider.
func TestMediaUnificationUIHarness(t *testing.T) {
	dir := os.Getenv("MUA_HARNESS_DIR")
	if dir == "" {
		t.Skip("explicit isolated harness directory required")
	}
	if !filepath.IsAbs(dir) {
		t.Fatal("absolute harness output required")
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	h, s, _, starts := mediaFixture(t)
	h.latestMu.Lock()
	delete(h.latestByDevice, "phone-a")
	h.latestMu.Unlock()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.ServeWS)
	mux.HandleFunc("/api/jarvis/v1/", h.ServeJarvisAPI)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	data := mediaPNG(t)
	sum := sha256.Sum256(data)
	_ = os.WriteFile(filepath.Join(dir, "fixture.png"), data, 0600)
	raw, _ := json.MarshalIndent(map[string]any{"isolated": true, "http_url": srv.URL, "ws_url": "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws", "device_id": "phone-a", "auth_token": "mua-token", "session_id": s.ID, "authority_instance_id": h.cfg.InstanceID, "old_schema_hash": contract.MediaHash(), "media_schema_hash": contract.MediaRefsHash(), "fixture_sha256": hex.EncodeToString(sum[:]), "fixture_size": len(data), "data_dir": h.cfg.DataDir, "maximum_lifetime_seconds": 600}, "", "  ")
	if e := os.WriteFile(filepath.Join(dir, "runtime.json"), raw, 0600); e != nil {
		t.Fatal(e)
	}
	deadline := time.NewTimer(600 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case got := <-starts:
			b, _ := json.Marshal(map[string]any{"request_id": got.request, "content": got.content, "images": got.images, "files": got.files, "fake_executor": true})
			f, e := os.OpenFile(filepath.Join(dir, "native-invocations.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if e != nil {
				t.Fatal(e)
			}
			_, e = f.Write(append(b, '\n'))
			f.Close()
			if e != nil {
				t.Fatal(e)
			}
		case <-tick.C:
			if _, e := os.Stat(filepath.Join(dir, "stop")); e == nil {
				return
			}
		case <-deadline.C:
			return
		}
	}
}

func TestMediaUnificationFragmentedPrefixAndCompleteReplay(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	data := mediaPNG(t)
	cmd, item, body := mediaPrepareItem(t, h, s, c, "r_fragments", data)
	init := clientproto.Command{SessionID: s.ID, UploadRequestID: item.UploadRequestID, UploadKind: item.UploadKind, Name: item.Name, MediaType: item.MediaType, SizeBytes: item.SizeBytes, MediaBatchID: cmd.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: body, ItemID: item.ItemID, ItemRevision: 1, SHA256: item.SHA256}
	c.uploads.initCommand(init)
	ready := waitForType(t, c, "attachment_upload_ready")
	id := ready["upload_id"].(string)
	for offset := 0; offset < len(data); {
		end := offset + 3
		if end > len(data) {
			end = len(data)
		}
		c.uploads.writeFrame(makeV2AttachmentFrame(id, int64(offset), data[offset:end]))
		waitForType(t, c, "attachment_upload_ack")
		offset = end
	}
	c.uploads.finish(id)
	waitForType(t, c, "attachment_upload_complete")
	c.uploads.initCommand(init)
	replay := waitForType(t, c, "attachment_upload_complete")
	if replay["media_batch_id"] != cmd.MediaBatchID || replay["sha256"] != item.SHA256 {
		t.Fatal(replay)
	}
}
func TestMediaUnificationOriginalLegacyPendingWitnessSurvivesAdvancingPrefix(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	id := resumableAttachmentID(c.deviceID, "old-original")
	dir := filepath.Join(h.media.root, attachmentSafe(s.ID, "session"), id)
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	prefix := []byte("\x00\x00\x00\x18ftypmp42")
	part := filepath.Join(dir, ".old.mp4.part")
	if e := os.WriteFile(part, prefix, 0600); e != nil {
		t.Fatal(e)
	}
	state := resumableUploadState{Version: 2, UploadID: id, RequestID: "old-original", SessionID: s.ID, DeviceID: c.deviceID, Name: "old.mp4", MediaType: "video/mp4", ExpectedSize: mediaVideoMaxBytes + 1, ReceivedSize: int64(len(prefix)), PartPath: part, FinalPath: filepath.Join(dir, "old.mp4")}
	sp := filepath.Join(dir, "upload-state.json")
	if e := mediaAtomicJSON(sp, state); e != nil {
		t.Fatal(e)
	}
	// The synthetic pre-policy image is captured by server startup, not by a
	// client flag. A fresh post-policy intent in the same directory has no right.
	if e := os.Remove(h.media.path()); e != nil {
		t.Fatal(e)
	}
	h.media = openMediaLifecycle(h.cfg.DataDir)
	h.media.captureCredentialEra(h.pairing)
	if h.media.err != nil {
		t.Fatal(h.media.err)
	}
	c.uploads.init(s.ID, "old-original", "old.mp4", "video/mp4", state.ExpectedSize)
	r := waitForType(t, c, "attachment_upload_ready")
	if r["upload_id"] != id {
		t.Fatal(r)
	}
	c.uploads.writeFrame(makeV2AttachmentFrame(id, int64(len(prefix)), []byte("more-original")))
	waitForType(t, c, "attachment_upload_ack")
	c.uploads.close()
	c.uploads.init(s.ID, "old-original", "old.mp4", "video/mp4", state.ExpectedSize)
	r = waitForType(t, c, "attachment_upload_ready")
	if eventInt64(r["received_bytes"]) <= int64(len(prefix)) {
		t.Fatal(r)
	}
	c.uploads.close()
	// Same length rewrite of the captured old prefix must not inherit rights.
	f, _ := os.OpenFile(part, os.O_WRONLY, 0600)
	_, _ = f.WriteAt([]byte("evil"), 0)
	_ = f.Close()
	c.uploads.init(s.ID, "old-original", "old.mp4", "video/mp4", state.ExpectedSize)
	waitForType(t, c, "attachment_upload_error")
	c.uploads.init(s.ID, "fresh-client-legacy-flag", "old.mp4", "video/mp4", state.ExpectedSize)
	waitForType(t, c, "attachment_upload_error")
}
func TestMediaUnificationAdmissionCrashAndOriginalReceipt(t *testing.T) {
	for _, phase := range []string{"prepare_before_queue", "queue_commit_before_receipt"} {
		t.Run(phase, func(t *testing.T) {
			h, s, c, _ := mediaFixture(t)
			data := mediaPNG(t)
			cmd, item, body := mediaPrepareItem(t, h, s, c, "r_crash", data)
			id := mediaUploadItem(t, h, s, c, cmd, item, body, data)
			h.media.fault = func(stage string) error {
				if stage == phase {
					return errors.New("isolated power loss")
				}
				return nil
			}
			raw, _ := json.Marshal(map[string]any{"type": "message", "request_id": cmd.MediaBatchID, "session_id": s.ID, "media_batch_id": cmd.MediaBatchID, "content": "", "images": []any{map[string]any{"attachment_id": id, "media_type": item.MediaType, "sha256": item.SHA256, "size_bytes": item.SizeBytes}}})
			route(h, c, string(raw))
			waitForType(t, c, "error")
			h.media.fault = nil
			h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_status", RequestID: "original-query", MediaBatchID: cmd.MediaBatchID, SessionID: s.ID})
			got := waitForType(t, c, "media_batch_receipt")
			expected := "admitting_unknown"
			if phase == "queue_commit_before_receipt" {
				expected = "admitted"
			}
			if got["state"] != expected {
				t.Fatal(got)
			}
			h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel-uncertain", MediaBatchID: cmd.MediaBatchID, SessionID: s.ID, UploadRequestID: item.UploadRequestID})
			got = waitForType(t, c, "media_batch_receipt")
			if got["state"] != expected {
				t.Fatal("cancel downgraded original", got)
			}
			if e := h.collectMediaGarbage(time.Now().Add(48 * time.Hour)); e != nil {
				t.Fatal(e)
			}
			m, e := h.unifiedUpload(s.ID, id)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = os.Stat(m.Path); e != nil {
				t.Fatal("unknown/admitted deleted")
			}
		})
	}
}
func TestMediaUnificationGCCrashReaderAndReplacement(t *testing.T) {
	for _, phase := range []string{"gc_tombstone_before_unlink", "gc_unlink_before_receipt"} {
		t.Run(phase, func(t *testing.T) {
			h, s, c, _ := mediaFixture(t)
			data := mediaPNG(t)
			cmd, item, body := mediaPrepareItem(t, h, s, c, "r_gc_crash", data)
			id := mediaUploadItem(t, h, s, c, cmd, item, body, data)
			h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID, UploadRequestID: item.UploadRequestID})
			waitForType(t, c, "media_batch_receipt")
			now := time.Now().Add(25 * time.Hour)
			h.media.fault = func(stage string) error {
				if stage == phase {
					return errors.New("crash")
				}
				return nil
			}
			if e := h.collectMediaGarbage(now); e == nil {
				t.Fatal("fault not reached")
			}
			h.media.fault = nil
			if e := h.collectMediaGarbage(now); e != nil {
				t.Fatal(e)
			}
			if h.media.data.GC[id].State != "unlinked" {
				t.Fatal(h.media.data.GC[id])
			}
		})
	}
	h, s, c, _ := mediaFixture(t)
	data := mediaPNG(t)
	cmd, item, body := mediaPrepareItem(t, h, s, c, "r_partial_gc", data)
	init := clientproto.Command{SessionID: s.ID, UploadRequestID: item.UploadRequestID, UploadKind: item.UploadKind, Name: item.Name, MediaType: item.MediaType, SizeBytes: item.SizeBytes, MediaBatchID: cmd.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: body, ItemID: item.ItemID, ItemRevision: 1, SHA256: item.SHA256}
	c.uploads.initCommand(init)
	r := waitForType(t, c, "attachment_upload_ready")
	id := r["upload_id"].(string)
	c.uploads.writeFrame(makeV2AttachmentFrame(id, 0, data[:4]))
	waitForType(t, c, "attachment_upload_ack")
	part := c.uploads.active[id].partPath
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID, UploadRequestID: item.UploadRequestID})
	waitForType(t, c, "media_batch_receipt")
	done, e := h.media.begin()
	if e != nil {
		t.Fatal(e)
	}
	h.media.data.Readers["held-history"] = mediaReadLease{ObjectID: id}
	e = h.media.save()
	done()
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now().Add(25 * time.Hour)
	if e = h.collectMediaGarbage(now); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(part); e != nil {
		t.Fatal("reader lease ignored")
	}
	done, e = h.media.begin()
	if e != nil {
		t.Fatal(e)
	}
	delete(h.media.data.Readers, "held-history")
	e = h.media.save()
	done()
	if e != nil {
		t.Fatal(e)
	}
	if e = h.collectMediaGarbage(now); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(part); !os.IsNotExist(e) {
		t.Fatal("closed partial not reclaimed", e)
	}
}
func TestMediaUnificationControlRawLimitAndAmbiguousJSON(t *testing.T) {
	h, _, c, _ := mediaFixture(t)
	for _, raw := range []string{`{"type":"media_batch_prepare","request_id":"a","request_id":"b"}`, strings.Repeat(" ", mediaControlMaxBytes) + `{"type":"media_batch_status"}`, `{"type":"media_batch_prepare","media_batch_revision":9007199254740992}`} {
		if h.validateMediaControl(c, []byte(raw)) {
			t.Fatal("ambiguous/oversize control accepted")
		}
		waitForType(t, c, "error")
	}
}

func TestMediaUnificationExecutionObservedFirstThreadAndExplicitResetFence(t *testing.T) {
	h, s, c, starts := mediaFixture(t)
	data := mediaPNG(t)
	cmd, item, body := mediaPrepareItem(t, h, s, c, "r_execution_fence", data)
	id := mediaUploadItem(t, h, s, c, cmd, item, body, data)
	raw, _ := json.Marshal(map[string]any{"type": "message", "request_id": cmd.MediaBatchID, "session_id": s.ID, "media_batch_id": cmd.MediaBatchID, "content": "", "images": []any{map[string]any{"attachment_id": id, "media_type": item.MediaType, "sha256": item.SHA256, "size_bytes": item.SizeBytes}}})
	route(h, c, string(raw))
	waitForType(t, c, "message_ack")
	select {
	case <-starts:
	case <-time.After(3 * time.Second):
		t.Fatal("not executing")
	}
	entry, found, e := h.messageQueue.Get(s.ID, cmd.MediaBatchID)
	if e != nil || !found {
		t.Fatal(e)
	}
	var p queuedPayload
	_ = json.Unmarshal(entry.Payload, &p)
	guard := h.withMediaInputGuard(context.Background(), s.ID, cmd.MediaBatchID, p)
	if e = backend.CheckTaskInputGuard(guard); e != nil {
		t.Fatal(e)
	}
	high := "high"
	_, _, proof := s.ApplyProviderMediaRuntimeMetadata(nil, &high, 0)
	if proof == nil {
		t.Fatal("observed metadata proof missing")
	}
	h.Emit(backend.MediaDefaultEffortObserved{Transition: proof})
	if e = backend.CheckTaskInputGuard(guard); e != nil {
		t.Fatal("provider metadata broke original", e)
	}
	s.SetResumeID("observed-first-thread")
	h.Emit(backend.NewSessionUUID(s.ID, s.ResumeID()))
	if e = backend.CheckTaskInputGuard(guard); e != nil {
		t.Fatal("proved first native thread not migrated", e)
	}
	s.SetResumeID("reset-other-thread")
	if e = backend.CheckTaskInputGuard(guard); e == nil {
		t.Fatal("explicit reset swallowed")
	}
}
func TestMediaUnificationHistoryDescriptorOnlyForExactOriginalRequest(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	data := mediaPNG(t)
	cmd, item, body := mediaPrepareItem(t, h, s, c, "r_history_projection", data)
	id := mediaUploadItem(t, h, s, c, cmd, item, body, data)
	done, e := h.media.begin()
	if e != nil {
		t.Fatal(e)
	}
	b := h.media.data.Batches[mediaKey(c.deviceID, jarvisTokenGeneration("mua-token"), cmd.MediaBatchID)]
	b.State = "admitted"
	e = h.media.save()
	done()
	if e != nil {
		t.Fatal(e)
	}
	rows := []map[string]any{{"role": "user", "request_id": cmd.MediaBatchID, "content": "<image name=[Image #1] path=\"/old/staging.png\">\n</image>\nEdited text"}, {"role": "user", "request_id": "foreign", "content": "original"}}
	got := h.projectMediaHistory(s, rows)
	images := got[0]["images"].([]map[string]any)
	if images[0]["attachment_id"] != id || images[0]["sha256"] != item.SHA256 || got[0]["content"] != "Edited text" {
		t.Fatal(got)
	}
	if _, ok := got[1]["images"]; ok {
		t.Fatal("foreign request gained ref")
	}
	if _, ok := rows[0]["images"]; ok {
		t.Fatal("provider cache mutated")
	}
}

func TestMediaUnificationBareImageRefCannotBypassFrozenBatch(t *testing.T) {
	h, s, c, starts := mediaFixture(t)
	data := mediaPNG(t)
	cmd, item, body := mediaPrepareItem(t, h, s, c, "r_no_bypass", data)
	id := mediaUploadItem(t, h, s, c, cmd, item, body, data)
	raw, _ := json.Marshal(map[string]any{"type": "message", "request_id": "r_forged_no_batch", "session_id": s.ID, "images": []any{map[string]any{"attachment_id": id, "media_type": item.MediaType, "sha256": item.SHA256, "size_bytes": item.SizeBytes}}})
	route(h, c, string(raw))
	got := waitForType(t, c, "error")
	if got["code"] != "media_batch_required" {
		t.Fatal(got)
	}
	select {
	case <-starts:
		t.Fatal("bare ref sent")
	default:
	}
	if _, found, e := h.messageQueue.Get(s.ID, "r_forged_no_batch"); e != nil || found {
		t.Fatal("bare ref admitted", e)
	}
	if mediaPrefixKind([]byte("\x00\x00\x00\x08wide")) != "video" || mediaPrefixKind([]byte("\x00\x00\x00\x08moov")) != "video" {
		t.Fatal("classic QuickTime MOV missing")
	}
}

func TestMediaUnificationOversizedLogicalSetRejectedBeforeOpeningRefs(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	images := []map[string]any{}
	for n := 0; n < 8; n++ {
		images = append(images, map[string]any{"attachment_id": "u_" + strings.Repeat("f", 32), "media_type": "image/jpeg", "size_bytes": backend.MediaImageMaxBytes, "sha256": strings.Repeat("f", 64)})
	}
	raw, _ := json.Marshal(map[string]any{"type": "message", "request_id": "r_huge_metadata", "session_id": s.ID, "media_batch_id": "r_huge_metadata", "images": images})
	route(h, c, string(raw))
	got := waitForType(t, c, "error")
	if got["code"] != "media_model_budget" {
		t.Fatal("attempted nonexistent file before metadata rejection", got)
	}
}

func TestMediaUnificationRepreparePreservesOriginalEffectDespiteConflictOrSourceABA(t *testing.T) {
	for _, state := range []string{"prepared", "admitting_unknown", "admitted", "cancelled_before_admission"} {
		for _, change := range []string{"descriptor", "source", "sourceABA"} {
			t.Run(state+"/"+change, func(t *testing.T) {
				h, s, c, _ := mediaFixture(t)
				data := mediaPNG(t)
				cmd, _, _ := mediaPrepareItem(t, h, s, c, "r_original_receipt", data)
				done, e := h.media.begin()
				if e != nil {
					t.Fatal(e)
				}
				b := h.media.data.Batches[mediaKey(c.deviceID, jarvisTokenGeneration("mua-token"), cmd.MediaBatchID)]
				b.State = state
				originalHash := b.BodyHash
				originalRevision := b.Revision
				e = h.media.save()
				done()
				if e != nil {
					t.Fatal(e)
				}
				if change == "descriptor" {
					cmd.Content = "changed after uncertainty"
				} else {
					s.SetResumeID("other-source")
					if change == "sourceABA" {
						s.SetResumeID("")
					}
				}
				cmd.RequestID = "new-prepare-query"
				h.handleMediaBatch(c, cmd)
				got := waitForType(t, c, "media_batch_receipt")
				if got["state"] != state || got["body_hash"] != originalHash || got["media_batch_revision"] != float64(originalRevision) {
					t.Fatal("original downgraded or replaced", got)
				}
				expected := "unknown"
				if state == "prepared" || state == "cancelled_before_admission" {
					expected = "none"
				}
				if state == "admitted" {
					expected = "admitted"
				}
				if got["known_effect"] != expected {
					t.Fatal(got)
				}
				if change == "descriptor" && got["error_code"] != "idempotency_conflict" {
					t.Fatal(got)
				}
				if state == "prepared" && change != "descriptor" && got["error_code"] != "source_changed" {
					t.Fatal(got)
				}
				// Reopen the actual durable original; the conflicting attempt cannot mutate it.
				reopened := openMediaLifecycle(h.cfg.DataDir)
				original := reopened.data.Batches[mediaKey(c.deviceID, jarvisTokenGeneration("mua-token"), cmd.MediaBatchID)]
				if original == nil || original.State != state || original.BodyHash != originalHash || original.Content != "" {
					t.Fatal("original receipt mutated", original)
				}
				if _, found, e := h.messageQueue.Get(s.ID, cmd.MediaBatchID); e != nil || found {
					t.Fatal("observation admitted new effect", e)
				}
			})
		}
	}
}
func TestMediaUnificationReprepareForeignActorCannotBorrowOriginalReceipt(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	data := mediaPNG(t)
	cmd, _, _ := mediaPrepareItem(t, h, s, c, "r_foreign_receipt", data)
	done, e := h.media.begin()
	if e != nil {
		t.Fatal(e)
	}
	key := mediaKey(c.deviceID, jarvisTokenGeneration("mua-token"), cmd.MediaBatchID)
	b := h.media.data.Batches[key]
	b.State = "admitted"
	originalHash := b.BodyHash
	e = h.media.save()
	done()
	if e != nil {
		t.Fatal(e)
	}
	other := sharedReadClient(t, h, "other-reader")
	h.registerLatest(other)
	other.ctx = context.Background()
	h.handleMediaBatch(other, clientproto.Command{Kind: "media_batch_status", RequestID: "foreign-query", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID})
	got := waitForType(t, other, "media_batch_receipt")
	if got["state"] != "not_found" || got["known_effect"] != "unknown" || got["body_hash"] != nil {
		t.Fatal("foreign receipt leaked", got)
	}
	cmd.RequestID = "foreign-prepare"
	h.handleMediaBatch(other, cmd)
	got = waitForType(t, other, "media_batch_receipt")
	if got["state"] != "prepared" || got["body_hash"] == originalHash {
		t.Fatal("borrowed original actor effect", got)
	}
	done, e = h.media.begin()
	if e != nil {
		t.Fatal(e)
	}
	if original := h.media.data.Batches[key]; original.State != "admitted" || original.BodyHash != originalHash {
		t.Fatal("foreign actor changed original", original)
	}
	done()
}
