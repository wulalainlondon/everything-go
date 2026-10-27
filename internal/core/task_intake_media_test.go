package core

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"github.com/coder/websocket"
)

type mediaIntakeInvocation struct {
	request, content string
	images           []backend.ImageAttachment
	files            []backend.FileAttachment
}
type mediaIntakeExecutor struct {
	*fakeExec
	starts  chan mediaIntakeInvocation
	release <-chan struct{}
}

func (e *mediaIntakeExecutor) Send(ctx context.Context, _ *session.Session, id, content string, images []backend.ImageAttachment, files []backend.FileAttachment) error {
	e.starts <- mediaIntakeInvocation{id, content, images, files}
	select {
	case <-e.release:
	case <-ctx.Done():
	}
	return nil
}
func mediaIntakeFixture(t *testing.T, kind string) (*Hub, *session.Session, func(string, any, string) map[string]any, *mediaIntakeExecutor) {
	t.Helper()
	root := t.TempDir()
	pair := governance.NewPairing(filepath.Join(root, "pair.json"))
	pair.Claim("media-token", "media-phone")
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", RootDir: root, DataDir: root}, pair, 0)
	s := h.registry.Create("s_media", "Media QA", root, kind, "original-model", "read-only", "")
	s.SetResumeID("original-thread")
	release := make(chan struct{})
	exec := &mediaIntakeExecutor{fakeExec: &fakeExec{sink: h}, starts: make(chan mediaIntakeInvocation, 2), release: release}
	h.SetExecutor(exec)
	t.Cleanup(func() {
		close(release)
		s.CloseAndWait()
		h.messageQueue.Close()
		h.delegations.Close()
		h.dispatches.Close()
	})
	conn, ctx, cleanup := dialWS(t, h)
	t.Cleanup(cleanup)
	send := func(v any) {
		t.Helper()
		raw, _ := json.Marshal(v)
		if e := conn.Write(ctx, websocket.MessageText, raw); e != nil {
			t.Fatal(e)
		}
	}
	receive := func(kind string) map[string]any {
		t.Helper()
		for {
			v := readEvent(t, ctx, conn)
			if v["type"] == kind {
				return v
			}
		}
	}
	send(map[string]any{"type": "hello", "auth_token": "media-token", "device_id": "media-phone", "protocol_version": 3})
	receive("hello_ack")
	c, e := contract.New()
	if e != nil {
		t.Fatal(e)
	}
	call := func(op string, input any, key string) map[string]any {
		t.Helper()
		if in, ok := input.(intakeInput); ok {
			raw, _ := json.Marshal(in)
			var wire map[string]any
			json.Unmarshal(raw, &wire)
			delete(wire, "session_id")
			if wire["images"] == nil {
				delete(wire, "images")
			}
			if wire["files"] == nil {
				delete(wire, "files")
			}
			input = wire
		}
		v := map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "media-intake", "operation": op, "input": input}
		if key != "" {
			v["idempotency_key"] = key
		}
		send(v)
		out := receive("task_api_response")
		raw, _ := json.Marshal(out)
		if e := c.Validate(raw, "Response"); e != nil {
			t.Fatalf("invalid response %s: %v", raw, e)
		}
		return out
	}
	return h, s, call, exec
}
func mediaIntakeVideo(t *testing.T, h *Hub, sid string) (intakeFile, string, string) {
	t.Helper()
	id := "u_" + strings.Repeat("a", 32)
	dir := filepath.Join(h.cfg.DataDir, "uploads", attachmentSafe(sid, "session"), id)
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "original.mp4")
	data := []byte("\x00\x00\x00\x18ftypmp42original-media-fixture")
	if e := os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	digest := sha256.Sum256(data)
	hash := hex.EncodeToString(digest[:])
	m := uploadedVideoManifest{Version: 2, UploadID: id, SessionID: sid, DeviceID: "media-phone", Name: "original.mp4", MediaType: "video/mp4", SizeBytes: int64(len(data)), SHA256: hash, Path: path}
	raw, _ := json.Marshal(m)
	if e := os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0600); e != nil {
		t.Fatal(e)
	}
	return intakeFile{AttachmentID: id, Name: m.Name, MediaType: m.MediaType, SizeBytes: m.SizeBytes}, path, hash
}
func TestMediaOnlyIntakeAuthenticatedPreservesOriginalBytesSourceAndOnce(t *testing.T) {
	for _, provider := range []string{backend.Codex, backend.Claude} {
		for _, kind := range []string{"image", "video", "mixed"} {
			t.Run(string(provider)+"/"+kind, func(t *testing.T) {
				h, s, call, exec := mediaIntakeFixture(t, provider)
				original := s.SettingsSnapshot()
				inspected := call("intake", map[string]any{"action": "inspect", "session_id": s.ID}, "")
				target := inspected["result"].(map[string]any)["target"]
				id := "r_" + strings.Repeat("1", 32)
				in := intakeInput{Action: "submit", RequestID: id, Target: intakeTarget{Instance: "i1", Session: s.ID, Thread: pointerMediaString(s.ResumeID()), Revision: original.ConfigRevision}, Goal: ""}
				image := intakeImage{Data: base64.StdEncoding.EncodeToString([]byte("original image bytes")), MediaType: "image/png"}
				path, hash := "", ""
				if kind != "video" {
					in.Images = []intakeImage{image}
				}
				if kind != "image" {
					var f intakeFile
					f, path, hash = mediaIntakeVideo(t, h, s.ID)
					in.Files = []intakeFile{f}
				}
				// target obtained through the real authenticated inspect must be identical.
				wantTarget, _ := json.Marshal(target)
				gotTarget, _ := json.Marshal(in.Target)
				var a, b any
				json.Unmarshal(wantTarget, &a)
				json.Unmarshal(gotTarget, &b)
				if !reflect.DeepEqual(a, b) {
					t.Fatal("target drift")
				}
				first := call("intake", in, id)
				if first["ok"] != true {
					t.Fatal(first)
				}
				receipt := first["result"].(map[string]any)
				var sent mediaIntakeInvocation
				select {
				case sent = <-exec.starts:
				case <-time.After(5 * time.Second):
					t.Fatal("accepted media not dispatched")
				}
				if sent.request != id || len(sent.images) != len(in.Images) || len(sent.files) != 0 {
					t.Fatal("native boundary changed request/media", sent)
				}
				if kind == "image" && sent.content != "" {
					t.Fatal("synthesized image instruction", sent.content)
				}
				if kind != "image" {
					expected, _, e := h.resolveUploadedVideos(s.ID, "", []backend.FileAttachment{{AttachmentID: in.Files[0].AttachmentID, Name: in.Files[0].Name, MediaType: in.Files[0].MediaType, SizeBytes: in.Files[0].SizeBytes}})
					if e != nil || sent.content != expected || !strings.Contains(sent.content, path) || !strings.Contains(sent.content, hash) {
						t.Fatal("not original verified file descriptor", e, sent.content)
					}
				}
				if len(sent.images) > 0 && (sent.images[0].Data != image.Data || sent.images[0].MediaType != image.MediaType) {
					t.Fatal("image bytes changed")
				}
				again := call("intake", in, id)
				if again["ok"] != true || again["result"].(map[string]any)["receipt_id"] != receipt["receipt_id"] {
					t.Fatal("duplicate did not read original receipt", again)
				}
				entry, found, e := h.messageQueue.Get(s.ID, id)
				if e != nil || !found || entry.Content != "" {
					t.Fatal("empty goal not preserved", entry, e)
				}
				task := call("get", map[string]any{"task_id": receipt["task_id"]}, "")
				if task["ok"] != true || task["result"].(map[string]any)["goal"] != "" {
					t.Fatal("empty goal projection failed", task)
				}
				source := task["result"].(map[string]any)["source"].(map[string]any)["identity"].(map[string]any)
				if source["request_id"] != id || source["session_id"] != s.ID || source["native_thread_id"] != "original-thread" {
					t.Fatal("source forged", source)
				}
				changed := in
				changed.Images = []intakeImage{{Data: "AQID", MediaType: "image/png"}}
				if kind == "image" {
					changed.Images[0].Data = "BAUG"
				}
				if out := call("intake", changed, id); out["ok"] != false || out["error"].(map[string]any)["code"] != "idempotency_conflict" {
					t.Fatal("changed media replay admitted", out)
				}
				snapshot, e := h.messageQueue.Snapshot(s.ID)
				if e != nil || len(snapshot.Items) != 1 {
					t.Fatal("duplicate effect", snapshot, e)
				}

				// A separately opened original SQLite store reads the same immutable
				// payload and journal, not an in-memory reconstructed media task.
				durable, err := messagequeue.Open(h.cfg.DataDir)
				if err != nil {
					t.Fatal(err)
				}
				persisted, exists, err := durable.Get(s.ID, id)
				if err != nil || !exists || !reflect.DeepEqual(persisted.Payload, entry.Payload) || persisted.PayloadHash != entry.PayloadHash {
					durable.Close()
					t.Fatal("durable media payload drift", err)
				}
				durable.Close()
				if !reflect.DeepEqual(session.ConfigurationFrom(s.SettingsSnapshot()), session.ConfigurationFrom(original)) {
					t.Fatal("model/sandbox/cwd/revision changed")
				}
				select {
				case extra := <-exec.starts:
					t.Fatal("replayed native input", extra)

				default:
				}
				waiting := in
				waiting.RequestID = "r_" + strings.Repeat("4", 32)
				ack := call("intake", waiting, waiting.RequestID)
				if ack["ok"] != true {
					t.Fatal("second media queue admission", ack)
				}
				r := ack["result"].(map[string]any)
				view := call("get", map[string]any{"task_id": r["task_id"]}, "")["result"].(map[string]any)
				out := call("cancel", map[string]any{"task_id": r["task_id"], "command_receipt_id": r["receipt_id"], "expected_revision": view["revision"], "target": target, "mode": "queued"}, "media-cancel-original")
				if out["ok"] != true {
					t.Fatal("media queue cannot cancel", out)
				}
				cancelled, exists, err := h.messageQueue.Get(s.ID, waiting.RequestID)
				if err != nil || !exists || cancelled.State != messagequeue.Cancelled {
					t.Fatal("media cancellation missing", cancelled, err)
				}
				select {
				case extra := <-exec.starts:
					t.Fatal("cancelled media dispatched", extra)
				default:
				}
			})
		}
	}
}
func pointerMediaString(s string) *string { return &s }
func TestMediaOnlyIntakeRejectsEmptyInvalidForeignAndTamperedWithoutQueue(t *testing.T) {
	for _, kind := range []string{"empty", "whitespace", "empty-envelopes", "bad-base64", "zero-byte-image", "foreign-video", "tampered-video", "desktop", "stale", "wrong-authority"} {
		t.Run(kind, func(t *testing.T) {
			h, s, call, exec := mediaIntakeFixture(t, backend.Codex)
			id := "r_" + strings.Repeat("2", 32)
			in := intakeInput{Action: "submit", RequestID: id, Target: intakeTarget{Instance: "i1", Session: s.ID, Thread: pointerMediaString(s.ResumeID())}, Goal: "", Images: []intakeImage{{Data: "AQID", MediaType: "image/png"}}}
			switch kind {
			case "empty":
				in.Images = nil
			case "whitespace":
				in.Images = nil
				in.Goal = " \n\t"
			case "empty-envelopes":
				in.Images = nil
				in.Files = []intakeFile{{Name: "", Content: "", MediaType: ""}}
			case "bad-base64":
				in.Images[0].Data = "%%%"
			case "zero-byte-image":
				in.Images[0].Data = "\n"
			case "foreign-video":
				f, _, _ := mediaIntakeVideo(t, h, "other-session")
				in.Images = nil
				in.Files = []intakeFile{f}
			case "tampered-video":
				f, path, _ := mediaIntakeVideo(t, h, s.ID)
				in.Images = nil
				in.Files = []intakeFile{f}
				data, _ := os.ReadFile(path)
				data[0] ^= 1
				if e := os.WriteFile(path, data, 0600); e != nil {
					t.Fatal(e)
				}
			case "desktop":
				h.controls.MarkDesktopOwner(s.ID, s.ResumeID())
			case "stale":
				in.Target.Revision = 99
			case "wrong-authority":
				in.Target.Instance = "foreign"
			}
			out := call("intake", in, id)
			want := "invalid_argument"
			if kind == "desktop" {
				want = "permission"
			}
			if kind == "stale" {
				want = "stale_revision"
			}
			if kind == "wrong-authority" {
				want = "wrong_authority"
			}
			if out["ok"] != false || out["error"].(map[string]any)["code"] != want {
				t.Fatal("invalid media/source refusal changed", kind, want, out)
			}
			if _, found, e := h.messageQueue.Get(s.ID, id); e != nil || found {
				t.Fatal("invalid input produced queue row", e)
			}
			select {
			case sent := <-exec.starts:
				t.Fatal("invalid input crossed native boundary", sent)
			default:
			}
		})
	}
}
func TestMediaOnlyIntakeReservedFirstEffectKeepsExactIDAndEmptyHash(t *testing.T) {
	h, call := jarvisFixture(t)
	created := createFixture(h, "ordinary_prompt")
	created.InstructionHash = jarvisTextHash("")
	if w := call("POST", "sessions/create", created, "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	s, _ := h.registry.Get(created.SessionID)
	release := s.HoldQueue()
	defer release()
	defer s.Close()
	caller := taskapiMediaCaller(h)
	input := intakeInput{Action: "submit", RequestID: created.FirstEffectID, Target: intakeTarget{Instance: h.cfg.InstanceID, Session: s.ID}, Goal: "", Images: []intakeImage{{Data: "AQID", MediaType: "image/png"}}}
	for _, kind := range []string{"wrong-id", "wrong-text", "original"} {
		t.Run(kind, func(t *testing.T) {
			in := input
			if kind == "wrong-id" {
				in.RequestID = "r_" + strings.Repeat("3", 32)
			}
			if kind == "wrong-text" {
				in.Goal = "synthetic analysis"
			}
			raw, _ := json.Marshal(in)
			_, e := h.Authorize(context.Background(), caller, taskapi.Request{Operation: "intake", Input: raw, IdempotencyKey: in.RequestID})
			if (e == nil) != (kind == "original") {
				t.Fatal("first-effect reservation weakened", e)
			}
		})
	}
	snapshot, e := h.messageQueue.Snapshot(s.ID)
	if e != nil || len(snapshot.Items) != 0 {
		t.Fatal("authorization itself dispatched", e)
	}
}
func taskapiMediaCaller(h *Hub) taskapi.VerifiedContext {
	return taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:phone", BindingKind: "paired_human", Transport: "authenticated_ws", NamespaceGeneration: jarvisTokenGeneration("jarvis-token")}
}
