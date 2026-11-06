package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"strings"
	"testing"
	"time"
)

type qaR4Media struct {
	binding     messagequeue.HumanBinding
	reservation messagequeue.HumanOperation
	prepare     clientproto.Command
	item        protocol.MediaBatchItem
	body        string
	data        []byte
}

func qaR4ReservePrepare(t *testing.T, h *Hub, s *session.Session, c *Client, id, previous string) qaR4Media {
	t.Helper()
	actor, e := h.mediaActor(c)
	if e != nil {
		t.Fatal(e)
	}
	b := messagequeue.HumanBinding{Device: "phone-a", Authority: h.cfg.InstanceID, Session: s.ID, Thread: s.ResumeID(), Config: s.SettingsSnapshot().ConfigRevision, Epoch: h.SessionRoleEpoch(s), Generation: actor.NamespaceGeneration}
	data := mediaPNG(t)
	sum := sha256.Sum256(data)
	assets, _ := json.Marshal([]map[string]any{{"kind": "image", "media_type": "image/png", "sha256": hex.EncodeToString(sum[:]), "name": "圖片.png", "size": len(data)}})
	reserve := messagequeue.HumanOperation{ID: "reserve_" + id, Kind: "reserve", Request: id, Content: "", IntentHash: strings.Repeat("b", 64), Assets: assets, Predecessor: previous}
	h.handleHumanInput(c, humanCoreCommand(b, reserve))
	out := waitForType(t, c, "queue_input_result")
	if out["result"].(map[string]any)["status"] != "committed" {
		t.Fatal(out)
	}
	cmd, item, body := mediaPrepareItem(t, h, s, c, id, data)
	return qaR4Media{b, reserve, cmd, item, body, data}
}
func qaR4UploadSeal(t *testing.T, h *Hub, s *session.Session, c *Client, v qaR4Media) {
	t.Helper()
	id := mediaUploadItem(t, h, s, c, v.prepare, v.item, v.body, v.data)
	op := messagequeue.HumanOperation{ID: "seal_" + v.reservation.Request, Kind: "seal", Request: v.reservation.Request, ExpectedRevision: 1, ContentRevision: 1, IntentHash: v.reservation.IntentHash, Content: "", ReadyProof: v.body}
	cmd := humanCoreCommand(v.binding, op)
	cmd.MediaBatchID = v.prepare.MediaBatchID
	cmd.Images = []backend.ImageAttachment{{AttachmentID: id, MediaType: v.item.MediaType, SHA256: v.item.SHA256, SizeBytes: v.item.SizeBytes}}
	h.handleHumanInput(c, cmd)
	receipt, e := h.messageQueue.ReadHumanOperation(v.binding, op.ID, "")
	if e != nil || receipt.Status != "committed" {
		t.Fatal(receipt, e)
	}
}

type qaR4Executor struct {
	*fakeExec
	h           *Hub
	calls       chan string
	firstNative bool
}

func (e *qaR4Executor) Send(ctx context.Context, s *session.Session, id, content string, images []backend.ImageAttachment, files []backend.FileAttachment) error {
	if err := backend.CheckTaskInputGuard(ctx); err != nil {
		return err
	}
	if e.firstNative && s.ResumeID() == "" {
		s.SetResumeID("native_first_exact")
		e.h.Emit(backend.NewSessionUUID(s.ID, s.ResumeID()))
		if err := e.h.messageQueue.RecordNativeAcceptance(s.ID, id, s.ResumeID(), "native_turn_exact"); err != nil {
			return err
		}
	}
	e.h.Emit(protocol.NewDone(s.ID, id))
	e.calls <- id
	return nil
}
func TestQARecheckTwoMediaBatchesPreparedBeforeSameFutureActivation(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	release := s.HoldQueue()
	defer func() { s.Close(); release() }()
	cfg := session.ConfigurationFrom(s.SettingsSnapshot())
	cfg.Model = "next"
	if _, e := s.SetFutureConfiguration(cfg, nil); e != nil {
		t.Fatal(e)
	}
	a := qaR4ReservePrepare(t, h, s, c, "r_media_a", "")
	qaR4UploadSeal(t, h, s, c, a)
	b := qaR4ReservePrepare(t, h, s, c, "r_media_b", a.reservation.Request)
	qaR4UploadSeal(t, h, s, c, b)
	calls := make(chan string, 3)
	h.SetExecutor(&qaR4Executor{fakeExec: &fakeExec{sink: h}, h: h, calls: calls})
	release()
	for _, id := range []string{a.reservation.Request, b.reservation.Request} {
		select {
		case got := <-calls:
			if got != id {
				t.Fatal("FIFO changed", got, id)
			}
		case <-time.After(3 * time.Second):
			entry, _, _ := h.messageQueue.GetEffective(s.ID, id)
			t.Fatalf("same original future settings media batch %s did not execute: state=%s message=%s source=%d", id, entry.State, entry.Message, s.SettingsSnapshot().SourceRevision)
		}
	}
}
func TestQARecheckPreparedBatchSurvivesProvenFirstNativeThread(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	release := s.HoldQueue()
	defer func() { s.Close(); release() }()
	// Both reservations/prepares are made while the exact native thread is blank.
	a := qaR4ReservePrepare(t, h, s, c, "r_native_a", "")
	qaR4UploadSeal(t, h, s, c, a)
	b := qaR4ReservePrepare(t, h, s, c, "r_pending_b", a.reservation.Request)
	calls := make(chan string, 3)
	h.SetExecutor(&qaR4Executor{fakeExec: &fakeExec{sink: h}, h: h, calls: calls, firstNative: true})
	release()
	select {
	case got := <-calls:
		if got != a.reservation.Request {
			t.Fatal(got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first native fixture not consumed")
	}
	if !h.messageQueue.HumanThreadCompatible(b.binding, s.ResumeID()) {
		t.Fatal("fixture lacks exact original owner/config native acceptance continuation")
	}
	// Status/PREPARE immutable fields stay original; only the server-owned
	// continuation can authorize original B's upload/initial seal on that thread.
	c.uploads.initCommand(clientproto.Command{SessionID: s.ID, UploadRequestID: b.item.UploadRequestID, UploadKind: b.item.UploadKind, Name: b.item.Name, MediaType: b.item.MediaType, SizeBytes: b.item.SizeBytes, MediaBatchID: b.prepare.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: b.body, ItemID: b.item.ItemID, ItemRevision: 1, SHA256: b.item.SHA256})
	deadline := time.After(3 * time.Second)
	for {
		select {
		case raw := <-c.send:
			var event map[string]any
			json.Unmarshal(raw, &event)
			if event["type"] == "attachment_upload_ready" {
				return
			}
			if event["type"] == "attachment_upload_error" {
				t.Fatalf("proven original first-thread continuation rejected still-prepared B: %v", event)
			}
		case <-deadline:
			t.Fatal("no original B upload receipt")
		}
	}
}
