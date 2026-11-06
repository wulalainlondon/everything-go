package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestQACorrectionOriginalCommittedMediaResolvesAfterLaterEdit(t *testing.T) {
	h, s, c, b, first, cmd := qaStagedMedia(t)
	h.media.fault = func(point string) error {
		if point == "queue_commit_before_receipt" {
			return errors.New("QA lost media receipt after exact queue commit")
		}
		return nil
	}
	h.handleHumanInput(c, cmd)
	h.media.fault = nil
	receipt, e := h.messageQueue.ReadHumanOperation(b, first.ID, "")
	if e != nil || receipt.Status != "committed" {
		t.Fatal("first not committed", receipt, e)
	}
	rows, _ := h.messageQueue.HumanInputs(b)
	v := rows[0]
	revision, membership, _ := h.messageQueue.HumanMembership(b)
	boot, now := h.messageQueue.HumanClock()
	begun, e := h.messageQueue.HumanApply(b, messagequeue.HumanOperation{ID: "second_begin", Kind: "begin_edit", Request: v.ID, ExpectedRevision: v.Revision, ContentRevision: v.ContentRevision, ExpectedHash: v.EffectiveHash, QueueRevision: &revision, MembershipHash: membership}, boot, now)
	if e != nil || begun.Status != "committed" {
		t.Fatal(begun, e)
	}
	lease := begun.Input.Lease
	second := messagequeue.HumanOperation{ID: "second_commit", Kind: "commit_edit", Request: v.ID, ExpectedRevision: begun.Input.Revision, ContentRevision: v.ContentRevision, ExpectedHash: v.EffectiveHash, IntentHash: v.IntentHash, Content: "later text only", Assets: json.RawMessage(`[]`), LeaseID: lease.ID, LeaseGeneration: lease.Generation, ReadyProof: v.IntentHash}
	h.handleHumanInput(c, humanCoreCommand(b, second))
	applied, e := h.messageQueue.ReadHumanOperation(b, second.ID, "")
	if e != nil || applied.Status != "committed" {
		t.Fatal("second not committed", applied, e)
	}
	entry, found, e := h.messageQueue.GetEffective(s.ID, v.ID)
	if e != nil || !found || entry.Content != "later text only" {
		t.Fatal(entry, found, e)
	}
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_status", RequestID: "original_status_after_edit", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID})
	batch := h.media.data.Batches[mediaKey(b.Device, b.Generation, cmd.MediaBatchID)]
	if batch.State != "admitted" {
		t.Fatalf("durably committed original content revision=%d now effective revision=%d but original media remains %s; exact historical receipt cannot settle", receipt.Input.ContentRevision, entry.ContentRevision, batch.State)
	}
}
func TestQACorrectionFutureDefaultSourceCanReserveWhileActiveSnapshotOlder(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	release := s.HoldQueue()
	defer func() { s.Close(); release() }()
	config := session.ConfigurationFrom(s.SettingsSnapshot())
	config.Model = "next-model"
	future, e := s.SetFutureConfiguration(config, nil)
	if e != nil {
		t.Fatal(e)
	}
	actor, e := h.mediaActor(c)
	if e != nil {
		t.Fatal(e)
	}
	binding := messagequeue.HumanBinding{Device: "phone-a", Authority: h.cfg.InstanceID, Session: s.ID, Thread: s.ResumeID(), Config: future.ConfigRevision, Epoch: h.SessionRoleEpoch(s), Generation: actor.NamespaceGeneration}
	reserve := messagequeue.HumanOperation{ID: "reserve_future", Kind: "reserve", Request: "r_future", Content: "future settings", IntentHash: strings.Repeat("d", 64), Assets: json.RawMessage(`[]`)}
	h.handleHumanInput(c, humanCoreCommand(binding, reserve))
	out := waitForType(t, c, "queue_input_result")
	if out["result"].(map[string]any)["status"] != "committed" {
		t.Fatalf("legitimate source advertised by SettingsSnapshot rejected because execution snapshot older: activeConfig=%d futureConfig=%d result=%v", s.Snapshot().ConfigRevision, future.ConfigRevision, out)
	}
}

func qaCorrectionReleasableStage(t *testing.T, visual ...string) (*Hub, *session.Session, *Client, messagequeue.HumanBinding, messagequeue.HumanOperation, clientproto.Command, func()) {
	t.Helper()
	h, s, c, _ := mediaFixture(t)
	release := s.HoldQueue()
	t.Cleanup(func() { s.Close(); release() })
	actor, e := h.mediaActor(c)
	if e != nil {
		t.Fatal(e)
	}
	b := messagequeue.HumanBinding{Device: "phone-a", Authority: h.cfg.InstanceID, Session: s.ID, Thread: s.ResumeID(), Config: s.SettingsSnapshot().ConfigRevision, Epoch: h.SessionRoleEpoch(s), Generation: actor.NamespaceGeneration}
	reserve := messagequeue.HumanOperation{ID: "reserve_original", Kind: "reserve", Request: "r_original", Content: "old", IntentHash: strings.Repeat("a", 64), Assets: json.RawMessage(`[]`)}
	h.handleHumanInput(c, humanCoreCommand(b, reserve))
	if v := waitForType(t, c, "queue_input_result"); v["result"].(map[string]any)["status"] != "committed" {
		t.Fatal(v)
	}
	seal := messagequeue.HumanOperation{ID: "seal_original", Kind: "seal", Request: reserve.Request, ExpectedRevision: 1, ContentRevision: 1, IntentHash: reserve.IntentHash, Content: reserve.Content, ReadyProof: reserve.IntentHash}
	sealCommand := humanCoreCommand(b, seal)
	if len(visual) > 0 {
		sealCommand.VisualPresentation = visual[0]
	}
	h.handleHumanInput(c, sealCommand)
	if v := waitForType(t, c, "queue_input_result"); v["result"].(map[string]any)["status"] != "committed" {
		t.Fatal(v)
	}
	inputs, _ := h.messageQueue.HumanInputs(b)
	v := inputs[0]
	rev, membership, _ := h.messageQueue.HumanMembership(b)
	boot, now := h.messageQueue.HumanClock()
	begin, e := h.messageQueue.HumanApply(b, messagequeue.HumanOperation{ID: "edit_begin", Kind: "begin_edit", Request: v.ID, ExpectedRevision: v.Revision, ContentRevision: v.ContentRevision, ExpectedHash: v.EffectiveHash, QueueRevision: &rev, MembershipHash: membership}, boot, now)
	if e != nil || begin.Status != "committed" {
		t.Fatal(begin, e)
	}
	data := mediaPNG(t)
	sum := sha256.Sum256(data)
	item := protocol.MediaBatchItem{ItemID: "edit_image", ItemRevision: 1, UploadRequestID: "vu_edit_image", UploadKind: "image", Name: "edit.png", MediaType: "image/png", SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	lease := begin.Input.Lease
	stage := humanMediaStage{Binding: b, Request: v.ID, BaseRevision: v.ContentRevision, BaseHash: v.EffectiveHash, LeaseID: lease.ID, LeaseGeneration: lease.Generation}
	raw, _ := json.Marshal(map[string]any{"binding": b, "stage": map[string]any{"request_id": stage.Request, "content_revision": stage.BaseRevision, "expected_hash": stage.BaseHash, "lease_id": stage.LeaseID, "lease_generation": stage.LeaseGeneration}})
	cfg := s.SettingsSnapshot().ConfigRevision
	prepare := clientproto.Command{Kind: "media_batch_prepare", RequestID: "prepare_edit", SessionID: s.ID, MediaBatchID: "batch_edit_v2", MediaBatchRevision: 1, Content: "edited", ExpectedConfigRevision: &cfg, MediaItems: []protocol.MediaBatchItem{item}, QueueInput: raw}
	h.handleMediaBatch(c, prepare)
	receipt := waitForType(t, c, "media_batch_receipt")
	if receipt["state"] != "prepared" {
		t.Fatal(receipt)
	}
	body := receipt["body_hash"].(string)
	id := mediaUploadItem(t, h, s, c, prepare, item, body, data)
	assets, _ := json.Marshal([]map[string]any{{"kind": "image", "media_type": item.MediaType, "sha256": item.SHA256, "name": item.Name, "size": item.SizeBytes}})
	op := messagequeue.HumanOperation{ID: "commit_edit_original", Kind: "commit_edit", Request: v.ID, ExpectedRevision: begin.Input.Revision, ContentRevision: v.ContentRevision, ExpectedHash: v.EffectiveHash, IntentHash: v.IntentHash, Content: "edited", Assets: assets, LeaseID: lease.ID, LeaseGeneration: lease.Generation, ReadyProof: body}
	cmd := humanCoreCommand(b, op)
	if len(visual) > 0 {
		cmd.VisualPresentation = visual[0]
	}
	cmd.MediaBatchID = prepare.MediaBatchID
	cmd.Images = []backend.ImageAttachment{{AttachmentID: id, MediaType: item.MediaType, SHA256: item.SHA256, SizeBytes: item.SizeBytes}}
	return h, s, c, b, op, cmd, release
}

type qaCorrectionConsumed struct {
	mode, request, content string
	images                 []backend.ImageAttachment
	guard                  error
}
type qaCorrectionExecutor struct {
	*fakeExec
	calls chan qaCorrectionConsumed
	busy  bool
}

func (e *qaCorrectionExecutor) QueuedSubmissionReady(context.Context, *session.Session, string) error {
	if e.busy {
		return backend.ErrNativeSubmissionBusy
	}
	return nil
}
func (e *qaCorrectionExecutor) Send(ctx context.Context, s *session.Session, id, content string, images []backend.ImageAttachment, files []backend.FileAttachment) error {
	g := backend.CheckTaskInputGuard(ctx)
	e.calls <- qaCorrectionConsumed{"send", id, content, images, g}
	return g
}
func (e *qaCorrectionExecutor) Steer(ctx context.Context, s *session.Session, id, content string, images []backend.ImageAttachment, files []backend.FileAttachment) (backend.SteerResult, error) {
	g := backend.CheckTaskInputGuard(ctx)
	e.calls <- qaCorrectionConsumed{"steer", id, content, images, g}
	return backend.SteerResult{StrictACK: true, RequestID: "native_original", TurnID: "native_turn"}, g
}
func TestQACorrectionCommittedEditedVersionConsumedByOriginalSendAndSteer(t *testing.T) {
	for _, mode := range []string{"send", "steer"} {
		t.Run(mode, func(t *testing.T) {
			h, s, c, b, op, cmd, release := qaCorrectionReleasableStage(t)
			h.handleHumanInput(c, cmd)
			receipt, e := h.messageQueue.ReadHumanOperation(b, op.ID, "")
			if e != nil || receipt.Status != "committed" {
				t.Fatal(receipt, e)
			}
			calls := make(chan qaCorrectionConsumed, 2)
			exec := &qaCorrectionExecutor{fakeExec: &fakeExec{sink: h}, calls: calls, busy: mode == "steer"}
			h.SetExecutor(exec)
			release()
			if mode == "steer" {
				h.Emit(backend.ObservedTurn{SessionID: s.ID, RequestID: "native_original", Phase: "running"})
				h.promoteQueuedMessage(c, clientproto.Command{SessionID: s.ID, RequestID: op.Request})
			}
			select {
			case got := <-calls:
				if got.mode != mode || got.request != op.Request || got.content != "edited" || len(got.images) != 1 || got.images[0].AttachmentID != cmd.Images[0].AttachmentID || got.images[0].SHA256 != cmd.Images[0].SHA256 || got.guard != nil {
					t.Fatalf("wrong effective version consumed: %+v", got)
				}
			case <-time.After(3 * time.Second):
				entry, _, _ := h.messageQueue.GetEffective(s.ID, op.Request)
				t.Fatalf("no %s consumption state=%s message=%s", mode, entry.State, entry.Message)
			}
			if mode == "steer" {
				if result := waitForType(t, c, "queue_action_result"); result["status"] != "accepted" {
					t.Fatal(result)
				}
			}
		})
	}
}

func TestQACorrectionIdentityStaysOriginalThroughFinalSQLiteCommit(t *testing.T) {
	h, s, c, b, op, cmd := qaStagedMedia(t)
	held, release := make(chan struct{}), make(chan struct{})
	holderDone := make(chan error, 1)
	h.media.fault = func(point string) error {
		if point == "prepare_before_queue" {
			snapshot := s.Snapshot()
			go func() {
				holderDone <- s.CommitInputSource(snapshot, func() error { close(held); <-release; return nil })
			}()
			<-held
		}
		return nil
	}
	done := make(chan struct{})
	go func() { h.handleHumanInput(c, cmd); close(done) }()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
		h.media.fault = nil
	}()
	blocked := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(stack, "session.(*Session).CommitInputSource") && strings.Contains(stack, "core.(*Hub).handleHumanInput") && strings.Contains(stack, "sync") {
				blocked = true
				break
			}
		}
		if blocked {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !blocked {
		t.Fatal("did not reach final commit source mutex after identity check")
	}
	originalIdentity := c.readIdentity.Load()
	replacement := *originalIdentity
	replacement.token = "retired-token"
	c.readIdentity.Store(&replacement)
	close(release)
	if e := <-holderDone; e != nil {
		t.Fatal(e)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("command stuck")
	}
	receipt, e := h.messageQueue.ReadHumanOperation(b, op.ID, "")
	if e != nil {
		t.Fatal(e)
	}
	if receipt.Status == "committed" {
		t.Fatalf("credential identity replaced after final check while source mutex awaited, yet exact original op committed version=%d", receipt.Input.ContentRevision)
	}
}
