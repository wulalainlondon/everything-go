package core

import (
	"context"
	"crypto/sha256"
	"database/sql"
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

func qaStagedMedia(t *testing.T, visual ...string) (*Hub, *session.Session, *Client, messagequeue.HumanBinding, messagequeue.HumanOperation, clientproto.Command) {
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
	return h, s, c, b, op, cmd
}
func TestQAStagedEditCommittedMediaReachesExecutionGuards(t *testing.T) {
	h, s, c, b, op, cmd := qaStagedMedia(t)
	h.handleHumanInput(c, cmd)
	receipt, e := h.messageQueue.ReadHumanOperation(b, op.ID, "")
	if e != nil || receipt.Status != "committed" {
		t.Fatal("setup did not commit", receipt, e)
	}
	entry, found, e := h.messageQueue.GetEffective(s.ID, op.Request)
	if e != nil || !found {
		t.Fatal(e)
	}
	var p queuedPayload
	if e = json.Unmarshal(entry.Payload, &p); e != nil {
		t.Fatal(e)
	}
	release, readErr := h.acquireModelMediaReader(s, op.Request, &p)
	if readErr == nil {
		defer release()
	}
	guardErr := backend.CheckTaskInputGuard(h.withMediaInputGuard(context.Background(), s.ID, op.Request, p))
	if readErr != nil || guardErr != nil {
		t.Fatalf("committed original request=%s media batch=%s cannot execute: reader=%v guard=%v", op.Request, p.MediaBatchID, readErr, guardErr)
	}
}
func TestQAStagedEditBeforeQueueFaultCanRecoverSameOperation(t *testing.T) {
	h, s, c, b, op, cmd := qaStagedMedia(t)
	h.media.fault = func(point string) error {
		if point == "prepare_before_queue" {
			return errors.New("QA exact before queue fault")
		}
		return nil
	}
	h.handleHumanInput(c, cmd)
	h.media.fault = nil
	first, _ := h.messageQueue.ReadHumanOperation(b, op.ID, "")
	batch := h.media.data.Batches[mediaKey(b.Device, b.Generation, cmd.MediaBatchID)]
	stateBefore := batch.State
	h.handleHumanInput(c, cmd)
	retry, _ := h.messageQueue.ReadHumanOperation(b, op.ID, "")
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_status", RequestID: "status_original", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID})
	batch = h.media.data.Batches[mediaKey(b.Device, b.Generation, cmd.MediaBatchID)]
	t.Logf("before queue fault: op=%s media=%s; same-op retry=%s media=%s", first.Status, stateBefore, retry.Status, batch.State)
	if first.Status != "rejected_known_none" || retry.Status != "rejected_known_none" || batch.State != "prepared" {
		t.Fatalf("original op not durably fenced and recovered: first=%s retry=%s media=%s", first.Status, retry.Status, batch.State)
	}
	entry, found, err := h.messageQueue.GetEffective(s.ID, op.Request)
	if err != nil || !found || entry.Content != "old" {
		t.Fatal("failed edit changed base", entry, found, err)
	}
	next := op
	next.ID = "human_retry_edit"
	cmd = humanCoreCommand(b, next)
	cmd.MediaBatchID = batch.ID
	cmd.Images = []backend.ImageAttachment{{AttachmentID: batch.Objects[batch.Items[0].UploadRequestID], MediaType: batch.Items[0].MediaType, SHA256: batch.Items[0].SHA256, SizeBytes: batch.Items[0].SizeBytes}}
	h.handleHumanInput(c, cmd)
	applied, err := h.messageQueue.ReadHumanOperation(b, next.ID, "")
	if err != nil || applied.Status != "committed" {
		t.Fatal("explicit new edit could not reuse original prepared bytes", applied, err)
	}
}

func TestQAStagedEditInitialSealCannotAdoptChangedSourceDuringRead(t *testing.T) {
	h, c, s, b := humanCoreFixture(t)
	h.cfg.InstanceID = "authority"
	b.Authority = h.cfg.InstanceID
	releaseActor := s.HoldQueue()
	defer func() { s.Close(); releaseActor() }()
	reserve := messagequeue.HumanOperation{ID: "reserve_source", Kind: "reserve", Request: "r_source", Content: "original", IntentHash: strings.Repeat("c", 64), Assets: json.RawMessage(`[]`)}
	h.handleHumanInput(c, humanCoreCommand(b, reserve))
	waitForType(t, c, "queue_input_result")
	seal := messagequeue.HumanOperation{ID: "seal_source", Kind: "seal", Request: reserve.Request, ExpectedRevision: 1, ContentRevision: 1, IntentHash: reserve.IntentHash, Content: reserve.Content, ReadyProof: reserve.IntentHash}
	held, release := make(chan struct{}), make(chan struct{})
	txDone := make(chan error, 1)
	go func() {
		txDone <- h.messageQueue.TaskJournal().Transaction(t.Context(), func(*sql.Tx) error { close(held); <-release; return nil })
	}()
	<-held
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		<-txDone
	}()
	done := make(chan struct{})
	go func() { h.handleHumanInput(c, humanCoreCommand(b, seal)); close(done) }()
	blocked := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(stack, "core.(*Hub).handleHumanInput") && strings.Contains(stack, "messagequeue.(*Store).ContextBarrier") && strings.Contains(stack, "database/sql.(*DB).conn") {
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
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Log(string(buf[:n]))
		t.Fatal("did not suspend actual post-thread-check context barrier read")
	}
	s.SetResumeID("unrelated_new_thread")
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("seal stuck")
	}
	entry, exists, e := h.messageQueue.Get(s.ID, reserve.Request)
	if e != nil {
		t.Fatal(e)
	}
	if exists {
		var p queuedPayload
		json.Unmarshal(entry.Payload, &p)
		t.Fatalf("original binding thread=%q seal crossed source to expected=%+v", b.Thread, p.ExpectedTarget)
	}
}

func TestQAStagedAdjacentCancellationCannotReviveOriginal(t *testing.T) {
	h, s, c, b, op, cmd := qaStagedMedia(t)
	rows, _ := h.messageQueue.HumanInputs(b)
	v := rows[0]
	boot, now := h.messageQueue.HumanClock()
	cancelled, e := h.messageQueue.HumanApply(b, messagequeue.HumanOperation{ID: "cancel-original", Kind: "cancel", Request: v.ID, ExpectedRevision: v.Revision}, boot, now)
	if e != nil || cancelled.Status != "committed" {
		t.Fatal(cancelled, e)
	}
	h.handleHumanInput(c, cmd)
	entry, found, e := h.messageQueue.GetEffective(s.ID, v.ID)
	if e != nil || !found || entry.State != messagequeue.Cancelled || entry.Content != "old" {
		t.Fatal(entry, found, e)
	}
	receipt, _ := h.messageQueue.ReadHumanOperation(b, op.ID, "")
	if receipt.Status == "committed" {
		t.Fatal("cancelled staged edit revived", receipt)
	}
	if h.mediaHumanStageLive(h.media.data.Batches[mediaKey(b.Device, b.Generation, cmd.MediaBatchID)].HumanStage) {
		t.Fatal("cancelled base still stage-live")
	}
}
func TestQAStagedAdjacentForeignSourceCannotAcquireStage(t *testing.T) {
	h, s, c, b, _, cmd := qaStagedMedia(t)
	batch := h.media.data.Batches[mediaKey(b.Device, b.Generation, cmd.MediaBatchID)]
	stage := *batch.HumanStage
	for _, change := range []string{"authority", "generation", "config", "basehash", "lease"} {
		t.Run(change, func(t *testing.T) {
			st := stage
			switch change {
			case "authority":
				st.Binding.Authority = "foreign"
			case "generation":
				st.Binding.Generation++
			case "config":
				st.Binding.Config++
			case "basehash":
				st.BaseHash = strings.Repeat("f", 64)
			case "lease":
				st.LeaseGeneration++
			}
			raw, _ := json.Marshal(map[string]any{"binding": st.Binding, "stage": map[string]any{"request_id": st.Request, "content_revision": st.BaseRevision, "expected_hash": st.BaseHash, "lease_id": st.LeaseID, "lease_generation": st.LeaseGeneration}})
			_, err := h.mediaHumanStage(c, s, clientproto.Command{MediaBatchID: "otherstage", QueueInput: raw}, true)
			if err == nil {
				t.Fatal("foreign stage acquired", change)
			}
		})
	}
}

func TestHumanStagedEditRetainsVisualModeWithFreshDerivedHashes(t *testing.T) {
	h, s, c, b, op, cmd := qaStagedMedia(t, "auto-v1")
	original, _, _ := h.messageQueue.GetEffective(s.ID, op.Request)
	h.handleHumanInput(c, cmd)
	receipt, err := h.messageQueue.ReadHumanOperation(b, op.ID, "")
	if err != nil || receipt.Status != "committed" {
		t.Fatal(receipt, err)
	}
	effective, _, _ := h.messageQueue.GetEffective(s.ID, op.Request)
	var before, after queuedPayload
	json.Unmarshal(original.Payload, &before)
	json.Unmarshal(effective.Payload, &after)
	if after.VisualPresentation != before.VisualPresentation || after.VisualUserHash == before.VisualUserHash || after.VisualEffectiveHash == before.VisualEffectiveHash {
		t.Fatal("visual mode or derived content hashes incorrect", before.VisualPresentation, after.VisualPresentation)
	}
}
