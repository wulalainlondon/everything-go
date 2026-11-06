package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/taskapi"
	"strings"
	"testing"
	"time"
)

func humanTypedIntakeCommand(b messagequeue.HumanBinding, op messagequeue.HumanOperation, images []backend.ImageAttachment, files []backend.FileAttachment, batch string) clientproto.Command {
	cmd := humanCoreCommand(b, op)
	cmd.Images = images
	cmd.Files = files
	cmd.MediaBatchID = batch
	input := intakeInput{Action: "submit", RequestID: op.Request, Target: intakeTarget{Instance: b.Authority, Session: b.Session, Revision: b.Config}, Goal: op.Content, MediaBatchID: batch, Images: []intakeImage{}, Files: []intakeFile{}}
	if b.Thread != "" {
		thread := b.Thread
		input.Target.Thread = &thread
	}
	for _, v := range images {
		input.Images = append(input.Images, intakeImage{AttachmentID: v.AttachmentID, SHA256: v.SHA256, SizeBytes: v.SizeBytes, Data: v.Data, MediaType: v.MediaType})
	}
	for _, v := range files {
		input.Files = append(input.Files, intakeFile{AttachmentID: v.AttachmentID, SHA256: v.SHA256, SizeBytes: v.SizeBytes, Content: v.Content, MediaType: v.MediaType, Name: v.Name})
	}
	// Marshal only the original submitted schema fields, not inspect-only session_id.
	wire := map[string]any{"action": input.Action, "submission_request_id": input.RequestID, "target": input.Target, "goal": input.Goal, "images": input.Images, "files": input.Files}
	if batch != "" {
		wire["media_batch_id"] = batch
	}
	envelope, _ := json.Marshal(map[string]any{"type": "media_task_api_request", "schema_hash": contract.MediaRefsHash(), "request": map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "qmu_original_intake", "operation": "intake", "idempotency_key": op.Request, "input": wire}})
	cmd.QueueInput, _ = json.Marshal(humanInputCommand{Binding: b, Operation: op, IntakeRequest: envelope})
	return cmd
}
func TestHumanTypedIntakeAtomicOriginalTaskAndEditableInput(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	release := s.HoldQueue()
	defer release()
	actor, err := h.mediaActor(c)
	if err != nil {
		t.Fatal(err)
	}
	b := messagequeue.HumanBinding{Device: "phone-a", Authority: h.cfg.InstanceID, Session: s.ID, Thread: s.ResumeID(), Config: s.SettingsSnapshot().ConfigRevision, Generation: actor.NamespaceGeneration}
	reserve := messagequeue.HumanOperation{ID: "reserve_formal", Kind: "reserve", Origin: "formal_intake", Request: "r_" + strings.Repeat("a", 32), Content: "original goal", IntentHash: strings.Repeat("e", 64), Assets: json.RawMessage(`[]`)}
	h.handleHumanInput(c, humanCoreCommand(b, reserve))
	waitForType(t, c, "queue_input_result")
	seal := messagequeue.HumanOperation{ID: "seal_formal", Kind: "seal", Request: reserve.Request, ExpectedRevision: 1, ContentRevision: 1, IntentHash: reserve.IntentHash, Content: reserve.Content, ReadyProof: reserve.IntentHash}
	cmd := humanTypedIntakeCommand(b, seal, nil, nil, "")
	h.handleHumanInput(c, cmd)
	receipt, err := h.messageQueue.ReadHumanOperation(b, seal.ID, "")
	if err != nil || receipt.Status != "committed" {
		t.Fatal("original formal seal not committed", receipt.Status, receipt.Code, err)
	}
	ns := taskapi.Namespace{Authority: b.Authority, StableScopeID: "device:" + b.Device, Generation: b.Generation, Path: "ordinary", Operation: "intake"}
	record, err := h.messageQueue.TaskJournal().Lookup(context.Background(), ns, reserve.Request)
	if err != nil {
		t.Fatal("formal task record missing from same commit", err)
	}
	entry, found, err := h.messageQueue.GetEffective(s.ID, reserve.Request)
	var payload queuedPayload
	json.Unmarshal(entry.Payload, &payload)
	if err != nil || !found || !payload.FormalIntake || payload.Content != "original goal" || record.RequestID != reserve.Request {
		t.Fatal("formal route lost", payload, err)
	}
	h.handleHumanInput(c, cmd)
	snapshot, err := h.messageQueue.Snapshot(s.ID)
	if err != nil || len(snapshot.Items) != 1 {
		t.Fatal("replay duplicated logical task", snapshot, err)
	}
	revision, membership, err := h.messageQueue.HumanMembership(b)
	if err != nil {
		t.Fatal(err)
	}
	inputs, _ := h.messageQueue.HumanInputs(b)
	v := inputs[0]
	boot, now := h.messageQueue.HumanClock()
	begin := messagequeue.HumanOperation{ID: "begin_formal_edit", Kind: "begin_edit", Request: reserve.Request, ExpectedRevision: v.Revision, ContentRevision: v.ContentRevision, ExpectedHash: v.EffectiveHash, QueueRevision: &revision, MembershipHash: membership}
	edit, err := h.messageQueue.HumanApply(b, begin, boot, now)
	if err != nil || edit.Status != "committed" {
		t.Fatal(edit, err)
	}
	v = *edit.Input
	commit := messagequeue.HumanOperation{ID: "commit_formal_edit", Kind: "commit_edit", Request: reserve.Request, ExpectedRevision: v.Revision, ContentRevision: v.ContentRevision, ExpectedHash: v.EffectiveHash, IntentHash: v.IntentHash, LeaseID: v.Lease.ID, LeaseGeneration: v.Lease.Generation, Content: "edited goal", Assets: json.RawMessage(`[]`), ReadyProof: reserve.IntentHash}
	h.handleHumanInput(c, humanCoreCommand(b, commit))
	edited, err := h.messageQueue.ReadHumanOperation(b, commit.ID, "")
	if err != nil || edited.Status != "committed" {
		t.Fatal("formal edit refused", edited.Status, edited.Code, err)
	}
	effective, _, _ := h.messageQueue.GetEffective(s.ID, reserve.Request)
	json.Unmarshal(effective.Payload, &payload)
	after, err := h.messageQueue.TaskJournal().Lookup(context.Background(), ns, reserve.Request)
	if err != nil || after.ReceiptID != record.ReceiptID || !payload.FormalIntake || payload.Content != "edited goal" || effective.ContentRevision != 2 {
		t.Fatal("edit replaced original task or lost formal policy", payload, effective.ContentRevision, err)
	}
}
func TestHumanTypedIntakePreparedSuccessorRetainsFirstNativeContinuation(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	release := s.HoldQueue()
	defer release()
	a := qaR4ReservePrepare(t, h, s, c, "r_plain_a", "")
	qaR4UploadSeal(t, h, s, c, a)
	actor, _ := h.mediaActor(c)
	b := messagequeue.HumanBinding{Device: "phone-a", Authority: h.cfg.InstanceID, Session: s.ID, Config: s.SettingsSnapshot().ConfigRevision, Generation: actor.NamespaceGeneration}
	data := mediaPNG(t)
	sum := sha256.Sum256(data)
	assets, _ := json.Marshal([]map[string]any{{"kind": "image", "media_type": "image/png", "sha256": hex.EncodeToString(sum[:]), "name": "圖片.png", "size": len(data)}})
	formal := messagequeue.HumanOperation{ID: "reserve_formal_image", Kind: "reserve", Origin: "formal_intake", Request: "r_" + strings.Repeat("c", 32), IntentHash: strings.Repeat("d", 64), Assets: assets, Predecessor: a.reservation.Request}
	h.handleHumanInput(c, humanCoreCommand(b, formal))
	waitForType(t, c, "queue_input_result")
	prepare, descriptor, body := mediaPrepareItem(t, h, s, c, formal.Request, data)
	calls := make(chan string, 3)
	h.SetExecutor(&qaR4Executor{fakeExec: &fakeExec{sink: h}, h: h, calls: calls, firstNative: true})
	release()
	select {
	case <-calls:
	case <-time.After(3 * time.Second):
		t.Fatal("initial native input absent")
	}
	upload := mediaUploadItem(t, h, s, c, prepare, descriptor, body, data)
	seal := messagequeue.HumanOperation{ID: "seal_formal_image", Kind: "seal", Request: formal.Request, ExpectedRevision: 1, ContentRevision: 1, IntentHash: formal.IntentHash, ReadyProof: body}
	h.handleHumanInput(c, humanTypedIntakeCommand(b, seal, []backend.ImageAttachment{{AttachmentID: upload, MediaType: descriptor.MediaType, SHA256: descriptor.SHA256, SizeBytes: descriptor.SizeBytes}}, nil, prepare.MediaBatchID))
	receipt, err := h.messageQueue.ReadHumanOperation(b, seal.ID, "")
	if err != nil || receipt.Status != "committed" {
		t.Fatal("original formal image continuation failed", receipt.Status, receipt.Code, err)
	}
	select {
	case id := <-calls:
		if id != formal.Request {
			t.Fatal("original formal successor changed", id)
		}
	case <-time.After(3 * time.Second):
		entry, _, _ := h.messageQueue.GetEffective(s.ID, formal.Request)
		t.Fatal("formal successor never consumed", entry.State, entry.Message)
	}
}
