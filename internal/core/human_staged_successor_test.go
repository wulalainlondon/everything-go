package core

import (
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

func TestHumanStagedSuccessorUploadsCommitsAndConsumesEditedBody(t *testing.T) {
	for _, transition := range []string{"config_activation", "initial_thread", "both"} {
		t.Run(transition, func(t *testing.T) {
			h, s, c, _ := mediaFixture(t)
			release := s.HoldQueue()
			defer func() { s.Close(); release() }()
			if transition != "initial_thread" {
				cfg := session.ConfigurationFrom(s.SettingsSnapshot())
				cfg.Model = "next"
				if _, e := s.SetFutureConfiguration(cfg, nil); e != nil {
					t.Fatal(e)
				}
			}
			actor, _ := h.mediaActor(c)
			b := messagequeue.HumanBinding{Device: "phone-a", Authority: h.cfg.InstanceID, Session: s.ID, Config: s.SettingsSnapshot().ConfigRevision, Epoch: h.SessionRoleEpoch(s), Generation: actor.NamespaceGeneration}
			reserveSeal := func(id, previous string) {
				reserve := messagequeue.HumanOperation{ID: "reserve_" + id, Kind: "reserve", Request: id, Content: "original " + id, IntentHash: strings.Repeat("d", 64), Assets: json.RawMessage(`[]`), Predecessor: previous}
				h.handleHumanInput(c, humanCoreCommand(b, reserve))
				waitForType(t, c, "queue_input_result")
				seal := messagequeue.HumanOperation{ID: "seal_" + id, Kind: "seal", Request: id, ExpectedRevision: 1, ContentRevision: 1, IntentHash: reserve.IntentHash, Content: reserve.Content, ReadyProof: reserve.IntentHash}
				h.handleHumanInput(c, humanCoreCommand(b, seal))
				receipt, e := h.messageQueue.ReadHumanOperation(b, seal.ID, "")
				if e != nil || receipt.Status != "committed" {
					t.Fatal(receipt, e)
				}
			}
			reserveSeal("r_sib_a", "")
			reserveSeal("r_sib_b", "r_sib_a")
			rows, _ := h.messageQueue.HumanInputs(b)
			var base messagequeue.HumanInput
			for _, v := range rows {
				if v.ID == "r_sib_b" {
					base = v
				}
			}
			rev, membership, _ := h.messageQueue.HumanMembership(b)
			boot, now := h.messageQueue.HumanClock()
			edit, e := h.messageQueue.HumanApply(b, messagequeue.HumanOperation{ID: "begin_sib_b", Kind: "begin_edit", Request: base.ID, ExpectedRevision: base.Revision, ContentRevision: base.ContentRevision, ExpectedHash: base.EffectiveHash, QueueRevision: &rev, MembershipHash: membership}, boot, now)
			if e != nil || edit.Status != "committed" {
				t.Fatal(edit, e)
			}
			data := mediaPNG(t)
			sum := sha256.Sum256(data)
			item := protocol.MediaBatchItem{ItemID: "stage_image", ItemRevision: 1, UploadRequestID: "vu_stage_b", UploadKind: "image", Name: "edit.png", MediaType: "image/png", SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
			lease := edit.Input.Lease
			raw, _ := json.Marshal(map[string]any{"binding": b, "stage": map[string]any{"request_id": base.ID, "content_revision": base.ContentRevision, "expected_hash": base.EffectiveHash, "lease_id": lease.ID, "lease_generation": lease.Generation}})
			cfg := b.Config
			prepare := clientproto.Command{Kind: "media_batch_prepare", RequestID: "prepare_staged_sib_b", SessionID: s.ID, MediaBatchID: "batch_edit_sib_b", MediaBatchRevision: 1, ExpectedConfigRevision: &cfg, Content: "edited B", MediaItems: []protocol.MediaBatchItem{item}, QueueInput: raw}
			h.handleMediaBatch(c, prepare)
			prepared := waitForType(t, c, "media_batch_receipt")
			if prepared["state"] != "prepared" {
				t.Fatal(prepared)
			}
			body := prepared["body_hash"].(string)
			calls := make(chan string, 3)
			h.SetExecutor(&qaR4Executor{fakeExec: &fakeExec{sink: h}, h: h, calls: calls, firstNative: transition != "config_activation"})
			release()
			select {
			case id := <-calls:
				if id != "r_sib_a" {
					t.Fatal(id)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("preceding A not consumed")
			}
			boot, now = h.messageQueue.HumanClock()
			if !h.messageQueue.HumanStageAllowed(b, base.ID, base.ContentRevision, base.EffectiveHash, lease.ID, lease.Generation, boot, now) {
				t.Fatal("staged base/lease no longer valid")
			}
			if transition != "config_activation" && !h.messageQueue.HumanThreadCompatible(b, s.ResumeID()) {
				t.Fatal("exact initial native acceptance unavailable")
			}
			upload := mediaUploadItem(t, h, s, c, prepare, item, body, data)
			assets, _ := json.Marshal([]map[string]any{{"kind": "image", "media_type": item.MediaType, "sha256": item.SHA256, "name": item.Name, "size": item.SizeBytes}})
			op := messagequeue.HumanOperation{ID: "commit_staged_successor", Kind: "commit_edit", Request: base.ID, ExpectedRevision: edit.Input.Revision, ContentRevision: base.ContentRevision, ExpectedHash: base.EffectiveHash, IntentHash: base.IntentHash, Content: "edited B", Assets: assets, LeaseID: lease.ID, LeaseGeneration: lease.Generation, ReadyProof: body}
			cmd := humanCoreCommand(b, op)
			cmd.MediaBatchID = prepare.MediaBatchID
			cmd.Images = []backend.ImageAttachment{{AttachmentID: upload, MediaType: item.MediaType, SHA256: item.SHA256, SizeBytes: item.SizeBytes}}
			h.handleHumanInput(c, cmd)
			receipt, e := h.messageQueue.ReadHumanOperation(b, op.ID, "")
			if e != nil || receipt.Status != "committed" {
				t.Fatal("staged successor edit not committed", receipt, e)
			}
			select {
			case id := <-calls:
				if id != base.ID {
					t.Fatal(id)
				}
			case <-time.After(3 * time.Second):
				entry, _, _ := h.messageQueue.GetEffective(s.ID, base.ID)
				t.Fatal("edited successor not consumed", entry.State, entry.Message)
			}
			entry, _, _ := h.messageQueue.GetEffective(s.ID, base.ID)
			var payload queuedPayload
			json.Unmarshal(entry.Payload, &payload)
			if payload.Content != "edited B" || payload.MediaBatchID != prepare.MediaBatchID {
				t.Fatal("old effective base was consumed", payload)
			}
			done, e := h.media.begin()
			if e != nil {
				t.Fatal(e)
			}
			defer done()
			batch := h.media.data.Batches[mediaKey(b.Device, b.Generation, prepare.MediaBatchID)]
			if batch.BodyHash != body || batch.Thread != b.Thread || batch.Config != b.Config || batch.HumanStage.Request != base.ID {
				t.Fatal("original staged descriptor changed", batch)
			}
		})
	}
}
