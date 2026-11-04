package core

import (
	"strings"
	"time"

	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
)

// A release retires one cancelled business's draft pin, never the object or
// another business's reference. The immutable receipt and pin change share one
// fsync/rename journal transaction. An ambiguous save is status-only recoverable.
type mediaDraftRelease struct {
	ID              string `json:"operation_id"`
	PayloadHash     string `json:"payload_hash"`
	Session         string `json:"session_id"`
	BatchID         string `json:"media_batch_id"`
	Revision        uint64 `json:"media_batch_revision"`
	BodyHash        string `json:"media_body_hash"`
	UploadRequestID string `json:"upload_request_id"`
	ItemID          string `json:"item_id"`
	ItemRevision    uint64 `json:"item_revision"`
	SHA256          string `json:"sha256"`
	UploadID        string `json:"upload_id"`
	HandoffBatchID  string `json:"handoff_batch_id,omitempty"`
	Phase           string `json:"phase"`
	Error           string `json:"error_code,omitempty"`
	At              int64  `json:"released_at_ms"`
}

func mediaReleaseDescriptor(b *mediaBatch, i protocol.MediaBatchItem) map[string]any {
	id := b.Objects[i.UploadRequestID]
	if id == "" {
		id = resumableAttachmentID(b.Device, i.UploadRequestID)
	}
	return map[string]any{"session_id": b.Session, "media_batch_id": b.ID, "media_batch_revision": b.Revision, "media_body_hash": b.BodyHash, "upload_request_id": i.UploadRequestID, "item_id": i.ItemID, "item_revision": i.ItemRevision, "sha256": i.SHA256, "upload_id": id}
}
func mediaDraftReleaseHash(cmd clientproto.Command) string {
	return mediaHash([]any{cmd.SessionID, cmd.MediaBatchID, cmd.MediaBatchRevision, cmd.MediaBodyHash, cmd.UploadRequestID, cmd.ItemID, cmd.ItemRevision, cmd.SHA256, cmd.UploadID, cmd.HandoffBatchID})
}
func (h *Hub) handleMediaDraftRelease(c *Client, cmd clientproto.Command) {
	id := cmd.RequestID
	status := cmd.Kind == "media_draft_release_status"
	if status {
		id = cmd.OperationID
	}
	event := map[string]any{"type": "media_draft_release_receipt", "request_id": cmd.RequestID, "operation_id": id, "session_id": cmd.SessionID, "authority_instance_id": h.cfg.InstanceID, "physical_delete": false}
	emit := func(phase, code string) {
		event["phase"] = phase
		if code != "" {
			event["error_code"] = code
		}
		c.enqueueEvent(event)
	}
	actor, e := h.mediaActor(c)
	if e != nil {
		emit("unknown", "caller_unbound")
		return
	}
	if _, e = h.intakeSession(cmd.SessionID); e != nil {
		emit("unknown", "permission")
		return
	}
	if id == "" || len(id) > 200 || cmd.RequestID == "" {
		emit("rejected_known_none", "invalid_argument")
		return
	}
	done, e := h.media.begin()
	if e != nil {
		emit("unknown", "store_unavailable")
		return
	}
	defer done()
	device := strings.TrimPrefix(actor.StableScopeID, "device:")
	key := mediaKey(device, actor.NamespaceGeneration, id)
	if prior := h.media.data.DraftReleases[key]; prior != nil {
		if prior.Session != cmd.SessionID {
			emit("unknown", "permission")
			return
		}
		mediaDraftReleaseReceipt(event, prior)
		if !status && prior.PayloadHash != mediaDraftReleaseHash(cmd) {
			event["error_code"] = "idempotency_conflict"
		}
		c.enqueueEvent(event)
		return
	}
	if status {
		emit("unknown", "not_found")
		return
	}
	r := &mediaDraftRelease{ID: id, PayloadHash: mediaDraftReleaseHash(cmd), Session: cmd.SessionID, BatchID: cmd.MediaBatchID, Revision: cmd.MediaBatchRevision, BodyHash: cmd.MediaBodyHash, UploadRequestID: cmd.UploadRequestID, ItemID: cmd.ItemID, ItemRevision: cmd.ItemRevision, SHA256: cmd.SHA256, UploadID: cmd.UploadID, HandoffBatchID: cmd.HandoffBatchID, Phase: "rejected_known_none"}
	batch := h.media.data.Batches[mediaKey(device, actor.NamespaceGeneration, cmd.MediaBatchID)]
	var item *protocol.MediaBatchItem
	switch {
	case batch == nil || batch.Session != cmd.SessionID:
		r.Error = "batch_not_found"
	case batch.State != "cancelled_before_admission":
		r.Error = "parent_not_cancelled"
	case batch.Revision != cmd.MediaBatchRevision || batch.BodyHash != cmd.MediaBodyHash:
		r.Error = "descriptor_conflict"
	case h.mediaBatchTarget(batch) != nil:
		r.Error = "source_changed"
	default:
		for j := range batch.Items {
			i := &batch.Items[j]
			if i.UploadRequestID == cmd.UploadRequestID && i.ItemID == cmd.ItemID && i.ItemRevision == cmd.ItemRevision && i.SHA256 == cmd.SHA256 {
				item = i
				break
			}
		}
		if item == nil {
			r.Error = "descriptor_conflict"
		} else {
			object := batch.Objects[item.UploadRequestID]
			if object == "" {
				object = resumableAttachmentID(device, item.UploadRequestID)
			}
			if cmd.UploadID != "" && cmd.UploadID != object {
				r.Error = "descriptor_conflict"
			} else {
				r.UploadID = object
			}
		}
	}
	if r.Error == "" && batch.ReleasedItems[cmd.UploadRequestID] != "" {
		r.Error = "already_released"
	}
	if r.Error == "" && cmd.HandoffBatchID != "" {
		next := h.media.data.Batches[mediaKey(device, actor.NamespaceGeneration, cmd.HandoffBatchID)]
		protected := false
		if next != nil && next.ID != batch.ID && next.Session == batch.Session && next.State == "prepared" && h.mediaBatchTarget(next) == nil {
			for _, i := range next.Items {
				if next.Objects[i.UploadRequestID] == r.UploadID && i.SHA256 == item.SHA256 && i.SizeBytes == item.SizeBytes && i.MediaType == item.MediaType {
					protected = true
				}
			}
		}
		if !protected {
			r.Error = "handoff_not_pinned"
		}
	}
	if r.Error == "" {
		r.Phase = "committed"
		r.At = time.Now().UnixMilli()
		if batch.ReleasedItems == nil {
			batch.ReleasedItems = map[string]string{}
		}
		batch.ReleasedItems[cmd.UploadRequestID] = id
	}
	h.media.data.DraftReleases[key] = r
	if h.media.fault != nil {
		if e = h.media.fault("draft_release_before_save"); e != nil {
			emit("unknown", "store_unavailable")
			return
		}
	}
	if e = h.media.save(); e != nil {
		emit("unknown", "store_unavailable")
		return
	}
	if h.media.fault != nil {
		if e = h.media.fault("draft_release_after_save"); e != nil {
			emit("unknown", "store_unavailable")
			return
		}
	}
	mediaDraftReleaseReceipt(event, r)
	c.enqueueEvent(event)
}
func mediaDraftReleaseReceipt(event map[string]any, r *mediaDraftRelease) {
	event["operation_id"] = r.ID
	event["session_id"] = r.Session
	event["media_batch_id"] = r.BatchID
	event["media_batch_revision"] = r.Revision
	event["media_body_hash"] = r.BodyHash
	event["upload_request_id"] = r.UploadRequestID
	event["item_id"] = r.ItemID
	event["item_revision"] = r.ItemRevision
	event["sha256"] = r.SHA256
	event["upload_id"] = r.UploadID
	event["handoff_batch_id"] = r.HandoffBatchID
	event["phase"] = r.Phase
	event["released_at_ms"] = r.At
	if r.Error != "" {
		event["error_code"] = r.Error
	}
}

// Only a matching committed original receipt can retire this exact draft pin.
// A new reference, including another alias in the SAME batch, stays independent.
func mediaDraftAbandonedAt(m *mediaLifecycle, b *mediaBatch, intent string) int64 {
	if b.State != "cancelled_before_admission" {
		return 0
	}
	if intent == b.CancelItem && intent != "" && !b.CancelKeepDraft {
		return b.Updated
	}
	operation := b.ReleasedItems[intent]
	object := b.Objects[intent]
	if object == "" {
		object = resumableAttachmentID(b.Device, intent)
	}
	r := m.data.DraftReleases[mediaKey(b.Device, b.Generation, operation)]
	if operation != "" && r != nil && r.Phase == "committed" && r.Session == b.Session && r.BatchID == b.ID && r.Revision == b.Revision && r.BodyHash == b.BodyHash && r.UploadRequestID == intent && r.UploadID == object {
		for _, i := range b.Items {
			if i.UploadRequestID == intent && i.ItemID == r.ItemID && i.ItemRevision == r.ItemRevision && i.SHA256 == r.SHA256 {
				return r.At
			}
		}
	}
	return 0
}
