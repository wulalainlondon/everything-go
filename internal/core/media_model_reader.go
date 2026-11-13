package core

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"

	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
)

func (h *Hub) acquireModelMediaReader(s *session.Session, request string, p *queuedPayload) (func(), error) {
	if p.MediaBatchID == "" {
		for _, f := range p.Files {
			if backend.IsAudioType(f.MediaType) {
				return nil, errors.New("recording requires original media admission")
			}
		}
		return func() {}, nil
	}
	done, e := h.media.begin()
	if e != nil {
		return nil, e
	}
	defer done()
	var batch *mediaBatch
	for _, b := range h.media.data.Batches {
		if b.ID == p.MediaBatchID && b.Session == s.ID && b.Device == p.OwnerDevice {
			if batch != nil {
				return nil, errors.New("ambiguous media actor")
			}
			batch = b
		}
	}
	if batch != nil && batch.State == "admitting_unknown" && mediaQueueBinding(batch, *p) {
		if entry, found, e := h.messageQueue.GetEffective(s.ID, request); e == nil && found {
			var original queuedPayload
			if json.Unmarshal(entry.Payload, &original) == nil && mediaQueueBinding(batch, original) {
				batch.State = "admitted"
				if e = h.media.save(); e != nil {
					return nil, e
				}
			}
		}
	}
	if batch == nil || batch.State != "admitted" || mediaBusinessID(batch) != request || !mediaQueueBinding(batch, *p) {
		return nil, errors.New("media admission unresolved")
	}
	if e := h.mediaExecutionCurrent(batch); e != nil {
		return nil, e
	}
	currentActor := false
	for _, b := range h.pairing.DeviceBindings() {
		sum := sha256.Sum256([]byte(b.Token))
		generation := binary.BigEndian.Uint64(sum[:8]) & ((1 << 53) - 1)
		if b.DeviceID == batch.Device && generation == batch.Generation {
			currentActor = true
		}
	}
	if !currentActor {
		return nil, errors.New("media original credential generation retired")
	}
	if err := h.restoreModelAudioInput(s, p, batch); err != nil {
		return nil, err
	}
	if h.media.data.Readers == nil {
		h.media.data.Readers = map[string]mediaReadLease{}
	}
	leases := []string{}
	for _, i := range batch.Items {
		id := batch.Objects[i.UploadRequestID]
		m, e := h.unifiedUpload(batch.Session, id)
		if e != nil || m.SizeBytes != i.SizeBytes || m.SHA256 != i.SHA256 {
			return nil, errors.New("media object binding changed")
		}
		if t, ok := h.media.data.GC[id]; ok && t.State != "" {
			return nil, errors.New("media object tombstoned")
		}
		lease := "model/" + s.ID + "/" + request + "/" + i.ItemID
		h.media.data.Readers[lease] = mediaReadLease{id, s.ID, m.Path, m.SHA256, m.SizeBytes, "model"}
		leases = append(leases, lease)
	}
	if e = h.media.save(); e != nil {
		return nil, e
	}
	return func() {
		release, e := h.media.begin()
		if e != nil {
			return
		}
		defer release()
		entry, found, e := h.messageQueue.GetEffective(s.ID, request)
		if e != nil || !found {
			return
		}
		var q queuedPayload
		if json.Unmarshal(entry.Payload, &q) != nil || q.MediaBatchID != p.MediaBatchID {
			return
		}
		if entry.State != messagequeue.Completed && entry.State != messagequeue.Failed && entry.State != messagequeue.Cancelled {
			return
		}
		for _, id := range leases {
			delete(h.media.data.Readers, id)
		}
		_ = h.media.save()
	}, nil
}

// Queue JSON intentionally drops server-only trust markers. Rebuild audio trust
// only inside the original admitted/current model-reader boundary, using the
// exact batch descriptor and freshly verified immutable upload bytes.
func (h *Hub) restoreModelAudioInput(s *session.Session, p *queuedPayload, batch *mediaBatch) error {
	files := append([]backend.FileAttachment(nil), p.Files...)
	for n, file := range files {
		if !backend.IsAudioType(file.MediaType) {
			continue
		}
		matched := false
		for _, item := range batch.Items {
			if batch.Objects[item.UploadRequestID] == file.AttachmentID && file.AttachmentID != "" && item.UploadKind == "file" && item.MediaType == file.MediaType && item.SHA256 == file.SHA256 && item.SizeBytes == file.SizeBytes && item.Name == file.Name {
				matched = true
				break
			}
		}
		if !matched {
			return errors.New("recording differs from original admitted batch")
		}
		file.VerifiedAudioPath = ""
		_, restored, err := h.resolveUploadedVideos(s.ID, "", []backend.FileAttachment{file})
		if err != nil {
			return err
		}
		if len(restored) != 1 || restored[0].VerifiedAudioPath == "" || restored[0].SHA256 != file.SHA256 || restored[0].SizeBytes != file.SizeBytes {
			return errors.New("recording verification unavailable at consumption")
		}
		files[n] = restored[0]
	}
	p.Files = files
	return nil
}
