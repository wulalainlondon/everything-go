package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

func mediaGCReferences(m *mediaLifecycle, id, ownReader string, now time.Time) bool {
	for _, b := range m.data.Batches {
		referenced := false
		sibling := false
		for intent, object := range b.Objects {
			if object == id {
				referenced = true
				if intent != b.CancelItem || b.CancelItem == "" {
					sibling = true
				}
			}
		}
		for _, i := range b.Items {
			if resumableAttachmentID(b.Device, i.UploadRequestID) == id {
				referenced = true
				if i.UploadRequestID != b.CancelItem || b.CancelItem == "" {
					sibling = true
				}
			}
		}
		if referenced && (sibling || b.State != "cancelled_before_admission" || b.Updated == 0 || now.UnixMilli()-b.Updated < int64(24*time.Hour/time.Millisecond)) {
			return true
		}
	}
	for key, r := range m.data.Readers {
		if key != ownReader && r.ObjectID == id {
			return true
		}
	}
	return false
}
func (h *Hub) mediaGCObject(sid, id string) (mediaGCTombstone, error) {
	var out mediaGCTombstone
	dir := filepath.Join(h.media.root, attachmentSafe(sid, "session"), id)
	m, e := h.unifiedUpload(sid, id)
	if e == nil {
		if m.Scope == nil {
			return out, errors.New("legacy completed object excluded")
		}
		return mediaGCTombstone{id, sid, m.Path, m.SHA256, m.SizeBytes, "tombstoned"}, nil
	}
	raw, e := os.ReadFile(filepath.Join(dir, "upload-state.json"))
	var state resumableUploadState
	if e != nil || json.Unmarshal(raw, &state) != nil || state.Scope == nil || state.UploadID != id || state.SessionID != sid || !jarvisSHA.MatchString(state.PrefixSHA256) {
		return out, errors.New("new pending identity unavailable")
	}
	path := state.PartPath
	if _, e := os.Lstat(path); os.IsNotExist(e) {
		if _, fe := os.Lstat(state.FinalPath); fe == nil {
			path = state.FinalPath
		}
	}
	return mediaGCTombstone{id, sid, path, state.PrefixSHA256, state.ReceivedSize, "tombstoned"}, nil
}

// The lifecycle guard is never held over a whole movie/hash or network RPC.
// A durable tombstone and exact GC reader pin separate verification from final
// unlink. Any original PREPARE/UNKNOWN/admitted/history/reader defeats deletion.
func (h *Hub) collectMediaGarbage(now time.Time) error {
	m := h.media
	if m == nil {
		return errors.New("media unavailable")
	}
	m.gcMu.Lock()
	defer m.gcMu.Unlock()
	done, e := m.begin()
	if e != nil {
		return e
	}
	candidates := map[string]string{}
	for _, b := range m.data.Batches {
		if b.State != "cancelled_before_admission" {
			continue
		}
		// Cancel fences the whole business, but only the named item is an
		// explicit abandonment. Editable siblings have no server draft handoff yet
		// and stay durable indefinitely rather than being guessed orphaned at24h.
		if b.CancelItem == "" {
			continue
		}
		if id := b.Objects[b.CancelItem]; id != "" {
			candidates[id] = b.Session
		}
		for _, i := range b.Items {
			if i.AttachmentID == "" && i.UploadRequestID == b.CancelItem {
				candidates[resumableAttachmentID(b.Device, i.UploadRequestID)] = b.Session
			}
		}
	}
	done()
	for id, sid := range candidates {
		if e := h.collectMediaObject(now, sid, id); e != nil {
			return e
		}
	}
	return nil
}
func (h *Hub) collectMediaObject(now time.Time, sid, id string) error {
	m := h.media
	done, e := m.begin()
	if e != nil {
		return e
	}
	own := "gc/" + id
	if mediaGCReferences(m, id, own, now) {
		done()
		return nil
	}
	object, e := h.mediaGCObject(sid, id)
	if e != nil {
		done()
		return nil
	}
	dir := filepath.Join(m.root, attachmentSafe(sid, "session"), id)
	if filepath.Dir(object.Path) != dir {
		done()
		return errors.New("GC object outside original directory")
	}
	if m.data.GC == nil {
		m.data.GC = map[string]mediaGCTombstone{}
	}
	if prior, ok := m.data.GC[id]; ok {
		if prior.Path != object.Path || prior.Hash != object.Hash || prior.Size != object.Size || prior.Session != sid {
			done()
			return errors.New("original GC tombstone changed")
		}
		if prior.State == "unlinked" {
			done()
			return nil
		}
	}
	before, e := os.Lstat(object.Path)
	if os.IsNotExist(e) {
		if _, ok := m.data.GC[id]; ok {
			object.State = "unlinked"
			m.data.GC[id] = object
			delete(m.data.Readers, own)
			e = m.save()
			done()
			return e
		}
		done()
		return nil
	}
	if e != nil || !before.Mode().IsRegular() || before.Size() != object.Size {
		done()
		return nil
	}
	f, e := os.Open(object.Path)
	if e != nil {
		done()
		return e
	}
	opened, e := f.Stat()
	if e != nil || !os.SameFile(before, opened) {
		f.Close()
		done()
		return errors.New("GC object replaced")
	}
	defer f.Close()
	m.data.GC[id] = object
	if m.data.Readers == nil {
		m.data.Readers = map[string]mediaReadLease{}
	}
	m.data.Readers[own] = mediaReadLease{id, sid, object.Path, object.Hash, object.Size, "gc"}
	if e = m.save(); e != nil {
		done()
		return e
	}
	if e = m.inject("gc_tombstone_before_unlink"); e != nil {
		done()
		return e
	}
	done()
	// Full streaming verification outside media/admission guard, exact originalFD.
	digest := sha256.New()
	n, e := io.Copy(digest, io.LimitReader(f, object.Size+1))
	if e != nil || n != object.Size || hex.EncodeToString(digest.Sum(nil)) != object.Hash {
		return errors.New("GC exact object digest mismatch; retain tombstone")
	}
	done, e = m.begin()
	if e != nil {
		return e
	}
	defer done()
	if mediaGCReferences(m, id, own, now) {
		return nil
	}
	current, e := os.Lstat(object.Path)
	held, se := f.Stat()
	if e != nil || se != nil || !os.SameFile(before, current) || !os.SameFile(before, held) || current.Size() != object.Size || current.ModTime() != before.ModTime() {
		return errors.New("GC final inode/path witness changed")
	}
	if prior := m.data.GC[id]; prior.Path != object.Path || prior.Hash != object.Hash || prior.Size != object.Size {
		return errors.New("GC durable tombstone changed")
	}
	if e = os.Remove(object.Path); e != nil {
		return e
	}
	if e = m.inject("gc_unlink_before_receipt"); e != nil {
		return e
	}
	directory, e := os.Open(dir)
	if e != nil {
		return e
	}
	e = directory.Sync()
	directory.Close()
	if e != nil {
		return e
	}
	object.State = "unlinked"
	m.data.GC[id] = object
	delete(m.data.Readers, own)
	return m.save()
}
func (h *Hub) StartMediaMaintenance(ctx context.Context) {
	if h.media == nil || h.media.err != nil {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				_ = h.collectMediaGarbage(now)
			}
		}
	}()
}
