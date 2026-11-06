package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// Execution witnesses are server-observed continuations of a frozen admitted
// source. They never change its PREPARE descriptor/hash or permit a new send.
func mediaExecutionSource(b *mediaBatch) (string, uint64) {
	if b.ExecutionSource != 0 {
		return b.ExecutionThread, b.ExecutionSource
	}
	return b.Thread, b.SourceRevision
}
func (h *Hub) mediaExecutionCurrent(b *mediaBatch) error {
	s, e := h.intakeSession(b.Session)
	if e != nil {
		return e
	}
	v := s.SettingsSnapshot()
	thread, revision := mediaExecutionSource(b)
	if v.ResumeID != thread || v.ConfigRevision != b.Config || v.SourceRevision != revision || h.SessionRoleEpoch(s) != b.RoleEpoch || !h.jarvisCredentialCurrent(b.Device, b.Generation) {
		return errors.New("media execution source retired")
	}
	return nil
}
func (h *Hub) mediaRunning(b *mediaBatch) bool {
	e, found, err := h.messageQueue.GetEffective(b.Session, mediaBusinessID(b))
	if err != nil || !found || e.State != messagequeue.Running {
		return false
	}
	var payload queuedPayload
	return json.Unmarshal(e.Payload, &payload) == nil && mediaQueueBinding(b, payload)
}
func (h *Hub) migrateMediaInitialThread(id, thread string) {
	if h.media == nil || thread == "" {
		return
	}
	done, e := h.media.begin()
	if e != nil {
		return
	}
	defer done()
	s, ok := h.registry.Get(id)
	if !ok {
		return
	}
	v := s.SettingsSnapshot()
	changed := false
	for _, b := range h.media.data.Batches {
		if b.Session == id && h.continueMediaInitialThread(b) {
			continue
		}
		old, rev := mediaExecutionSource(b)
		if b.Session == id && b.State == "admitted" && old == "" && v.ResumeID == thread && v.ConfigRevision == b.Config && v.SourceRevision == rev+1 && h.SessionRoleEpoch(s) == b.RoleEpoch && h.mediaRunning(b) && h.jarvisCredentialCurrent(b.Device, b.Generation) {
			b.ExecutionThread = thread
			b.ExecutionSource = v.SourceRevision
			changed = true
		}
	}
	if changed {
		_ = h.media.save()
	}
}
func (h *Hub) migrateMediaDefaultEffort(t *session.DefaultEffortTransition) {
	if h.media == nil || t == nil {
		return
	}
	done, e := h.media.begin()
	if e != nil {
		return
	}
	defer done()
	s, found := h.registry.Get(t.Source())
	if !found {
		return
	}
	changed := false
	for _, b := range h.media.data.Batches {
		thread, rev := mediaExecutionSource(b)
		if b.Session != s.ID || b.State != "admitted" || !h.mediaRunning(b) || !h.jarvisCredentialCurrent(b.Device, b.Generation) {
			continue
		}
		epoch := h.SessionRoleEpoch(s)
		e = s.WithMediaDefaultEffortTransition(t, func(before, after session.Snapshot, _ string) error {
			if before.ResumeID != thread || before.SourceRevision != rev || after.ResumeID != thread || before.ConfigRevision != b.Config || after.ConfigRevision != b.Config || before.Cwd != after.Cwd || before.Backend != after.Backend || before.Model != after.Model || before.Sandbox != after.Sandbox || before.Hidden != after.Hidden || epoch != b.RoleEpoch {
				return errors.New("media observed metadata source changed")
			}
			b.ExecutionThread = thread
			b.ExecutionSource = after.SourceRevision
			changed = true
			return nil
		})
		_ = e
	}
	if changed {
		_ = h.media.save()
	}
}
func (h *Hub) withMediaInputGuard(ctx context.Context, sid, request string, p queuedPayload) context.Context {
	if p.MediaBatchID == "" {
		return ctx
	}
	prior := ctx
	return backend.WithTaskInputGuard(ctx, func() error {
		if e := backend.CheckTaskInputGuard(prior); e != nil {
			return e
		}
		done, e := h.media.begin()
		if e != nil {
			return e
		}
		defer done()
		for _, b := range h.media.data.Batches {
			if b.Session == sid && b.ID == p.MediaBatchID && mediaBusinessID(b) == request && b.Device == strings.TrimPrefix(p.OwnerDevice, "device:") && mediaQueueBinding(b, p) && b.State == "admitted" {
				return h.mediaExecutionCurrent(b)
			}
		}
		return errors.New("media original admission unavailable")
	})
}

// mediaContinuationOwned verifies the original frozen logical input, including
// a not-yet-ready reservation. A name, current thread, or session alone is never
// evidence that a prepared upload belongs to this actor continuation.
func (h *Hub) mediaContinuationOwned(b *mediaBatch) bool {
	if b == nil || h.messageQueue == nil || (b.State != "prepared" && b.State != "admitted") || !h.jarvisCredentialCurrent(b.Device, b.Generation) {
		return false
	}
	// A prepared edit belongs to its exact live lease, not to the old effective
	// media payload. That payload intentionally remains unchanged until commit.
	// Once admitted, the effective version below becomes its ownership proof.
	if b.State == "prepared" && b.HumanStage != nil {
		binding, found := h.messageQueue.HumanBindingForRequest(b.Session, mediaBusinessID(b))
		return found && binding.Device == b.Device && binding.Generation == b.Generation && binding.Config == b.Config && binding.Epoch == b.RoleEpoch && binding.Authority == h.cfg.InstanceID && b.HumanStage.Binding == binding && h.mediaHumanStageLive(b.HumanStage)
	}
	if entry, found, err := h.messageQueue.GetEffective(b.Session, mediaBusinessID(b)); err == nil && found && (entry.State == messagequeue.Queued || entry.State == messagequeue.Running) {
		var payload queuedPayload
		return json.Unmarshal(entry.Payload, &payload) == nil && mediaQueueBinding(b, payload)
	}
	if b.State != "prepared" {
		return false
	}
	binding, found := h.messageQueue.HumanBindingForRequest(b.Session, mediaBusinessID(b))
	if !found || binding.Device != b.Device || binding.Generation != b.Generation || binding.Config != b.Config || binding.Epoch != b.RoleEpoch || binding.Authority != h.cfg.InstanceID {
		return false
	}
	rows, err := h.messageQueue.HumanInputs(binding)
	if err != nil {
		return false
	}
	for _, v := range rows {
		if v.ID != mediaBusinessID(b) || v.Binding != binding || v.State != "reserved_waiting_assets" || v.Lease != nil || v.Content != b.Content {
			continue
		}
		var assets []map[string]any
		if json.Unmarshal(v.Assets, &assets) != nil || len(assets) != len(b.Items) {
			return false
		}
		remaining := append([]protocol.MediaBatchItem(nil), b.Items...)
		for _, asset := range assets {
			at := -1
			for i, item := range remaining {
				if asset["kind"] == item.UploadKind && asset["name"] == item.Name && asset["media_type"] == item.MediaType && asset["sha256"] == item.SHA256 && asset["size"] == float64(item.SizeBytes) {
					at = i
					break
				}
			}
			if at < 0 {
				return false
			}
			remaining = append(remaining[:at], remaining[at+1:]...)
		}
		return len(remaining) == 0
	}
	return false
}

// Only a native serial actor can advance existing original batches across its
// configuration activation. Waiting/prepared successors receive the same exact
// source continuation; their own frozen config/body/revision are not changed.
func (h *Hub) migrateMediaQueuedConfiguration(s *session.Session, request string, p queuedPayload) error {
	if h.media == nil {
		if p.MediaBatchID == "" {
			return nil
		}
		return errors.New("media queued configuration unavailable")
	}
	if p.Configuration == nil {
		return errors.New("media queued configuration unavailable")
	}
	done, err := h.media.begin()
	if err != nil {
		if p.MediaBatchID == "" {
			return nil
		}
		return err
	}
	defer done()
	if p.MediaBatchID == "" {
		relevant := false
		for _, b := range h.media.data.Batches {
			if b.Session == s.ID && (b.State == "prepared" || b.State == "admitted") {
				relevant = true
				break
			}
		}
		if !relevant {
			return nil
		}
	}
	type prior struct {
		b      *mediaBatch
		thread string
		source uint64
	}
	changed := []prior{}
	var before, after session.Snapshot
	var config session.Configuration
	err = s.WithQueuedConfigurationActivation(request, func(b, a session.Snapshot, c session.Configuration) error {
		if c != *p.Configuration || b.ResumeID != a.ResumeID || b.Cwd != a.Cwd || b.Hidden != a.Hidden || b.Hidden || b.Backend != a.Backend || b.Sandbox != a.Sandbox {
			return errors.New("media exact queued configuration witness changed")
		}
		before, after, config = b, a, c
		return nil
	})
	if err == nil {
		// Never borrow the queue database while holding Session.mu: a concurrent
		// original writer commits that same DB under CommitInputSource. Keep media
		// bytes locked, qualify original queue ownership here, then re-pin the exact
		// private actor witness for the final journal write. Cancellation/edit guards
		// are still checked independently at each batch admission/native effect.
		originalFound := p.MediaBatchID == ""
		epoch := h.SessionRoleEpoch(s)
		candidates := []*mediaBatch{}
		for _, b := range h.media.data.Batches {
			if b.Session != s.ID || b.RoleEpoch != epoch {
				continue
			}
			thread, revision := mediaExecutionSource(b)
			if thread != before.ResumeID || revision != before.SourceRevision || !h.mediaContinuationOwned(b) {
				continue
			}
			if b.ID == p.MediaBatchID && mediaBusinessID(b) == request && b.Config == config.Revision && mediaQueueBinding(b, p) {
				originalFound = true
			}
			if revision != after.SourceRevision {
				candidates = append(candidates, b)
			}
		}
		if !originalFound {
			err = errors.New("media original queued configuration witness unavailable")
		} else {
			err = s.WithQueuedConfigurationActivation(request, func(b, a session.Snapshot, c session.Configuration) error {
				if b.SourceRevision != before.SourceRevision || a.SourceRevision != after.SourceRevision || b.ResumeID != before.ResumeID || a.ResumeID != after.ResumeID || c != config {
					return errors.New("media exact queued configuration witness changed")
				}
				for _, batch := range candidates {
					changed = append(changed, prior{batch, batch.ExecutionThread, batch.ExecutionSource})
					batch.ExecutionThread = before.ResumeID
					batch.ExecutionSource = after.SourceRevision
				}
				if len(changed) > 0 {
					return h.media.save()
				}
				return nil
			})
		}
	}

	if err != nil {
		for _, old := range changed {
			old.b.ExecutionThread = old.thread
			old.b.ExecutionSource = old.source
		}
	}
	if p.MediaBatchID == "" {
		return nil
	}
	return err
}

// A blank thread may continue only across one exact initial thread transition
// with durable native acceptance for the same original device/config/epoch.
// This runs under the media lock, including when acceptance was recorded after
// the SessionUUID event. No new prepare descriptor or payload is manufactured.
func (h *Hub) continueMediaInitialThread(b *mediaBatch) bool {
	thread, revision := mediaExecutionSource(b)
	if thread != "" || !h.mediaContinuationOwned(b) {
		return false
	}
	s, ok := h.registry.Get(b.Session)
	if !ok {
		return false
	}
	v := s.SettingsSnapshot()
	binding, found := h.messageQueue.HumanBindingForRequest(b.Session, mediaBusinessID(b))
	if !found || binding.Device != b.Device || binding.Generation != b.Generation || binding.Config != b.Config || binding.Epoch != b.RoleEpoch || binding.Authority != h.cfg.InstanceID || binding.Thread != "" || v.ResumeID == "" || v.SourceRevision != revision+1 || v.ConfigRevision != b.Config || v.Hidden || h.SessionRoleEpoch(s) != b.RoleEpoch || !h.messageQueue.HumanThreadCompatible(binding, v.ResumeID) {
		return false
	}
	oldThread, oldSource := b.ExecutionThread, b.ExecutionSource
	b.ExecutionThread = v.ResumeID
	b.ExecutionSource = v.SourceRevision
	if h.media.save() != nil {
		b.ExecutionThread = oldThread
		b.ExecutionSource = oldSource
		return false
	}
	return true
}
