package core

import (
	"context"
	"errors"
	"strings"

	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
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
	e, found, err := h.messageQueue.Get(b.Session, b.ID)
	if err != nil || !found || e.State != messagequeue.Running {
		return false
	}
	return true
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
			if b.Session == sid && b.ID == request && b.Device == strings.TrimPrefix(p.OwnerDevice, "device:") && mediaQueueBinding(b, p) && b.State == "admitted" {
				return h.mediaExecutionCurrent(b)
			}
		}
		return errors.New("media original admission unavailable")
	})
}
