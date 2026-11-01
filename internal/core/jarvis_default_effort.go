package core

import (
	"errors"
	"everything-go/internal/session"
	"log"
)

// This is called only at original host metadata/normal-terminal transitions.
// Snapshot reads never repair a stale role and arbitrary SourceRevision changes
// remain fenced. Session lock + original role epoch/revision/source CAS prevent
// a late callback, human settings change, revoke or A->B->A from being adopted.
func (h *Hub) migrateJarvisDefaultEffort(t *session.DefaultEffortTransition) {
	if t == nil || h.jarvisRoles == nil || h.partners == nil {
		return
	}
	h.jarvisRoleMu.Lock()
	committed := false
	defer func() {
		h.jarvisRoleMu.Unlock()
		if committed {
			h.emitJarvisRoleChanged()
		}
	}()
	_, a := h.jarvisRoles.Snapshot()
	s, found := h.registry.Get(t.Source())
	if !found || a == nil || !a.Enabled || a.Target.Session != t.Source() || a.Epoch != t.RoleEpoch() || !h.jarvisRoleEligible(s) || a.PolicyHash != h.jarvisRolePolicyHash() || !h.jarvisCredentialCurrent(a.Device, a.Generation) {
		return
	}
	p, ok := h.partners.ForSession(s.ID)
	if !ok || p.Hidden || p.ID != a.Partner {
		return
	}
	e := s.WithDefaultEffortTransition(t, func(before, after session.Snapshot, reason string) error {
		if before.SourceRevision != a.SourceRevision || before.ResumeID != a.Target.Thread || after.ResumeID != before.ResumeID || before.ConfigRevision != a.Target.Revision || after.ConfigRevision != before.ConfigRevision || before.Cwd != after.Cwd || before.Backend != after.Backend || before.Model != after.Model || before.Sandbox != after.Sandbox || before.ServiceTier != after.ServiceTier || before.CollaborationMode != after.CollaborationMode || before.Personality != after.Personality || before.Hidden != after.Hidden {
			return errors.New("role_default_effort_source_changed")
		}
		return h.jarvisRoles.MigrateDefaultEffort(s.ID, before.ResumeID, before.ConfigRevision, before.SourceRevision, after.SourceRevision, a.Epoch, a.Revision, reason)
	})
	if e != nil {
		log.Printf("[jarvis-role] default-effort witness not migrated: %v", e)
		return
	}
	committed = true
}

func (h *Hub) prepareJarvisEndTurn(s *session.Session) func() {
	release, transition := s.PrepareEndTurnWithDefaultEffort()
	h.migrateJarvisDefaultEffort(transition)
	return release
}

func (h *Hub) migrateJarvisInitialThread(id, thread string) {
	h.jarvisRoleMu.Lock()
	committed := false
	defer func() {
		h.jarvisRoleMu.Unlock()
		if committed {
			h.emitJarvisRoleChanged()
		}
	}()
	s, found := h.registry.Get(id)
	_, a := h.jarvisRoles.Snapshot()
	if !found || a == nil || !a.Enabled || a.Target.Session != id || a.Target.Thread != "" || thread == "" || !h.jarvisRoleEligible(s) || a.PolicyHash != h.jarvisRolePolicyHash() || !h.jarvisCredentialCurrent(a.Device, a.Generation) {
		return
	}
	now := s.SettingsSnapshot()
	// The initial SetResumeID is exactly one known source transition. Default
	// metadata has its own earlier receipt; an unproved config/model mutation
	// must not be swallowed merely because a first SessionUUID arrives later.
	if now.ResumeID != thread || now.ConfigRevision != a.Target.Revision || now.SourceRevision != a.SourceRevision+1 {
		return
	}
	if e := h.jarvisRoles.MigrateWitness(id, "", thread, now.ConfigRevision, now.SourceRevision); e == nil {
		committed = true
	}
}
