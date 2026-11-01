package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/jarvisrole"
	"everything-go/internal/partners"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
)

type jarvisRolePolicy struct {
	ExcludedSessions []string `json:"excluded_sessions"`
}

func (h *Hub) jarvisRolePolicyHash() string {
	b, e := os.ReadFile(filepath.Join(h.cfg.DataDir, "jarvis-role-policy.json"))
	if os.IsNotExist(e) {
		b = []byte("jarvis-local-operator-policy-v1")
	} else if e != nil {
		return "unavailable"
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (h *Hub) jarvisRoleExcluded(id string) bool {
	// Only an operator-owned file can narrow this bundle; no model/UI policy setter.
	b, e := os.ReadFile(filepath.Join(h.cfg.DataDir, "jarvis-role-policy.json"))
	if os.IsNotExist(e) {
		return false
	}
	if e != nil {
		return true
	}
	var p jarvisRolePolicy
	if json.Unmarshal(b, &p) != nil {
		return true
	}
	for _, v := range p.ExcludedSessions {
		if v == id {
			return true
		}
	}
	return false
}
func (h *Hub) jarvisRoleEligible(s *session.Session) bool {
	if s == nil || !h.controllerInScope(s) || s.State() == session.Closed || s.Snapshot().Hidden || strings.HasPrefix(s.ID, "s_dg_") || strings.HasPrefix(s.ID, "pm_") || !h.controls.MobileMayWrite(s.ID) || h.workspaceReservedSession(s.ID) || h.jarvisRoleExcluded(s.ID) {
		return false
	}
	if s.Backend() != backend.Codex && s.Backend() != backend.Claude {
		return false
	}
	p, e := h.PMConfiguration(s.ID)
	return e == nil && p == nil
}
func (h *Hub) currentJarvisRole() *jarvisrole.Assignment {
	if h.jarvisRoles == nil || h.partners == nil {
		return nil
	}
	_, a := h.jarvisRoles.Snapshot()
	if a == nil || !a.Enabled || a.PolicyHash != h.jarvisRolePolicyHash() || !h.jarvisCredentialCurrent(a.Device, a.Generation) {
		return nil
	}
	s, ok := h.registry.Get(a.Target.Session)
	if !ok || !h.jarvisRoleEligible(s) || s.ResumeID() != a.Target.Thread || s.SettingsSnapshot().ConfigRevision != a.Target.Revision || s.SettingsSnapshot().SourceRevision != a.SourceRevision {
		return nil
	}
	p, ok := h.partners.ForSession(s.ID)
	if !ok || p.ID != a.Partner || p.Hidden {
		return nil
	}
	return a
}

// SessionRoleEpoch is host-only adapter evidence, never model-supplied input.
func (h *Hub) SessionRoleEpoch(s *session.Session) uint64 {
	a := h.currentJarvisRole()
	if a != nil && s != nil && a.Target.Session == s.ID {
		return a.Epoch
	}
	return 0
}
func (h *Hub) deriveJarvisGrant(ctx context.Context, parent string, g sessiondispatch.Grant) sessiondispatch.Grant {
	a := h.currentJarvisRole()
	if epoch, known := backend.ContextRoleEpoch(ctx); known && (a == nil || epoch != a.Epoch) {
		return g
	}
	if a == nil || a.Target.Session != parent {
		return g
	}
	if !g.Enabled {
		g.Local = false
		g.Instances = nil
		g.Sessions = nil
		g.Steer = false
	}
	g.Enabled = true
	g.RoleEpoch = a.Epoch
	for _, s := range h.registry.List() {
		if h.jarvisRoleEligible(s) {
			g.RoleSessions = append(g.RoleSessions, h.cfg.InstanceID+":"+s.ID)
		}
	}
	return g
}
func (h *Hub) jarvisRoleDispatchEpoch(ctx context.Context, parent, instance, target string) uint64 {
	base, e := h.dispatches.BaseGrant(ctx, parent)
	if e != nil || base.Allows(h.cfg.InstanceID, instance, target) {
		return 0
	}
	a := h.currentJarvisRole()
	if a != nil && a.Target.Session == parent && instance == h.cfg.InstanceID {
		return a.Epoch
	}
	return 0
}
func (h *Hub) jarvisRoleDispatchCurrent(r sessiondispatch.Record) bool {
	if r.RoleEpoch == 0 {
		return true
	}
	a := h.currentJarvisRole()
	return a != nil && a.Epoch == r.RoleEpoch && a.Target.Session == r.ParentID
}

type jarvisRoleRequest struct {
	Authority       string            `json:"authority_instance_id"`
	Operation       string            `json:"operation_id,omitempty"`
	Expected        uint64            `json:"expected_assignment_revision,omitempty"`
	Target          jarvisrole.Target `json:"target,omitempty"`
	Partner         string            `json:"partner_id,omitempty"`
	ProfileRevision uint64            `json:"expected_profile_revision,omitempty"`
}

func (h *Hub) serveJarvisRole(w http.ResponseWriter, r *http.Request, device, path string) {
	fail := func(status int, code string) {
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "ok": false, "error_code": code})
	}
	if h.jarvisRoles == nil || h.partners == nil {
		fail(503, "role_storage_unavailable")
		return
	}
	if path == "role/audio-lease" || path == "role/audio-lease/status" {
		h.serveJarvisAudioLease(w, r, device, path)
		return
	}
	if r.Method != http.MethodPost {
		fail(405, "method_not_allowed")
		return
	}
	var in jarvisRoleRequest
	if !jarvisDecode(w, r, &in) {
		return
	}
	if in.Authority != h.cfg.InstanceID {
		fail(403, "wrong_authority")
		return
	}
	actor := jarvisRequestPrincipal(r, device)
	if path == "role/operation" {
		o, ok := h.jarvisRoles.Lookup(actor, in.Operation)
		if !ok {
			fail(404, "operation_not_found")
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "operation": o})
		return
	}
	if path == "role/snapshot" || path == "role/residency" {
		_ = json.NewEncoder(w).Encode(h.jarvisRoleSnapshot())
		return
	}
	if path != "role/assign" && path != "role/revoke" {
		fail(404, "unknown_operation")
		return
	}
	if !jarvisID.MatchString(in.Operation) {
		fail(400, "invalid_operation_id")
		return
	}
	// The paired HTTP human settings channel is the authority, not a body flag.
	// Role setters are deliberately absent from all native tool registrations.
	h.jarvisRoleMu.Lock()
	defer h.jarvisRoleMu.Unlock()
	hash := jarvisRoleInputHash(path, in)
	if o, ok := h.jarvisRoles.Lookup(actor, in.Operation); ok {
		if o.Hash != hash {
			fail(409, "idempotency_conflict")
			return
		}
		v := h.jarvisRoleSnapshot()
		v["operation"] = o
		_ = json.NewEncoder(w).Encode(v)
		return
	}
	var a *jarvisrole.Assignment
	action := "revoke"
	if path == "role/assign" {
		action = "assign"
		s, ok := h.registry.Get(in.Target.Session)
		if !ok || !h.jarvisRoleEligible(s) || in.Target.Authority != h.cfg.InstanceID {
			fail(403, "role_source_forbidden")
			return
		}
		if s.ResumeID() != in.Target.Thread || s.SettingsSnapshot().ConfigRevision != in.Target.Revision {
			fail(409, "session_binding_changed")
			return
		}
		p, ok := h.partners.ForSession(s.ID)
		if !ok || p.ID != in.Partner || p.Revision != in.ProfileRevision || p.Hidden {
			fail(409, "profile_revision_conflict")
			return
		}
		if p.NativeThreadID != "" && p.NativeThreadID != s.ResumeID() {
			fail(409, "profile_thread_changed")
			return
		}
		a = &jarvisrole.Assignment{SourceRevision: s.SettingsSnapshot().SourceRevision, PolicyHash: h.jarvisRolePolicyHash(), Partner: p.ID, Target: in.Target}
	}
	if h.inboxDevice(r) != device {
		fail(403, "pairing_changed")
		return
	}
	var o jarvisrole.Operation
	var e error
	commit := func() error {
		h.voiceMu.Lock()
		defer h.voiceMu.Unlock()
		_, old := h.jarvisRoles.Snapshot()
		if (old != nil && h.jarvisRoleVoiceBusyLocked(old.Target.Session)) || (a != nil && h.jarvisRoleVoiceBusyLocked(a.Target.Session)) {
			return errors.New("role_voice_busy_or_release_unknown")
		}
		var err error
		o, err = h.jarvisRoles.Apply(actor, device, jarvisRequestGeneration(r), in.Operation, action, hash, in.Expected, a)
		return err
	}
	if a != nil {
		e = h.partners.WithProfileCAS(in.Target.Session, in.ProfileRevision, func(p partners.Profile) error {
			if p.ID != in.Partner || p.Hidden {
				return errors.New("profile_revision_conflict")
			}
			s, ok := h.registry.Get(in.Target.Session)
			if !ok || !h.jarvisRoleEligible(s) || s.ResumeID() != in.Target.Thread || s.SettingsSnapshot().ConfigRevision != in.Target.Revision || s.SettingsSnapshot().SourceRevision != a.SourceRevision || h.inboxDevice(r) != device {
				return errors.New("session_binding_changed")
			}
			return commit()
		})
	} else {
		e = commit()
	}
	if e != nil {
		fail(409, e.Error())
		return
	}
	h.jarvisResidentMu.Lock()
	h.jarvisResident = jarvisResidentStatus{Desired: a != nil, Actual: "pending", Epoch: o.Epoch, Reason: "awaiting_provider_observation", ToolState: "unknown"}
	if a == nil {
		h.jarvisResident.Actual = "inactive"
		h.jarvisResident.Reason = "no_assignment"
	}
	h.jarvisResidentNext = 0
	h.jarvisResidentMu.Unlock()
	h.Emit(map[string]any{"type": "jarvis_role_changed", "authority_instance_id": h.cfg.InstanceID, "assignment_revision": o.Revision, "role_epoch": o.Epoch})
	v := h.jarvisRoleSnapshot()
	v["operation"] = o
	_ = json.NewEncoder(w).Encode(v)
}
func (h *Hub) jarvisRoleSnapshot() map[string]any {
	rev, a := h.jarvisRoles.Snapshot()
	var p *partners.Profile
	if a != nil {
		if v, ok := h.partners.ForSession(a.Target.Session); ok && v.ID == a.Partner {
			p = &v
		}
	}
	candidates := []map[string]any{}
	for _, s := range h.registry.List() {
		if !h.jarvisRoleEligible(s) {
			continue
		}
		var profile *partners.Profile
		if v, ok := h.partners.ForSession(s.ID); ok && !v.Hidden {
			profile = &v
		}
		candidates = append(candidates, map[string]any{"target": jarvisrole.Target{Authority: h.cfg.InstanceID, Session: s.ID, Thread: s.ResumeID(), Revision: s.SettingsSnapshot().ConfigRevision}, "partner": profile, "eligible": true, "reason": ""})
	}
	state := "none"
	epoch := uint64(0)
	if a != nil {
		state = "reconciliation_required"
		epoch = a.Epoch
		if h.currentJarvisRole() != nil {
			state = "active"
		}
	}
	caps := []map[string]any{}
	for _, id := range []string{"local_session_read", "local_queue_dispatch", "local_steer", "original_result_read", "cancel_waiting", "partner_identity", "context_handoff"} {
		caps = append(caps, map[string]any{"id": id, "state": state, "reason": "original_source_and_effect_gates_retained"})
	}
	caps = append(caps, h.jarvisServiceReadiness(a)...)
	blockers := []string{}
	if a != nil {
		h.voiceMu.Lock()
		voiceBusy := h.jarvisRoleVoiceBusyLocked(a.Target.Session)
		h.voiceMu.Unlock()
		if voiceBusy {
			blockers = append(blockers, "voice_or_transition_or_release_unsettled")
		}
		if h.jarvisRoles.AudioBusy(a.Target.Session) {
			blockers = append(blockers, "speech_or_release_unsettled")
		}
	}
	return map[string]any{"role_change_blockers": blockers, "schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "owner_domain": h.jarvisRoles.Domain(), "assignment_revision": rev, "assignment": a, "partner": p, "candidates": candidates, "bundle": map[string]any{"id": "jarvis_operator_v1", "version": 1, "role_epoch": epoch, "state": state, "capabilities": caps, "limitations": []string{"local_authority_only", "original_base_grants_and_service_scopes_preserved", "role_is_not_an_external_effect_intent", "provider_ready_requires_fresh_observation"}}, "residency": h.jarvisResidentSnapshot(a), "checked_at": time.Now().UnixMilli()}
}

func (h *Hub) controllerGrantForCaller(ctx context.Context, c backend.SessionControlCaller) (sessiondispatch.Grant, error) {
	g, e := h.dispatches.BaseGrant(ctx, c.Parent.ID)
	if e != nil {
		return g, e
	}
	a := h.currentJarvisRole()
	if a != nil && a.Target.Session == c.Parent.ID && c.RoleEpoch == a.Epoch {
		return h.deriveJarvisGrant(ctx, c.Parent.ID, g), nil
	}
	return g, nil
}
func (h *Hub) withJarvisDispatchGuard(ctx context.Context, sessionID, request string) context.Context {
	if h.dispatches == nil {
		return ctx
	}
	original := ctx
	guard := func() error {
		if e := backend.CheckTaskInputGuard(original); e != nil {
			return e
		}
		r, ok, e := h.dispatches.UnifiedByExecution(context.Background(), h.cfg.InstanceID, sessionID, request)
		if e != nil {
			return e
		}
		if !ok || r.RoleEpoch == 0 {
			return nil
		}
		if !h.jarvisRoleDispatchCurrent(r) {
			return errors.New("jarvis_role_epoch_retired")
		}
		target, found := h.registry.Get(r.SessionID)
		if !found || !h.jarvisRoleEligible(target) || target.ResumeID() != r.ThreadID || target.SettingsSnapshot().ConfigRevision != r.ConfigRevision {
			return errors.New("jarvis_role_target_changed")
		}
		return nil
	}
	return backend.WithTaskInputGuard(ctx, guard)
}

// Only exact pre-existing role-stamped receipts survive demotion. This does not
// restore a catalog, a target-wide read grant, or any new effect capability.
func (h *Hub) jarvisRoleOriginalReceiptReadable(r sessiondispatch.Record) bool {
	if r.RoleEpoch == 0 || r.ParentThreadID == "" || !r.ParentConfigKnown {
		return false
	}
	parent, ok := h.registry.Get(r.ParentID)
	if !ok || !h.jarvisRoleEligible(parent) || parent.ResumeID() != r.ParentThreadID || parent.SettingsSnapshot().ConfigRevision != r.ParentConfigRevision {
		return false
	}
	return true
}
func (h *Hub) jarvisRoleHasOriginalResults(ctx context.Context, parent, thread string, revision uint64) bool {
	rows, e := h.dispatches.ListForResults(ctx, parent)
	if e != nil {
		return false
	}
	for _, r := range rows {
		if r.ParentThreadID == thread && r.ParentConfigRevision == revision && h.jarvisRoleOriginalReceiptReadable(r) {
			return true
		}
	}
	return false
}

func (h *Hub) emitJarvisRoleChanged() {
	if h.jarvisRoles == nil {
		return
	}
	rev, a := h.jarvisRoles.Snapshot()
	epoch := rev
	if a != nil {
		epoch = a.Epoch
	}
	h.Emit(map[string]any{"type": "jarvis_role_changed", "authority_instance_id": h.cfg.InstanceID, "assignment_revision": rev, "role_epoch": epoch})
}

func jarvisRoleInputHash(action string, in jarvisRoleRequest) string {
	return jarvisHash([]any{1, strings.TrimPrefix(action, "role/"), in.Authority, in.Operation, in.Expected, in.Target.Authority, in.Target.Session, in.Target.Thread, in.Target.Revision, in.Partner, in.ProfileRevision})
}
