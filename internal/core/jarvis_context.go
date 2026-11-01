package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

func (h *Hub) contextBlockers(s *session.Session) []string {
	out := []string{}
	if !h.eventSessionAllowed(s) || h.workspaceReservedSession(s.ID) {
		out = append(out, "source_write_scope_unavailable")
	}
	if h.jarvisRoles != nil && h.jarvisRoles.AudioBusy(s.ID) {
		out = append(out, "speech_or_release_unsettled")
	}
	if s.State() != session.Idle || s.QueueLen() != 0 || s.ActiveQueuedID() != "" {
		out = append(out, "session_not_idle")
	}
	if h.toolRepairRunning.Load() {
		out = append(out, "tool_repair_active")
	}
	if h.messageQueue == nil {
		return append(out, "durable_queue_unavailable")
	}
	if guard, err := h.messageQueue.PipelineStore().Guard(context.Background(), s.ID); err != nil {
		out = append(out, "pipeline_state_unknown")
	} else if !guard.Ready {
		out = append(out, "pipeline_outstanding_or_unknown")
	}
	snap, e := h.messageQueue.Snapshot(s.ID)
	if e != nil {
		out = append(out, "queue_unknown")
	} else {
		for _, v := range snap.Items {
			if v.State == messagequeue.Queued || v.State == messagequeue.Running || v.State == messagequeue.Uncertain || v.State == messagequeue.Steering {
				out = append(out, "queue_or_send_unsettled")
				break
			}
		}
	}
	h.voiceMu.Lock()
	for _, call := range h.voiceClients {
		if call.session == s {
			out = append(out, "voice_or_release_unsettled")
			break
		}
	}
	h.voiceMu.Unlock()
	if h.interactions != nil {
		unsettled, e := h.interactions.UnsettledForSession(context.Background(), s.ID)
		if e != nil {
			out = append(out, "tool_interaction_unknown")
		} else if unsettled {
			out = append(out, "tool_interaction_unsettled")
		}
	}
	if h.dispatches != nil {
		execution, delivery, e := h.dispatches.ContextUnsettled(context.Background(), s.ID)
		if e != nil {
			out = append(out, "child_state_unknown")
		} else {
			if execution {
				out = append(out, "child_execution_unsettled")
			}
			if delivery {
				out = append(out, "old_return_pending_wait_first")
			}
		}
	}
	if h.delegations != nil {
		records, e := h.delegations.ListByParent(context.Background(), s.ID)
		if e != nil {
			out = append(out, "delegation_state_unknown")
		} else {
			for _, d := range records {
				if d.State != "terminal" && d.State != "cancelled" {
					out = append(out, "child_execution_unsettled")
					break
				}
				if d.DeliveryState != "delivered" && d.DeliveryState != "not_requested" {
					out = append(out, "old_return_pending_wait_first")
					break
				}
			}
		}
	}
	return out
}

func (h *Hub) serveContext(w http.ResponseWriter, r *http.Request, device, path string) {
	if path == "context/commit" || path == "context/prepare" {
		h.jarvisRoleMu.Lock()
		defer h.jarvisRoleMu.Unlock()
	}
	if r.Method != http.MethodPost || h.messageQueue == nil {
		http.Error(w, "context reset unavailable", 503)
		return
	}
	var in struct {
		jarvisWorkScope
		IntentID                 string `json:"intent_id,omitempty"`
		Mode                     string `json:"mode,omitempty"`
		Handoff                  string `json:"handoff,omitempty"`
		HandoffRevision          uint64 `json:"handoff_revision,omitempty"`
		HandoffBoundaryHash      string `json:"handoff_boundary_hash,omitempty"`
		SeedRequestID            string `json:"seed_request_id,omitempty"`
		BindingRevision          uint64 `json:"binding_revision,omitempty"`
		ExpectedRevision         uint64 `json:"expected_revision,omitempty"`
		BoundaryHash             string `json:"confirmed_boundary_hash,omitempty"`
		HandoffHash              string `json:"confirmed_handoff_hash,omitempty"`
		ConfirmedBindingRevision uint64 `json:"confirmed_binding_revision,omitempty"`
		IncludeArchive           bool   `json:"include_archive,omitempty"`
		ArchiveOffset            int    `json:"archive_offset,omitempty"`
		ArchiveLimit             int    `json:"archive_limit,omitempty"`
	}
	if !jarvisDecode(w, r, &in) {
		return
	}
	scope := jarvisRequestPrincipal(r, device)
	if path == "context/get" {
		v, e := h.messageQueue.ContextReset(r.Context(), scope, in.IntentID)
		s, ok := h.registry.Get(in.Manager.SessionID)
		if e != nil || !ok || v.SessionID != s.ID || v.ThreadID != in.Manager.ThreadID || v.ConfigRevision != in.Manager.Revision || in.Authority != h.cfg.InstanceID || in.Manager.Authority != h.cfg.InstanceID || !h.eventSessionAllowed(s) {
			http.Error(w, "original reset scope unavailable", 403)
			return
		}
		v = h.reconcileContextResetReadback(r, scope, s, v)
		out := map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "reset": v}
		if v.Phase == "completed" || v.Phase == "awaiting_seed" {
			oldFound := false
			for _, id := range s.ResumeIDs() {
				if id == v.ThreadID {
					oldFound = true
				}
			}
			if oldFound {
				if _, _, err := h.messageQueue.ContextArchive(r.Context(), scope, v.IntentID); err == nil {
					out["old_history_preserved"] = true
					generation := "initialized"
					if s.ResumeID() == "" {
						generation = "awaiting_initialization"
					}
					out["native_generation"] = generation
					out["new_target"] = jarvisTarget{Authority: h.cfg.InstanceID, SessionID: s.ID, ThreadID: s.ResumeID(), Revision: s.SettingsSnapshot().ConfigRevision}
				}
			}
		}
		if in.IncludeArchive {
			raw, hash, e := h.messageQueue.ContextArchive(r.Context(), scope, in.IntentID)
			if e != nil {
				http.Error(w, "archive unavailable", 503)
				return
			}
			offset := in.ArchiveOffset
			if offset < 0 || offset > len(raw) || offset < len(raw) && !utf8.RuneStart(raw[offset]) {
				http.Error(w, "invalid archive offset", 400)
				return
			}
			limit := in.ArchiveLimit
			if limit <= 0 || limit > 16000 {
				limit = 16000
			}
			end := offset + limit
			if end > len(raw) {
				end = len(raw)
			}
			for end < len(raw) && !utf8.RuneStart(raw[end]) {
				end--
			}
			out["archive_page"] = map[string]any{"text": string(raw[offset:end]), "offset": offset, "next_offset": end, "total_bytes": len(raw), "complete": end == len(raw)}
			out["archive_readback"] = map[string]any{"sha256": hash, "complete": true}
		}
		if h.inboxDevice(r) != device || !h.eventSessionAllowed(s) {
			http.Error(w, "pairing changed", 403)
			return
		}
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	s, _, e := h.workManager(r, device, in.jarvisWorkScope)
	if e != nil {
		http.Error(w, e.Error(), 403)
		return
	}
	provider, ok := h.exec.(backend.ContextResetExecutor)
	if cap, yes := h.exec.(interface{ SupportsContextReset(*session.Session) bool }); yes && !cap.SupportsContextReset(s) {
		ok = false
	}
	if path == "context/status" {
		snap := s.Snapshot()
		var used, max any
		if snap.ContextUsedKnown {
			used = snap.ContextUsed
		}
		if snap.ContextMax > 0 {
			max = snap.ContextMax
		}
		blockers := h.contextBlockers(s)
		if !ok {
			blockers = append(blockers, "provider_reset_unsupported")
		}
		reserved, e := h.messageQueue.ContextBarrier(r.Context(), s.ID)
		if e != nil || reserved {
			blockers = append(blockers, "generation_reserved_lookup_original")
		}
		bindings := []any{}
		if h.partners != nil {
			if p, ok := h.partners.ForSession(s.ID); ok {
				bindings = append(bindings, map[string]any{"kind": "private_partner", "id": p.ID, "revision": p.Revision, "thread_id": p.NativeThreadID})
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "context": map[string]any{"provider": snap.Backend, "thread_id": snap.ResumeID, "source_revision": snap.SourceRevision, "config_revision": snap.ConfigRevision, "used_tokens": used, "max_tokens": max, "usage_freshness": "last_observed"}, "reset_capability": ok, "blockers": blockers, "bindings": bindings, "sampled_at_ms": time.Now().UnixMilli()})
		return
	}
	if path == "context/boundary" {
		if !ok || len(h.contextBlockers(s)) > 0 {
			http.Error(w, "context boundary unavailable while unsettled", 409)
			return
		}
		before := s.Snapshot()
		export, err := provider.ExportContext(r.Context(), s)
		if err != nil || !export.Complete || export.ThreadID != before.ResumeID || !h.workScopeCurrent(r, device, in.jarvisWorkScope, s) || s.Snapshot().SourceRevision != before.SourceRevision {
			http.Error(w, "complete context boundary unavailable", 409)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "handoff_boundary_hash": jarvisHash(export), "boundary_hash": jarvisHash(export), "source_revision": before.SourceRevision, "thread_id": before.ResumeID, "config_revision": before.ConfigRevision, "sampled_at_ms": time.Now().UnixMilli()})
		return
	}
	if !jarvisID.MatchString(in.IntentID) || !ok {
		http.Error(w, "invalid reset intent or provider unsupported", 400)
		return
	}
	if path == "context/prepare" {
		if old, e := h.messageQueue.ContextReset(r.Context(), scope, in.IntentID); e == nil {
			if old.SessionID != s.ID || old.ThreadID != in.Manager.ThreadID || old.Mode != in.Mode || old.Handoff != in.Handoff || old.HandoffRevision != in.HandoffRevision || old.HandoffBoundaryHash != in.HandoffBoundaryHash || old.SeedRequestID != in.SeedRequestID {
				http.Error(w, "immutable reset conflict", 409)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "reset": old})
			return
		}
		if in.Mode != "direct" && in.Mode != "handoff" || len(in.Handoff) > 32000 || in.Mode == "handoff" && (strings.TrimSpace(in.Handoff) == "" || in.HandoffRevision == 0 || !jarvisSHA.MatchString(in.HandoffBoundaryHash) || !jarvisIntakeID.MatchString(in.SeedRequestID)) {
			http.Error(w, "invalid direct or reviewed handoff", 400)
			return
		}
		blockers := h.contextBlockers(s)
		if len(blockers) > 0 {
			_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "phase": "blocked", "blockers": blockers})
			return
		}
		snapshot := s.Snapshot()
		export, e := provider.ExportContext(r.Context(), s)
		if e != nil || !export.Complete || export.ThreadID != snapshot.ResumeID {
			code := "complete_history_or_native_idle_unavailable"
			if e != nil && strings.Contains(e.Error(), "opaque_tool_effect") {
				code = "opaque_tool_effect_settlement_not_proven"
			}
			http.Error(w, code, 409)
			return
		}
		if !h.workScopeCurrent(r, device, in.jarvisWorkScope, s) || s.Snapshot().SourceRevision != snapshot.SourceRevision {
			http.Error(w, "context changed during export", 409)
			return
		}

		if in.Mode == "handoff" && jarvisHash(export) != in.HandoffBoundaryHash {
			rejected, err := h.messageQueue.RejectContextReset(r.Context(), scope, jarvisHash(in), messagequeue.ContextReset{IntentID: in.IntentID, SessionID: s.ID, ThreadID: snapshot.ResumeID, SourceRevision: snapshot.SourceRevision, ConfigRevision: snapshot.ConfigRevision, Mode: in.Mode, Handoff: in.Handoff, HandoffHash: jarvisTextHash(in.Handoff), HandoffRevision: in.HandoffRevision, HandoffBoundaryHash: in.HandoffBoundaryHash, SeedRequestID: in.SeedRequestID}, "handoff_boundary_changed")
			if err != nil || rejected.Phase != "rejected" {
				http.Error(w, "original rejection persistence uncertain; lookup original", 409)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "intent_id": in.IntentID, "phase": "rejected", "no_effect": true, "reason": "handoff_boundary_changed", "reset": rejected})
			return
		}

		raw, _ := json.Marshal(export)
		v := messagequeue.ContextReset{IntentID: in.IntentID, SessionID: s.ID, ThreadID: snapshot.ResumeID, SourceRevision: snapshot.SourceRevision, ConfigRevision: snapshot.ConfigRevision, Mode: in.Mode, BoundaryHash: jarvisHash(export), ArchiveHash: jarvisHash(json.RawMessage(raw)), Handoff: in.Handoff, HandoffHash: jarvisTextHash(in.Handoff), HandoffRevision: in.HandoffRevision, HandoffBoundaryHash: in.HandoffBoundaryHash, SeedRequestID: in.SeedRequestID, BindingRevision: in.BindingRevision}
		saved, e := h.messageQueue.SaveContextReset(r.Context(), scope, jarvisHash(in), v, raw)
		if e != nil {
			http.Error(w, e.Error(), 409)
			return
		}
		_, hash, e := h.messageQueue.ContextArchive(r.Context(), scope, in.IntentID)
		if e != nil {
			http.Error(w, "archive readback failed; context untouched", 503)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "reset": saved, "archive_readback": map[string]any{"sha256": hash, "complete": true}})
		return
	}
	if path == "context/cancel" {
		v, e := h.messageQueue.ContextReset(r.Context(), scope, in.IntentID)
		if e != nil || v.SessionID != s.ID || v.ThreadID != in.Manager.ThreadID || v.Phase != "prepared" || v.Revision != in.ExpectedRevision {
			http.Error(w, "only original prepared reset may be cancelled", 409)
			return
		}
		v, e = h.messageQueue.ContextResetPhase(r.Context(), scope, in.IntentID, "prepared", "cancelled", "")
		if e != nil {
			http.Error(w, "cancel receipt unknown; lookup original", 503)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "reset": v, "provider_effect": false})
		return
	}
	if path != "context/commit" {
		http.NotFound(w, r)
		return
	}
	v, e := h.messageQueue.ContextReset(r.Context(), scope, in.IntentID)
	if e != nil || v.SessionID != s.ID || v.ThreadID != in.Manager.ThreadID {
		http.Error(w, "original reset unavailable", 403)
		return
	}
	if v.Phase != "prepared" {
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "reset": v, "replayed": false})
		return
	}
	if v.Revision != in.ExpectedRevision || v.BoundaryHash != in.BoundaryHash || v.Mode == "handoff" && v.HandoffHash != in.HandoffHash || s.Snapshot().SourceRevision != v.SourceRevision {
		http.Error(w, "reviewed reset boundary changed", 409)
		return
	}
	if h.partners != nil {
		if p, found := h.partners.ForSession(s.ID); found && (p.Revision != v.BindingRevision || in.ConfirmedBindingRevision != p.Revision || p.NativeThreadID != v.ThreadID) {
			http.Error(w, "explicit owned profile CAS required", 409)
			return
		}
	}
	if blockers := h.contextBlockers(s); len(blockers) > 0 {
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "phase": "blocked", "blockers": blockers})
		return
	}
	// Actor reservation and durable admission barrier must both precede any
	// destructive detach. DB transaction rejects queued/unknown concurrent input.
	release, e := s.ReserveContextGeneration(v.IntentID, v.ThreadID, v.SourceRevision)
	if e != nil {
		http.Error(w, e.Error(), 409)
		return
	}
	defer release()
	v, e = h.messageQueue.ReserveContextReset(r.Context(), scope, v.IntentID, v.Revision)
	if e != nil {
		http.Error(w, e.Error(), 409)
		return
	}
	block := func(reason string) {
		updated, _ := h.messageQueue.ContextResetPhase(r.Context(), scope, v.IntentID, v.Phase, "blocked", reason)
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "reset": updated})
	}
	export, e := provider.ExportContext(r.Context(), s)
	if e != nil || !export.Complete || jarvisHash(export) != v.BoundaryHash {
		block("history_or_native_idle_changed")
		return
	}
	if !h.workScopeCurrent(r, device, in.jarvisWorkScope, s) || s.Snapshot().SourceRevision != v.SourceRevision || len(h.contextBlockers(s)) > 0 {
		block("scope_or_safe_idle_changed")
		return
	}
	if _, _, e = h.messageQueue.ContextArchive(r.Context(), scope, v.IntentID); e != nil {
		block("history_readback_failed")
		return
	}
	v, e = h.messageQueue.ContextResetPhase(r.Context(), scope, v.IntentID, "reserved", "reset_issued", "")
	if e != nil {
		http.Error(w, "phase persistence failed; no reset issued", 503)
		return
	}
	// All durable reads/phase writes have completed. Do not carry a stale
	// authorization across either those waits or the provider idle RPC.
	effectContext := backend.WithContextResetGuard(r.Context(), func() bool {
		now := s.Snapshot()
		if h.jarvisRoles != nil && h.jarvisRoles.AudioBusy(s.ID) {
			return false
		}
		if !h.workScopeCurrent(r, device, in.jarvisWorkScope, s) || now.SourceRevision != v.SourceRevision || now.ResumeID != v.ThreadID || now.ConfigRevision != v.ConfigRevision || !s.ContextResetReserved() || h.toolRepairRunning.Load() || !h.controls.MobileMayWrite(s.ID) {
			return false
		}
		h.voiceMu.Lock()
		defer h.voiceMu.Unlock()
		for _, call := range h.voiceClients {
			if call.session == s {
				return false
			}
		}
		return true
	})
	if e = backend.CheckContextResetGuard(effectContext); e != nil {
		block("scope_changed_after_durable_wait")
		return
	}
	_, e = provider.ResetContext(effectContext, s, v.ThreadID)
	if e != nil {
		v, _ = h.messageQueue.ContextResetPhase(context.Background(), scope, v.IntentID, "reset_issued", "unknown", "provider_detach_unconfirmed_lookup_original")
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "reset": v})
		return
	}
	// This is an acknowledged detach, not a provider-created new thread.
	if s.ResumeID() != "" {
		v, _ = h.messageQueue.ContextResetPhase(context.Background(), scope, v.IntentID, "reset_issued", "unknown", "provider_readback_mismatch")
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "reset": v})
		return
	}
	if e = h.registry.PersistDurably(); e != nil {
		v, _ = h.messageQueue.ContextResetPhase(context.Background(), scope, v.IntentID, "reset_issued", "unknown", "session_persistence_unknown")
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "reset": v})
		return
	}
	if h.partners != nil {
		if _, exists := h.partners.ForSession(s.ID); exists {
			if e = h.partners.ResetThreadCAS(s.ID, v.ThreadID, v.BindingRevision); e != nil {
				v, _ = h.messageQueue.ContextResetPhase(context.Background(), scope, v.IntentID, "reset_issued", "unknown", "binding_migration_pending")
				_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "reset": v})
				return
			}
		}
	}
	if h.jarvisRoles != nil {
		if e = h.jarvisRoles.MigrateWitness(s.ID, v.ThreadID, "", s.SettingsSnapshot().ConfigRevision, s.SettingsSnapshot().SourceRevision); e != nil {
			v, _ = h.messageQueue.ContextResetPhase(context.Background(), scope, v.IntentID, "reset_issued", "unknown", "role_binding_migration_pending")
			_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "reset": v})
			return
		}
	}
	h.emitJarvisRoleChanged()
	phase := "completed"
	if v.Mode == "handoff" {
		phase = "awaiting_seed"
	}
	v, e = h.messageQueue.ContextResetPhase(context.Background(), scope, v.IntentID, "reset_issued", phase, "")
	if e != nil {
		http.Error(w, "reset receipt unknown; lookup original", 503)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "reset": v, "native_generation": "awaiting_initialization", "old_history_preserved": true, "seed_submission": "original_intake_required", "new_target": jarvisTarget{Authority: h.cfg.InstanceID, SessionID: s.ID, ThreadID: "", Revision: s.SettingsSnapshot().ConfigRevision}})
}

func (h *Hub) contextAdmissionBlocked(s *session.Session) bool {
	if s.ContextResetReserved() {
		return true
	}
	if h.messageQueue == nil {
		return false
	}
	blocked, e := h.messageQueue.ContextBarrier(context.Background(), s.ID)
	return e != nil || blocked
}

// Reconcile only durable/local acknowledged phases. This performs no provider
// reset, no model Send and no replay of an issued/unknown operation.
func (h *Hub) reconcileContextResetReadback(r *http.Request, scope string, s *session.Session, v messagequeue.ContextReset) messagequeue.ContextReset {
	if v.Phase != "unknown" && v.Phase != "reset_issued" && v.Phase != "reserved" {
		return v
	}
	if v.Phase == "reserved" {
		if s.ResumeID() == v.ThreadID && s.Snapshot().SourceRevision == v.SourceRevision {
			updated, e := h.messageQueue.ContextResetPhase(r.Context(), scope, v.IntentID, v.Phase, "blocked", "recovered_before_native_issue")
			if e == nil {
				return updated
			}
		}
		return v
	}
	snap := s.Snapshot()
	if snap.ResumeID != "" || snap.SourceRevision != v.SourceRevision+1 || snap.ConfigRevision != v.ConfigRevision {
		return v
	}
	oldFound := false
	for _, id := range s.ResumeIDs() {
		if id == v.ThreadID {
			oldFound = true
		}
	}
	if !oldFound {
		return v
	}
	if _, _, e := h.messageQueue.ContextArchive(r.Context(), scope, v.IntentID); e != nil {
		return v
	}
	if e := h.registry.PersistDurably(); e != nil {
		return v
	}
	if h.partners != nil {
		if p, found := h.partners.ForSession(s.ID); found {
			if p.NativeThreadID == v.ThreadID && p.Revision == v.BindingRevision {
				if e := h.partners.ResetThreadCAS(s.ID, v.ThreadID, v.BindingRevision); e != nil {
					return v
				}
			} else if p.NativeThreadID != "" || p.Revision != v.BindingRevision+1 {
				return v
			}
		}
	}
	phase := "completed"
	if v.Mode == "handoff" {
		phase = "awaiting_seed"
	}
	updated, e := h.messageQueue.ContextResetPhase(r.Context(), scope, v.IntentID, v.Phase, phase, "")
	if e == nil {
		return updated
	}
	return v
}
