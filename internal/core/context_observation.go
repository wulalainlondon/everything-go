package core

import (
	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"time"
)

const contextFreshMS int64 = 300000

func contextScope(authority string, s session.Snapshot) messagequeue.ContextScope {
	return messagequeue.ContextScope{Authority: authority, SessionID: s.ID, Backend: s.Backend, Generation: s.ResumeID, SourceRevision: s.SourceRevision}
}
func (h *Hub) contextObservation(s session.Snapshot) *protocol.ContextObservation {
	now := time.Now().UnixMilli()
	o := &protocol.ContextObservation{SchemaVersion: 1, AuthorityInstanceID: h.cfg.InstanceID, SessionID: s.ID, Backend: s.Backend, NativeGeneration: s.ResumeID, SourceRevision: s.SourceRevision, Usage: protocol.ContextUsageObservation{Status: "unknown"}, Compactions: protocol.ContextCompactions{Coverage: "unsupported", AsOfMS: now, Supported: s.Backend == "codex"}}
	if o.Compactions.Supported {
		o.Compactions.Coverage = "not_tracked"
	}
	if s.Backend != "codex" && s.Backend != "claude" {
		return o
	}
	if s.ResumeID == "" {
		return o
	}
	if h.messageQueue == nil {
		o.Compactions.Coverage = "unavailable"
		return o
	}
	r, found, err := h.messageQueue.ContextObservation(contextScope(h.cfg.InstanceID, s))
	if err != nil {
		o.Compactions.Coverage = "unavailable"
		return o
	}
	if !found {
		return o
	}
	if o.Compactions.Supported {
		o.Compactions.Completed = &r.Completed
		o.Compactions.Coverage = "tracked_only"
		o.Compactions.TrackedSinceMS = &r.TrackedSince
		o.Compactions.AsOfMS = r.AsOf
	}
	if r.Usage != nil {
		o.Usage = *r.Usage
		o.Usage.ReportedAtMS = &r.UsageAt
		until := r.UsageAt + contextFreshMS
		o.Usage.FreshUntilMS = &until
		o.Usage.Status = "stale"
		if r.InvalidatedAt > 0 || r.UsageRevision != s.SourceRevision {
			o.Usage.Status = "invalidated"
		} else if o.Usage.UsedTokens == nil || o.Usage.MaxTokens == nil {
			o.Usage.Status = "unknown"
		} else if *o.Usage.UsedTokens >= 0 && *o.Usage.MaxTokens > 0 && now <= until {
			o.Usage.Status = "fresh"
		}
	}
	return o
}
func (h *Hub) observeContextTelemetry(e backend.ContextTelemetry) {
	if h.messageQueue == nil || e.EventID == "" {
		return
	}
	s, ok := h.registry.Get(e.SessionID)
	if !ok {
		return
	}
	snap := s.Snapshot()
	if snap.ResumeID == "" || snap.ResumeID != e.NativeGeneration || snap.SourceRevision != e.SourceRevision || snap.Backend != e.Backend {
		return
	}
	if (e.Completed || e.CompletionState != "") && e.Backend != "codex" {
		return
	} // Claude has no reviewed canonical completion source yet.
	if e.UsedTokens != nil && *e.UsedTokens < 0 {
		e.UsedTokens = nil
	}
	if e.MaxTokens != nil && *e.MaxTokens <= 0 {
		e.MaxTokens = nil
	}
	var u *protocol.ContextUsageObservation
	if !e.Completed && e.CompletionState == "" {
		if e.Origin == "" || (e.UsedTokens == nil && e.MaxTokens == nil) {
			return
		}
		u = &protocol.ContextUsageObservation{UsedTokens: e.UsedTokens, MaxTokens: e.MaxTokens, Origin: &e.Origin, MaxOrigin: &e.MaxOrigin, Status: "unknown"}
		if e.ReportedAtMS > 0 {
			if e.ReportedAtMS > time.Now().Add(time.Minute).UnixMilli() {
				return
			}
			u.ReportedAtMS = &e.ReportedAtMS
		}
	}
	state := e.CompletionState
	if e.Completed {
		state = "completed"
	}
	if err := h.messageQueue.RecordContextObservation(contextScope(h.cfg.InstanceID, snap), e.EventID, u, state, e.Aliases, time.Now().UnixMilli()); err != nil {
		// Persistence failure is explicit/unavailable, never an optimistic increment.
		event := h.contextObservation(s.Snapshot())
		event.Compactions.Completed = nil
		event.Compactions.Coverage = "unavailable"
		h.broadcastContextObservation(protocol.ContextObservationEvent{Type: "context_observation", SessionID: s.ID, AuthorityInstanceID: h.cfg.InstanceID, ContextObservation: *event})
		return
	}
	current := s.Snapshot()
	o := h.contextObservation(current)
	if o.NativeGeneration == e.NativeGeneration && o.SourceRevision == e.SourceRevision {
		used, max := -1, -1
		known := o.Usage.Status != "invalidated" && o.Usage.UsedTokens != nil
		if known {
			used = *o.Usage.UsedTokens
		}
		if o.Usage.Status != "invalidated" && o.Usage.MaxTokens != nil {
			max = *o.Usage.MaxTokens
		}
		s.SetContextForGeneration(e.NativeGeneration, e.SourceRevision, used, max, known)
	}
	h.broadcastContextObservation(protocol.ContextObservationEvent{Type: "context_observation", SessionID: s.ID, AuthorityInstanceID: h.cfg.InstanceID, ContextObservation: *o})
}

// Match ordinary session visibility and never fan out to provisional probes.
func (h *Hub) broadcastContextObservation(event protocol.ContextObservationEvent) {
	s, ok := h.registry.Get(event.SessionID)
	if !ok || !h.eventSessionAllowed(s) {
		return
	}
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		if !c.enrollmentOnly && !c.inventoryProbe {
			clients = append(clients, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range clients {
		c.enqueueEvent(event)
	}
}
