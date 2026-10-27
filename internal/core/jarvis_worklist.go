package core

import (
	"encoding/json"
	"errors"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type jarvisWorkScope struct {
	Authority string       `json:"authority_instance_id"`
	Manager   jarvisTarget `json:"manager"`
}

func (h *Hub) workManager(r *http.Request, device string, in jarvisWorkScope) (*session.Session, string, error) {
	s, ok := h.registry.Get(in.Manager.SessionID)
	if !ok || in.Authority != h.cfg.InstanceID || in.Manager.Authority != h.cfg.InstanceID || !h.eventSessionAllowed(s) || s.ResumeID() != in.Manager.ThreadID || s.SettingsSnapshot().ConfigRevision != in.Manager.Revision || h.inboxDevice(r) != device {
		return nil, "", errors.New("manager_scope_changed")
	}
	return s, jarvisRequestPrincipal(r, device), nil
}
func (h *Hub) workScopeCurrent(r *http.Request, device string, in jarvisWorkScope, s *session.Session) bool {
	now, _, e := h.workManager(r, device, in)
	return e == nil && now == s
}
func (h *Hub) workLinkProjection(r *http.Request, device string, manager *session.Session, l messagequeue.WorkLink) (map[string]any, error) {
	if l.Authority != h.cfg.InstanceID {
		return nil, errors.New("remote_source_not_read_here")
	}
	target, found := h.registry.Get(l.SessionID)
	if !found || target.Snapshot().Hidden || l.Kind != "delegation" && !h.eventSessionAllowed(target) {
		return nil, errors.New("target_scope_unavailable")
	}
	// Original immutable target tuples may describe a retained old generation;
	// reading it never authorizes a send or silently rebinds to today's thread.
	switch l.Kind {
	case "intake", "assignment":
		_, _, failure := h.jarvisReadOneWork(r, device, jarvisWorkRef{SessionID: l.SessionID, RequestID: l.RequestID})
		if failure != nil {
			return nil, errors.New(failure.Code)
		}
	case "controller":
		if h.dispatches == nil {
			return nil, errors.New("controller_unavailable")
		}
		d, ok, e := h.dispatches.Get(r.Context(), l.NativeID)
		if e != nil || !ok || d.ParentID != manager.ID || d.ParentThreadID != manager.ResumeID() || !d.ParentConfigKnown || d.ParentConfigRevision != manager.SettingsSnapshot().ConfigRevision || d.InstanceID != l.Authority || d.SessionID != l.SessionID || d.RequestID != l.RequestID || d.ThreadID != l.ThreadID || d.ConfigRevision != l.ConfigRevision {
			return nil, errors.New("original_controller_relation_required")
		}
		g, e := h.dispatches.Grant(r.Context(), manager.ID)
		if e != nil || !g.Allows(h.cfg.InstanceID, l.Authority, l.SessionID) {
			return nil, errors.New("controller_grant_changed")
		}
	case "delegation":
		if h.delegations == nil {
			return nil, errors.New("delegation_unavailable")
		}
		d, ok, e := h.delegations.ByID(r.Context(), l.NativeID)
		if e != nil || !ok || d.ParentSessionID != manager.ID || d.ChildSessionID != l.SessionID || d.ChildRequestID != l.RequestID || d.SourceThread != manager.ResumeID() || d.SourceRevision != manager.SettingsSnapshot().ConfigRevision {
			return nil, errors.New("original_delegation_relation_required")
		}
		thread, revision, _, e := h.delegations.ExecutionBinding(r.Context(), d.ID)
		if e != nil || thread != l.ThreadID || revision != l.ConfigRevision {
			return nil, errors.New("delegation_target_binding_changed")
		}
	default:
		return nil, errors.New("unsupported_execution_link")
	}
	// Canonical Task API is reused when an exact original command exists.
	for _, j := range h.apiJournals() {
		if record, e := j.OriginalCommandForRequest(r.Context(), l.SessionID, l.RequestID); e == nil {
			projection, e := h.apiTask(r.Context(), record)
			if e != nil {
				return nil, e
			}
			h.overlayLiveWorkStart(projection, l)
			return projection, nil
		}
	}
	e, ok, err := h.messageQueue.Get(l.SessionID, l.RequestID)
	if err != nil || !ok {
		return nil, errors.New("original_queue_receipt_unavailable")
	}
	axes := map[string]string{"admission": "queued", "execution": "not_started", "final": "unavailable", "artifact": "none", "qa": "not_requested", "delivery": "not_requested", "cancellation": "none"}
	switch e.State {
	case messagequeue.Running:
		axes["execution"] = "handoff"
		if _, ok, e := h.messageQueue.ExactNativeAcceptance(l.SessionID, l.RequestID); ok && e == nil {
			axes["execution"] = "native_consumed"
		}
	case messagequeue.Completed:
		axes["execution"] = "succeeded"
		axes["final"] = "unanchored"
	case messagequeue.Failed:
		axes["execution"] = "failed"
	case messagequeue.Uncertain, messagequeue.Steering:
		axes["execution"] = "unknown"
	case messagequeue.Cancelled:
		axes["execution"] = "unknown"
		axes["cancellation"] = "confirmed"
	}
	projection := map[string]any{"identity": l, "axes": axes, "revision": e.UpdatedAt, "fetched_at_ms": time.Now().UnixMilli(), "fresh_until_ms": time.Now().Add(12 * time.Second).UnixMilli()}
	if seal, err := h.messageQueue.TaskSeal(r.Context(), l.SessionID, l.RequestID, ""); err == nil && e.State == messagequeue.Completed {
		axes["final"] = "sealed"
		projection["result_ref"] = map[string]string{"session_id": l.SessionID, "request_id": l.RequestID, "seal_id": seal.ID}
	}
	if l.Kind == "controller" {
		d, _, _ := h.dispatches.Get(r.Context(), l.NativeID)
		axes["delivery"] = d.DeliveryState
		projection["return_error"] = d.DeliveryError
	}
	if l.Kind == "delegation" {
		d, _, _ := h.delegations.ByID(r.Context(), l.NativeID)
		axes["delivery"] = d.DeliveryState
		projection["return_error"] = d.DeliveryError
		if qa, verdict, e := h.delegations.QAForRound(r.Context(), l.NativeID); e == nil && qa != "" {
			axes["qa"] = verdict
			if verdict == "" {
				axes["qa"] = "pending"
			}
		}
	}
	h.overlayLiveWorkStart(projection, l)
	return projection, nil
}
func (h *Hub) overlayLiveWorkStart(projection map[string]any, l messagequeue.WorkLink) {
	p, ok, e := h.messageQueue.NativeStart(l.SessionID, l.RequestID)
	if e != nil || !ok {
		return
	}
	projection["native_start"] = p
	// A durable start fact survives terminal, but never regresses the execution.
	entry, found, e := h.messageQueue.Get(l.SessionID, l.RequestID)
	if e == nil && found && entry.State == messagequeue.Running {
		switch axes := projection["axes"].(type) {
		case map[string]string:
			if axes["execution"] == "handoff" || axes["execution"] == "native_consumed" {
				axes["execution"] = "native_running"
			}
		case map[string]any:
			if axes["execution"] == "handoff" || axes["execution"] == "native_consumed" {
				axes["execution"] = "native_running"
			}
		}
	}
}
func (h *Hub) serveWorklist(w http.ResponseWriter, r *http.Request, device, path string) {
	if r.Method != http.MethodPost || h.messageQueue == nil {
		http.Error(w, "worklist unavailable", 503)
		return
	}
	if path == "worklist/notifications" {
		h.serveNoticePolicy(w, r, device)
		return
	}
	switch path {
	case "worklist/item":
		var in struct {
			jarvisWorkScope
			messagequeue.WorkMutation
		}
		if !jarvisDecode(w, r, &in) {
			return
		}
		manager, scope, e := h.workManager(r, device, in.jarvisWorkScope)
		if e != nil {
			http.Error(w, e.Error(), 403)
			return
		}
		if in.RecordingState == "plan_ready" || in.RecordingState == "awaiting_user_decision" {
			if in.Plan == nil || !jarvisSHA.MatchString(in.Plan.SHA256) {
				http.Error(w, "verified plan file hash required", 409)
				return
			}
			path, err := h.scopedPath(in.Plan.Path)
			if err != nil {
				http.Error(w, "plan outside authorized source", 403)
				return
			}
			file, err := os.Open(path)
			if err != nil {
				http.Error(w, "plan file unavailable", 409)
				return
			}
			raw, err := io.ReadAll(io.LimitReader(file, 8<<20+1))
			file.Close()
			if err != nil || len(raw) > 8<<20 || jarvisTextHash(string(raw)) != in.Plan.SHA256 {
				http.Error(w, "plan file hash or bound changed", 409)
				return
			}
		}
		for _, l := range in.Links {
			if _, e = h.workLinkProjection(r, device, manager, l); e != nil {
				http.Error(w, e.Error(), 403)
				return
			}
		}
		if !h.workScopeCurrent(r, device, in.jarvisWorkScope, manager) {
			http.Error(w, "scope changed", 403)
			return
		}
		out, e := h.messageQueue.MutateWork(r.Context(), scope, manager.ID, in.WorkMutation)
		if e != nil {
			http.Error(w, e.Error(), 409)
			return
		}
		if !h.workScopeCurrent(r, device, in.jarvisWorkScope, manager) {
			http.Error(w, "scope changed; lookup original", 403)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "item": out, "authority_instance_id": h.cfg.InstanceID, "recorded": true, "dispatched": false})
	case "worklist/snapshot", "worklist/get", "worklist/notices":
		var in struct {
			jarvisWorkScope
			Cursor   string `json:"cursor,omitempty"`
			Limit    int    `json:"limit,omitempty"`
			ItemID   string `json:"item_id,omitempty"`
			IntentID string `json:"intent_id,omitempty"`
		}
		if !jarvisDecode(w, r, &in) {
			return
		}
		manager, scope, e := h.workManager(r, device, in.jarvisWorkScope)
		if e != nil {
			http.Error(w, e.Error(), 403)
			return
		}
		after := ""
		if in.Cursor != "" {
			prefix := h.messageQueue.WorkCursor(scope, jarvisHash(in.jarvisWorkScope), "")
			if !strings.HasPrefix(in.Cursor, prefix) {
				http.Error(w, "cursor scope expired", 409)
				return
			}
			after = strings.TrimPrefix(in.Cursor, prefix)
		}
		items := []messagequeue.WorkItem{}
		next := ""
		if in.IntentID != "" {
			if policy, err := h.messageQueue.NoticePolicyOperation(r.Context(), scope, manager.ID, in.IntentID); err == nil {
				_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "subscription": policy, "provider_sent": false})
				return
			}
			item, err := h.messageQueue.WorkOperation(r.Context(), scope, manager.ID, in.IntentID)
			e = err
			if e == nil {
				items = append(items, item)
			}
		} else if in.ItemID != "" {
			item, err := h.messageQueue.WorkItem(r.Context(), scope, manager.ID, in.ItemID)
			e = err
			if e == nil {
				items = append(items, item)
			}
		} else {
			items, next, e = h.messageQueue.WorkItems(r.Context(), scope, manager.ID, after, in.Limit)
		}
		if e != nil {
			http.Error(w, "worklist read unavailable", 503)
			return
		}
		rows := []any{}
		links := []messagequeue.WorkLink{}
		partial := false
		for _, item := range items {
			executions := []any{}
			visible := item
			visible.Links = []messagequeue.WorkLink{}
			for _, l := range item.Links {
				projection, err := h.workLinkProjection(r, device, manager, l)
				if err != nil {
					partial = true
					executions = append(executions, map[string]any{"error": map[string]any{"code": "scope_or_source_unavailable", "retryable": false}})
					continue
				}
				visible.Links = append(visible.Links, l)
				links = append(links, l)
				executions = append(executions, map[string]any{"link": l, "projection": projection})
			}
			// Item title/body belong to the source manager scope; inaccessible target
			// projections contain no target names, links, avatars or result bytes.
			rows = append(rows, map[string]any{"item": visible, "executions": executions})
		}
		// Source-owned controller/delegation rounds are discovered without copying
		// their execution state into metadata or manufacturing a new dispatch.
		if in.Cursor == "" && in.ItemID == "" && in.IntentID == "" {
			discovered, sourcePartial := h.discoveredWorkItems(r, manager)
			partial = partial || sourcePartial
			for _, item := range discovered {
				executions := []any{}
				visible := item
				visible.Links = []messagequeue.WorkLink{}
				for _, l := range item.Links {
					p, err := h.workLinkProjection(r, device, manager, l)
					if err != nil {
						partial = true
						continue
					}
					executions = append(executions, map[string]any{"link": l, "projection": p})
					links = append(links, l)
					visible.Links = append(visible.Links, l)
				}
				if len(executions) > 0 {
					rows = append(rows, map[string]any{"item": visible, "executions": executions, "read_only": true})
				}
			}
		}
		notices, e := h.messageQueue.WorkNotices(r.Context(), scope, manager.ID, links)
		if e != nil {
			http.Error(w, "notice read unavailable", 503)
			return
		}
		if !h.workScopeCurrent(r, device, in.jarvisWorkScope, manager) {
			http.Error(w, "scope changed", 403)
			return
		}
		pushes, _ := h.messageQueue.StartPushes(r.Context(), scope, manager.ID)
		_, policy, _ := h.messageQueue.CurrentNoticePolicy(r.Context(), scope, manager.ID)
		cursor := ""
		if next != "" {
			cursor = h.messageQueue.WorkCursor(scope, jarvisHash(in.jarvisWorkScope), next)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "items": rows, "notices": notices, "push_receipts": pushes, "notification_policy": policy, "cursor": cursor, "coverage": map[string]any{"kind": "scope_owned_items", "partial": partial, "all_tasks": false, "more": next != ""}, "sampled_at_ms": time.Now().UnixMilli(), "fresh_until_ms": time.Now().Add(12 * time.Second).UnixMilli(), "notification_capabilities": map[string]bool{"visual": true, "push": h.fcm != nil, "speech": false}})
	default:
		http.NotFound(w, r)
	}
}

func (h *Hub) discoveredWorkItems(r *http.Request, manager *session.Session) ([]messagequeue.WorkItem, bool) {
	out := []messagequeue.WorkItem{}
	partial := false
	if h.dispatches != nil {
		if records, e := h.dispatches.List(r.Context(), manager.ID); e == nil {
			for _, d := range records {
				if d.InstanceID != h.cfg.InstanceID {
					partial = true
					continue
				}
				out = append(out, messagequeue.WorkItem{ID: "controller_" + d.ID, Kind: "execution", Title: truncateGraphemes(d.Content, 100), Body: d.Content, RecordingState: "recorded", SourceRefs: []string{d.OriginRequestID}, Links: []messagequeue.WorkLink{{Kind: "controller", Authority: d.InstanceID, SessionID: d.SessionID, ThreadID: d.ThreadID, ConfigRevision: d.ConfigRevision, RequestID: d.RequestID, NativeID: d.ID}}, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt, Revision: 1})
			}
		} else {
			partial = true
		}
	}
	if h.delegations != nil {
		if records, e := h.delegations.ListByParent(r.Context(), manager.ID); e == nil {
			for _, d := range records {
				thread, revision, _, err := h.delegations.ExecutionBinding(r.Context(), d.ID)
				if err != nil {
					partial = true
					continue
				}
				out = append(out, messagequeue.WorkItem{ID: "delegation_" + d.ID, Kind: "execution", Title: d.ChildName, Body: d.Instruction, RecordingState: "recorded", SourceRefs: []string{d.OriginRequestID}, Links: []messagequeue.WorkLink{{Kind: "delegation", Authority: h.cfg.InstanceID, SessionID: d.ChildSessionID, ThreadID: thread, ConfigRevision: revision, RequestID: d.ChildRequestID, NativeID: d.ID}}, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt, Revision: 1})
			}
		}
	}
	return out, partial
}
