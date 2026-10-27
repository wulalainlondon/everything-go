package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// This is a read adapter over the existing stores and original TaskAPI freeze
// table/codec, never a second execution ledger or scheduler.
type jarvisWorkSnapshotSource struct {
	hub               *Hub
	request           *http.Request
	device, principal string
	manager           *session.Session
	input             jarvisWorkScope
}

func (s jarvisWorkSnapshotSource) Capture(ctx context.Context, scope taskapi.ReadScope) (taskapi.Freeze, error) {
	for attempt := 0; attempt < 5; attempt++ {
		before, e := s.hub.apiWatermarks(ctx)
		if e != nil {
			return taskapi.Freeze{}, e
		}
		items := []messagequeue.WorkItem{}
		after := ""
		for {
			page, next, err := s.hub.messageQueue.WorkItems(ctx, s.principal, s.manager.ID, after, 30)
			if err != nil {
				return taskapi.Freeze{}, err
			}
			items = append(items, page...)
			if next == "" {
				break
			}
			after = next
		}
		discovered, _ := s.hub.discoveredWorkItems(s.request, s.manager)
		ordinary, _ := s.hub.discoveredManagerIntakes(s.request, s.device, s.manager)
		virtual := map[string]bool{}
		for _, item := range append(discovered, ordinary...) {
			virtual[item.ID] = true
			items = append(items, item)
		}
		rows := []taskapi.SnapshotRow{}
		seen := map[string]bool{}
		for _, item := range items {
			if seen[item.ID] {
				continue
			}
			seen[item.ID] = true
			row := s.hub.workSummaryRow(s.request, s.device, s.manager, item, virtual[item.ID])
			rows = append(rows, taskapi.SnapshotRow{Key: item.ID, Value: row})
		}

		// Capture each auxiliary channel once for the entire freeze, then join
		// immutable copies into rows. Repeated exact executions cannot observe
		// two different outbox states across pages of the same snapshot.
		allLinks := []messagequeue.WorkLink{}
		linkSeen := map[messagequeue.WorkLink]bool{}
		for _, row := range rows {
			value := row.Value.(map[string]any)
			item := value["item"].(messagequeue.WorkItem)
			for _, l := range item.Links {
				if !linkSeen[l] {
					linkSeen[l] = true
					allLinks = append(allLinks, l)
				}
			}
		}
		notices, err := s.hub.messageQueue.WorkNotices(ctx, s.principal, s.manager.ID, allLinks)
		if err != nil {
			return taskapi.Freeze{}, err
		}
		pushes, err := s.hub.messageQueue.StartPushesForLinks(ctx, s.principal, s.manager.ID, allLinks)
		if err != nil {
			return taskapi.Freeze{}, err
		}
		_, policy, err := s.hub.messageQueue.CurrentNoticePolicy(ctx, s.principal, s.manager.ID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return taskapi.Freeze{}, err
		}
		for i := range rows {
			value := rows[i].Value.(map[string]any)
			item := value["item"].(messagequeue.WorkItem)
			selected := map[string]bool{}
			ns := []messagequeue.WorkNotice{}
			ps := []messagequeue.StartPush{}
			for _, n := range notices {
				for _, l := range item.Links {
					if n.Start.SessionID == l.SessionID && n.Start.RequestID == l.RequestID {
						if !selected[n.EventID] {
							ns = append(ns, n)
							selected[n.EventID] = true
						}
						break
					}
				}
			}
			for _, p := range pushes {
				if selected[p.EventID] {
					ps = append(ps, p)
				}
			}
			value["notices"] = ns
			value["push_receipts"] = ps
			value["notification_policy"] = policy
		}
		rows = append(rows, taskapi.SnapshotRow{Key: "\x00jarvis_header", Value: map[string]any{"jarvis_snapshot_header": true, "notification_policy": policy}})
		sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
		raw, _ := json.Marshal(rows)
		if len(raw) > 32<<20 {
			return taskapi.Freeze{}, errors.New("worklist frozen summary capacity; original records preserved")
		}
		afterVector, e := s.hub.apiWatermarks(ctx)
		if e != nil {
			return taskapi.Freeze{}, e
		}
		if jarvisHash(before) != jarvisHash(afterVector) {
			continue
		}
		if !s.hub.workScopeCurrent(s.request, s.device, s.input, s.manager) {
			return taskapi.Freeze{}, errors.New("cursor_expired")
		}
		f := taskapi.Freeze{ID: "jw_" + randomID(), ExpiresAt: time.Now().Add(5 * time.Minute).UnixMilli(), Watermarks: before}
		var local uint64
		for _, m := range before {
			if m.Store == "ordinary" {
				local = m.Sequence
			}
		}
		if e = s.hub.messageQueue.TaskJournal().FreezeChecked(ctx, scope, f, rows, local); e != nil {
			if v, ok := e.(*taskapi.APIError); ok && v.Code == "busy" {
				continue
			}
			return taskapi.Freeze{}, e
		}
		return f, nil
	}
	return taskapi.Freeze{}, errors.New("busy: source stores changed during capture")
}
func (s jarvisWorkSnapshotSource) Page(ctx context.Context, scope taskapi.ReadScope, f taskapi.Freeze, after string, limit int) ([]taskapi.SnapshotRow, bool, error) {
	rows, e := s.hub.messageQueue.TaskJournal().Frozen(ctx, scope, f)
	if e != nil {
		return nil, false, e
	}
	out := []taskapi.SnapshotRow{}
	for _, row := range rows {
		if row.Key > after {
			out = append(out, row)
			if len(out) > limit {
				return out[:limit], true, nil
			}
		}
	}
	return out, false, nil
}
func (h *Hub) workSummaryRow(r *http.Request, device string, manager *session.Session, item messagequeue.WorkItem, readonly bool) map[string]any {
	visible := jarvisWorkTitlePresentation(item)
	visible.BodyHash = jarvisTextHash(item.Body)
	visible.Links = []messagequeue.WorkLink{}
	executions := []any{}
	partial := false
	for i, l := range item.Links {
		p, e := h.workLinkProjection(r, device, manager, l)
		if e != nil {
			partial = true
			if i < 8 {
				executions = append(executions, map[string]any{"error": map[string]any{"code": "scope_or_source_unavailable", "retryable": false}})
			}
			continue
		}
		if i < 8 {
			visible.Links = append(visible.Links, l)
			executions = append(executions, map[string]any{"link": l, "projection": jarvisWorkProjectionSummary(p)})
		}
	}
	visible = jarvisWorkSummary(visible, item)
	if partial || readonly && len(visible.Links) == 0 {
		visible.Title = "來源待確認"
		visible.TitleHash = ""
		visible.TitleTruncated = false
		visible.Body = ""
		visible.BodyHash = ""
		visible.SourceRefs = []string{}
		visible.DetailTruncated = true
		visible.Plan = nil
		visible.Decision = nil
	}
	return map[string]any{"authorization_links": item.Links, "item": visible, "executions": executions, "read_only": readonly, "executions_truncated": len(item.Links) > 8, "source_partial": partial}
}
func (h *Hub) serveFrozenWorklist(w http.ResponseWriter, r *http.Request, device string, in jarvisWorkScope, manager *session.Session, principal, cursor string, limit int) {
	if limit < 1 || limit > 8 {
		limit = 8
	}
	if h.taskCursor == nil {
		http.Error(w, "snapshot codec unavailable", 503)
		return
	}
	// Scope binds the actual credential epoch and immutable selected manager.
	scope := taskapi.ReadScope{Authority: h.cfg.InstanceID, StableScopeID: principal, FilterHash: jarvisHash(in)}
	source := jarvisWorkSnapshotSource{h, r, device, principal, manager, in}
	engineLimit := limit
	if cursor == "" {
		engineLimit++
	}
	page, e := (taskapi.SnapshotEngine{Codec: h.taskCursor, Source: source}).Page(r.Context(), scope, []string{"self", "source_children"}, cursor, engineLimit)
	if e != nil {
		status := 409
		if !strings.Contains(e.Error(), "cursor_expired") {
			status = 503
		}
		http.Error(w, e.Error(), status)
		return
	}
	rows := []any{}
	notices := []any{}
	pushes := []any{}
	seenNotice := map[string]bool{}
	seenPush := map[string]bool{}
	var policy any

	for _, raw := range page.Items {
		// Frozen bytes never bypass current authorization. A revoked original row
		// remains an explicit opaque slot, not a visible stale private title.
		encoded, _ := json.Marshal(raw)
		var row struct {
			AuthorizationLinks []messagequeue.WorkLink `json:"authorization_links"`
			Header             bool                    `json:"jarvis_snapshot_header"`
			Policy             json.RawMessage         `json:"notification_policy"`
			Notices            []map[string]any        `json:"notices"`
			Pushes             []map[string]any        `json:"push_receipts"`
			Item               messagequeue.WorkItem   `json:"item"`
			ReadOnly           bool                    `json:"read_only"`
		}
		if json.Unmarshal(encoded, &row) != nil {
			http.Error(w, "frozen row unavailable", 503)
			return
		}
		if len(row.Policy) > 0 {
			policy = row.Policy
		}
		if row.Header {
			continue
		}
		allowed := true
		for _, link := range row.AuthorizationLinks {
			if _, err := h.workLinkProjection(r, device, manager, link); err != nil {
				allowed = false
				break
			}
		}
		if !allowed {
			rows = append(rows, map[string]any{"item": map[string]any{"item_id": row.Item.ID, "kind": row.Item.Kind, "title": "來源待確認", "body": "", "recording_state": "recorded", "links": []any{}, "source_refs": []any{}, "revision": row.Item.Revision, "detail_truncated": true}, "executions": []any{map[string]any{"error": map[string]any{"code": "scope_or_source_unavailable", "retryable": false}}}, "read_only": true})
			continue
		}
		for _, n := range row.Notices {
			id, _ := n["event_id"].(string)
			if id != "" && !seenNotice[id] {
				seenNotice[id] = true
				notices = append(notices, n)
			}
		}
		for _, p := range row.Pushes {
			id, _ := p["event_id"].(string)
			if id != "" && !seenPush[id] {
				seenPush[id] = true
				pushes = append(pushes, p)
			}
		}
		// Auxiliary fields stay at the response's original public locations.
		var public map[string]any
		_ = json.Unmarshal(encoded, &public)
		// A still-valid cursor may reference a pre-upgrade freeze containing an
		// empty/oversized legacy title. Keep its stored bytes and identity while
		// applying the same explicitly marked display-only contract guard.
		if presented := jarvisWorkTitlePresentation(row.Item); presented.Title != row.Item.Title {
			public["item"] = presented
		}
		delete(public, "authorization_links")
		delete(public, "notices")
		delete(public, "push_receipts")
		delete(public, "notification_policy")
		rows = append(rows, public)
	}
	if !h.workScopeCurrent(r, device, in, manager) {
		http.Error(w, "scope changed", 403)
		return
	}
	jarvisWorkJSON(w, map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "items": rows, "cursor": page.NextPageCursor, "freeze_id": page.FreezeID, "expires_at_ms": page.ExpiresAt, "watermarks": page.Watermarks, "events_cursor": page.EventsCursor, "consistency": page.Consistency, "coverage": map[string]any{"kind": "scope_owned_items", "partial": true, "all_tasks": false, "more": page.HasMore, "ordinary_manager": "original_paired_intakes", "controller": "original_local_relations_latest_100", "delegation": "original_local_relations", "workspace": "explicit_authorized_links_only", "remote": "not_read_here", "notices": "frozen_selected_exact_executions"}, "notices": notices, "push_receipts": pushes, "notification_policy": policy, "sampled_at_ms": page.ExpiresAt - 5*60*1000, "fresh_until_ms": page.ExpiresAt - 5*60*1000 + 12000, "notification_capabilities": map[string]bool{"visual": true, "push": h.fcm != nil, "speech": false}})
}
func (h *Hub) discoveredManagerIntakes(r *http.Request, device string, manager *session.Session) ([]messagequeue.WorkItem, bool) {
	parts := strings.Split(jarvisRequestPrincipal(r, device), "\x00")
	if len(parts) != 2 {
		return nil, true
	}
	generation, e := strconv.ParseUint(parts[1], 10, 64)
	if e != nil {
		return nil, true
	}
	records, e := h.messageQueue.TaskJournal().List(r.Context(), "device:"+device, generation)
	if e != nil {
		return nil, true
	}
	out := []messagequeue.WorkItem{}
	for _, record := range records {
		if record.SessionID != manager.ID {
			continue
		}
		var meta apiMetadata
		if json.Unmarshal(record.Metadata, &meta) != nil || meta.Intake == nil {
			continue
		}
		var input intakeInput
		if json.Unmarshal(meta.Request.Input, &input) != nil {
			continue
		}
		thread := ""
		if input.Target.Thread != nil {
			thread = *input.Target.Thread
		}
		out = append(out, messagequeue.WorkItem{ID: "intake_" + record.RequestID, Kind: "execution", Title: jarvisIntakeWorkTitle(meta.Intake.Goal, input), Body: meta.Intake.Goal, RecordingState: "recorded", Links: []messagequeue.WorkLink{{Kind: "intake", Authority: h.cfg.InstanceID, SessionID: record.SessionID, ThreadID: thread, ConfigRevision: input.Target.Revision, RequestID: record.RequestID}}, SourceRefs: []string{record.ReceiptID}, Revision: 1, CreatedAt: record.CreatedAt, UpdatedAt: record.CreatedAt})
	}
	return out, false
}
