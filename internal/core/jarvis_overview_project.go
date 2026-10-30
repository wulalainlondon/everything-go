package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"everything-go/internal/messagequeue"
	"everything-go/internal/taskapi"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

type overviewProjectSource struct {
	h                                       *Hub
	r                                       *http.Request
	device, principal, scopeHash, projectID string
	input                                   jarvisWorkScope
	header                                  *map[string]any
}

func (s overviewProjectSource) Capture(ctx context.Context, scope taskapi.ReadScope) (taskapi.Freeze, error) {
	for attempt := 0; attempt < 5; attempt++ {
		before, e := s.h.apiWatermarks(ctx)
		if e != nil {
			return taskapi.Freeze{}, e
		}
		projects, _, e := s.h.overviewProjects(s.r, s.device, s.principal)
		if e != nil {
			return taskapi.Freeze{}, e
		}
		var target map[string]any
		for _, p := range projects {
			if p["project_id"] == s.projectID {
				target = p
				break
			}
		}
		if target == nil {
			return taskapi.Freeze{}, errors.New("project_not_found")
		}
		header := overviewPublic(target)
		delete(header, "work_items")
		delete(header, "executions")
		work := target["_work_rows"].([]map[string]any)
		header["work_item_count"] = len(work)
		headerBytes, _ := json.Marshal(header)
		if len(headerBytes) > 64<<10 {
			return taskapi.Freeze{}, errors.New("project_header_capacity")
		}
		rows := []taskapi.SnapshotRow{{Key: "\x00project_header", Value: map[string]any{"section": "projects", "value": header, "authorization": target["_authorization"]}}}
		for _, row := range work {
			item := row["item"].(overviewWorkItem)
			rows = append(rows, taskapi.SnapshotRow{Key: "w:" + item.SourceManager + ":" + item.SourceItemID, Value: map[string]any{"section": "project_item", "value": item, "executions": row["executions"], "authorization": row["authorization"]}})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
		raw, _ := json.Marshal(rows)
		if len(raw) > 32<<20 {
			return taskapi.Freeze{}, errors.New("project_snapshot_capacity")
		}
		after, e := s.h.apiWatermarks(ctx)
		if e != nil {
			return taskapi.Freeze{}, e
		}
		if jarvisHash(before) != jarvisHash(after) {
			continue
		}
		_, _, hash, e := s.h.overviewScope(s.r, s.device, s.input)
		if e != nil || hash != s.scopeHash {
			return taskapi.Freeze{}, errors.New("cursor_expired")
		}
		f := taskapi.Freeze{ID: "jp_" + randomID(), ExpiresAt: time.Now().Add(5 * time.Minute).UnixMilli(), Watermarks: before}
		var local uint64
		for _, m := range before {
			if m.Store == "ordinary" {
				local = m.Sequence
			}
		}
		if e = s.h.messageQueue.TaskJournal().FreezeChecked(ctx, scope, f, rows, local); e != nil {
			if strings.Contains(e.Error(), "busy") {
				continue
			}
			return taskapi.Freeze{}, e
		}
		return f, nil
	}
	return taskapi.Freeze{}, errors.New("busy")
}
func (s overviewProjectSource) Page(ctx context.Context, scope taskapi.ReadScope, f taskapi.Freeze, after string, limit int) ([]taskapi.SnapshotRow, bool, error) {
	rows, e := s.h.messageQueue.TaskJournal().Frozen(ctx, scope, f)
	if e != nil {
		return nil, false, e
	}
	out := []taskapi.SnapshotRow{}
	for _, row := range rows {
		if row.Key == "\x00project_header" {
			raw, _ := json.Marshal(row.Value)
			var header map[string]any
			if json.Unmarshal(raw, &header) != nil || !s.h.overviewFrozenAllowed(s.r, s.device, header) {
				return nil, false, errors.New("project_source_changed")
			}
			*s.header = header["value"].(map[string]any)
		}
	}
	for _, row := range rows {
		if row.Key != "\x00project_header" && row.Key > after {
			out = append(out, row)
			if len(out) > limit {
				return out[:limit], true, nil
			}
		}
	}
	return out, false, nil
}
func (h *Hub) serveOverviewProject(w http.ResponseWriter, r *http.Request, device string, in overviewRequest, principal, scopeHash string) {
	if in.ProjectID == "" || h.taskCursor == nil {
		overviewError(w, "project_or_storage_unavailable", 400)
		return
	}
	if in.Limit < 1 || in.Limit > 20 {
		in.Limit = 20
	}
	scope := taskapi.ReadScope{Authority: h.cfg.InstanceID, StableScopeID: principal, FilterHash: jarvisHash([]string{"overview-project-detail-v1", scopeHash, in.ProjectID})}
	header := map[string]any{}
	source := overviewProjectSource{h, r, device, principal, scopeHash, in.ProjectID, in.jarvisWorkScope, &header}
	page, e := (taskapi.SnapshotEngine{Codec: h.taskCursor, Source: source}).Page(r.Context(), scope, []string{"self"}, in.Cursor, in.Limit)
	if e != nil {
		switch {
		case strings.Contains(e.Error(), "cursor_expired"):
			overviewError(w, "expired_cursor", 409)
		case strings.Contains(e.Error(), "project_source_changed"):
			overviewError(w, "permission_denied", 403)
		case strings.Contains(e.Error(), "project_not_found"):
			overviewError(w, "not_found", 404)
		default:
			overviewError(w, "project_source_or_capacity_unavailable", 503)
		}
		return
	}
	items := []any{}
	executions := []any{}
	partial := header["source_partial"] == true
	for _, item := range page.Items {
		raw, _ := json.Marshal(item)
		var row map[string]any
		if json.Unmarshal(raw, &row) != nil || !h.overviewFrozenAllowed(r, device, row) {
			partial = true
			continue
		}
		if row["section"] != "project_item" {
			continue
		}
		items = append(items, row["value"])
		if es, ok := row["executions"].([]any); ok {
			executions = append(executions, es...)
		}
	}
	header["work_items"] = items
	header["executions"] = executions
	coverage := map[string]any{"complete": !page.HasMore && !partial, "loaded": len(items), "partial_sources": partial}
	if !partial {
		coverage["total_exact"] = header["work_item_count"]
	}
	jarvisWorkJSON(w, map[string]any{"project": header, "cursor": page.NextPageCursor, "freeze_id": page.FreezeID, "watermarks": page.Watermarks, "sampled_at_ms": page.ExpiresAt - 300000, "fresh_until_ms": page.ExpiresAt - 288000, "coverage": coverage})
}

// A plan locator is resolved only from the selected authorized project's
// existing original PlanRefs. There is deliberately no client path parameter.
func (h *Hub) readOverviewPlan(w http.ResponseWriter, r *http.Request, device string, in overviewRequest, principal, scopeHash string) {
	if !jarvisSHA.MatchString(in.SHA256) || in.PlanRevision == 0 {
		overviewError(w, "verified_plan_required", 400)
		return
	}
	projects, _, e := h.overviewProjects(r, device, principal)
	if e != nil {
		overviewError(w, "source_unavailable", 503)
		return
	}
	var selected *messagequeue.WorkPlan
	var navigation overviewNavigation
	for _, p := range projects {
		if p["project_id"] != in.ProjectID {
			continue
		}
		navigation = p["navigation"].(overviewNavigation)
		if annotation, ok := p["annotation"].(overviewAnnotation); ok && annotation.Plan != nil && annotation.Plan.SHA256 == in.SHA256 && annotation.Plan.Revision == in.PlanRevision {
			selected = annotation.Plan
		}
		if selected == nil {
			for _, item := range p["work_items"].([]overviewWorkItem) {
				if item.Plan != nil && item.Plan.SHA256 == in.SHA256 && item.Plan.Revision == in.PlanRevision {
					selected = item.Plan
					if source, ok := h.registry.Get(item.SourceManager); ok {
						navigation = h.overviewNavigation(source)
					}
					break
				}
			}
		}
	}
	if selected == nil {
		overviewError(w, "not_found", 404)
		return
	}
	raw, e := h.overviewPlanBytes(selected)
	if e != nil {
		overviewError(w, "plan_version_or_bytes_unavailable", 409)
		return
	}
	if !utf8.Valid(raw) {
		overviewError(w, "plan_text_encoding_unsupported", 415)
		return
	}
	if in.Offset < 0 || in.Offset > len(raw) || in.Offset < len(raw) && !utf8.RuneStart(raw[in.Offset]) {
		overviewError(w, "invalid_range", 416)
		return
	}
	limit := in.Limit
	if limit < 4 || limit > 65536 {
		limit = 65536
	}
	end := in.Offset + limit
	if end > len(raw) {
		end = len(raw)
	}
	for end > in.Offset && !utf8.Valid(raw[in.Offset:end]) {
		end--
	}
	jarvisWorkJSON(w, map[string]any{"sha256": selected.SHA256, "revision": selected.Revision, "bytes_base64": base64.StdEncoding.EncodeToString(raw[in.Offset:end]), "offset": in.Offset, "next_offset": end, "byte_length": end - in.Offset, "total_bytes": len(raw), "complete": end == len(raw), "truncated": end < len(raw), "fresh_until_ms": time.Now().Add(12 * time.Second).UnixMilli(), "source_navigation": navigation, "mime_type": "text/plain; charset=utf-8"})
}
