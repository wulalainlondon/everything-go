package core

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type overviewSnapshotSource struct {
	h                                     *Hub
	r                                     *http.Request
	device, principal, scopeHash, section string
	input                                 jarvisWorkScope
	manager                               *session.Session
	coveragePartial                       *bool
}

func (s overviewSnapshotSource) Capture(ctx context.Context, scope taskapi.ReadScope) (taskapi.Freeze, error) {
	for attempt := 0; attempt < 5; attempt++ {
		before, e := s.h.apiWatermarks(ctx)
		if e != nil {
			return taskapi.Freeze{}, e
		}
		projects, attention, e := s.h.overviewProjects(s.r, s.device, s.principal)
		if e != nil {
			return taskapi.Freeze{}, e
		}
		buckets := [][]taskapi.SnapshotRow{{}, {}, {}}
		partial := false
		for _, p := range projects {
			if p["source_partial"] == true {
				partial = true
			}
		}
		for _, a := range attention {
			if s.section == "all" || s.section == "attention" {
				buckets[0] = append(buckets[0], taskapi.SnapshotRow{Value: map[string]any{"section": "attention", "value": overviewPublic(a), "authorization": a["_authorization"]}})
			}
		}
		for _, p := range projects {
			if s.section == "all" || s.section == "projects" {
				buckets[1] = append(buckets[1], taskapi.SnapshotRow{Value: map[string]any{"section": "projects", "value": overviewProjectSummary(p), "authorization": p["_authorization"]}})
			}
		}
		if s.section == "all" || s.section == "albums" {
			collections, e := s.h.messageQueue.OverviewRecords(ctx, s.h.overviewStoreScope(s.principal), "collection")
			if e != nil {
				return taskapi.Freeze{}, e
			}
			for _, v := range collections {
				var c overviewCollection
				if json.Unmarshal(v.Payload, &c) != nil {
					return taskapi.Freeze{}, errors.New("corrupt_metadata")
				}
				c.Revision = v.Revision
				c.UpdatedAt = v.UpdatedAt
				c.PinCount = len(c.Pins)
				if !s.h.overviewCollectionAllowed(ctx, s.principal, c) {
					partial = true
					continue
				}
				buckets[2] = append(buckets[2], taskapi.SnapshotRow{Value: map[string]any{"section": "albums", "value": overviewAlbumSummary(c)}})
			}
		}
		// Rank preserves each original section's order (including unknown first)
		// while giving every nonempty section an initial preview in all mode.
		rows := []taskapi.SnapshotRow{{Key: "\x00overview_header", Value: map[string]any{"section": "coverage", "partial": partial}}}
		for rank := 0; ; rank++ {
			added := false
			for _, bucket := range buckets {
				if rank < len(bucket) {
					row := bucket[rank]
					row.Key = fmt.Sprintf("r:%09d", len(rows))
					rows = append(rows, row)
					added = true
				}
			}
			if !added {
				break
			}
		}
		raw, _ := json.Marshal(rows)
		if len(raw) > 16<<20 {
			return taskapi.Freeze{}, errors.New("snapshot_capacity_originals_preserved")
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
		f := taskapi.Freeze{ID: "jo_" + randomID(), ExpiresAt: time.Now().Add(5 * time.Minute).UnixMilli(), Watermarks: before}
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
func (s overviewSnapshotSource) Page(ctx context.Context, scope taskapi.ReadScope, f taskapi.Freeze, after string, limit int) ([]taskapi.SnapshotRow, bool, error) {
	rows, e := s.h.messageQueue.TaskJournal().Frozen(ctx, scope, f)
	if e != nil {
		return nil, false, e
	}
	out := []taskapi.SnapshotRow{}
	for _, row := range rows {
		if row.Key == "\x00overview_header" {
			b, _ := json.Marshal(row.Value)
			var v struct {
				Partial bool `json:"partial"`
			}
			_ = json.Unmarshal(b, &v)
			if s.coveragePartial != nil {
				*s.coveragePartial = v.Partial
			}
		}
	}
	used := 0
	for _, row := range rows {
		if row.Key != "\x00overview_header" && row.Key > after {
			n := overviewPublicRowBytes(row)
			if used+n > 224<<10 {
				if len(out) == 0 {
					return nil, false, errors.New("snapshot_row_capacity_originals_preserved")
				}
				return out, true, nil
			}
			used += n
			out = append(out, row)
			if len(out) > limit {
				return out[:limit], true, nil
			}
		}
	}
	return out, false, nil
}
func (h *Hub) overviewFrozenAllowed(r *http.Request, device string, raw map[string]any) bool {
	authBytes, _ := json.Marshal(raw["authorization"])
	var auth []overviewAuthorization
	if json.Unmarshal(authBytes, &auth) != nil || !h.overviewAuthorizationsAllowed(r, device, auth) {
		return false
	}
	section, _ := raw["section"].(string)
	b, _ := json.Marshal(raw["value"])
	switch section {
	case "coverage":
		return true
	case "attention":
		var a struct {
			Navigation overviewNavigation `json:"navigation"`
		}
		if json.Unmarshal(b, &a) != nil || !h.overviewNavAllowed(a.Navigation) {
			return false
		}
	case "project_item":
		return true
	case "projects":
		var p struct {
			Sources    []overviewNavigation `json:"sources"`
			Annotation overviewAnnotation   `json:"annotation"`
		}
		if json.Unmarshal(b, &p) != nil {
			return false
		}
		for _, n := range p.Sources {
			if !h.overviewNavAllowed(n) {
				return false
			}
		}
		for _, m := range p.Annotation.Completed {
			if !h.overviewEvidence(r, device, jarvisRequestPrincipal(r, device), m) {
				return false
			}
		}
	case "albums":
		var c overviewCollection
		if json.Unmarshal(b, &c) != nil || !h.overviewCollectionAllowed(r.Context(), jarvisRequestPrincipal(r, device), c) {
			return false
		}
	default:
		return false
	}
	return true
}
func (h *Hub) serveOverviewSnapshot(w http.ResponseWriter, r *http.Request, device string, in overviewRequest, manager *session.Session, principal, scopeHash string) {
	if in.Section == "" {
		in.Section = "all"
	}
	if in.Section != "all" && in.Section != "attention" && in.Section != "projects" && in.Section != "albums" {
		overviewError(w, "invalid_section", 400)
		return
	}
	if h.taskCursor == nil {
		overviewError(w, "storage_unavailable", 503)
		return
	}
	if in.Limit < 1 || in.Limit > 20 {
		in.Limit = 20
	}
	scope := taskapi.ReadScope{Authority: h.cfg.InstanceID, StableScopeID: principal, FilterHash: jarvisHash([]string{"overview-v1", scopeHash, in.Section})}
	coveragePartial := false
	source := overviewSnapshotSource{h, r, device, principal, scopeHash, in.Section, in.jarvisWorkScope, manager, &coveragePartial}
	page, e := (taskapi.SnapshotEngine{Codec: h.taskCursor, Source: source}).Page(r.Context(), scope, []string{"self"}, in.Cursor, in.Limit)
	if e != nil {
		code := 503
		if strings.Contains(e.Error(), "cursor_expired") {
			code = 409
		}
		overviewError(w, "expired_cursor_or_source_changed", code)
		return
	}
	sections := map[string][]any{"attention": {}, "projects": {}, "albums": {}}
	redacted := false
	for _, item := range page.Items {
		b, _ := json.Marshal(item)
		var row map[string]any
		if json.Unmarshal(b, &row) != nil || !h.overviewFrozenAllowed(r, device, row) {
			redacted = true
			continue
		}
		section, _ := row["section"].(string)
		if section == "coverage" {
			continue
		}
		sections[section] = append(sections[section], row["value"])
	}
	_, _, current, e := h.overviewScope(r, device, in.jarvisWorkScope)
	if e != nil || current != scopeHash {
		overviewError(w, "scope_changed", 403)
		return
	}
	out := map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "scope_revision": scopeHash, "freeze_id": page.FreezeID, "sampled_at_ms": page.ExpiresAt - 300000, "fresh_until_ms": page.ExpiresAt - 288000, "watermarks": page.Watermarks, "cursor": page.NextPageCursor, "coverage": map[string]any{"kind": "authorized_ordinary_sources", "partial": page.HasMore || redacted || coveragePartial, "all_tasks": false, "remote": "not_read_here", "total_exact_available": false, "work_scope": "paired_owned_metadata_and_original_relations", "controller_relation_limit": 100, "historical_media": "canonical_current_generation_only"}, "consistency": page.Consistency}
	for name, items := range sections {
		cursor := ""
		if in.Section == name || in.Section == "all" {
			cursor = page.NextPageCursor
		}
		out[name] = map[string]any{"items": items, "cursor": cursor, "coverage": map[string]any{"complete": !page.HasMore && !redacted && !coveragePartial, "loaded": len(items)}}
	}
	jarvisWorkJSON(w, out)
}

func overviewProjectSummary(p map[string]any) map[string]any {
	out := overviewPublic(p)
	if items, ok := out["work_items"].([]overviewWorkItem); ok {
		out["work_item_count"] = len(items)
		if len(items) > 8 {
			out["work_items"] = items[:8]
			out["detail_truncated"] = true
		}
	}
	if executions, ok := out["executions"].([]any); ok && len(executions) > 8 {
		out["executions"] = executions[:8]
		out["executions_truncated"] = true
	}
	// Preview size includes full original plan descriptors, which can be wide.
	// Preserve complete detail rows in the frozen detail reader instead of
	// making the whole snapshot unreadable or truncating an exact PlanRef.
	for {
		raw, _ := json.Marshal(out)
		if len(raw) <= 112<<10 {
			break
		}
		if items, ok := out["work_items"].([]overviewWorkItem); ok && len(items) > 0 {
			out["work_items"] = items[:len(items)-1]
			out["detail_truncated"] = true
			continue
		}
		if es, ok := out["executions"].([]any); ok && len(es) > 0 {
			out["executions"] = es[:len(es)-1]
			out["executions_truncated"] = true
			continue
		}
		break
	}
	return out
}

func overviewPublicRowBytes(row taskapi.SnapshotRow) int {
	raw, _ := json.Marshal(row.Value)
	var object map[string]any
	if json.Unmarshal(raw, &object) != nil {
		return 1 << 30
	}
	public, _ := json.Marshal(map[string]any{"value": object["value"], "executions": object["executions"]})
	return len(public) + 4
}

func overviewAlbumSummary(c overviewCollection) overviewCollection {
	pins := []overviewPin{}
	for _, p := range c.Pins {
		if p.ID == c.Cover {
			pins = append(pins, p)
			break
		}
	}
	c.Pins = pins
	return c
}
