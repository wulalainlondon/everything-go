package core

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"everything-go/internal/attachmentjournal"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"image"
	"image/color"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type overviewRequest struct {
	jarvisWorkScope
	Operation        string                 `json:"-"`
	Fields           map[string]bool        `json:"-"`
	InputHash        string                 `json:"-"`
	Section          string                 `json:"section,omitempty"`
	Cursor           string                 `json:"cursor,omitempty"`
	Limit            int                    `json:"limit,omitempty"`
	ProjectID        string                 `json:"project_id,omitempty"`
	CollectionID     string                 `json:"collection_id,omitempty"`
	IntentID         string                 `json:"intent_id,omitempty"`
	PlanRevision     uint64                 `json:"revision,omitempty"`
	ExpectedRevision uint64                 `json:"expected_revision,omitempty"`
	Title            string                 `json:"title,omitempty"`
	Phase            string                 `json:"phase,omitempty"`
	NextStep         string                 `json:"next_step,omitempty"`
	Plan             *messagequeue.WorkPlan `json:"plan_ref,omitempty"`
	PlanSource       *overviewPlanSource    `json:"plan_source,omitempty"`
	Completed        []overviewMilestone    `json:"completed_milestones,omitempty"`
	ItemID           string                 `json:"item_id,omitempty"`
	SourceManager    string                 `json:"source_manager,omitempty"`
	State            string                 `json:"state,omitempty"`
	Instruction      string                 `json:"instruction,omitempty"`
	SessionID        string                 `json:"session_id,omitempty"`
	Action           string                 `json:"action,omitempty"`
	Name             string                 `json:"name,omitempty"`
	AttachmentID     string                 `json:"attachment_id,omitempty"`
	PinID            string                 `json:"pin_id,omitempty"`
	SHA256           string                 `json:"sha256,omitempty"`
	Variant          string                 `json:"variant,omitempty"`
	Offset           int                    `json:"offset,omitempty"`
}
type overviewMilestone struct {
	ID            string `json:"id"`
	Evidence      string `json:"evidence_ref"`
	SourceManager string `json:"source_manager"`
}
type overviewWorkItem struct {
	messagequeue.WorkItem
	SourceManager string `json:"source_manager"`
	SourceItemID  string `json:"source_item_id"`
}
type overviewAuthorization struct {
	Manager overviewNavigation      `json:"manager"`
	Links   []messagequeue.WorkLink `json:"links"`
}
type overviewAnnotation struct {
	Title      string                 `json:"title"`
	Phase      string                 `json:"phase"`
	NextStep   string                 `json:"next_step"`
	Plan       *messagequeue.WorkPlan `json:"plan_ref,omitempty"`
	PlanSource *overviewPlanSource    `json:"plan_source,omitempty"`
	Completed  []overviewMilestone    `json:"completed_milestones"`
	Sources    []overviewNavigation   `json:"sources"`
}
type overviewNavigation struct {
	Authority    string `json:"authority_instance_id"`
	SessionID    string `json:"session_id"`
	ThreadID     string `json:"thread_id"`
	Revision     uint64 `json:"config_revision"`
	RequestID    string `json:"request_id,omitempty"`
	AttachmentID string `json:"attachment_id,omitempty"`
}
type overviewPin struct {
	ID         string             `json:"pin_id"`
	AssetID    string             `json:"asset_id"`
	SHA256     string             `json:"source_sha256"`
	MIME       string             `json:"mime_type"`
	Bytes      int                `json:"byte_length"`
	Width      int                `json:"width"`
	Height     int                `json:"height"`
	Navigation overviewNavigation `json:"navigation"`
	CreatedAt  int64              `json:"created_at_ms"`
}
type overviewCollection struct {
	ID        string        `json:"collection_id"`
	Name      string        `json:"name"`
	Revision  uint64        `json:"revision"`
	UpdatedAt int64         `json:"updated_at_ms"`
	PinCount  int           `json:"pin_count"`
	Cover     string        `json:"cover_pin_id,omitempty"`
	Pins      []overviewPin `json:"pins"`
}

func (h *Hub) overviewNavigation(s *session.Session) overviewNavigation {
	return overviewNavigation{Authority: h.cfg.InstanceID, SessionID: s.ID, ThreadID: s.ResumeID(), Revision: s.SettingsSnapshot().ConfigRevision}
}
func (h *Hub) overviewSessions() []*session.Session {
	out := []*session.Session{}
	for _, s := range h.registry.List() {
		if h.eventSessionAllowed(s) {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (h *Hub) overviewScope(r *http.Request, device string, in jarvisWorkScope) (*session.Session, string, string, error) {
	m, p, e := h.workManager(r, device, in)
	if e != nil {
		return nil, "", "", e
	}
	refs := []overviewNavigation{}
	grants := map[string]uint64{}
	for _, s := range h.overviewSessions() {
		refs = append(refs, h.overviewNavigation(s))
		if h.dispatches != nil {
			grant, err := h.dispatches.Grant(r.Context(), s.ID)
			if err != nil {
				return nil, "", "", err
			}
			grants[s.ID] = grant.Revision
		}
	}
	return m, p, jarvisHash(struct {
		Manager          jarvisWorkScope
		Principal        string
		Sources          []overviewNavigation
		ControllerGrants map[string]uint64
	}{in, p, refs, grants}), nil
}
func (h *Hub) overviewNavAllowed(n overviewNavigation) bool {
	s, ok := h.registry.Get(n.SessionID)
	return ok && n.Authority == h.cfg.InstanceID && h.eventSessionAllowed(s) && s.ResumeID() == n.ThreadID && s.SettingsSnapshot().ConfigRevision == n.Revision
}
func overviewError(w http.ResponseWriter, code string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code}})
}
func (h *Hub) serveOverview(w http.ResponseWriter, r *http.Request, device, path string) {
	if r.Method != http.MethodPost || h.messageQueue == nil {
		overviewError(w, "unsupported_capability", 503)
		return
	}
	var in overviewRequest
	rawInput, readErr := io.ReadAll(io.LimitReader(r.Body, jarvisMaxBytes+1))
	if readErr != nil || len(rawInput) > jarvisMaxBytes {
		overviewError(w, "invalid_request", 400)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(rawInput))
	if !jarvisDecode(w, r, &in) {
		return
	}
	in.Operation = path
	var inputObject map[string]any
	if json.Unmarshal(rawInput, &inputObject) != nil {
		overviewError(w, "invalid_request", 400)
		return
	}
	var canonical bytes.Buffer
	encoder := json.NewEncoder(&canonical)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(inputObject)
	in.Fields = map[string]bool{}
	for k := range inputObject {
		in.Fields[k] = true
	}
	in.InputHash = jarvisTextHash(path + "\n" + strings.TrimSuffix(canonical.String(), "\n"))
	mutation := path == "overview/project/annotation" || path == "overview/decision/record" || path == "overview/collections/mutate"
	manager, principal, scopeHash, e := h.overviewScope(r, device, in.jarvisWorkScope)
	if e != nil {
		if mutation && jarvisID.MatchString(in.IntentID) {
			receipt, err := h.messageQueue.RejectOverviewIntent(r.Context(), h.overviewStoreScope(jarvisRequestPrincipal(r, device)), in.IntentID, in.Operation, in.InputHash, "scope_changed")
			if err == nil && receipt.Phase == "rejected" && receipt.KnownNoEffect {
				w.WriteHeader(403)
				value := map[string]any{"error": map[string]string{"code": "scope_changed"}, "receipt": receipt}
				h.overviewEnvelope(value, "")
				jarvisWorkJSON(w, value)
				return
			}
		}
		overviewError(w, "scope_changed", 403)
		return
	}
	if mutation {
		h.jarvisMu.Lock()
		defer h.jarvisMu.Unlock()
	}
	{
		originalWriter := w
		capture := &overviewResponseCapture{header: make(http.Header), status: 200}
		w = capture
		defer func() {
			if capture.status == 200 {
				_, _, now, err := h.overviewScope(r, device, in.jarvisWorkScope)
				if err != nil || now != scopeHash {
					capture.status = 403
					capture.body.Reset()
					_ = json.NewEncoder(&capture.body).Encode(map[string]any{"error": map[string]string{"code": "scope_changed"}})
				}
			}
			if mutation && jarvisID.MatchString(in.IntentID) && (capture.status == 400 || capture.status == 403 || capture.status == 404 || capture.status == 409 || capture.status == 413) {
				var failure struct {
					Error struct {
						Code string `json:"code"`
					} `json:"error"`
				}
				_ = json.Unmarshal(capture.body.Bytes(), &failure)
				if failure.Error.Code == "" {
					failure.Error.Code = "pre_effect_rejected"
				}
				if receipt, err := h.messageQueue.RejectOverviewIntent(r.Context(), h.overviewStoreScope(principal), in.IntentID, in.Operation, in.InputHash, failure.Error.Code); err == nil && receipt.Phase == "rejected" && receipt.KnownNoEffect {
					capture.body.Reset()
					_ = json.NewEncoder(&capture.body).Encode(map[string]any{"error": map[string]string{"code": failure.Error.Code}, "receipt": receipt})
				} else if !errors.Is(err, messagequeue.ErrWorkConflict) {
					capture.status = 503
					capture.body.Reset()
					_ = json.NewEncoder(&capture.body).Encode(map[string]any{"error": map[string]string{"code": "intent_status_unknown"}})
				}
			}
			var envelope map[string]any
			if json.Unmarshal(capture.body.Bytes(), &envelope) == nil {
				h.overviewEnvelope(envelope, scopeHash)
				capture.body.Reset()
				_ = json.NewEncoder(&capture.body).Encode(envelope)
			}
			for k, v := range capture.header {
				originalWriter.Header()[k] = v
			}
			originalWriter.WriteHeader(capture.status)
			_, _ = originalWriter.Write(capture.body.Bytes())
		}()
	}
	if len(in.Title) > 720 || len(in.Phase) > 240 || len(in.NextStep) > 2000 || len(in.Name) > 240 || len(in.Instruction) > 32000 || len(in.Completed) > 200 || in.Plan != nil && len(in.Plan.Checklist) > 200 {
		overviewError(w, "invalid_metadata", 400)
		return
	}
	if path == "overview/project/annotation" || path == "overview/decision/record" || path == "overview/collections/mutate" {
		if prior, err := h.messageQueue.OverviewOperation(r.Context(), h.overviewStoreScope(principal), in.IntentID); err == nil {
			if prior.InputHash != in.InputHash || prior.Operation != path {
				overviewError(w, "revision_or_intent_conflict", 409)
				return
			}
			if !h.overviewReceiptAllowed(r, device, prior) {
				overviewError(w, "permission_denied", 403)
				return
			}
			jarvisWorkJSON(w, prior)
			return
		} else if !errors.Is(err, sql.ErrNoRows) {
			overviewError(w, "storage_unavailable", 503)
			return
		}
	}
	switch path {
	case "overview/snapshot":
		h.serveOverviewSnapshot(w, r, device, in, manager, principal, scopeHash)
		return
	case "overview/project/get":
		h.serveOverviewProject(w, r, device, in, principal, scopeHash)
	case "overview/project/plan":
		h.readOverviewPlan(w, r, device, in, principal, scopeHash)

	case "overview/project/annotation":
		h.mutateOverviewProject(w, r, device, in, principal, scopeHash)
	case "overview/decision/record":
		h.recordOverviewDecision(w, r, device, in, principal, scopeHash)
	case "overview/assets/list":
		h.listOverviewAssets(w, r, in, principal, scopeHash)

	case "overview/collections/get":
		c, err := h.readOverviewCollection(r.Context(), principal, in.CollectionID)
		if err != nil {
			overviewError(w, "not_found", 404)
			return
		}
		if !h.overviewCollectionAllowed(r.Context(), principal, c) {
			overviewError(w, "permission_denied", 403)
			return
		}
		jarvisWorkJSON(w, map[string]any{"collection": c, "pins": c.Pins, "coverage": map[string]any{"complete": true, "loaded": len(c.Pins), "total_exact": len(c.Pins)}, "scope_revision": scopeHash})
	case "overview/collections/mutate":
		h.mutateOverviewCollection(w, r, device, in, principal, scopeHash)
	case "overview/assets/read":
		h.readOverviewAsset(w, r, in, principal, scopeHash)
	case "overview/operation/get":
		receipt, err := h.messageQueue.OverviewOperation(r.Context(), h.overviewStoreScope(principal), in.IntentID)
		if errors.Is(err, sql.ErrNoRows) {
			jarvisWorkJSON(w, map[string]any{"phase": "not_dispatched", "intent_id": in.IntentID})
			return
		}
		if err != nil {
			overviewError(w, "storage_unavailable", 503)
			return
		}
		if !h.overviewReceiptAllowed(r, device, receipt) {
			overviewError(w, "permission_denied", 403)
			return
		}
		jarvisWorkJSON(w, receipt)
	default:
		overviewError(w, "unsupported_capability", 404)
	}
}
func (h *Hub) overviewWorkItems(r *http.Request, device, principal string, s *session.Session) ([]messagequeue.WorkItem, bool, error) {
	out := []messagequeue.WorkItem{}
	after := ""
	for {
		page, next, e := h.messageQueue.WorkItems(r.Context(), principal, s.ID, after, 30)
		if e != nil {
			return nil, true, e
		}
		out = append(out, page...)
		if len(out) > 2000 {
			return nil, true, errors.New("metadata_capacity")
		}
		if next == "" {
			break
		}
		after = next
	}
	d, partial := h.discoveredWorkItems(r, s)
	if h.dispatches != nil {
		records, err := h.dispatches.List(r.Context(), s.ID)
		if err != nil || len(records) >= 100 {
			partial = true
		}
	}
	if h.delegations != nil {
		if _, err := h.delegations.ListByParent(r.Context(), s.ID); err != nil {
			partial = true
		}
	}
	o, p := h.discoveredManagerIntakes(r, device, s)
	out = append(out, d...)
	out = append(out, o...)
	return out, partial || p, nil
}
func (h *Hub) overviewProjects(r *http.Request, device, principal string) ([]map[string]any, []map[string]any, error) {
	projects := map[string]map[string]any{}
	attention := []map[string]any{}
	for _, s := range h.overviewSessions() {
		snap := s.Snapshot()
		cwd, e := h.scopedPath(snap.Cwd)
		trustedProject := strings.TrimSpace(snap.Cwd) != "" && e == nil && cwd != ""
		if trustedProject {
			st, err := os.Stat(cwd)
			trustedProject = err == nil && st.IsDir()
		}
		pid := "project_" + jarvisHash([]string{h.cfg.InstanceID, cwd})[:32]
		var p map[string]any
		if trustedProject {
			p = projects[pid]
		}
		if trustedProject && p == nil {
			title := filepath.Base(cwd)
			if title == "." || title == string(filepath.Separator) {
				title = "未命名專案"
			}
			p = map[string]any{"project_id": pid, "namespace": "folder", "project_kind": "folder", "source_label": h.cfg.InstanceName, "folder_label": filepath.Base(filepath.Dir(cwd)) + "/" + filepath.Base(cwd), "title": title, "revision": uint64(0), "phase": "unassociated", "next_step": "尚未關聯計畫", "navigation": h.overviewNavigation(s), "sources": []overviewNavigation{}, "work_items": []overviewWorkItem{}, "executions": []any{}, "progress": map[string]any{"state": "unassociated"}, "_authorization": []overviewAuthorization{}, "_work_rows": []map[string]any{}}
			projects[pid] = p
		}
		if p != nil {
			p["sources"] = append(p["sources"].([]overviewNavigation), h.overviewNavigation(s))
		}
		items, partial, e := h.overviewWorkItems(r, device, principal, s)
		if e != nil {
			return nil, nil, e
		}
		if partial && p != nil {
			p["source_partial"] = true
		}
		seen := map[string]bool{}
		for _, item := range items {
			if seen[item.ID] {
				continue
			}
			seen[item.ID] = true
			row := h.workSummaryRow(r, device, s, item, strings.HasPrefix(item.ID, "intake_") || strings.HasPrefix(item.ID, "controller_") || strings.HasPrefix(item.ID, "delegation_"))
			visible := row["item"].(messagequeue.WorkItem)
			if row["source_partial"] != true {
				visible.Plan = item.Plan
				if item.Decision != nil {
					visible.Decision = &messagequeue.WorkDecision{State: item.Decision.State, SourceRef: item.Decision.SourceRef}
				}
			}
			if row["source_partial"] == true {
				continue
			}
			auth := overviewAuthorization{Manager: h.overviewNavigation(s), Links: item.Links}
			if p != nil {
				p["work_items"] = append(p["work_items"].([]overviewWorkItem), overviewWorkItem{WorkItem: visible, SourceManager: s.ID, SourceItemID: item.ID})
				p["executions"] = append(p["executions"].([]any), row["executions"].([]any)...)
				p["_authorization"] = append(p["_authorization"].([]overviewAuthorization), auth)
				p["_work_rows"] = append(p["_work_rows"].([]map[string]any), map[string]any{"item": overviewWorkItem{WorkItem: visible, SourceManager: s.ID, SourceItemID: item.ID}, "executions": row["executions"], "authorization": []overviewAuthorization{auth}})
			}
			state := item.RecordingState
			unknown := false
			for _, raw := range row["executions"].([]any) {
				b, _ := json.Marshal(raw)
				if bytes.Contains(b, []byte(`"unknown"`)) {
					unknown = true
				}
			}
			if item.Decision != nil && item.Decision.State == "pending" || state == "awaiting_user_decision" || state == "plan_ready" || unknown {
				if unknown {
					state = "unknown"
				}
				attention = append(attention, map[string]any{"item_id": s.ID + ":" + item.ID, "source_item_id": item.ID, "source_manager": s.ID, "title": visible.Title, "summary": visible.Body, "state": state, "revision": item.Revision, "navigation": h.overviewNavigation(s), "executions": row["executions"], "read_only": row["read_only"], "_authorization": []overviewAuthorization{auth}})
			}
		}
	}
	out := []map[string]any{}
	for pid, p := range projects {
		if record, e := h.messageQueue.OverviewRecord(r.Context(), h.overviewStoreScope(principal), "project", pid); e == nil {
			var a overviewAnnotation
			if json.Unmarshal(record.Payload, &a) != nil {
				return nil, nil, errors.New("corrupt_metadata")
			}
			if a.Completed == nil {
				a.Completed = []overviewMilestone{}
			}
			p["revision"] = record.Revision
			allowed := true
			for _, n := range a.Sources {
				if !h.overviewNavAllowed(n) {
					allowed = false
				}
			}
			if !allowed {
				p["annotation_state"] = "restricted"
				out = append(out, p)
				continue
			}
			p["revision"] = record.Revision
			if a.Title != "" {
				p["title"] = a.Title
			}
			p["phase"] = a.Phase
			p["next_step"] = a.NextStep
			p["annotation"] = a
			p["progress"] = h.overviewProgress(r, device, principal, a)
			if p["source_partial"] == true {
				p["progress"] = map[string]any{"state": "unverified", "reason": "partial_sources"}
			}
		} else if !errors.Is(e, sql.ErrNoRows) {
			return nil, nil, e
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["project_id"].(string) < out[j]["project_id"].(string) })
	sort.SliceStable(attention, func(i, j int) bool {
		a, b := attention[i]["state"], attention[j]["state"]
		if a != b {
			return a == "unknown" || a == "awaiting_user_decision" && b != "unknown"
		}
		return attention[i]["item_id"].(string) < attention[j]["item_id"].(string)
	})
	return out, attention, nil
}

// Milestone denominators come from the exact structured plan bytes, never a
// user supplied unchecked list, test counts, runtime duration, or model prose.
func (h *Hub) overviewPlanBytes(p *messagequeue.WorkPlan) ([]byte, error) {
	if p == nil || p.Revision == 0 || !jarvisSHA.MatchString(p.SHA256) {
		return nil, errors.New("verified_plan_required")
	}
	path, e := h.scopedPath(p.Path)
	if e != nil {
		return nil, e
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	raw, e := io.ReadAll(io.LimitReader(f, 8<<20+1))
	if e != nil || len(raw) > 8<<20 || jarvisTextHash(string(raw)) != p.SHA256 {
		return nil, errors.New("hash_mismatch")
	}
	return raw, nil
}
func (h *Hub) verifyOverviewPlan(p *messagequeue.WorkPlan) error {
	raw, e := h.overviewPlanBytes(p)
	if e != nil {
		return e
	}
	var plan struct {
		Schema     int    `json:"schema_version"`
		Revision   uint64 `json:"revision"`
		Milestones []struct {
			ID string `json:"id"`
		} `json:"milestones"`
	}
	if json.Unmarshal(raw, &plan) != nil || plan.Schema != 1 || plan.Revision != p.Revision || len(plan.Milestones) == 0 || len(plan.Milestones) != len(p.Checklist) {
		return errors.New("structured_plan_milestones_required")
	}
	seen := map[string]bool{}
	for i, m := range plan.Milestones {
		if m.ID == "" || seen[m.ID] || p.Checklist[i] != m.ID {
			return errors.New("milestone_set_mismatch")
		}
		seen[m.ID] = true
	}
	return nil
}
func (h *Hub) overviewEvidence(r *http.Request, device, principal string, m overviewMilestone) bool {
	s, ok := h.registry.Get(m.SourceManager)
	if !ok || !h.eventSessionAllowed(s) {
		return false
	}
	item, e := h.messageQueue.WorkItem(r.Context(), principal, s.ID, m.Evidence)
	if e != nil {
		return false
	}
	if len(item.Links) == 0 {
		if item.Decision == nil || item.Decision.State != "approved" {
			return false
		}
		receipt, err := h.messageQueue.OverviewOperation(r.Context(), h.overviewStoreScope(principal), item.Decision.SourceRef)
		if err != nil || receipt.Operation != "overview/decision/record" || receipt.Phase != "applied" {
			return false
		}
		var evidence struct {
			Item   messagequeue.WorkItem `json:"item"`
			Source overviewNavigation    `json:"source"`
		}
		if json.Unmarshal(receipt.Record.Payload, &evidence) != nil || evidence.Item.ID != item.ID || evidence.Item.Revision != item.Revision || evidence.Source.SessionID != s.ID || !h.overviewNavAllowed(evidence.Source) {
			return false
		}
		return jarvisHash(evidence.Item) == jarvisHash(item)
	}
	for _, l := range item.Links {
		p, e := h.workLinkProjection(r, device, s, l)
		if e != nil {
			return false
		}
		raw, _ := json.Marshal(p["axes"])
		var axes map[string]string
		_ = json.Unmarshal(raw, &axes)
		if axes["execution"] != "succeeded" || axes["final"] != "sealed" || axes["qa"] != "PASS" && axes["qa"] != "pass" || axes["delivery"] != "delivered" {
			return false
		}
	}
	return true
}
func (h *Hub) overviewProgress(r *http.Request, device, principal string, a overviewAnnotation) map[string]any {
	if !h.overviewPlanSourceAllowed(r, device, principal, a) || h.verifyOverviewPlan(a.Plan) != nil {
		return map[string]any{"state": "unverified"}
	}
	seen := map[string]bool{}
	for _, m := range a.Completed {
		if seen[m.ID] || !h.overviewEvidence(r, device, principal, m) {
			return map[string]any{"state": "unverified"}
		}
		found := false
		for _, id := range a.Plan.Checklist {
			if id == m.ID {
				found = true
			}
		}
		if !found {
			return map[string]any{"state": "unverified"}
		}
		seen[m.ID] = true
	}
	total := len(a.Plan.Checklist)
	return map[string]any{"state": "verified_milestones", "completed": len(seen), "total": total, "percent": len(seen) * 100 / total, "denominator_complete": true, "plan_ref": a.Plan}
}
func (h *Hub) mutateOverviewProject(w http.ResponseWriter, r *http.Request, device string, in overviewRequest, principal, scopeHash string) {
	if !jarvisID.MatchString(in.IntentID) {
		overviewError(w, "invalid_intent", 400)
		return
	}
	projects, _, e := h.overviewProjects(r, device, principal)
	if e != nil {
		overviewError(w, "source_offline", 503)
		return
	}
	found := false
	for _, p := range projects {
		if p["project_id"] == in.ProjectID {
			found = true
		}
	}
	if !found {
		overviewError(w, "not_found", 404)
		return
	}
	a := overviewAnnotation{}
	if previous, err := h.messageQueue.OverviewRecord(r.Context(), h.overviewStoreScope(principal), "project", in.ProjectID); err == nil {
		if json.Unmarshal(previous.Payload, &a) != nil {
			overviewError(w, "storage_unavailable", 503)
			return
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		overviewError(w, "storage_unavailable", 503)
		return
	}
	for _, source := range a.Sources {
		if !h.overviewNavAllowed(source) {
			a = overviewAnnotation{}
			break
		}
	}
	oldPlanHash := jarvisHash([]any{a.Plan, a.PlanSource})
	previousPlanHash := jarvisHash(a.Plan)
	if in.Fields["title"] {
		a.Title = in.Title
	}
	if in.Fields["phase"] {
		a.Phase = in.Phase
	}
	if in.Fields["next_step"] {
		a.NextStep = in.NextStep
	}
	if in.Fields["plan_ref"] {
		a.Plan = in.Plan
	}
	if in.Fields["plan_source"] && !in.Fields["plan_ref"] {
		overviewError(w, "plan_source_requires_plan_ref", 400)
		return
	}
	if in.Fields["plan_ref"] {
		if a.Plan == nil {
			a.PlanSource = nil
		} else {
			selected := in.PlanSource
			if selected == nil && jarvisHash(in.Plan) == previousPlanHash && a.PlanSource != nil {
				selected = a.PlanSource
			}
			var err error
			a.PlanSource, err = h.selectOverviewPlanSource(projects, in.ProjectID, a.Plan, selected)
			if err != nil {
				overviewError(w, "exact_plan_source_required", 409)
				return
			}
		}
		if jarvisHash([]any{a.Plan, a.PlanSource}) != oldPlanHash {
			a.Completed = []overviewMilestone{}
		}
	}
	if in.Fields["completed_milestones"] {
		a.Completed = in.Completed
	}
	if a.Completed == nil {
		a.Completed = []overviewMilestone{}
	}
	for _, p := range projects {
		if p["project_id"] == in.ProjectID {
			a.Sources = p["sources"].([]overviewNavigation)
		}
	}
	if in.Fields["plan_ref"] && a.Plan != nil {
		if _, err := h.overviewPlanBytes(a.Plan); err != nil {
			overviewError(w, "hash_or_plan_mismatch", 409)
			return
		}
	}
	if (in.Fields["completed_milestones"] || in.Fields["plan_ref"]) && len(a.Completed) > 0 && h.verifyOverviewPlan(a.Plan) != nil {
		overviewError(w, "verified_plan_milestones_required", 409)
		return
	}

	if in.Fields["completed_milestones"] || in.Fields["plan_ref"] {
		for _, m := range a.Completed {
			if !h.overviewEvidence(r, device, principal, m) {
				overviewError(w, "completion_evidence_required", 409)
				return
			}
		}
		if len(a.Completed) > 0 && h.overviewProgress(r, device, principal, a)["state"] != "verified_milestones" {
			overviewError(w, "invalid_milestone_set", 409)
			return
		}
	}
	for _, m := range a.Completed {
		if source, ok := h.registry.Get(m.SourceManager); ok {
			a.Sources = append(a.Sources, h.overviewNavigation(source))
		}
	}
	// Refuse an unsupported annotation before effect; a saved annotation must
	// leave room for at least one original detail row in the closed transport.
	for _, project := range projects {
		if project["project_id"] != in.ProjectID {
			continue
		}
		header := overviewPublic(project)
		delete(header, "work_items")
		delete(header, "executions")
		header["annotation"] = a
		header["progress"] = h.overviewProgress(r, device, principal, a)
		header["phase"] = a.Phase
		header["next_step"] = a.NextStep
		if a.Title != "" {
			header["title"] = a.Title
		}
		encoded, _ := json.Marshal(header)
		if len(encoded) > 64<<10 {
			overviewError(w, "annotation_capacity_originals_preserved", 409)
			return
		}
	}
	raw, _ := json.Marshal(a)
	_, _, now, e := h.overviewScope(r, device, in.jarvisWorkScope)
	if e != nil || now != scopeHash {
		overviewError(w, "scope_changed", 403)
		return
	}
	out, e := h.messageQueue.MutateOverview(r.Context(), h.overviewStoreScope(principal), "project", in.ProjectID, in.IntentID, in.Operation, in.InputHash, in.ExpectedRevision, raw, nil)
	if e != nil {
		overviewMutationFailure(w, e)
		return
	}
	jarvisWorkJSON(w, out)
}
func (h *Hub) recordOverviewDecision(w http.ResponseWriter, r *http.Request, device string, in overviewRequest, principal, scopeHash string) {
	s, ok := h.registry.Get(in.SourceManager)
	if !ok || !h.eventSessionAllowed(s) {
		overviewError(w, "permission_denied", 403)
		return
	}
	item, e := h.messageQueue.WorkItem(r.Context(), principal, s.ID, in.ItemID)
	if e != nil {
		overviewError(w, "not_found", 404)
		return
	}
	for _, l := range item.Links {
		if _, e = h.workLinkProjection(r, device, s, l); e != nil {
			overviewError(w, "permission_denied", 403)
			return
		}
	}
	_, _, current, e := h.overviewScope(r, device, in.jarvisWorkScope)
	if e != nil || current != scopeHash {
		overviewError(w, "scope_changed", 403)
		return
	}
	decision := messagequeue.WorkDecision{State: in.State, SourceRef: in.IntentID, Instruction: in.Instruction}
	out, e := h.messageQueue.RecordOverviewDecision(r.Context(), principal, h.overviewStoreScope(principal), s.ID, in.ItemID, in.IntentID, in.Operation, in.InputHash, in.ExpectedRevision, decision, mustOverviewJSON(h.overviewNavigation(s)))
	if e != nil {
		overviewMutationFailure(w, e)
		return
	}
	jarvisWorkJSON(w, out)
}
func (h *Hub) readOverviewCollection(ctx context.Context, principal, id string) (overviewCollection, error) {
	v, e := h.messageQueue.OverviewRecord(ctx, h.overviewStoreScope(principal), "collection", id)
	var c overviewCollection
	if e == nil {
		e = json.Unmarshal(v.Payload, &c)
		c.Revision = v.Revision
		c.UpdatedAt = v.UpdatedAt
		c.PinCount = len(c.Pins)
	}
	return c, e
}
func (h *Hub) overviewPinAllowed(ctx context.Context, principal string, p overviewPin) bool {
	if !h.overviewNavAllowed(p.Navigation) {
		return false
	}
	a, err := h.overviewStoredSource(ctx, principal, p.Navigation.AttachmentID)
	if err != nil {
		return false
	}
	n, e := h.overviewAttachmentNavigation(a)
	return e == nil && n == p.Navigation
}
func (h *Hub) overviewCollectionAllowed(ctx context.Context, principal string, c overviewCollection) bool {
	for _, p := range c.Pins {
		if !h.overviewPinAllowed(ctx, principal, p) {
			return false
		}
	}
	return true
}
func (h *Hub) overviewReceiptAllowed(r *http.Request, device string, v messagequeue.OverviewReceipt) bool {
	if v.Phase == "rejected" && v.KnownNoEffect {
		return true
	}
	if v.Operation == "overview/decision/record" {
		var payload struct {
			Item   messagequeue.WorkItem `json:"item"`
			Source overviewNavigation    `json:"source"`
		}
		if json.Unmarshal(v.Record.Payload, &payload) != nil || !h.overviewNavAllowed(payload.Source) {
			return false
		}
		return h.overviewAuthorizationsAllowed(r, device, []overviewAuthorization{{Manager: payload.Source, Links: payload.Item.Links}})
	}
	var a overviewAnnotation
	if v.Operation == "overview/project/annotation" {
		if json.Unmarshal(v.Record.Payload, &a) != nil {
			return false
		}
		for _, n := range a.Sources {
			if !h.overviewNavAllowed(n) {
				return false
			}
		}
		return true
	}
	var c overviewCollection
	if json.Unmarshal(v.Record.Payload, &c) == nil && c.ID != "" {
		return h.overviewCollectionAllowed(r.Context(), jarvisRequestPrincipal(r, device), c)
	}
	return true
}
func (h *Hub) inspectOverviewImage(a attachmentjournal.Record) (messagequeue.OverviewAsset, error) {
	var out messagequeue.OverviewAsset
	info, e := os.Lstat(a.Path)
	if e != nil || !info.Mode().IsRegular() || info.Size() > 20<<20 {
		return out, errors.New("source_or_decode_limit")
	}
	f, e := os.Open(a.Path)
	if e != nil {
		return out, e
	}
	defer f.Close()
	actual, e := f.Stat()
	if e != nil || !os.SameFile(info, actual) {
		return out, errors.New("source_changed")
	}
	raw, e := io.ReadAll(io.LimitReader(f, 20<<20+1))
	if e != nil || len(raw) > 20<<20 {
		return out, errors.New("decode_limit")
	}
	if e = overviewStaticRaster(raw); e != nil {
		return out, e
	}
	cfg, format, e := image.DecodeConfig(bytes.NewReader(raw))
	if e != nil || cfg.Width < 1 || cfg.Height < 1 || int64(cfg.Width)*int64(cfg.Height) > 40_000_000 {
		return out, errors.New("decode_limit")
	}
	out = messagequeue.OverviewAsset{SHA256: jarvisTextHash(string(raw)), MIME: map[string]string{"jpeg": "image/jpeg", "png": "image/png", "gif": "image/gif"}[format], Width: cfg.Width, Height: cfg.Height, Bytes: raw}
	out.ID = "asset_" + out.SHA256
	return out, nil
}
func (h *Hub) managedOverviewAsset(a attachmentjournal.Record) (messagequeue.OverviewAsset, error) {
	out, e := h.inspectOverviewImage(a)
	if e != nil {
		return out, e
	}
	raw := out.Bytes
	cfg := image.Config{Width: out.Width, Height: out.Height}

	img, _, e := image.Decode(bytes.NewReader(raw))
	if e != nil {
		return out, e
	}
	tw, th := cfg.Width, cfg.Height
	if tw > 512 || th > 512 {
		if tw >= th {
			th = th * 512 / tw
			tw = 512
		} else {
			tw = tw * 512 / th
			th = 512
		}
	}
	if tw < 1 {
		tw = 1
	}
	if th < 1 {
		th = 1
	}
	for {
		thumb := image.NewNRGBA(image.Rect(0, 0, tw, th))
		for y := 0; y < th; y++ {
			for x := 0; x < tw; x++ {
				thumb.Set(x, y, color.NRGBAModel.Convert(img.At(x*cfg.Width/tw, y*cfg.Height/th)))
			}
		}
		var b bytes.Buffer
		if e = png.Encode(&b, thumb); e != nil {
			return out, e
		}
		if b.Len() <= 512<<10 {
			out.Thumbnail = b.Bytes()
			break
		}
		if tw == 1 && th == 1 {
			return out, errors.New("thumbnail_limit")
		}
		tw = max(1, tw/2)
		th = max(1, th/2)
	}
	return out, nil
}
func (h *Hub) mutateOverviewCollection(w http.ResponseWriter, r *http.Request, device string, in overviewRequest, principal, scopeHash string) {
	if !jarvisID.MatchString(in.IntentID) || len(in.CollectionID) < 1 || len(in.CollectionID) > 120 {
		overviewError(w, "invalid_intent", 400)
		return
	}
	if prior, e := h.messageQueue.OverviewOperation(r.Context(), h.overviewStoreScope(principal), in.IntentID); e == nil {
		if prior.InputHash != in.InputHash {
			overviewError(w, "revision_or_intent_conflict", 409)
			return
		}
		if !h.overviewReceiptAllowed(r, device, prior) {
			overviewError(w, "permission_denied", 403)
			return
		}
		jarvisWorkJSON(w, prior)
		return
	}
	c, e := h.readOverviewCollection(r.Context(), principal, in.CollectionID)
	if errors.Is(e, sql.ErrNoRows) && in.Action == "create" {
		c = overviewCollection{ID: in.CollectionID, Name: in.Name, Pins: []overviewPin{}}
	} else if e != nil {
		overviewError(w, "not_found", 404)
		return
	}
	if !h.overviewCollectionAllowed(r.Context(), principal, c) {
		overviewError(w, "permission_denied", 403)
		return
	}
	var asset *messagequeue.OverviewAsset
	switch in.Action {
	case "create":
		if in.ExpectedRevision != 0 || c.Revision != 0 || strings.TrimSpace(in.Name) == "" {
			overviewError(w, "revision_conflict", 409)
			return
		}
	case "rename":
		if strings.TrimSpace(in.Name) == "" {
			overviewError(w, "invalid_name", 400)
			return
		}
		c.Name = in.Name
	case "pin":
		a, ok := h.attachments.Get(in.AttachmentID)
		s, found := h.registry.Get(a.SessionID)
		if !ok || !found || !h.eventSessionAllowed(s) || a.RequestID == "" || a.Kind != "media" {
			overviewError(w, "permission_denied", 403)
			return
		}
		v, e := h.managedOverviewAsset(a)
		if e != nil {
			overviewError(w, "source_or_decode_limit", 409)
			return
		}
		if !jarvisSHA.MatchString(in.SHA256) || v.SHA256 != in.SHA256 {
			overviewError(w, "hash_mismatch", 409)
			return
		}
		v.SourceAttachment = a.AttachmentID
		a.AckedByDevice = nil
		a.PinnedByWork = nil
		v.SourceJSON = mustOverviewJSON(a)
		asset = &v
		n, err := h.overviewAttachmentNavigation(a)
		if err != nil {
			overviewError(w, "original_source_binding_required", 409)
			return
		}
		pid := "pin_" + jarvisHash([]string{c.ID, a.AttachmentID, v.SHA256})[:32]
		for _, p := range c.Pins {
			if p.ID == pid {
				overviewError(w, "pin_already_exists", 409)
				return
			}
		}
		if len(c.Pins) >= 200 {
			overviewError(w, "collection_capacity_originals_preserved", 409)
			return
		}
		c.Pins = append(c.Pins, overviewPin{pid, v.ID, v.SHA256, v.MIME, len(v.Bytes), v.Width, v.Height, n, time.Now().UnixMilli()})
		if c.Cover == "" {
			c.Cover = pid
		}
	case "unpin", "cover":
		found := false
		pins := []overviewPin{}
		for _, p := range c.Pins {
			if p.ID == in.PinID {
				found = true
				if in.Action == "cover" {
					c.Cover = p.ID
					pins = append(pins, p)
				}
			} else {
				pins = append(pins, p)
			}
		}
		if !found {
			overviewError(w, "not_found", 404)
			return
		}
		c.Pins = pins
		if in.Action == "unpin" && c.Cover == in.PinID {
			c.Cover = ""
			if len(pins) > 0 {
				c.Cover = pins[0].ID
			}
		}
	default:
		overviewError(w, "unsupported_operation", 400)
		return
	}
	c.PinCount = len(c.Pins)
	raw, _ := json.Marshal(c)
	_, _, now, e := h.overviewScope(r, device, in.jarvisWorkScope)
	// New pin provenance commits with bytes; recheck its original navigation
	// directly before SQL registration. Existing pins still use retained SQL proof.
	allowed := true
	if asset == nil {
		allowed = h.overviewCollectionAllowed(r.Context(), principal, c)
	} else {
		for _, pin := range c.Pins {
			if pin.AssetID == asset.ID {
				allowed = allowed && h.overviewNavAllowed(pin.Navigation)
			} else {
				allowed = allowed && h.overviewPinAllowed(r.Context(), principal, pin)
			}
		}
	}
	if e != nil || now != scopeHash || !allowed {
		overviewError(w, "scope_changed", 403)
		return
	}
	out, e := h.messageQueue.MutateOverview(r.Context(), h.overviewStoreScope(principal), "collection", c.ID, in.IntentID, in.Operation, in.InputHash, in.ExpectedRevision, raw, asset)
	if e != nil {
		overviewMutationFailure(w, e)
		return
	}

	c.Revision = out.Revision
	c.UpdatedAt = out.Record.UpdatedAt
	jarvisWorkJSON(w, map[string]any{"phase": out.Phase, "operation": out.Operation, "intent_id": out.IntentID, "input_hash": out.InputHash, "revision": out.Revision, "record": out.Record, "collection": c})
}
func (h *Hub) readOverviewAsset(w http.ResponseWriter, r *http.Request, in overviewRequest, principal, scopeHash string) {
	c, e := h.readOverviewCollection(r.Context(), principal, in.CollectionID)
	if e != nil || !h.overviewCollectionAllowed(r.Context(), principal, c) {
		overviewError(w, "permission_denied", 403)
		return
	}
	var pin *overviewPin
	for i := range c.Pins {
		if c.Pins[i].ID == in.PinID {
			pin = &c.Pins[i]
		}
	}
	if pin == nil {
		overviewError(w, "not_found", 404)
		return
	}
	if pin.SHA256 != in.SHA256 {
		overviewError(w, "hash_mismatch", 409)
		return
	}
	source, sourceErr := h.overviewStoredSource(r.Context(), principal, pin.Navigation.AttachmentID)
	if sourceErr != nil {
		overviewError(w, "source_unavailable", 404)
		return
	}
	current, err := h.inspectOverviewImage(source)
	if err != nil {
		overviewError(w, "source_unavailable", 404)
		return
	}
	if current.SHA256 != pin.SHA256 {
		overviewError(w, "source_version_changed", 409)
		return
	}
	a, e := h.messageQueue.OverviewAsset(r.Context(), h.overviewStoreScope(principal), pin.AssetID)
	if e != nil || a.SHA256 != pin.SHA256 || jarvisTextHash(string(a.Bytes)) != pin.SHA256 {
		overviewError(w, "hash_mismatch", 409)
		return
	}
	raw, mime := a.Bytes, a.MIME
	if in.Variant == "thumbnail" {
		raw = a.Thumbnail
		mime = "image/png"
	} else if in.Variant != "original" {
		overviewError(w, "invalid_variant", 400)
		return
	}
	if in.Offset < 0 || in.Offset > len(raw) {
		overviewError(w, "invalid_range", 416)
		return
	}
	limit := in.Limit
	if limit < 1 || limit > 65536 {
		limit = 65536
	}
	end := in.Offset + limit
	if end > len(raw) {
		end = len(raw)
	}
	if !h.overviewPinAllowed(r.Context(), principal, *pin) {
		overviewError(w, "scope_changed", 403)
		return
	}
	jarvisWorkJSON(w, map[string]any{"mime_type": mime, "source_sha256": a.SHA256, "sha256": a.SHA256, "variant_sha256": jarvisTextHash(string(raw)), "bytes_base64": base64.StdEncoding.EncodeToString(raw[in.Offset:end]), "byte_length": end - in.Offset, "total_bytes": len(raw), "offset": in.Offset, "next_offset": end, "complete": end == len(raw), "width": a.Width, "height": a.Height, "scope_revision": scopeHash, "fresh_until_ms": time.Now().Add(12 * time.Second).UnixMilli()})
}

func mustOverviewJSON(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
func overviewPublic(p map[string]any) map[string]any {
	v := map[string]any{}
	for k, x := range p {
		if !strings.HasPrefix(k, "_") {
			v[k] = x
		}
	}
	return v
}
func (h *Hub) overviewAuthorizationsAllowed(r *http.Request, device string, refs []overviewAuthorization) bool {
	for _, a := range refs {
		if !h.overviewNavAllowed(a.Manager) {
			return false
		}
		s, _ := h.registry.Get(a.Manager.SessionID)
		for _, l := range a.Links {
			if _, e := h.workLinkProjection(r, device, s, l); e != nil {
				return false
			}
		}
	}
	return true
}
func (h *Hub) overviewAttachmentNavigation(a attachmentjournal.Record) (overviewNavigation, error) {
	var n overviewNavigation
	s, ok := h.registry.Get(a.SessionID)
	if !ok || !h.eventSessionAllowed(s) || a.RequestID == "" {
		return n, errors.New("source_unavailable")
	}
	entry, ok, e := h.messageQueue.Get(a.SessionID, a.RequestID)
	if e != nil || !ok {
		return n, errors.New("original_request_required")
	}
	var payload queuedPayload
	if json.Unmarshal(entry.Payload, &payload) != nil || payload.ExpectedTarget == nil {
		return n, errors.New("original_source_binding_required")
	}
	thread := payload.ExpectedTarget.ThreadID
	if thread == "" {
		if proof, found, err := h.messageQueue.ExactNativeAcceptance(a.SessionID, a.RequestID); err == nil && found {
			thread = proof.ThreadID
		} else if proof, found, err := h.messageQueue.ProviderEvidence(a.SessionID, a.RequestID); err == nil && found {
			thread = proof.ConversationID
		}
	}
	if thread == "" {
		return n, errors.New("original_source_binding_required")
	}
	n = overviewNavigation{Authority: h.cfg.InstanceID, SessionID: a.SessionID, ThreadID: thread, Revision: payload.ExpectedTarget.Revision, RequestID: a.RequestID, AttachmentID: a.AttachmentID}
	if !h.overviewNavAllowed(n) {
		return n, errors.New("source_generation_changed")
	}
	return n, nil
}

func (h *Hub) listOverviewAssets(w http.ResponseWriter, r *http.Request, in overviewRequest, principal, scopeHash string) {
	s, ok := h.registry.Get(in.SessionID)
	if !ok || !h.eventSessionAllowed(s) || h.attachments == nil {
		overviewError(w, "permission_denied", 403)
		return
	}
	nav := h.overviewNavigation(s)
	if h.taskCursor == nil {
		overviewError(w, "storage_unavailable", 503)
		return
	}
	scope := taskapi.ReadScope{Authority: h.cfg.InstanceID, StableScopeID: principal, FilterHash: jarvisHash([]string{"overview-assets-v1", scopeHash, in.SessionID})}
	var before, high uint64
	freeze := taskapi.Freeze{ID: "ja_" + randomID(), ExpiresAt: time.Now().Add(5 * time.Minute).UnixMilli()}
	if in.Cursor != "" {
		var after string
		var err error
		freeze, after, err = h.taskCursor.SourcePagePosition("overview-assets-v1", in.Cursor, scope)
		if err != nil {
			overviewError(w, "expired_cursor", 409)
			return
		}
		before, err = strconv.ParseUint(after, 10, 64)
		if err != nil || len(freeze.Watermarks) != 1 || freeze.Watermarks[0].Store != "attachments" {
			overviewError(w, "expired_cursor", 409)
			return
		}
		high = freeze.Watermarks[0].Sequence
	}

	records, more, high := h.attachments.SessionPage(s.ID, before, high, in.Limit)
	out := []any{}
	for _, a := range records {
		if a.Kind != "media" || !strings.HasPrefix(a.MIMEType, "image/") {
			continue
		}
		n, e := h.overviewAttachmentNavigation(a)
		if e != nil {
			continue
		}
		v, e := h.inspectOverviewImage(a)
		if e != nil {
			continue
		}
		out = append(out, map[string]any{"attachment_id": a.AttachmentID, "session_id": a.SessionID, "request_id": a.RequestID, "name": a.DisplayName, "mime_type": v.MIME, "created_at_ms": a.CreatedAt, "source_sha256": v.SHA256, "byte_length": len(v.Bytes), "width": v.Width, "height": v.Height, "navigation": n})
	}
	cursor := ""
	if more && len(records) > 0 {
		freeze.Watermarks = []taskapi.Watermark{{Store: "attachments", Sequence: high}}
		var err error
		cursor, err = h.taskCursor.SourcePageCursor("overview-assets-v1", scope, freeze, strconv.FormatUint(records[len(records)-1].Sequence, 10))
		if err != nil {
			overviewError(w, "storage_unavailable", 503)
			return
		}
	}

	if !h.overviewNavAllowed(nav) {
		overviewError(w, "scope_changed", 403)
		return
	}
	jarvisWorkJSON(w, map[string]any{"items": out, "cursor": cursor, "coverage": map[string]any{"complete": !more, "loaded": len(out)}, "scope_revision": scopeHash})
}

type overviewResponseCapture struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *overviewResponseCapture) Header() http.Header         { return w.header }
func (w *overviewResponseCapture) WriteHeader(status int)      { w.status = status }
func (w *overviewResponseCapture) Write(b []byte) (int, error) { return w.body.Write(b) }
func overviewMutationFailure(w http.ResponseWriter, e error) {
	switch {
	case errors.Is(e, messagequeue.ErrWorkConflict):
		overviewError(w, "revision_conflict", 409)
	case strings.Contains(e.Error(), "capacity"):
		overviewError(w, "storage_capacity_originals_preserved", 409)
	case strings.HasPrefix(e.Error(), "invalid_") || strings.HasPrefix(e.Error(), "explicit_"):
		overviewError(w, "invalid_metadata", 400)
	default:
		overviewError(w, "storage_or_commit_unknown", 503)
	}
}

func (h *Hub) overviewStoreScope(principal string) string {
	return "overview-v1\x00" + h.cfg.InstanceID + "\x00" + principal
}
func (h *Hub) overviewStoredSource(ctx context.Context, principal, attachment string) (attachmentjournal.Record, error) {
	var record attachmentjournal.Record
	raw, e := h.messageQueue.OverviewSource(ctx, h.overviewStoreScope(principal), attachment)
	if e == nil {
		e = json.Unmarshal(raw, &record)
	}
	if e == nil && record.AttachmentID != attachment {
		e = errors.New("source_identity_conflict")
	}
	return record, e
}
func (h *Hub) overviewEnvelope(value map[string]any, scopeHash string) {
	value["schema_version"] = 1
	value["authority_instance_id"] = h.cfg.InstanceID
	value["scope_revision"] = scopeHash
	if original, ok := value["receipt"]; ok {
		raw, _ := json.Marshal(original)
		var receipt map[string]any
		if json.Unmarshal(raw, &receipt) == nil {
			receipt["authority_instance_id"] = h.cfg.InstanceID
			receipt["schema_version"] = 1
			value["receipt"] = receipt
		}
	}
}
