package core

import (
	"errors"
	"everything-go/internal/messagequeue"
	"net/http"
)

// Source IDs are selected by the caller. Navigation is always derived from the
// original authorized WorkItem source, never accepted from client metadata.
type overviewPlanSource struct {
	SourceManager string              `json:"source_manager"`
	SourceItemID  string              `json:"source_item_id"`
	Navigation    *overviewNavigation `json:"navigation,omitempty"`
}

func (h *Hub) selectOverviewPlanSource(projects []map[string]any, projectID string, plan *messagequeue.WorkPlan, selected *overviewPlanSource) (*overviewPlanSource, error) {
	candidates := []overviewWorkItem{}
	for _, project := range projects {
		if project["project_id"] != projectID {
			continue
		}
		for _, item := range project["work_items"].([]overviewWorkItem) {
			if item.Plan == nil || overviewPlanHash(item.Plan) != overviewPlanHash(plan) {
				continue
			}
			if selected != nil && (item.SourceManager != selected.SourceManager || item.SourceItemID != selected.SourceItemID) {
				continue
			}
			candidates = append(candidates, item)
		}
	}
	if len(candidates) != 1 {
		return nil, errors.New("exact_plan_source_required")
	}
	item := candidates[0]
	s, ok := h.registry.Get(item.SourceManager)
	if !ok || !h.eventSessionAllowed(s) {
		return nil, errors.New("plan_source_unavailable")
	}
	navigation := h.overviewNavigation(s)
	return &overviewPlanSource{SourceManager: item.SourceManager, SourceItemID: item.SourceItemID, Navigation: &navigation}, nil
}

func (h *Hub) overviewPlanSourceAllowed(r *http.Request, device, principal string, a overviewAnnotation) bool {
	if a.Plan == nil || a.PlanSource == nil || a.PlanSource.Navigation == nil {
		return false
	}
	for _, nav := range a.Sources {
		if !h.overviewNavAllowed(nav) {
			return false
		}
	}
	source := a.PlanSource
	if source.Navigation.SessionID != source.SourceManager || !h.overviewNavAllowed(*source.Navigation) {
		return false
	}
	item, e := h.messageQueue.WorkItem(r.Context(), principal, source.SourceManager, source.SourceItemID)
	if e != nil || item.Plan == nil || overviewPlanHash(item.Plan) != overviewPlanHash(a.Plan) {
		return false
	}
	return h.overviewAuthorizationsAllowed(r, device, []overviewAuthorization{{Manager: *source.Navigation, Links: item.Links}})
}

func overviewReadablePlan(plan *messagequeue.WorkPlan) *messagequeue.WorkPlan {
	if plan == nil || plan.Path == "" || !jarvisSHA.MatchString(plan.SHA256) || plan.Revision == 0 {
		return nil
	}
	copy := *plan
	if copy.Checklist == nil {
		copy.Checklist = []string{}
	}
	return &copy
}

// Nil and empty checklist both mean exactly zero known milestones in original
// draft metadata. Request input hashes remain the exact original full JSON.
func overviewPlanHash(plan *messagequeue.WorkPlan) string {
	if plan == nil {
		return jarvisHash(nil)
	}
	copy := *plan
	if copy.Checklist == nil {
		copy.Checklist = []string{}
	}
	return jarvisHash(copy)
}
