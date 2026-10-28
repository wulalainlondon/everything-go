package core

import (
	"context"
	"everything-go/internal/taskapi"
	p "everything-go/internal/taskpipeline"
	"fmt"
	"strings"
	"time"
)

// Global Jarvis decisions and project work items retain separate namespaces.
// No global mission is silently attached to a private project workflow.
func (h *Hub) pipelineWorkRef(ctx context.Context, c taskapi.VerifiedContext, manager string, step p.Step) (*p.Fact, error) {
	if step.WorkItem == nil {
		return nil, nil
	}
	ref := step.WorkItem
	if ref.ProjectID == "jarvis_worklist" {
		if c.BindingKind != "paired_human" && c.BindingKind != "automation_service" {
			return nil, unifiedError("permission")
		}
		device := strings.TrimPrefix(c.StableScopeID, "device:")
		if device == c.StableScopeID || !h.jarvisCredentialCurrent(device, c.NamespaceGeneration) {
			return nil, unifiedError("permission")
		}
		item, e := h.messageQueue.WorkItem(ctx, fmt.Sprintf("%s\x00%d", device, c.NamespaceGeneration), manager, ref.ItemID)
		if e != nil {
			return nil, e
		}
		if item.Revision != ref.Revision {
			return nil, unifiedError("stale_revision")
		}
		if step.Predicate.Kind != "human_decision" {
			return nil, nil
		}
		f := p.Fact{Source: p.Locator{Kind: "workspace", Authority: h.cfg.InstanceID, NativeID: item.ID, Session: manager, Request: step.Predicate.DecisionRef, RoundID: fmt.Sprintf("decision:%s:%d", item.ID, item.Revision)}, Store: "jarvis_worklist_human_decision", Sequence: item.Revision, EvidenceStatus: "pending_human_decision", ObservedAt: time.Now().UnixMilli()}
		if step.Source != nil {
			linked := false
			for _, link := range item.Links {
				if link.Authority == step.Source.Authority && link.SessionID == step.Source.Session && link.RequestID == step.Source.Request && link.ThreadID == step.Source.Thread && link.ConfigRevision == step.Source.Revision && (step.Source.NativeID == "" || link.NativeID == step.Source.NativeID) {
					linked = true
				}
			}
			if !linked {
				return nil, unifiedError("decision_source_unbound")
			}
			sourceFact, e := h.pipelineFact(ctx, c, manager, *step.Source)
			if e != nil {
				return nil, e
			}
			if !p.PredicateMet(p.Predicate{Kind: "success_sealed", InputRevision: step.Predicate.InputRevision, InputHash: step.Predicate.InputHash}, sourceFact) {
				return &f, nil
			}
			f.InputRevision = sourceFact.InputRevision
			f.InputHash = sourceFact.InputHash
			if step.Predicate.ArtifactHash != "" {
				if item.Plan == nil || item.Plan.SHA256 != step.Predicate.ArtifactHash || item.Plan.Revision == 0 || (item.RecordingState != "plan_ready" && item.RecordingState != "awaiting_user_decision") {
					return nil, unifiedError("decision_plan_version_unverified")
				}
				if e = h.pipelineArtifactFact(ctx, step.Predicate, &sourceFact); e != nil {
					return nil, e
				}
				f.ArtifactHash = sourceFact.ArtifactHash
			}
		}
		if item.Decision != nil && item.Decision.State == "approved" && item.Decision.SourceRef != "" && item.Decision.SourceRef == step.Predicate.DecisionRef {
			f.DecisionRef = item.Decision.SourceRef
			f.EvidenceStatus = "paired_human_decision"
		}
		return &f, nil
	}
	if h.work == nil {
		return nil, unifiedError("work_item_adapter_unavailable")
	}
	item, e := h.work.GetItem(ctx, ref.ItemID)
	if e != nil {
		return nil, e
	}
	project, e := h.work.GetProject(ctx, ref.ProjectID)
	if e != nil {
		return nil, e
	}
	if item.ProjectID != project.ID || project.AuthorityInstanceID != h.cfg.InstanceID || project.ArchivedAt != nil || item.ArchivedAt != nil || item.Version != ref.Revision {
		return nil, unifiedError("scope_changed")
	}
	human := c.BindingKind == "paired_human" || (c.BindingKind == "automation_service" && strings.HasPrefix(c.StableScopeID, "device:") && h.jarvisCredentialCurrent(strings.TrimPrefix(c.StableScopeID, "device:"), c.NamespaceGeneration))
	if !human {
		linked, e := h.work.PipelineSessionLinked(ctx, item.ID, manager, c.SourceResumeID)
		if e != nil {
			return nil, e
		}
		if !linked {
			return nil, unifiedError("permission")
		}
	}
	if step.Predicate.Kind == "human_decision" {
		return nil, unifiedError("project_decision_exact_receipt_required")
	}
	return nil, nil
}
