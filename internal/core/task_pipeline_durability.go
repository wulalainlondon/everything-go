package core

import (
	"context"
	"everything-go/internal/taskapi"
	p "everything-go/internal/taskpipeline"
)

var pipelineDurabilityOps = []string{"source_status", "notification_list", "notification_ack", "notification_retry", "trigger_policy_get", "trigger_policy_set"}

func (h *Hub) pipelineDurabilityOperation(ctx context.Context, b taskapi.VerifiedContext, scope p.Scope, in pipelineInput, req taskapi.Request, check func(context.Context) error, disclose func(p.Record) error, projection bool) (any, error) {
	s := h.messageQueue.PipelineStore()
	op := req.Operation
	if h.unifiedBoundedWorker(b) {
		return nil, unifiedError("permission")
	}
	if op == "trigger_policy_get" {
		return s.TriggerPolicy(ctx, scope)
	}
	if op == "trigger_policy_set" {
		// A dispatch grant does not confer global operator rights. This policy only
		// controls this real paired principal's rules in its exact current manager.
		if b.BindingKind != "paired_human" {
			return nil, unifiedError("permission")
		}
		return s.MutateDelivery(ctx, scope, req.IdempotencyKey, op, unifiedHash(req), "", "", in.Expected, in.Enabled, check)
	}
	r, _, found, e := s.Get(ctx, scope, in.ID)
	if e != nil {
		return nil, e
	}
	if !found {
		return nil, unifiedError("not_found")
	}
	if e = disclose(r); e != nil {
		return nil, e
	}
	switch op {
	case "source_status":
		items, e := s.SourceCursors(ctx, scope, in.ID)
		if e != nil {
			return nil, e
		}
		currentItems := []p.SourceCursor{}
		for _, c := range items {
			if c.DefinitionRevision == r.DefinitionRevision {
				j := h.apiJournals()[c.Store]
				if j == nil {
					return nil, unifiedError("source_unverified")
				}
				epoch, err := j.Identity(ctx)
				if err != nil {
					return nil, err
				}
				if c.StoreIdentity == r.Manager.Authority+":"+c.Store+":"+epoch {
					currentItems = append(currentItems, c)
				}
			}
		}
		items = currentItems
		complete := true
		for _, v := range items {
			if v.DefinitionRevision != r.DefinitionRevision {
				old, _, err := s.OriginalDefinition(ctx, r.ID, v.DefinitionRevision)
				if err != nil {
					return nil, err
				}
				if err = disclose(old); err != nil {
					return nil, err
				}
			}
			if v.DefinitionRevision == r.DefinitionRevision && (v.State == "partial" || v.State == "offline" || v.State == "evidence_gap") {
				complete = false
			}
		}
		expected := 0
		for _, step := range p.Steps(r) {
			if step.Source != nil {
				expected += len(pipelineSourceDomains(*step.Source))
			}
		}
		current := 0
		for _, v := range items {
			if v.DefinitionRevision == r.DefinitionRevision {
				current++
			}
		}
		complete = complete && current == expected
		floor, e := s.EventFloor(ctx, in.ID)
		if e != nil {
			return nil, e
		}
		wm, e := s.EventWatermark(ctx, in.ID)
		if e != nil {
			return nil, e
		}
		return map[string]any{"event_retention_floor": floor, "event_watermark": wm, "kind": "source_status", "items": items, "complete": complete, "atomic_across_stores": false}, nil
	case "notification_list":
		items, next, e := s.Notifications(ctx, scope, in.ID, in.Cursor, in.Limit)
		if e != nil {
			return nil, e
		}
		for _, n := range items {
			if n.DefinitionRevision != r.DefinitionRevision {
				old, _, err := s.OriginalDefinition(ctx, r.ID, n.DefinitionRevision)
				if err != nil {
					return nil, err
				}
				if err = disclose(old); err != nil {
					return nil, err
				}
			}
		}
		return map[string]any{"kind": "notifications", "items": items, "next_cursor": next, "complete": next == ""}, nil
	case "notification_ack", "notification_retry":
		if projection || b.BindingKind != "paired_human" {
			return nil, unifiedError("permission")
		}
		final := func(ctx context.Context) error {
			if e := check(ctx); e != nil {
				return e
			}
			return h.pipelineDisclosure(ctx, b, scope.Manager.Session, r)
		}
		return s.MutateDelivery(ctx, scope, req.IdempotencyKey, op, unifiedHash(req), in.ID, in.NotificationID, in.Expected, false, final)
	}
	return nil, unifiedError("unsupported")
}
