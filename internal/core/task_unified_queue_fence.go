package core

import (
	"context"
	"everything-go/internal/taskapi"
)

type unifiedQueueIdentityFence struct {
	Identity    jarvisTarget
	MayBeUnborn bool
	RoleEpoch   uint64
}

// Capture exact original source/target tuples before yielding to an owner writer.
// Registry predicates are rechecked after database admission; controller grant
// revisions are checked under that owner's existing transaction, avoiding a
// recursive read of the same SQLite writer connection.
func (h *Hub) unifiedQueueFences(ctx context.Context, q unifiedQueue, rows []taskapi.QueueRow, eligible map[string]bool) ([]unifiedQueueIdentityFence, map[string]uint64, error) {
	out := []unifiedQueueIdentityFence{}
	grants := map[string]uint64{}
	controller := func(id string) error {
		d, ok, e := h.dispatches.Get(ctx, id)
		if e != nil {
			return e
		}
		if !ok || !d.ParentConfigKnown {
			return unifiedError("scope_changed")
		}
		out = append(out, unifiedQueueIdentityFence{jarvisTarget{h.cfg.InstanceID, d.ParentID, d.ParentThreadID, d.ParentConfigRevision}, false, d.RoleEpoch}, unifiedQueueIdentityFence{jarvisTarget{h.cfg.InstanceID, d.SessionID, d.ThreadID, d.ConfigRevision}, false, 0})
		g, e := h.dispatches.Grant(ctx, d.ParentID)
		if e != nil {
			return e
		}
		grants[d.ParentID] = g.Revision
		return nil
	}
	delegation := func(id string) error {
		d, ok, e := h.delegations.ByID(ctx, id)
		if e != nil {
			return e
		}
		if !ok {
			return unifiedError("scope_changed")
		}
		thread, rev, bound, e := h.delegations.SourceBinding(ctx, id)
		if e != nil {
			return e
		}
		if !bound {
			return unifiedError("scope_changed")
		}
		out = append(out, unifiedQueueIdentityFence{jarvisTarget{h.cfg.InstanceID, d.ParentSessionID, thread, rev}, false, 0})
		thread, rev, _, e = h.delegations.ExecutionBinding(ctx, id)
		if e != nil {
			return e
		}
		out = append(out, unifiedQueueIdentityFence{jarvisTarget{h.cfg.InstanceID, d.ChildSessionID, thread, rev}, q.Kind == "delegation_waiting", 0})
		return nil
	}
	for _, r := range rows {
		if !eligible[r.ID] {
			continue
		}
		switch q.Kind {
		case "controller_waiting_admission":
			if e := controller(r.ID); e != nil {
				return nil, nil, e
			}
		case "delegation_waiting":
			if e := delegation(r.ID); e != nil {
				return nil, nil, e
			}
		case "session_input":
			d, ok, e := h.dispatches.UnifiedByExecution(ctx, h.cfg.InstanceID, r.Session, r.Request)
			if e != nil {
				return nil, nil, e
			}
			if ok {
				if e = controller(d.ID); e != nil {
					return nil, nil, e
				}
				continue
			}
			d2, ok, e := h.delegations.ByChildRequest(ctx, r.Session, r.Request)
			if e != nil {
				return nil, nil, e
			}
			if ok {
				if e = delegation(d2.ID); e != nil {
					return nil, nil, e
				}
			}
		}
	}
	return out, grants, nil
}
func (h *Hub) checkUnifiedQueueIdentities(fences []unifiedQueueIdentityFence) error {
	for _, f := range fences {
		if f.RoleEpoch > 0 {
			a := h.currentJarvisRole()
			if a == nil || a.Epoch != f.RoleEpoch || a.Target.Session != f.Identity.SessionID {
				return unifiedError("permission")
			}
		}
		if _, ok := h.registry.Get(f.Identity.SessionID); !ok && f.MayBeUnborn {
			continue
		}
		if e := h.unifiedTarget(f.Identity, true); e != nil {
			return e
		}
	}
	return nil
}
