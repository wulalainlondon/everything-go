package core

import (
	"context"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

type unifiedDisclosureCollectorKey struct{}

func registerUnifiedDisclosure(ctx context.Context, check func(context.Context) error) {
	if list, ok := ctx.Value(unifiedDisclosureCollectorKey{}).(*[]func(context.Context) error); ok {
		*list = append(*list, check)
	}
}

type unifiedReadIdentity struct {
	identity   jarvisTarget
	session    *session.Session
	generation uint64
}

// Capture the ORIGINAL relation before a result/queue store await. The closure
// performs only authorization/identity reads and is called after the final data
// read; it must not re-read that data and reopen the same authorize-before-await
// window. A config/thread ABA is detected by the original SourceRevision.
func (h *Hub) unifiedDisclosureFence(ctx context.Context, c taskapi.VerifiedContext, manager string, r unifiedResolved) (func(context.Context) error, func() error, error) {
	identities := []unifiedReadIdentity{}
	for _, identity := range []jarvisTarget{h.unifiedIdentity(manager), r.Parent, r.Target} {
		if identity.SessionID == "" {
			continue
		}
		s, ok := h.registry.Get(identity.SessionID)
		if !ok {
			if identity == r.Target && r.Locator.Kind == "delegation" && identity.ThreadID == "" {
				continue
			}
			return nil, nil, unifiedError("scope_changed")
		}
		if e := h.unifiedTarget(identity, false); e != nil {
			return nil, nil, e
		}
		identities = append(identities, unifiedReadIdentity{identity, s, s.SettingsSnapshot().SourceRevision})
	}
	checkLocal := func() error {
		for _, saved := range identities {
			current, ok := h.registry.Get(saved.identity.SessionID)
			if !ok || current != saved.session || current.SettingsSnapshot().SourceRevision != saved.generation {
				return unifiedError("scope_changed")
			}
			if e := h.unifiedTarget(saved.identity, false); e != nil {
				return e
			}
		}
		return nil
	}
	fence := func(ctx context.Context) error {
		if e := checkLocal(); e != nil {
			return e
		}
		if r.Canonical != nil {
			if e := h.checkAPIRecord(ctx, c, *r.Canonical); e != nil {
				return e
			}
		}
		switch r.Locator.Kind {
		case "controller":
			d, ok, e := h.dispatches.Get(ctx, r.Locator.NativeID)
			if e != nil {
				return e
			}
			if !ok || d.ParentID != r.Parent.SessionID || d.ParentThreadID != r.Parent.ThreadID || d.ParentConfigKnown && d.ParentConfigRevision != r.Parent.Revision || d.InstanceID != r.Target.Authority || d.SessionID != r.Target.SessionID || d.ThreadID != r.Target.ThreadID || d.ConfigRevision != r.Target.Revision || d.RequestID != r.Locator.RequestID {
				return unifiedError("scope_changed")
			}
			g, e := h.dispatches.Grant(ctx, d.ParentID)
			if e != nil {
				return e
			}
			if g.Revision != r.ControllerGrantRevision || !g.Allows(h.cfg.InstanceID, d.InstanceID, d.SessionID) && !h.jarvisRoleOriginalReceiptReadable(d) {
				return unifiedError("permission")
			}
		case "delegation":
			d, ok, e := h.delegations.ByID(ctx, r.Locator.NativeID)
			if e != nil {
				return e
			}
			if !ok || d.ParentSessionID != r.Parent.SessionID || d.ChildSessionID != r.Target.SessionID || d.ChildRequestID != r.Locator.RequestID {
				return unifiedError("scope_changed")
			}
			thread, rev, bound, e := h.delegations.SourceBinding(ctx, d.ID)
			if e != nil {
				return e
			}
			if !bound || thread != r.Parent.ThreadID || rev != r.Parent.Revision {
				return unifiedError("scope_changed")
			}
			target, _, e := h.unifiedDelegationReadTarget(ctx, d, r.InitializedDelegation)
			if e != nil {
				return e
			}
			if target != r.Target {
				return unifiedError("scope_changed")
			}
		}
		return checkLocal()
	}
	registerUnifiedDisclosure(ctx, fence)
	return fence, checkLocal, nil
}
func (r unifiedResolved) disclosureCurrent(ctx context.Context) error {
	if r.ReadFence == nil {
		return taskapi.Failure("scope_changed", "known_none", "refresh_snapshot")
	}
	return r.ReadFence(ctx)
}
