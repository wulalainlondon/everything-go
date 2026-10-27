package core

import (
	"context"
	"everything-go/internal/messagequeue"
	"everything-go/internal/taskapi"
	"strings"
	"time"
)

type unifiedDocumentGrantBinding struct {
	SourceGenerations map[string]uint64 `json:"source_generations"`
	Consumer          jarvisTarget      `json:"consumer"`
	ConsumerPrincipal string            `json:"consumer_principal"`
	Owner             jarvisTarget      `json:"owner"`
	OriginalTarget    jarvisTarget      `json:"original_target"`
	OwnerPrincipal    string            `json:"owner_principal"`
	OwnerGeneration   uint64            `json:"owner_generation"`
	ControllerParent  string            `json:"controller_parent,omitempty"`
	GrantRevision     uint64            `json:"grant_revision,omitempty"`
}

func (h *Hub) checkUnifiedDocumentGrant(ctx context.Context, c taskapi.VerifiedContext, manager string, g messagequeue.UnifiedDocumentGrant, b unifiedDocumentGrantBinding) error {
	if g.Revoked || g.Expires <= time.Now().UnixMilli() || b.Consumer.SessionID != manager || b.ConsumerPrincipal != "session:"+b.Consumer.SessionID {
		return unifiedError("permission")
	}
	ownerHuman := c.BindingKind == "paired_human" && c.StableScopeID == b.OwnerPrincipal && c.NamespaceGeneration == b.OwnerGeneration
	ownerNative := c.StableScopeID == b.OwnerPrincipal && c.NamespaceGeneration == b.OwnerGeneration && c.SourceSessionID == b.Owner.SessionID && c.SourceResumeID == b.Owner.ThreadID && c.SourceConfigRevision == b.Owner.Revision
	consumerNative := c.StableScopeID == b.ConsumerPrincipal && c.SourceSessionID == manager
	if !ownerHuman && !ownerNative && !consumerNative {
		return unifiedError("permission")
	}
	if !h.unifiedPrincipalCurrent(b.OwnerPrincipal, b.OwnerGeneration) {
		return unifiedError("permission")
	}
	local := func() error {
		if !h.unifiedPrincipalCurrent(b.OwnerPrincipal, b.OwnerGeneration) {
			return unifiedError("permission")
		}
		for _, target := range []jarvisTarget{b.Owner, b.OriginalTarget, b.Consumer} {
			if e := h.unifiedTarget(target, false); e != nil {
				return e
			}
		}
		for id, generation := range b.SourceGenerations {
			s, ok := h.registry.Get(id)
			if !ok || s.SettingsSnapshot().SourceRevision != generation {
				return unifiedError("scope_changed")
			}
		}
		return nil
	}
	if err := local(); err != nil {
		return err
	}

	if b.ControllerParent != "" {
		grant, e := h.dispatches.Grant(ctx, b.ControllerParent)
		if e != nil {
			return e
		}
		if grant.Revision != b.GrantRevision || !grant.Allows(h.cfg.InstanceID, b.OriginalTarget.Authority, b.OriginalTarget.SessionID) {
			return unifiedError("permission")
		}
	}
	parts := strings.Split(g.OwnerScope, "\x00")
	if len(parts) != 2 || parts[0] != b.OwnerPrincipal || parts[1] != b.Owner.SessionID || g.OwnerGeneration != b.OwnerGeneration {
		return unifiedError("permission")
	}
	return local()
}

// Consumption does not reuse a native owner's issuance authority as a consumer
// identity. The owner can read through its own exact document route instead.
func (h *Hub) checkUnifiedDocumentConsumption(ctx context.Context, c taskapi.VerifiedContext, manager string, g messagequeue.UnifiedDocumentGrant, b unifiedDocumentGrantBinding) error {
	ownerHuman := c.BindingKind == "paired_human" && c.StableScopeID == b.OwnerPrincipal && c.NamespaceGeneration == b.OwnerGeneration
	consumerNative := c.StableScopeID == b.ConsumerPrincipal && c.SourceSessionID == manager
	if !ownerHuman && !consumerNative {
		return unifiedError("permission")
	}
	return h.checkUnifiedDocumentGrant(ctx, c, manager, g, b)
}
func (h *Hub) newUnifiedDocumentGrantBinding(ctx context.Context, c taskapi.VerifiedContext, manager string, resolved unifiedResolved, consumer jarvisTarget) (unifiedDocumentGrantBinding, error) {
	b := unifiedDocumentGrantBinding{Consumer: consumer, ConsumerPrincipal: "session:" + consumer.SessionID, Owner: h.unifiedIdentity(manager), OriginalTarget: resolved.Target, OwnerPrincipal: c.StableScopeID, OwnerGeneration: c.NamespaceGeneration}
	b.SourceGenerations = map[string]uint64{}
	for _, identity := range []jarvisTarget{b.Owner, b.OriginalTarget, b.Consumer} {
		s, ok := h.registry.Get(identity.SessionID)
		if !ok {
			return b, unifiedError("scope_changed")
		}
		b.SourceGenerations[identity.SessionID] = s.SettingsSnapshot().SourceRevision
	}
	if resolved.Locator.Kind == "controller" {
		b.ControllerParent = resolved.Parent.SessionID
		grant, e := h.dispatches.Grant(ctx, b.ControllerParent)
		if e != nil {
			return b, e
		}
		b.GrantRevision = grant.Revision
	}
	return b, nil
}
