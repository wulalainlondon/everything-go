package core

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"strings"
)

type unifiedInputBinding struct {
	RoleEpoch        uint64                 `json:"role_epoch,omitempty"`
	InitialTarget    bool                   `json:"initial_target,omitempty"`
	GrantTarget      string                 `json:"grant_target,omitempty"`
	InputFiles       []intakeFile           `json:"input_files"`
	Uploads          []unifiedUploadBinding `json:"uploads,omitempty"`
	Source           jarvisTarget           `json:"source"`
	Target           jarvisTarget           `json:"target"`
	Principal        string                 `json:"principal"`
	Generation       uint64                 `json:"generation"`
	ControllerParent string                 `json:"controller_parent,omitempty"`
	GrantRevision    uint64                 `json:"grant_revision,omitempty"`
}

func (h *Hub) unifiedPrincipalCurrent(principal string, generation uint64) bool {
	if strings.HasPrefix(principal, "session:") {
		return generation == 0
	}
	if !strings.HasPrefix(principal, "device:") {
		return false
	}
	for _, b := range h.pairing.DeviceBindings() {
		if "device:"+b.DeviceID != principal {
			continue
		}
		sum := sha256.Sum256([]byte(b.Token))
		if binary.BigEndian.Uint64(sum[:8])&((1<<53)-1) == generation {
			return true
		}
	}
	return false
}
func (h *Hub) unifiedBindingSourcesCurrent(b unifiedInputBinding) error {
	if b.RoleEpoch > 0 {
		a := h.currentJarvisRole()
		if a == nil || a.Epoch != b.RoleEpoch || a.Target.Session != b.ControllerParent {
			return unifiedError("permission")
		}
	}
	if !h.unifiedPrincipalCurrent(b.Principal, b.Generation) {
		return unifiedError("caller_unbound")
	}
	if e := h.unifiedTarget(b.Source, true); e != nil {
		return e
	}
	target := b.Target
	if b.InitialTarget && target.ThreadID == "" {
		if _, exists := h.registry.Get(target.SessionID); !exists {
			return nil
		}
		target.ThreadID = h.unifiedIdentity(target.SessionID).ThreadID
	}
	return h.unifiedTarget(target, true)
}
func (h *Hub) unifiedBindingLocalCurrent(b unifiedInputBinding) error {
	if e := h.unifiedBindingSourcesCurrent(b); e != nil {
		return e
	}
	for _, u := range b.Uploads {
		if e := h.verifyUnifiedUpload(u); e != nil {
			return e
		}
	}
	return h.unifiedBindingSourcesCurrent(b)
}
func (h *Hub) unifiedBindingCurrent(ctx context.Context, b unifiedInputBinding) error {
	if e := h.unifiedBindingLocalCurrent(b); e != nil {
		return e
	}
	if b.ControllerParent != "" {
		g, e := h.dispatches.Grant(ctx, b.ControllerParent)
		if e != nil {
			return e
		}
		target := b.GrantTarget
		if target == "" {
			target = b.Target.SessionID
		}
		if g.Revision != b.GrantRevision || !g.Allows(h.cfg.InstanceID, b.Target.Authority, target) {
			return unifiedError("permission")
		}
	}
	// The grant read may have waited on SQLite. No further yielding operation
	// follows this final original credential and registry generation comparison.
	return h.unifiedBindingSourcesCurrent(b)
}
func (h *Hub) unifiedExecutionContext(ctx context.Context, s *session.Session, e messagequeue.Entry, p queuedPayload) (context.Context, error) {
	raw := e.EffectiveBinding
	if p.UnifiedBinding != nil {
		raw, _ = json.Marshal(p.UnifiedBinding)
	}
	if len(raw) == 0 && h.dispatches != nil {
		d, found, err := h.dispatches.UnifiedByExecution(ctx, h.cfg.InstanceID, e.SessionID, e.RequestID)
		if err != nil {
			return ctx, err
		}
		if found {
			raw, err = h.dispatches.UnifiedEffectiveBinding(ctx, d.SessionID, d.ID)
			if err != nil {
				return ctx, err
			}
		}
	}
	if len(raw) == 0 && h.delegations != nil {
		d, found, err := h.delegations.ByChildRequest(ctx, e.SessionID, e.RequestID)
		if err != nil {
			return ctx, err
		}
		if found {
			raw, err = h.delegations.UnifiedEffectiveBinding(ctx, "waiting", d.ID)
			if err != nil {
				return ctx, err
			}
		}
	}
	if len(raw) == 0 {
		return ctx, nil
	}
	var binding unifiedInputBinding
	if json.Unmarshal(raw, &binding) != nil {
		return ctx, unifiedError("scope_changed")
	}
	expectedRevision, expectedHash := e.ContentRevision, e.EffectiveHash
	guard := func() error {
		if err := h.unifiedBindingCurrent(context.Background(), binding); err != nil {
			return err
		}
		current, found, err := h.messageQueue.EffectiveInput(context.Background(), e.SessionID, e.RequestID)
		if err != nil {
			return err
		}
		if !found || current.State != messagequeue.Running || expectedRevision > 0 && current.ContentRevision != expectedRevision || expectedHash != "" && current.EffectiveHash != expectedHash {
			return unifiedError("native_boundary_crossed")
		}
		actual, ok := h.registry.Get(s.ID)
		initialMatches := binding.InitialTarget && binding.Target.ThreadID == "" && binding.Target.SessionID == s.ID && binding.Target.Revision == s.SettingsSnapshot().ConfigRevision && p.ExpectedTarget != nil && p.ExpectedTarget.ThreadID == ""
		if !ok || actual != s || !p.ExpectedTarget.matches(s) && !initialMatches {
			return unifiedError("scope_changed")
		}
		if err = h.delegationExecutionGuard(context.Background(), s, e.RequestID, true); err != nil {
			return err
		}
		return h.unifiedBindingCurrent(context.Background(), binding)
	}
	if err := guard(); err != nil {
		return ctx, err
	}
	return backend.WithTaskInputGuard(ctx, guard), nil
}
func (h *Hub) makeUnifiedInputBinding(ctx context.Context, c taskapi.VerifiedContext, manager string, target jarvisTarget, controllerParent string) ([]byte, error) {
	source := h.unifiedIdentity(manager)
	b := unifiedInputBinding{Source: source, Target: target, Principal: c.StableScopeID, Generation: c.NamespaceGeneration, ControllerParent: controllerParent}
	if controllerParent != "" {
		g, e := h.dispatches.Grant(ctx, controllerParent)
		if e != nil {
			return nil, e
		}
		if !g.Allows(h.cfg.InstanceID, target.Authority, target.SessionID) {
			return nil, unifiedError("permission")
		}
		b.GrantRevision = g.Revision
		b.RoleEpoch = h.jarvisRoleDispatchEpoch(ctx, controllerParent, target.Authority, target.SessionID)
	}
	if e := h.unifiedBindingCurrent(ctx, b); e != nil {
		return nil, e
	}
	return json.Marshal(b)
}
