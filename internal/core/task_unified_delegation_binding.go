package core

import (
	"context"

	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
)

// The empty execution thread remains the original admission expectation. Reads
// may project its first initialized Codex thread only from the immutable native
// pin AND the same original round's provider-correlated acceptance. No binding,
// grant, queue expectation or historical record is updated by this projection.
type unifiedDelegationInitialization struct {
	Native messagequeue.NativeReceipt
}

func (h *Hub) unifiedDelegationReadTarget(ctx context.Context, d delegation.Record, captured *unifiedDelegationInitialization) (jarvisTarget, *unifiedDelegationInitialization, error) {
	thread, rev, cwdIdentity, err := h.delegations.ExecutionBinding(ctx, d.ID)
	target := jarvisTarget{h.cfg.InstanceID, d.ChildSessionID, thread, rev}
	if err != nil {
		return target, nil, err
	}
	if thread != "" {
		if captured != nil {
			return target, nil, unifiedError("scope_changed")
		}
		return target, nil, nil
	}
	pin, pinRev, pinned, err := h.delegations.NativeThread(ctx, d.ChildSessionID)
	if err != nil {
		return target, nil, err
	}
	if !pinned {
		if captured != nil {
			return target, nil, unifiedError("scope_changed")
		}
		return target, nil, nil // genuinely uninitialized, never registry adoption
	}
	if pin == "" || pinRev != rev || cwdIdentity == "" {
		return target, nil, unifiedError("scope_changed")
	}
	provider, formal, err := h.delegations.APIBackend(ctx, d.ChildSessionID)
	if err != nil {
		return target, nil, err
	}
	round, found, err := h.delegations.Round(ctx, d.ID)
	if err != nil {
		return target, nil, err
	}
	// Initial formal round only. Legacy/unbound records and a later round's
	// thread are not upgraded by a read of the initial empty expectation.
	if !formal || provider != backend.Codex || !found || round.Authority != h.cfg.InstanceID || round.LogicalID != d.ID || round.Version != 1 {
		return target, nil, unifiedError("scope_changed")
	}
	child, found := h.registry.Get(d.ChildSessionID)
	if !found || h.validateDelegationChild(ctx, d, child, pin, rev, true) != nil {
		return target, nil, unifiedError("scope_changed")
	}
	if current, e := taskRootIdentity(d.Cwd); e != nil || current != cwdIdentity {
		return target, nil, unifiedError("scope_changed")
	}
	if scope, e := h.TaskWorkerScope(child); e != nil || scope == nil {
		return target, nil, unifiedError("scope_changed")
	}
	native, accepted, e := h.messageQueue.ExactNativeAcceptanceContext(ctx, d.ChildSessionID, d.ChildRequestID)
	if e != nil {
		return target, nil, e
	}
	if !accepted || native.ThreadID != pin || !h.nativeCorrelationKnown(child, pin, d.ChildRequestID, native.TurnID) {
		return target, nil, unifiedError("scope_changed")
	}
	proof := captured
	if proof == nil {
		proof = &unifiedDelegationInitialization{Native: native}
	} else if proof.Native != native {
		return target, nil, unifiedError("scope_changed")
	}
	target.ThreadID = pin
	if captured == nil {
		// The final transport disclosure runs outside any original MQ writer
		// transaction. Re-reading its canonical acceptance here also detects a
		// conflict after an await. In-transaction relation fences use that
		// same writer transaction for this canonical authority check.
		registerUnifiedDisclosure(ctx, func(ctx context.Context) error {
			native, accepted, e := h.messageQueue.ExactNativeAcceptance(d.ChildSessionID, d.ChildRequestID)
			if e != nil {
				return e
			}
			if !accepted || native != proof.Native {
				return unifiedError("scope_changed")
			}
			return nil
		})
	}
	return target, proof, nil
}
