package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

func (h *Hub) formalContinuationTask(ctx context.Context, parent *session.Session, args json.RawMessage) (string, error) {
	var in backend.DelegationContinuation
	if json.Unmarshal(args, &in) != nil || in.Mode != "queue" {
		return "", nil
	}
	root, round, err := h.continuitySourceAt(ctx, parent, in.DelegationID, nil)
	if err != nil {
		return "", err
	}
	_, formal, err := h.delegations.APIBackend(ctx, root.ChildSessionID)
	if err != nil || !formal {
		return "", err
	}
	return apiID(h.cfg.InstanceID, "delegation", round.LogicalID), nil
}

// Original native owner command -> original formal TaskAPI append enforcer.
// The SAME bound caller, canonical task, profile, scope and store own effects.
func (h *Hub) continueFormalOwner(ctx context.Context, caller taskapi.BoundCaller, parent *session.Session, command delegation.ContinuityCommand, request taskapi.Request, in backend.DelegationContinuation) (any, error) {
	bound, err := caller.Check(ctx)
	if err != nil {
		return nil, err
	}
	if in.InstanceID != h.cfg.InstanceID || in.Mode != "queue" || in.ActiveRequestID != "" {
		return nil, taskapi.Failure("invalid_argument", "known_none", "correct_input")
	}
	record, err := h.apiRecord(ctx, bound, command.EffectTaskID)
	if err != nil {
		return nil, err
	}
	task, err := h.apiTask(ctx, record)
	if err != nil {
		return nil, err
	}
	input, _ := json.Marshal(map[string]any{"task_id": record.TaskID, "expected_revision": task["revision"], "target": map[string]any{"instance_id": in.InstanceID, "session_id": record.SessionID, "expected_native_thread_id": in.ExpectedThreadID, "expected_config_revision": in.ExpectedConfigRevision}, "content": in.Content, "mode": "queue", "return_to_parent": true, "expected_previous_round_id": in.ExpectedPreviousID})
	inner := taskapi.Request{Type: request.Type, Version: request.Version, CorrelationID: request.CorrelationID, Operation: "append", IdempotencyKey: request.IdempotencyKey, Input: input}
	raw, _ := json.Marshal(inner)
	response := h.taskService.Execute(ctx, caller, raw)
	if !response.OK {
		return nil, response.Error
	}
	return h.recoverFormalOwner(ctx, bound, parent, command)
}

// A reserved command only queries its original canonical append receipt. It
// never regenerates a revision/input or invokes a second mutation.
func (h *Hub) recoverFormalOwner(ctx context.Context, bound taskapi.VerifiedContext, parent *session.Session, command delegation.ContinuityCommand) (any, error) {
	ns := taskapi.Namespace{Authority: h.cfg.InstanceID, StableScopeID: bound.StableScopeID, Generation: bound.NamespaceGeneration, Path: "delegation", Operation: "append", TaskID: command.EffectTaskID}
	receipt, err := h.delegations.TaskJournal().Lookup(ctx, ns, command.Key)
	if err != nil {
		return nil, taskapi.Failure("unknown_acceptance", "unknown", "lookup_original")
	}
	if receipt.TaskID != command.EffectTaskID || receipt.ScopeID != bound.StableScopeID || receipt.Generation != bound.NamespaceGeneration {
		return nil, taskapi.Failure("permission", "known_none", "lookup_original")
	}
	r, found, err := h.delegations.ByChildRequest(ctx, receipt.SessionID, receipt.RequestID)
	if err != nil || !found || r.ParentSessionID != parent.ID {
		return nil, taskapi.Failure("unknown_acceptance", "known_receipt", "lookup_original")
	}
	return h.continuityReceipt(ctx, r)
}
