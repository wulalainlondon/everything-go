package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/delegation"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

// Paired human continuity uses its own verified source and never invents a
// provider turn/tool call. It reuses the original owner/round/QA store methods.
func (h *Hub) unifiedHumanQA(ctx context.Context, caller taskapi.BoundCaller, req taskapi.Request, in unifiedInput, original taskapi.Request) (any, error) {
	c, e := caller.Check(ctx)
	if e != nil {
		return nil, e
	}
	if c.BindingKind != "paired_human" {
		return nil, unifiedError("permission")
	}
	manager, e := h.unifiedManager(c, in)
	if e != nil {
		return nil, e
	}
	parent, ok := h.registry.Get(manager)
	if !ok {
		return nil, unifiedError("permission")
	}
	check := func() error {
		current, e := caller.Check(ctx)
		if e != nil {
			return e
		}
		if current != c {
			return unifiedError("caller_unbound")
		}
		return h.unifiedTarget(in.Manager, true)
	}
	var payload struct {
		Continuity struct {
			Action    string          `json:"action"`
			Arguments json.RawMessage `json:"arguments"`
		} `json:"continuity"`
	}
	if json.Unmarshal(original.Input, &payload) != nil {
		return nil, unifiedError("invalid_argument")
	}
	var args map[string]string
	if json.Unmarshal(payload.Continuity.Arguments, &args) != nil {
		return nil, unifiedError("invalid_argument")
	}
	if payload.Continuity.Action != "bind_qa" && payload.Continuity.Action != "record_review" {
		return nil, unifiedError("unsupported")
	}
	if e = check(); e != nil {
		return nil, e
	}
	digest, _ := taskapi.IntentHash(original)
	command := delegation.ContinuityCommand{Parent: manager, Key: original.IdempotencyKey, Hash: digest, Action: payload.Continuity.Action, Request: "paired:" + c.StableScopeID, Call: "unified:" + original.IdempotencyKey}
	fresh, e := h.delegations.ReserveContinuityCommand(ctx, command)
	if e != nil {
		return nil, e
	}
	if !fresh {
		old, found, e := h.delegations.ContinuityCommand(ctx, manager, command.Key)
		if e != nil {
			return nil, e
		}
		if !found || old.Hash != digest {
			return nil, unifiedError("idempotency_conflict")
		}
		if old.State != "completed" {
			return nil, taskapi.Failure("unknown", "unknown", "lookup_original")
		}
		var data any
		e = json.Unmarshal(old.Result, &data)
		return data, e
	}
	var data any
	if payload.Continuity.Action == "bind_qa" {
		data, e = h.bindDelegationQAChecked(parent, args["owner_delegation_id"], args["qa_delegation_id"], check)
	} else {
		data, e = h.recordDelegationReviewChecked(parent, args["owner_round_id"], args["qa_round_id"], check)
	}
	if e != nil {
		return nil, e
	}
	raw, _ := json.Marshal(data)
	if e = check(); e != nil {
		return nil, e
	}
	if e = h.delegations.FinishContinuityCommand(ctx, command, "completed", raw, ""); e != nil {
		return nil, e
	}
	return data, nil
}
func (h *Hub) unifiedContinuityRead(ctx context.Context, c taskapi.VerifiedContext, manager string, original taskapi.Request) (any, bool, error) {
	var p struct {
		Continuity *struct {
			Action string `json:"action"`
		} `json:"continuity"`
	}
	if json.Unmarshal(original.Input, &p) != nil || p.Continuity == nil {
		return nil, false, nil
	}
	command, found, e := h.delegations.ContinuityCommand(ctx, manager, original.IdempotencyKey)
	if e != nil {
		return nil, true, e
	}
	digest, _ := taskapi.IntentHash(original)
	if !found || command.Hash != digest || command.State != "completed" {
		return nil, true, taskapi.Failure("unknown", "unknown", "lookup_original")
	}
	parent, ok := h.registry.Get(manager)
	if !ok || parent.State() == session.Closed {
		return nil, true, unifiedError("scope_changed")
	}
	var data any
	e = json.Unmarshal(command.Result, &data)
	return data, true, e
}
