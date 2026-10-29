package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	contract "everything-go/contracts/task-api/controller-results-v9"
	cr "everything-go/internal/controllerresults"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

var controllerResultOps = []string{"capabilities", "read_dispatch_inbox", "read_dispatch_group", "read_dispatch_final", "ack_dispatch_results", "mark_dispatch_results_seen", "get_dispatch_return_status", "dispatch_result_events", "set_dispatch_return_control", "operation_get"}

type controllerResultInput struct {
	Manager       cr.Identity  `json:"manager"`
	Cursor        string       `json:"cursor"`
	Limit         int          `json:"limit"`
	Group         string       `json:"group_id"`
	ReadReceipt   string       `json:"read_receipt_id"`
	Versions      []cr.Version `json:"versions"`
	After         uint64       `json:"after_sequence"`
	Expected      uint64       `json:"expected_revision"`
	Mode          string       `json:"mode"`
	Paused        bool         `json:"paused"`
	Deadline      int64        `json:"deadline_ms"`
	MaxBatches    int          `json:"max_batches"`
	MaxNoProgress int          `json:"max_no_progress"`
	Original      string       `json:"original_intent_id"`
	Hash          string       `json:"expected_input_hash"`
}

func resultTarget(i cr.Identity) jarvisTarget {
	return jarvisTarget{i.Authority, i.Session, i.Thread, i.Revision}
}
func (h *Hub) resultScope(ctx context.Context, b taskapi.VerifiedContext, m cr.Identity) (cr.Scope, error) {
	if _, e := h.unifiedManager(b, unifiedInput{Manager: resultTarget(m)}); e != nil {
		return cr.Scope{}, e
	}
	if e := h.unifiedTarget(resultTarget(m), false); e != nil {
		return cr.Scope{}, e
	}
	if h.unifiedBoundedWorker(b) {
		return cr.Scope{}, unifiedError("permission")
	}
	g, e := h.dispatches.Grant(ctx, m.Session)
	if e != nil {
		return cr.Scope{}, e
	}
	if !g.Enabled {
		return cr.Scope{}, unifiedError("permission")
	}
	s, ok := h.registry.Get(m.Session)
	if !ok {
		return cr.Scope{}, unifiedError("scope_changed")
	}
	return cr.Scope{Parent: m, Generation: s.Snapshot().SourceRevision, GrantRevision: g.Revision}, nil
}

// Every page/group/body reference is revalidated as a whole. Revocation never
// silently removes rows and presents the remainder as a complete empty catalog.
func (h *Hub) resultDisclosure(ctx context.Context, scope cr.Scope) error {
	if e := h.unifiedTarget(resultTarget(scope.Parent), false); e != nil {
		return e
	}
	s, ok := h.registry.Get(scope.Parent.Session)
	if !ok || s.Snapshot().SourceRevision != scope.Generation {
		return unifiedError("scope_changed")
	}
	g, e := h.dispatches.Grant(ctx, scope.Parent.Session)
	if e != nil {
		return e
	}
	if !g.Enabled {
		return unifiedError("permission")
	}
	if g.Revision != scope.GrantRevision {
		return unifiedError("scope_changed")
	}
	records, e := h.dispatches.ListForResults(ctx, scope.Parent.Session)
	if e != nil {
		return e
	}
	for _, raw := range records {
		r, e := h.controllerResultOriginalParent(ctx, raw)
		if e != nil {
			continue
		}
		if r.ParentThreadID != scope.Parent.Thread || !r.ParentConfigKnown || r.ParentConfigRevision != scope.Parent.Revision {
			continue
		}
		if !g.Allows(h.cfg.InstanceID, r.InstanceID, r.SessionID) {
			return unifiedError("permission")
		}
		if r.InstanceID == h.cfg.InstanceID {
			if e = h.unifiedTarget(jarvisTarget{r.InstanceID, r.SessionID, r.ThreadID, r.ConfigRevision}, false); e != nil {
				return e
			}
		} else {
			return unifiedError("unsupported")
		}
	}
	return nil
}
func (h *Hub) executeControllerResults(ctx context.Context, caller taskapi.BoundCaller, raw []byte) (taskapi.Response, bool) {
	var req taskapi.Request
	if json.Unmarshal(raw, &req) != nil || req.Type != "task_api_controller_results_request" {
		return taskapi.Response{}, false
	}
	op := req.Operation
	resp := taskapi.Response{Authority: h.cfg.InstanceID, Type: "task_api_controller_results_response", Version: contract.Version, CorrelationID: req.CorrelationID, Operation: &op}
	fail := func(e error) (taskapi.Response, bool) {
		resp.OK = false
		resp.Result = nil
		var api *taskapi.APIError
		switch {
		case errors.Is(e, cr.ErrConflict):
			api = taskapi.Failure("stale_revision", "known_none", "refresh_snapshot")
		case errors.Is(e, cr.ErrGap):
			api = taskapi.Failure("cursor_expired", "known_none", "refresh_snapshot")
		case errors.Is(e, cr.ErrScope):
			api = taskapi.Failure("scope_changed", "known_none", "refresh_snapshot")
		case errors.Is(e, cr.ErrBudget):
			api = taskapi.Failure("permission", "known_none", "refresh_snapshot")
		default:
			if !errors.As(e, &api) {
				api = taskapi.Failure("unknown", "unknown", "lookup_original")
			}
		}
		// Keep a closed rc9 error without leaking arbitrary adapter error strings.
		allowed := map[string]bool{"invalid_argument": true, "unsupported": true, "caller_unbound": true, "wrong_authority": true, "permission": true, "scope_changed": true, "stale_revision": true, "cursor_expired": true, "not_found": true, "unknown": true}
		code := api.Code
		acceptance := api.Acceptance
		if !allowed[code] || acceptance != "known_none" && acceptance != "unknown" {
			code = "unknown"
			acceptance = "unknown"
		}
		action := "refresh_snapshot"
		if acceptance == "unknown" {
			action = "lookup_original"
		}
		resp.Error = taskapi.Failure(code, acceptance, action)
		if len(resp.CorrelationID) > 256 {
			resp.CorrelationID = ""
		}
		validOp := false
		for _, name := range controllerResultOps {
			validOp = validOp || name == op
		}
		if !validOp {
			resp.Operation = nil
		}
		return resp, true
	}
	if req.Version != contract.Version {
		return fail(unifiedError("unsupported"))
	}
	if e := contract.Validate(raw); e != nil {
		return fail(unifiedError("invalid_argument"))
	}
	if h.dispatches == nil || h.messageQueue == nil {
		return fail(unifiedError("unsupported"))
	}
	b, e := caller.Check(ctx)
	if e != nil {
		return fail(e)
	}
	if b.Authority != h.cfg.InstanceID {
		return fail(unifiedError("wrong_authority"))
	}
	role := "native_agent"
	if b.BindingKind == "paired_human" {
		role = "paired_human"
	}
	if h.unifiedBoundedWorker(b) {
		role = "bounded_worker"
	}
	if op == "capabilities" {
		allowed := []string{}
		for _, x := range controllerResultOps {
			if role == "paired_human" && x == "ack_dispatch_results" {
				continue
			}
			if role == "native_agent" && x == "mark_dispatch_results_seen" {
				continue
			}
			allowed = append(allowed, x)
		}
		if role == "bounded_worker" {
			allowed = []string{"capabilities"}
		}
		resp.OK = true
		resp.Result = map[string]any{"kind": "capabilities", "schema_hash": contract.Hash(), "authority_instance_id": h.cfg.InstanceID, "role": role, "allowed_operations": allowed, "features": map[string]bool{"session_controller_results_v2": true, "passive_inbox": true, "native_version_ack": role != "bounded_worker" && role != "paired_human", "human_seen": role == "paired_human", "aggregate_return": true, "default_notify_only": true}, "limits": map[string]int{"default_groups": 20, "max_groups": 50, "max_batch_groups": 32, "max_request_bytes": 262144, "max_response_bytes": 8388608}}
		return resp, true
	}
	var in controllerResultInput
	if json.Unmarshal(req.Input, &in) != nil {
		return fail(unifiedError("invalid_argument"))
	}
	if b.BindingKind != "paired_human" {
		native, known, err := h.messageQueue.ExactNativeAcceptanceContext(ctx, b.SourceSessionID, b.SourceRequestID)
		if err != nil || !known || native.ThreadID != b.SourceResumeID || native.TurnID != b.NativeTurnID {
			return fail(unifiedError("caller_unbound"))
		}
	}
	scope, e := h.resultScope(ctx, b, in.Manager)
	if e != nil {
		return fail(e)
	}
	source, _ := h.registry.Get(in.Manager.Session)
	captured := source.Snapshot().SourceRevision
	local := func() error {
		s, ok := h.registry.Get(in.Manager.Session)
		if !ok || s != source || s.Snapshot().SourceRevision != captured || s.ResumeID() != in.Manager.Thread || s.SettingsSnapshot().ConfigRevision != in.Manager.Revision || s.Snapshot().Hidden || s.State() == session.Closed {
			return cr.ErrScope
		}
		return nil
	}
	check := func() error {
		current, e := caller.Check(ctx)
		if e != nil {
			return e
		}
		if current != b {
			return unifiedError("caller_unbound")
		}
		if e = local(); e != nil {
			return e
		}
		return h.resultDisclosure(ctx, scope)
	}
	if e = check(); e != nil {
		return fail(e)
	}
	proof := cr.ReadProof{Principal: b.StableScopeID, Request: b.SourceRequestID, Turn: b.NativeTurnID, Native: b.BindingKind != "paired_human"}
	if !proof.Native {
		proof.Invocation = b.InvocationGeneration
	}
	store := h.dispatches.Results()
	hash := unifiedHash(req)
	var result any
	switch op {
	case "read_dispatch_inbox":
		if proof.Native {
			e = h.messageQueue.WithNativeAcceptanceRead(ctx, func(txctx context.Context) error {
				current, err := caller.Check(txctx)
				if err != nil || current != b {
					return unifiedError("caller_unbound")
				}
				native, known, err := h.messageQueue.ExactNativeAcceptanceContext(txctx, b.SourceSessionID, b.SourceRequestID)
				if err != nil || !known || native.ThreadID != b.SourceResumeID || native.TurnID != b.NativeTurnID {
					return unifiedError("caller_unbound")
				}
				result, err = store.Read(txctx, scope, proof, in.Cursor, in.Limit)
				return err
			})
		} else {
			result, e = store.Read(ctx, scope, proof, in.Cursor, in.Limit)
		}
	case "read_dispatch_group":
		result, e = store.Members(ctx, scope, in.Group, in.Cursor, in.Limit)
	case "read_dispatch_final":
		var bytesInput struct {
			Offset int    `json:"offset"`
			Limit  int    `json:"limit"`
			Seal   string `json:"seal_id"`
			Hash   string `json:"expected_hash"`
		}
		_ = json.Unmarshal(req.Input, &bytesInput)
		page, err := store.Members(ctx, scope, in.Group, "", 1)
		if err != nil {
			return fail(err)
		}
		g := page.Group
		if g.State != "final" || g.Final == nil || g.Final.SealID != bytesInput.Seal || g.Final.Hash != bytesInput.Hash {
			return fail(unifiedError("scope_changed"))
		}
		target, ok := h.registry.Get(g.Execution.Target.Session)
		if !ok {
			return fail(unifiedError("scope_changed"))
		}
		seal, err := h.jarvisResultSeal(ctx, target, g.Execution.Request, g.Final.SealID)
		if err != nil || seal.Hash != g.Final.Hash {
			return fail(unifiedError("scope_changed"))
		}
		body := []byte(seal.Text)
		offset := bytesInput.Offset
		limit := bytesInput.Limit
		if offset < 0 || offset > len(body) || limit < 1 || limit > 49152 {
			return fail(unifiedError("invalid_argument"))
		}
		end := offset + limit
		if end > len(body) {
			end = len(body)
		}
		result = map[string]any{"kind": "dispatch_result_bytes", "group_id": g.ID, "seal_id": seal.ID, "full_hash": seal.Hash, "total_bytes": len(body), "offset": offset, "next_offset": end, "has_more": end < len(body), "encoding": "base64", "data": base64.StdEncoding.EncodeToString(body[offset:end])}
	case "dispatch_result_events":
		result, e = store.Events(ctx, scope, in.After, in.Limit)
	case "get_dispatch_return_status":
		var status cr.Status
		status, e = store.Status(ctx, scope)
		if e == nil {
			c := status.Control
			active := ""
			for _, batch := range status.Batches {
				if batch.State == "unknown" {
					active = "original_receipt_unknown"
					break
				}
				if batch.State == "queued" || batch.State == "running" {
					active = "batch_inflight"
				}
				if batch.State == "frozen" && batch.ControlRevision == c.Revision && c.Issued > 0 {
					c.Issued--
				}
			}
			status.WakeReason = h.controllerResultWaitReason(ctx, scope, c, false)
			if active != "" {
				status.WakeReason = active
			}
			result = status
		}
	case "operation_get":
		var found bool
		result, found, e = store.Operation(ctx, scope, in.Original, in.Hash)
		if e == nil && !found {
			e = unifiedError("not_found")
		}
	case "ack_dispatch_results", "mark_dispatch_results_seen":
		if op == "ack_dispatch_results" && (!proof.Native || proof.Request == "" || proof.Turn == "") {
			return fail(unifiedError("permission"))
		}
		if op == "mark_dispatch_results_seen" && proof.Native {
			return fail(unifiedError("permission"))
		}
		// No MQ callback is evaluated while the dispatch SQLite connection is held.
		// The current source/grant were checked immediately before and are checked
		// again before disclosure; local generation fence runs in the writer.
		kind := "ack"
		if op == "mark_dispatch_results_seen" {
			kind = "seen"
		}
		if proof.Native {
			e = h.messageQueue.WithNativeAcceptanceRead(ctx, func(txctx context.Context) error {
				current, err := caller.Check(txctx)
				if err != nil || current != b {
					return unifiedError("caller_unbound")
				}
				native, known, err := h.messageQueue.ExactNativeAcceptanceContext(txctx, b.SourceSessionID, b.SourceRequestID)
				if err != nil || !known || native.ThreadID != b.SourceResumeID || native.TurnID != b.NativeTurnID {
					return unifiedError("caller_unbound")
				}
				result, err = store.Mark(txctx, scope, proof, in.ReadReceipt, in.Versions, req.IdempotencyKey, hash, kind, local, b.ToolCallID)
				return err
			})
		} else {
			result, e = store.Mark(ctx, scope, proof, in.ReadReceipt, in.Versions, req.IdempotencyKey, hash, kind, local)
		}
	case "set_dispatch_return_control":
		if e = h.unifiedTarget(resultTarget(in.Manager), true); e != nil {
			return fail(e)
		}
		h.messageQueueMu.Lock()
		control := cr.Control{Mode: in.Mode, Paused: in.Paused, Deadline: in.Deadline, MaxBatches: in.MaxBatches, MaxNoProgress: in.MaxNoProgress}
		if proof.Native {
			e = h.messageQueue.WithNativeAcceptanceRead(ctx, func(txctx context.Context) error {
				current, err := caller.Check(txctx)
				if err != nil || current != b {
					return unifiedError("caller_unbound")
				}
				native, known, err := h.messageQueue.ExactNativeAcceptanceContext(txctx, b.SourceSessionID, b.SourceRequestID)
				if err != nil || !known || native.ThreadID != b.SourceResumeID || native.TurnID != b.NativeTurnID {
					return unifiedError("caller_unbound")
				}
				result, err = store.SetControl(txctx, scope, in.Expected, control, req.IdempotencyKey, hash, local, cr.NativeProvenance{Source: scope.Parent, Request: proof.Request, Turn: proof.Turn, CallID: b.ToolCallID})
				return err
			})
		} else {
			result, e = store.SetControl(ctx, scope, in.Expected, control, req.IdempotencyKey, hash, local)
		}
		h.messageQueueMu.Unlock()
	default:
		return fail(unifiedError("unsupported"))
	}
	if e != nil {
		return fail(e)
	}
	if e = check(); e != nil {
		return fail(e)
	}
	resp.Result = result
	resp.OK = true
	encoded, e := json.Marshal(resp)
	if e != nil {
		return fail(e)
	}
	if e = contract.ValidateResponse(encoded); e != nil {
		return fail(e)
	}
	if op == "set_dispatch_return_control" || op == "ack_dispatch_results" || op == "mark_dispatch_results_seen" || (op == "read_dispatch_inbox" && proof.Native) {
		h.publishControllerResultInvalidation(ctx, scope)
	}
	return resp, true
}
