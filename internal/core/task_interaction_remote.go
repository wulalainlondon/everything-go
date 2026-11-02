package core

import (
	"context"
	"encoding/json"
	"errors"
	ic "everything-go/contracts/task-api/interactions-v5"
	base "everything-go/contracts/task-api/v1"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	ti "everything-go/internal/taskinteraction"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"
)

type interactionPeerPayload struct {
	Action       string     `json:"action"`
	Record       *ti.Record `json:"record,omitempty"`
	ID           string     `json:"interaction_id,omitempty"`
	ParentThread string     `json:"parent_thread_id,omitempty"`
	Key          string     `json:"idempotency_key,omitempty"`
	Revision     uint64     `json:"expected_revision,omitempty"`
	Answer       string     `json:"answer,omitempty"`
	Actor        string     `json:"actor,omitempty"`
	ActorID      string     `json:"actor_id,omitempty"`
}
type interactionPeerResult struct {
	Record       ti.Record `json:"interaction"`
	CommandFound bool      `json:"command_found"`
	Accepted     bool      `json:"accepted"`
}

func (h *Hub) validateRemoteInteraction(ctx context.Context, q ti.Record) error {
	b := q.Binding
	if b.Kind != "controller" || b.ParentAuthority != h.cfg.InstanceID || b.Authority == h.cfg.InstanceID || h.dispatches == nil {
		return errors.New("interaction_remote_lineage_unavailable")
	}
	r, found, e := h.dispatches.Get(ctx, b.LogicalID)
	if e != nil || !found || r.InstanceID != b.Authority || r.ParentID != b.Parent || r.ParentThreadID != b.ParentThread || r.ParentExecutionRequestID != b.ParentRequest || r.SessionID != b.Child || r.ThreadID != b.ChildThread || r.ConfigRevision != b.ChildRevision {
		return errors.New("interaction_remote_binding_mismatch")
	}
	if b.ChildRequest != "scjob_"+remoteControllerID(h.cfg.InstanceID, r.ID) && b.ChildRequest != r.ExecutionRequestID {
		return errors.New("interaction_remote_request_mismatch")
	}
	parent, ok := h.registry.Get(b.Parent)
	if !ok || !h.controllerInScope(parent) || parent.State() == session.Closed || parent.Snapshot().Hidden || parent.ResumeID() != b.ParentThread || parent.SettingsSnapshot().ConfigRevision != b.ParentRevision {
		return errors.New("interaction_parent_changed")
	}
	grant, e := h.dispatches.Grant(ctx, b.Parent)
	if e != nil || !grant.Allows(h.cfg.InstanceID, b.Authority, b.Child) {
		return errors.New("interaction_grant_revoked")
	}
	return nil
}
func (h *Hub) syncInteractionRemote(ctx context.Context, q ti.Record) {
	if h.interactions == nil || q.Binding.Authority != h.cfg.InstanceID || q.Binding.Kind != "controller" || h.interactions.RelayVersion(ctx, q.ID) >= q.Version {
		return
	}
	public := q
	public.AckToken = ""
	public.NativePayload = nil
	var result interactionPeerResult
	e := h.controllerRemoteRequest(ctx, q.Binding.ParentAuthority, "interaction", controllerEnvelope{ParentID: q.Binding.Parent, Interaction: &interactionPeerPayload{Action: "sync", Record: &public}}, &result)
	if e == nil && result.Accepted && result.Record.ID == q.ID {
		_ = h.interactions.RelayAccepted(ctx, q.ID, q.Version)
		if q.Notification == "remote_unavailable" || q.Notification == "remote_unsupported" {
			_, _ = h.interactions.Change(ctx, q.ID, "", "", "", func(v *ti.Record) error { v.Notification = "accepted_by_remote_inbox"; v.Error = ""; return nil })
		}
		return
	}
	state, reason := "remote_unavailable", "父端問題通知尚未確認；可在來源子工作回答。"
	var refused controllerRemoteFailure
	if errors.As(e, &refused) && (refused.Status == http.StatusNotFound || refused.Status == http.StatusNotImplemented) {
		state, reason = "remote_unsupported", "父端尚未支援派工問答；請在來源子工作回答。"
	}
	_, _ = h.interactions.Change(ctx, q.ID, "", "", "", func(v *ti.Record) error { v.Notification = state; v.Error = reason; return nil })
}
func (h *Hub) remoteInteractionCommand(ctx context.Context, c taskapi.VerifiedContext, req taskapi.Request, in interactionInput, q ti.Record) (any, error) {
	if e := h.validateRemoteInteraction(ctx, q); e != nil {
		return nil, e
	}
	if in.Action == "answer" && q.NativeQuestion != "" {
		return nil, errors.New("native_human_question_use_original_reply")
	}
	actor := "parent_agent"
	if c.BindingKind == "paired_human" {
		actor = "human"
	}
	if actor == "parent_agent" && (c.SourceSessionID != q.Binding.Parent || c.SourceResumeID != q.Binding.ParentThread) {
		return nil, errors.New("interaction_parent_required")
	}
	scope := c.StableScopeID
	fresh, e := h.interactions.ReserveRemoteOnce(ctx, scope, req.IdempotencyKey, ti.Hash(in), q.ID)
	if e != nil {
		return nil, e
	}
	if outcome, found, err := h.interactions.RemoteFailure(ctx, scope, req.IdempotencyKey, q.ID); err != nil {
		return nil, err
	} else if found && outcome.State == "rejected" {
		return interactionSubmissionView(q.ID, req.IdempotencyKey, h.cfg.InstanceID, outcome), nil
	}
	action := in.Action
	// A persisted unknown request is only queried. Never resend an answer after
	// transport loss, even when a current read still says pending.
	if !fresh {
		action = "receipt"
	}
	payload := &interactionPeerPayload{Action: action, ID: q.ID, ParentThread: q.Binding.ParentThread, Key: req.IdempotencyKey, Revision: in.Revision, Answer: in.Answer, Actor: actor, ActorID: c.StableScopeID}
	var result interactionPeerResult
	e = h.controllerRemoteRequest(ctx, q.Binding.Authority, "interaction", controllerEnvelope{ParentID: q.Binding.Parent, Interaction: payload}, &result)
	if e != nil {
		outcome := classifyInteractionRemoteFailure(e, fresh)
		if fresh {
			if err := h.interactions.SaveRemoteFailure(ctx, scope, req.IdempotencyKey, q.ID, outcome); err != nil {
				return nil, err
			}
		}
		view := interactionSubmissionView(q.ID, req.IdempotencyKey, h.cfg.InstanceID, outcome)
		h.addInteractionOriginalOutcome(ctx, view, scope, req.IdempotencyKey, q.ID)
		return view, nil
	}
	if result.Record.ID != q.ID {
		return nil, errors.New("interaction_remote_binding_mismatch")
	}
	r, e := h.interactions.Import(ctx, result.Record)
	if e != nil {
		return nil, e
	}
	if result.CommandFound {
		_ = h.interactions.RemoteComplete(ctx, scope, req.IdempotencyKey)
	}
	view := interactionView(r, false)
	h.addInteractionOriginalOutcome(ctx, view, scope, req.IdempotencyKey, q.ID)
	addInteractionReceiptRoute(view, q.ID, req.IdempotencyKey, h.cfg.InstanceID)
	view["answer_submission"] = "unknown"
	if result.CommandFound {
		view["answer_submission"] = "accepted"
	}
	return view, nil
}

// Called only after existing relay signature/nonce verification. Reverse
// notification authority comes from the exact original outgoing dispatch and
// current original-parent grant, never from a new generic reverse-controller grant.
func (h *Hub) serveInteractionPeer(w http.ResponseWriter, r *http.Request, origin string, body []byte) {
	fail := func(code string, status int) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
	}
	if h.interactions == nil {
		fail("cross_instance_interactions_unsupported", 409)
		return
	}
	var envelope controllerEnvelope
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if dec.Decode(&envelope) != nil || dec.Decode(&struct{}{}) != io.EOF || envelope.Interaction == nil || envelope.ParentID == "" {
		fail("invalid_interaction_envelope", 400)
		return
	}
	in := envelope.Interaction
	ctx := r.Context()
	var result interactionPeerResult
	if in.Action == "sync" {
		if in.Record == nil || in.Record.Binding.Authority != origin || in.Record.Binding.Parent != envelope.ParentID || in.Record.Binding.ParentAuthority != h.cfg.InstanceID || in.Record.ID == "" || utf8.RuneCountInString(in.Record.Question) > 8000 {
			fail("interaction_remote_binding_mismatch", 403)
			return
		}
		if e := h.validateRemoteInteraction(ctx, *in.Record); e != nil {
			fail(e.Error(), 403)
			return
		}
		q, e := h.interactions.Import(ctx, *in.Record)
		if e != nil {
			fail("interaction_remote_conflict", 409)
			return
		}
		result = interactionPeerResult{Record: q, Accepted: true}
		h.wakeInteractions()
	} else {
		q, e := h.interactions.Get(ctx, in.ID)
		if e != nil || q.Binding.Authority != h.cfg.InstanceID || q.Binding.ParentAuthority != origin || q.Binding.Parent != envelope.ParentID || q.Binding.ParentThread != in.ParentThread {
			fail("interaction_remote_binding_mismatch", 403)
			return
		}
		if e = h.validInteraction(ctx, q); e != nil {
			fail(e.Error(), 403)
			return
		}
		if in.Actor != "human" && in.Actor != "parent_agent" {
			fail("interaction_actor_invalid", 403)
			return
		}
		if in.Actor == "parent_agent" && in.ActorID != "session:"+q.Binding.Parent {
			fail("interaction_actor_invalid", 403)
			return
		}
		if in.Actor == "human" && !strings.HasPrefix(in.ActorID, "device:") {
			fail("interaction_actor_invalid", 403)
			return
		}
		scope := "peer:" + origin + ":" + in.ActorID
		if in.Key == "" || len(in.Key) > 160 {
			fail("interaction_original_key_required", 400)
			return
		}
		switch in.Action {
		case "answer":
			if q.NativeQuestion != "" {
				fail("native_human_question_use_original_reply", 409)
				return
			}
			if !h.controls.MobileMayWrite(q.Binding.Child) {
				fail("interaction_child_desktop_owned", 409)
				return
			}
			q, e = h.interactions.Answer(ctx, q.ID, scope, in.Key, ti.Hash(struct {
				Action, Answer string
				Revision       uint64
			}{in.Action, in.Answer, in.Revision}), in.Revision, in.Actor, in.ActorID, in.Answer)
		case "escalate":
			q, e = h.interactions.Change(ctx, q.ID, scope, in.Key, ti.Hash(in.Action), func(v *ti.Record) error {
				if v.State != "pending_parent" && v.State != "pending_human" {
					return errors.New("answer_already_reserved")
				}
				v.HumanOnly = true
				v.State = "pending_human"
				return nil
			})
		case "receipt":
		default:
			fail("unsupported_interaction_action", 400)
			return
		}
		if e != nil {
			fail(e.Error(), 409)
			return
		}
		command, found, e := h.interactions.Command(ctx, scope, in.Key)
		if e != nil || found && command.ID != q.ID {
			fail("interaction_receipt_mismatch", 409)
			return
		}
		result = interactionPeerResult{Record: q, Accepted: true, CommandFound: found}
		h.publishInteraction(q)
	}
	// Full provider payloads (including authorization questions) stay on the
	// original child connection; replica/receipt reads never relay them.
	result.Record.NativePayload = nil
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (h *Hub) remoteInteractionReceipt(ctx context.Context, c taskapi.VerifiedContext, q ti.Record, key string) (any, error) {
	if e := h.validateRemoteInteraction(ctx, q); e != nil {
		return nil, e
	}
	if outcome, found, err := h.interactions.RemoteFailure(ctx, c.StableScopeID, key, q.ID); err != nil {
		return nil, err
	} else if found && outcome.State == "rejected" {
		return interactionSubmissionView(q.ID, key, h.cfg.InstanceID, outcome), nil
	}
	actor := "parent_agent"
	if c.BindingKind == "paired_human" {
		actor = "human"
	}
	var result interactionPeerResult
	e := h.controllerRemoteRequest(ctx, q.Binding.Authority, "interaction", controllerEnvelope{ParentID: q.Binding.Parent, Interaction: &interactionPeerPayload{Action: "receipt", ID: q.ID, ParentThread: q.Binding.ParentThread, Key: key, Actor: actor, ActorID: c.StableScopeID}}, &result)
	if e != nil {
		view := interactionSubmissionView(q.ID, key, h.cfg.InstanceID, classifyInteractionRemoteFailure(e, false))
		h.addInteractionOriginalOutcome(ctx, view, c.StableScopeID, key, q.ID)
		return view, nil
	}
	if result.Record.ID != q.ID {
		return nil, errors.New("interaction_remote_binding_mismatch")
	}
	updated, e := h.interactions.Import(ctx, result.Record)
	if e != nil {
		return nil, e
	}
	view := interactionView(updated, false)
	h.addInteractionOriginalOutcome(ctx, view, c.StableScopeID, key, q.ID)
	addInteractionReceiptRoute(view, q.ID, key, h.cfg.InstanceID)
	view["answer_submission"] = "unknown"
	if result.CommandFound {
		view["answer_submission"] = "accepted"
	}
	view["command_found"] = result.CommandFound
	if result.CommandFound {
		_ = h.interactions.RemoteComplete(ctx, c.StableScopeID, key)
	}
	return view, nil
}

// Only fixed service error codes are displayed. Arbitrary remote text can
// contain private data and must never enter receipts or model-visible output.
func safeInteractionRemoteCode(code string) string {
	switch code {
	case "cross_instance_interactions_unsupported", "invalid_interaction_envelope", "interaction_remote_binding_mismatch", "interaction_remote_conflict", "interaction_actor_invalid", "interaction_original_key_required", "native_human_question_use_original_reply", "interaction_child_desktop_owned", "unsupported_interaction_action", "interaction_receipt_mismatch", "interaction_child_changed", "interaction_relation_changed", "interaction_parent_changed", "interaction_grant_revoked", "interaction_revision_changed", "human_answer_required", "answer_already_reserved", "idempotency_conflict", "controller_remote_not_authorized_or_unsupported":
		return code
	default:
		return "remote_error_redacted"
	}
}
func classifyInteractionRemoteFailure(err error, originalAttempt bool) ti.RemoteFailure {
	f := ti.RemoteFailure{State: "unknown", Code: "controller_remote_unreachable"}
	var remote controllerRemoteFailure
	if !errors.As(err, &remote) {
		return f
	}
	f.Status, f.Code = remote.Status, safeInteractionRemoteCode(remote.Message)
	if !originalAttempt {
		return f
	}
	// 408/429/5xx and unrecognized 409s remain uncertain. A known conflict
	// involving an existing command is not proof of non-execution either.
	preEffect := remote.Status == http.StatusUnauthorized || remote.Status == http.StatusForbidden || remote.Status == http.StatusNotFound || remote.Status == http.StatusMethodNotAllowed
	switch remote.Message {
	case "cross_instance_interactions_unsupported", "invalid_interaction_envelope", "interaction_remote_binding_mismatch", "interaction_actor_invalid", "interaction_original_key_required", "native_human_question_use_original_reply", "interaction_child_desktop_owned", "unsupported_interaction_action", "interaction_child_changed", "interaction_relation_changed", "interaction_parent_changed", "interaction_grant_revoked", "human_answer_required":
		preEffect = preEffect || remote.Status == http.StatusConflict || remote.Status == http.StatusBadRequest
	}
	if preEffect {
		f.State = "rejected"
	}
	return f
}
func addInteractionReceiptRoute(view map[string]any, id, key, authority string) {
	view["original_key"] = key
	view["receipt_via_existing_task_tool"] = map[string]any{
		"tool": "task_get", "api_version": base.Version, "operation": "get",
		"input": map[string]any{"original_receipt": taskapi.Locator{Authority: authority, Path: "controller", Operation: "append", TaskID: "ti5:" + id, Key: key}},
	}
	view["receipt_via_interactions"] = map[string]any{"api_version": ic.Version, "operation": "interactions", "input": map[string]any{"action": "receipt", "interaction_id": id, "original_key": key}}
	view["next_action"] = "Query the original receipt with this exact key; do not repeat the mutation or mint a replacement key."
}
func interactionSubmissionView(id, key, authority string, f ti.RemoteFailure) map[string]any {
	view := map[string]any{"interaction_id": id, "answer_submission": f.State, "acceptance": "unknown"}
	if f.State == "rejected" {
		view["acceptance"] = "known_none"
	}
	if f.Status != 0 {
		view["http_status"] = f.Status
	}
	if f.Code != "" {
		view["error_code"] = f.Code
	}
	addInteractionReceiptRoute(view, id, key, authority)
	return view
}

func (h *Hub) addInteractionOriginalOutcome(ctx context.Context, view map[string]any, scope, key, id string) {
	if f, found, e := h.interactions.RemoteFailure(ctx, scope, key, id); e == nil && found {
		view["original_attempt"] = f
	}
}
