package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"strings"
)

func (h *Hub) unifiedReply(ctx context.Context, c taskapi.VerifiedContext, manager string, req taskapi.Request, in unifiedInput, check func(context.Context) error) (any, error) {
	m := unifiedMutation(c, manager, req, check)
	// Original receipt first. A lost ACK never creates a successor request.
	if r, found, e := h.messageQueue.UnifiedOperation(ctx, m); e != nil {
		return nil, e
	} else if found {
		return r, nil
	}
	if h.delegations != nil {
		if r, found, e := h.delegations.UnifiedOperation(ctx, m); e != nil {
			return nil, e
		} else if found {
			return r, nil
		}
	}
	r, e := h.resolveUnified(ctx, c, manager, in.Locator)
	if e != nil {
		return nil, e
	}
	if r.Parent.SessionID == "" || r.Target.ThreadID == "" || in.ReplyTo != r.Locator.RequestID || in.ReplyHash != r.ReplyHash {
		return nil, unifiedError("scope_changed")
	}
	source, target := r.Parent, r.Target
	if in.Direction == "worker_to_parent" {
		source, target = r.Target, r.Parent
	} else if in.Direction != "parent_to_worker" {
		return nil, unifiedError("invalid_argument")
	}
	if source != in.Source || target != in.Target || source.SessionID != manager {
		return nil, unifiedError("permission")
	}
	if c.BindingKind != "paired_human" && c.SourceSessionID != source.SessionID {
		return nil, unifiedError("permission")
	}
	if e = h.unifiedTarget(source, true); e != nil {
		return nil, e
	}
	if e = h.unifiedTarget(target, true); e != nil {
		return nil, e
	}
	preparedContent, preparedImages, preparedFiles, uploadBindings, mediaErr := h.prepareUnifiedMedia(ctx, c, target, in.Content, in.Images, in.Files)
	if mediaErr != nil {
		return nil, mediaErr
	}
	delegationWriter := false
	baseCheck := m.Revalidate
	var originalGrantRevision uint64
	if r.Locator.Kind == "controller" {
		g, err := h.dispatches.Grant(ctx, r.Parent.SessionID)
		if err != nil {
			return nil, err
		}
		originalGrantRevision = g.Revision
	}
	m.Revalidate = func(ctx context.Context) error {
		if err := baseCheck(ctx); err != nil {
			return err
		}

		if delegationWriter {
			if err := h.unifiedDelegationWriterCurrent(ctx, r); err != nil {
				return err
			}
		} else if err := r.disclosureCurrent(ctx); err != nil {
			return err
		}
		if r.Locator.Kind == "controller" {
			g, err := h.dispatches.Grant(ctx, r.Parent.SessionID)
			if err != nil {
				return err
			}
			if g.Revision != originalGrantRevision || !g.Allows(h.cfg.InstanceID, r.Target.Authority, r.Target.SessionID) {
				return unifiedError("permission")
			}
		}
		if err := h.unifiedTarget(source, true); err != nil {
			return err
		}
		return h.unifiedTarget(target, true)
	}
	body, _ := json.Marshal(map[string]any{"locator": r.Locator, "direction": in.Direction, "source": source, "target": target, "reply_to_request_id": in.ReplyTo, "reply_to_hash": in.ReplyHash, "content": in.Content, "images": in.Images, "files": in.Files, "delivery": in.Delivery, "actor_kind": c.BindingKind, "human_authorization": c.BindingKind == "paired_human", "automatic_response_requested": false})
	if in.Delivery == "metadata" {
		return h.messageQueue.UnifiedReply(ctx, m, body)
	}
	if in.Delivery != "queue" {
		return nil, unifiedError("invalid_argument")
	}
	// Generic reports do not call the interaction answer API, so human-only
	// questions remain unresolved until their original typed human answer.
	bindingRaw, bindingErr := h.makeUnifiedInputBinding(ctx, c, manager, target, "")
	if bindingErr != nil {
		return nil, bindingErr
	}
	var binding unifiedInputBinding
	if json.Unmarshal(bindingRaw, &binding) != nil {
		return nil, unifiedError("invalid_argument")
	}
	if r.Locator.Kind == "controller" {
		g, e := h.dispatches.Grant(ctx, r.Parent.SessionID)
		if e != nil {
			return nil, e
		}
		binding.ControllerParent = r.Parent.SessionID
		binding.GrantTarget = r.Target.SessionID
		binding.GrantRevision = g.Revision
		binding.RoleEpoch = h.jarvisRoleDispatchEpoch(ctx, r.Parent.SessionID, r.Target.Authority, r.Target.SessionID)
	}
	binding.Uploads = uploadBindings
	binding.InputFiles = in.Files
	m.Binding, _ = json.Marshal(binding)
	if in.Direction == "parent_to_worker" && r.Locator.Kind == "delegation" {
		// Match the original provision/continuation lock order. No MQ lock may
		// precede delegationMu, and no delegation SQL callback re-enters that DB.
		h.delegationMu.Lock()
		defer h.delegationMu.Unlock()
		if e := m.Revalidate(ctx); e != nil {
			return nil, e
		}
		parent, _ := h.registry.Get(r.Parent.SessionID)
		base, round, child, e := h.continuityBase(parent, r.Locator.NativeID)
		if e != nil {
			return nil, e
		}
		if e = h.validateDelegationBindingForContinuation(ctx, base, parent, child); e != nil {
			return nil, e
		}
		latest, e := h.delegations.LatestRound(ctx, round.LogicalID)
		if e != nil {
			return nil, e
		}
		if latest.ID != round.ID {
			return nil, unifiedError("stale_revision")
		}
		id := "dgfollow_" + taskapi.Digest([]any{m.Scope, m.Generation, m.Intent})[:32]
		d := delegation.Record{ID: id, LogicalID: round.LogicalID, PreviousID: round.ID, Authority: h.cfg.InstanceID, Mode: "queue", ParentSessionID: parent.ID, OriginRequestID: c.SourceRequestID, ToolCallID: "unified-reply:" + req.IdempotencyKey, IntentHash: m.Hash, ChildSessionID: child.ID, ChildRequestID: "r_" + id, ParentRequestID: "dgreturn_" + id, ChildName: base.ChildName, Cwd: base.Cwd, Instruction: in.Content, Model: base.Model, Effort: base.Effort, Sandbox: base.Sandbox, SourceThread: base.SourceThread, SourceRevision: base.SourceRevision, ExpectedThread: base.ExpectedThread, ExpectedRevision: base.ExpectedRevision, WorkspaceIdentity: base.WorkspaceIdentity, GrantDigest: delegationGrantDigest(base, base.SourceThread, base.SourceRevision)}
		if d.OriginRequestID == "" {
			d.OriginRequestID = "paired-reply:" + req.IdempotencyKey
		}
		baseCheck := m.Revalidate
		m.Revalidate = func(ctx context.Context) error {
			if e := baseCheck(ctx); e != nil {
				return e
			}
			if e = h.unifiedTarget(source, true); e != nil {
				return e
			}
			return h.unifiedTarget(target, true)
		}
		replyPayload, _ := json.Marshal(queuedPayload{Content: preparedContent, Images: preparedImages, Files: preparedFiles})

		// Full original relation was checked outside its writer above. Within that
		// writer, immutable accepted profile and captured identity generations are
		// checked without borrowing delegation's SQL connection recursively.
		delegationWriter = true
		var receipt taskapi.MutationReceipt
		create := func(writeCtx context.Context) error {
			if e := m.Revalidate(writeCtx); e != nil {
				return e
			}
			var err error
			receipt, err = h.delegations.CreateUnifiedReply(writeCtx, m, d, body, replyPayload)
			return err
		}
		if r.InitializedDelegation != nil {
			e = h.messageQueue.WithNativeAcceptanceRead(ctx, create)
		} else {
			e = create(ctx)
		}
		if e == nil {
			h.wakeDelegations()
		}
		return receipt, e
	}
	// Parent returns and existing-controller follow-ups are ordinary inputs in
	// their original actor. The reply relation remains metadata, never a grant.
	request := "ur_" + taskapi.Digest([]any{m.Scope, m.Generation, m.Intent})[:32]
	targetSession, ok := h.registry.Get(target.SessionID)
	if !ok {
		return nil, unifiedError("scope_changed")
	}
	config := session.ConfigurationFrom(targetSession.SettingsSnapshot())
	purpose := "instruction"
	if in.Direction == "worker_to_parent" {
		purpose = "result_return"
	}
	p := queuedPayload{RoleEpoch: h.SessionRoleEpoch(targetSession), Content: preparedContent, Images: preparedImages, Files: preparedFiles, MessagePurpose: purpose, ExpectedTarget: &dispatchTargetExpectation{target.ThreadID, target.Revision}, Configuration: &config, OwnerDevice: strings.TrimPrefix(c.StableScopeID, "device:")}
	p.UnifiedBinding = &binding
	payload, _ := json.Marshal(p)
	queueCheck := m.Revalidate
	m.Revalidate = func(ctx context.Context) error {
		if e := queueCheck(ctx); e != nil {
			return e
		}
		if e := h.unifiedTarget(source, true); e != nil {
			return e
		}
		return h.unifiedTarget(target, true)
	}
	entry, created, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: target.SessionID, RequestID: request, Content: in.Content, Payload: payload, Unified: &m, UnifiedData: body})
	if e != nil {
		return nil, e
	}
	if created {
		h.submitAPINamed(targetSession, entry.RequestID)
	}
	receipt, found, e := h.messageQueue.UnifiedOperation(ctx, m)
	if e != nil {
		return nil, e
	}
	if !found {
		return nil, taskapi.Failure("unknown", "unknown", "lookup_original")
	}
	return receipt, nil
}
