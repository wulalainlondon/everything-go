package core

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

func (h *Hub) unifiedQueueView(ctx context.Context, q unifiedQueue, eligible map[string]bool) (taskapi.QueueView, error) {
	switch q.Kind {
	case "session_input":
		return h.messageQueue.UnifiedQueue(ctx, q.Target.SessionID, eligible)
	case "delegation_waiting":
		if h.delegations != nil {
			return h.delegations.UnifiedQueue(ctx, eligible)
		}
	case "controller_waiting_admission":
		if h.dispatches != nil {
			return h.dispatches.UnifiedQueue(ctx, q.Target.SessionID, eligible)
		}
	}
	return taskapi.QueueView{}, unifiedError("unsupported")
}
func (h *Hub) unifiedRole(ctx context.Context, manager string, target string) error {
	if h.dispatches == nil {
		return unifiedError("unsupported")
	}
	g, e := h.dispatches.Grant(ctx, manager)
	if e != nil {
		return e
	}
	if !g.Enabled || !g.Allows(h.cfg.InstanceID, h.cfg.InstanceID, target) {
		return unifiedError("permission")
	}
	return h.unifiedTarget(h.unifiedIdentity(manager), true)
}

// A queue's opaque barriers still disclose membership. Require the original
// dispatch role before reading any rows, and pin its revision across every await.
// The delegation capacity board is global: its selector is the manager, while
// each row retains the original target grant check in unifiedEligible.
func (h *Hub) unifiedQueueReadRole(ctx context.Context, manager string, q unifiedQueue) (uint64, error) {
	if h.dispatches == nil {
		return 0, unifiedError("unsupported")
	}
	g, e := h.dispatches.Grant(ctx, manager)
	if e != nil {
		return 0, e
	}
	allowed := g.Allows(h.cfg.InstanceID, q.Target.Authority, q.Target.SessionID)
	if q.Kind == "delegation_waiting" {
		allowed = q.Target.SessionID == manager && g.AllowsInstance(h.cfg.InstanceID, h.cfg.InstanceID)
	}
	if !allowed {
		return 0, unifiedError("permission")
	}
	if e := h.unifiedTarget(h.unifiedIdentity(manager), true); e != nil {
		return 0, e
	}
	return g.Revision, nil
}
func (h *Hub) unifiedEligible(ctx context.Context, manager string, q unifiedQueue, rows []taskapi.QueueRow) (map[string]bool, error) {
	out := map[string]bool{}
	for _, r := range rows {
		if r.State != "queued" {
			continue
		}
		if e := h.unifiedRole(ctx, manager, r.Session); e != nil {
			var api *taskapi.APIError
			if errors.As(e, &api) {
				continue
			}
			return nil, e
		}
		switch q.Kind {
		case "delegation_waiting":
			d, ok, e := h.delegations.ByID(ctx, r.ID)
			if e != nil {
				return nil, e
			}
			if ok && d.ChildSessionID == r.Session && d.ChildRequestID == r.Request && d.Mode != "steer" {
				thread, rev, bound, e := h.delegations.SourceBinding(ctx, d.ID)
				if e != nil {
					return nil, e
				}
				source := jarvisTarget{h.cfg.InstanceID, d.ParentSessionID, thread, rev}
				if !bound || h.unifiedTarget(source, false) != nil {
					continue
				}
				if child, exists := h.registry.Get(d.ChildSessionID); exists {
					thread, rev, _, err := h.delegations.ExecutionBinding(ctx, d.ID)
					if err != nil {
						return nil, err
					}
					if h.unifiedTarget(jarvisTarget{h.cfg.InstanceID, child.ID, thread, rev}, true) != nil {
						continue
					}
				}
				out[r.ID] = true
			}
		case "controller_waiting_admission":
			d, ok, e := h.dispatches.Get(ctx, r.ID)
			if e != nil {
				return nil, e
			}
			if ok && (d.State == "prepared" || d.State == "pending") && d.SessionID == r.Session && d.RequestID == r.Request && d.Mode == "queue" && d.VoiceID == "" && d.InstanceID == h.cfg.InstanceID {
				if !d.ParentConfigKnown || h.unifiedTarget(jarvisTarget{h.cfg.InstanceID, d.ParentID, d.ParentThreadID, d.ParentConfigRevision}, false) != nil || h.unifiedTarget(jarvisTarget{h.cfg.InstanceID, d.SessionID, d.ThreadID, d.ConfigRevision}, true) != nil {
					continue
				}
				originalGrant, err := h.dispatches.Grant(ctx, d.ParentID)
				if err != nil {
					return nil, err
				}
				if !originalGrant.Allows(h.cfg.InstanceID, d.InstanceID, d.SessionID) {
					continue
				}
				out[r.ID] = true
			}
		case "session_input":
			entry, ok, e := h.messageQueue.Get(r.Session, r.Request)
			if e != nil {
				return nil, e
			}
			if !ok {
				continue
			}
			if _, accepted, e := h.messageQueue.ExactNativeAcceptance(r.Session, r.Request); e != nil {
				return nil, e
			} else if accepted {
				continue
			}
			var p queuedPayload
			if json.Unmarshal(entry.Payload, &p) != nil {
				continue
			}
			if p.VoiceContext || p.MessagePurpose == "result_return" || p.MessagePurpose == "control" || p.MessagePurpose == "question_reply" || p.ExternalPeer != "" || p.ExternalReviewOnly || p.WorkspaceTask != nil {
				continue
			}
			delegated, e := h.delegations.HasChildReceipt(ctx, r.Session, r.Request)
			if e != nil {
				return nil, e
			}
			controlled, e := h.dispatches.HasTargetReceipt(ctx, h.cfg.InstanceID, r.Session, r.Request)
			if e != nil {
				return nil, e
			}
			if delegated {
				d, found, err := h.delegations.ByChildRequest(ctx, r.Session, r.Request)
				if err != nil {
					return nil, err
				}
				if found {
					thread, rev, bound, err := h.delegations.SourceBinding(ctx, d.ID)
					if err != nil {
						return nil, err
					}
					if bound && h.unifiedTarget(jarvisTarget{h.cfg.InstanceID, d.ParentSessionID, thread, rev}, false) == nil && h.unifiedTarget(h.unifiedIdentity(r.Session), true) == nil && p.ExpectedTarget != nil && p.ExpectedTarget.ThreadID == h.unifiedIdentity(r.Session).ThreadID && p.ExpectedTarget.Revision == h.unifiedIdentity(r.Session).Revision {
						out[r.ID] = true
					}
				}
				continue
			}
			if controlled {
				d, found, err := h.dispatches.UnifiedByExecution(ctx, h.cfg.InstanceID, r.Session, r.Request)
				if err != nil {
					return nil, err
				}
				if found && d.ParentConfigKnown && d.Mode == "queue" && d.VoiceID == "" && h.unifiedTarget(jarvisTarget{h.cfg.InstanceID, d.ParentID, d.ParentThreadID, d.ParentConfigRevision}, false) == nil && h.unifiedTarget(jarvisTarget{h.cfg.InstanceID, d.SessionID, d.ThreadID, d.ConfigRevision}, true) == nil {
					g, err := h.dispatches.Grant(ctx, d.ParentID)
					if err != nil {
						return nil, err
					}
					out[r.ID] = g.Allows(h.cfg.InstanceID, d.InstanceID, d.SessionID)
				}
				continue
			}
			links, e := h.messageQueue.LegacySourceLinks(ctx)
			if e != nil {
				return nil, e
			}
			for _, l := range links {
				if l.Source.SessionID == manager && l.Target.SessionID == r.Session && l.RequestID == r.Request && !l.Revoked && h.legacyLinkVisible(l) {
					out[r.ID] = true
				}
			}
		}
	}
	return out, nil
}
func (h *Hub) unifiedQueueOperation(ctx context.Context, c taskapi.VerifiedContext, manager string, req taskapi.Request, in unifiedInput, check func(context.Context) error) (any, error) {
	q := in.Queue
	var resolved unifiedResolved
	var e error
	if req.Operation == "queue_edit" {
		resolved, e = h.resolveUnified(ctx, c, manager, in.Locator)
		if e != nil {
			return nil, e
		}
		if !resolved.CanEdit {
			return nil, unifiedError("permission")
		}
		if resolved.HasEntry {
			q = unifiedQueue{"session_input", resolved.Target}
			in.Item = resolved.Locator.RequestID
		} else if resolved.Locator.Kind == "delegation" {
			q = unifiedQueue{"delegation_waiting", h.unifiedIdentity(manager)}
			in.Item = resolved.Locator.NativeID
		} else if resolved.Locator.Kind == "controller" {
			q = unifiedQueue{"controller_waiting_admission", resolved.Target}
			in.Item = resolved.Locator.NativeID
		} else {
			return nil, unifiedError("unsupported")
		}
	}
	if req.Operation == "operation_get" && q.Kind == "" {
		return h.unifiedGeneralOperation(ctx, c, manager, in, check)
	}
	if e = h.unifiedTarget(q.Target, req.Operation != "queue_snapshot" && req.Operation != "operation_get"); e != nil {
		return nil, e
	}
	if q.Kind == "delegation_waiting" && q.Target.SessionID != manager {
		return nil, unifiedError("permission")
	}
	m := taskapi.QueueMutation{Scope: c.StableScopeID + "\x00" + manager, Generation: c.NamespaceGeneration, Intent: req.IdempotencyKey, Hash: unifiedHash(req), Operation: req.Operation, Item: in.Item, Relative: in.Relative, Placement: in.Placement, MembershipHash: in.Membership, ExpectedRevision: in.ExpectedQueue, ExpectedContentRevision: in.ExpectedContent, ExpectedHash: in.ExpectedHash, Content: in.Content, Revalidate: check}
	if req.Operation == "operation_get" {
		m.Operation = in.OriginalOperation
		m.Intent = in.OriginalKey
		m.Hash = ""
		var r taskapi.MutationReceipt
		var found bool
		switch q.Kind {
		case "session_input":
			r, found, e = h.messageQueue.UnifiedOperation(ctx, m)
		case "delegation_waiting":
			r, found, e = h.delegations.UnifiedOperation(ctx, m)
		case "controller_waiting_admission":
			r, found, e = h.dispatches.UnifiedOperation(ctx, m)
		default:
			e = unifiedError("unsupported")
		}
		if e != nil {
			return nil, e
		}
		if !found {
			return nil, unifiedError("not_found")
		}
		return r, nil
	}
	if req.IdempotencyKey != "" {
		var old taskapi.MutationReceipt
		var found bool
		switch q.Kind {
		case "session_input":
			old, found, e = h.messageQueue.UnifiedOperation(ctx, m)
		case "delegation_waiting":
			old, found, e = h.delegations.UnifiedOperation(ctx, m)
		case "controller_waiting_admission":
			old, found, e = h.dispatches.UnifiedOperation(ctx, m)
		}
		if e != nil {
			return nil, e
		}
		if found {
			return old, nil
		}
	}
	// Hold the original actor before any queue metadata can be changed. An actor
	// already removed from its mailbox is a boundary, never mutable waiting work.
	var s *session.Session
	var release func()
	var actorIDs []string
	if q.Kind == "session_input" && req.Operation != "queue_snapshot" {
		var ok bool
		s, ok = h.registry.Get(q.Target.SessionID)
		if !ok {
			return nil, unifiedError("scope_changed")
		}
		h.messageQueueMu.Lock()
		defer h.messageQueueMu.Unlock()
		release = s.HoldQueue()
		defer release()
		actorIDs = s.WaitingIDs()
	}
	roleRevision := uint64(0)
	var readRoleFence func(context.Context) error
	if req.Operation == "queue_snapshot" {
		roleRevision, e = h.unifiedQueueReadRole(ctx, manager, q)
		if e != nil {
			return nil, e
		}
		saved := []unifiedReadIdentity{}
		for _, identity := range []jarvisTarget{h.unifiedIdentity(manager), q.Target} {
			current, ok := h.registry.Get(identity.SessionID)
			if !ok {
				return nil, unifiedError("scope_changed")
			}
			saved = append(saved, unifiedReadIdentity{identity, current, current.SettingsSnapshot().SourceRevision})
		}
		readRoleFence = func(ctx context.Context) error {
			checkLocal := func() error {
				for _, original := range saved {
					current, ok := h.registry.Get(original.identity.SessionID)
					if !ok || current != original.session || current.SettingsSnapshot().SourceRevision != original.generation {
						return unifiedError("scope_changed")
					}
					if err := h.unifiedTarget(original.identity, false); err != nil {
						return err
					}
				}
				return nil
			}
			if err := checkLocal(); err != nil {
				return err
			}
			revision, err := h.unifiedQueueReadRole(ctx, manager, q)
			if err != nil {
				return err
			}
			if revision != roleRevision {
				return unifiedError("scope_changed")
			}
			return checkLocal()
		}
		registerUnifiedDisclosure(ctx, readRoleFence)
	}
	v, e := h.unifiedQueueView(ctx, q, nil)
	if e != nil {
		return nil, e
	}
	if len(v.Items) > 4096 {
		return nil, unifiedError("unsupported")
	}
	eligible, e := h.unifiedEligible(ctx, manager, q, v.Items)
	if e != nil {
		return nil, e
	}
	v, e = h.unifiedQueueView(ctx, q, eligible)
	if e != nil {
		return nil, e
	}
	if req.Operation != "queue_snapshot" && h.dispatches != nil {
		g, grantErr := h.dispatches.Grant(ctx, manager)
		if grantErr != nil {
			return nil, grantErr
		}
		roleRevision = g.Revision
	}
	scopedMembership := taskapi.Digest(struct {
		Membership   string
		RoleRevision uint64
		Scope        string
		Generation   uint64
		Manager      string
	}{v.MembershipHash, roleRevision, c.StableScopeID, c.NamespaceGeneration, manager})
	publicHandles := map[string]string{}
	readableRows := map[string]unifiedResolved{}
	scopeHandle := c.StableScopeID + "\x00" + manager + "\x00" + taskapi.Digest(c.NamespaceGeneration)
	for _, row := range v.Items {
		var locator unifiedLocator
		switch q.Kind {
		case "session_input":
			locator = unifiedLocator{Kind: "request", Authority: h.cfg.InstanceID, SessionID: row.Session, RequestID: row.Request}
		case "delegation_waiting":
			locator = unifiedLocator{Kind: "delegation", Authority: h.cfg.InstanceID, NativeID: row.ID}
		case "controller_waiting_admission":
			locator = unifiedLocator{Kind: "controller", Authority: h.cfg.InstanceID, NativeID: row.ID}
		}
		resolved, err := h.resolveUnified(ctx, c, manager, locator)
		if err == nil {
			readableRows[row.ID] = resolved
			publicHandles[row.ID] = row.ID
		} else {
			publicHandles[row.ID] = h.taskCursor.OpaqueHandle(scopeHandle, q.Kind+"\x00"+row.ID)
		}
	}
	scopedMembership = taskapi.Digest([]any{scopedMembership, h.taskCursor.OpaqueHandle(scopeHandle, v.MembershipHash)})
	if req.Operation == "queue_snapshot" {
		items := []any{}
		for _, r := range v.Items {
			resolved, canRead := readableRows[r.ID]

			classification := "barrier"
			if r.Eligible {
				classification = "eligible_dispatched_job"
			}
			canEdit := canRead && resolved.CanEdit && r.State == "queued"
			if q.Kind == "controller_waiting_admission" && !r.Eligible {
				canEdit = false
			}
			row := map[string]any{"item_id": publicHandles[r.ID], "classification": classification, "barrier_reason": r.Barrier, "content_revision": r.ContentRevision, "content_hash": r.ContentHash, "can_edit": canEdit, "can_move": r.Eligible, "state": r.State}
			if !canRead {
				row["content_revision"] = 0
				row["content_hash"] = ""
			}
			if canRead {
				row["locator"] = resolved.Locator
			}
			items = append(items, row)
		}
		for _, resolved := range readableRows {
			if err := resolved.disclosureCurrent(ctx); err != nil {
				return nil, err
			}
		}
		if err := readRoleFence(ctx); err != nil {
			return nil, err
		}
		return map[string]any{"queue": q, "revision": v.Revision, "membership_hash": scopedMembership, "items": items, "coverage": "complete", "order_owner": "original_store_actor"}, nil
	}
	if req.Operation == "queue_move" {
		if in.Membership != scopedMembership {
			return nil, unifiedError("stale_revision")
		}
		resolveHandle := func(handle string) (string, bool) {
			for id, public := range publicHandles {
				if public == handle {
					return id, true
				}
			}
			return "", false
		}
		var ok bool
		m.Item, ok = resolveHandle(in.Item)
		if !ok {
			return nil, unifiedError("stale_revision")
		}
		m.Relative, ok = resolveHandle(in.Relative)
		if !ok {
			return nil, unifiedError("stale_revision")
		}
		m.PublicHandles = publicHandles
		m.MembershipHash = v.MembershipHash
		m.RoleSession = manager
		m.RoleRevision = roleRevision
		if q.Kind != "delegation_waiting" {
			if e = h.unifiedRole(ctx, manager, q.Target.SessionID); e != nil {
				return nil, e
			}
		}
		if s != nil {
			pending := []string{}
			for _, r := range v.Items {
				if r.State == "queued" {
					pending = append(pending, r.ID)
				}
			}
			if taskapi.Digest(pending) != taskapi.Digest(actorIDs) {
				return nil, unifiedError("native_boundary_crossed")
			}
		}
	}
	if req.Operation == "queue_edit" {
		controllerParent := ""
		if resolved.Locator.Kind == "controller" {
			controllerParent = resolved.Parent.SessionID
		}
		var binding []byte
		var bindingErr error
		_, targetExists := h.registry.Get(resolved.Target.SessionID)
		if resolved.Locator.Kind == "delegation" && !targetExists && resolved.Target.ThreadID == "" && in.Images == nil && in.Files == nil {
			b := unifiedInputBinding{Source: h.unifiedIdentity(manager), Target: resolved.Target, Principal: c.StableScopeID, Generation: c.NamespaceGeneration, InitialTarget: true}
			bindingErr = h.unifiedBindingLocalCurrent(b)
			binding, _ = json.Marshal(b)
		} else {
			binding, bindingErr = h.makeUnifiedInputBinding(ctx, c, manager, resolved.Target, controllerParent)
		}
		if bindingErr != nil {
			return nil, bindingErr
		}
		m.Binding = binding
		if s != nil {
			found := false
			for _, id := range actorIDs {
				if id == in.Item {
					found = true
				}
			}
			if !found {
				return nil, unifiedError("native_boundary_crossed")
			}
		}
		if resolved.HasEntry {
			var p queuedPayload
			if json.Unmarshal(resolved.Entry.Payload, &p) != nil {
				return nil, unifiedError("invalid_argument")
			}
			prepared, uploads, changed, mediaErr := h.prepareUnifiedEditMedia(ctx, c, resolved, in, p)
			if mediaErr != nil {
				return nil, mediaErr
			}
			m.AllowAttachments = changed
			m.Payload, _ = json.Marshal(prepared)
			m.Content = in.Content
			if len(uploads) > 0 {
				var b unifiedInputBinding
				_ = json.Unmarshal(m.Binding, &b)
				b.Uploads = uploads
				m.Binding, _ = json.Marshal(b)
			}
		} else {
			var p queuedPayload
			var raw, oldBinding []byte
			var readErr error
			if q.Kind == "delegation_waiting" {
				raw, readErr = h.delegations.UnifiedEffectivePayload(ctx, "waiting", in.Item)
				if readErr == nil && len(raw) == 0 {
					raw, readErr = h.delegations.UnifiedReplyPayload(ctx, in.Item)
				}
				if readErr == nil {
					oldBinding, readErr = h.delegations.UnifiedEffectiveBinding(ctx, "waiting", in.Item)
				}
			} else {
				raw, readErr = h.dispatches.UnifiedEffectivePayload(ctx, q.Target.SessionID, in.Item)
				if readErr == nil {
					oldBinding, readErr = h.dispatches.UnifiedEffectiveBinding(ctx, q.Target.SessionID, in.Item)
				}
			}
			if readErr != nil {
				return nil, readErr
			}
			if len(raw) > 0 && json.Unmarshal(raw, &p) != nil {
				return nil, unifiedError("invalid_argument")
			}
			resolved.Entry.EffectiveBinding = oldBinding
			prepared, uploads, _, mediaErr := h.prepareUnifiedEditMedia(ctx, c, resolved, in, p)
			if mediaErr != nil {
				return nil, mediaErr
			}
			m.Payload, _ = json.Marshal(prepared)
			m.AllowAttachments = true
			var binding unifiedInputBinding
			_ = json.Unmarshal(m.Binding, &binding)
			binding.Uploads = uploads
			binding.InputFiles = unifiedOriginalMediaFiles(resolved, in)
			m.Binding, _ = json.Marshal(binding)
		}
		if resolved.HasEntry {
			var binding unifiedInputBinding
			_ = json.Unmarshal(m.Binding, &binding)
			binding.InputFiles = unifiedOriginalMediaFiles(resolved, in)
			m.Binding, _ = json.Marshal(binding)
		}

	}
	var fences []unifiedQueueIdentityFence
	if req.Operation == "queue_move" {
		var err error
		fences, m.GrantRevisions, err = h.unifiedQueueFences(ctx, q, v.Items, eligible)
		if err != nil {
			return nil, err
		}
	}
	if req.Operation == "queue_edit" && len(m.Binding) > 0 {
		var b unifiedInputBinding
		if json.Unmarshal(m.Binding, &b) != nil {
			return nil, unifiedError("scope_changed")
		}
		if b.ControllerParent != "" {
			if m.GrantRevisions == nil {
				m.GrantRevisions = map[string]uint64{}
			}
			m.GrantRevisions[b.ControllerParent] = b.GrantRevision
		}
	}
	// Recheck original role/source at the latest pre-transaction boundary; each
	// original owner then atomically checks canonical state and membership CAS.
	originalCheck := m.Revalidate
	m.Revalidate = func(ctx context.Context) error {
		if e := originalCheck(ctx); e != nil {
			return e
		}
		if req.Operation == "queue_move" && q.Kind != "controller_waiting_admission" {
			g, e := h.dispatches.Grant(ctx, manager)
			if e != nil {
				return e
			}
			if g.Revision != roleRevision {
				return unifiedError("scope_changed")
			}
		}
		if err := h.checkUnifiedQueueIdentities(fences); err != nil {
			return err
		}
		if q.Kind != "controller_waiting_admission" {
			for parent, expected := range m.GrantRevisions {
				g, err := h.dispatches.Grant(ctx, parent)
				if err != nil {
					return err
				}
				if g.Revision != expected {
					return unifiedError("scope_changed")
				}
			}
		}
		if len(m.Binding) > 0 {
			var b unifiedInputBinding
			if json.Unmarshal(m.Binding, &b) != nil {
				return unifiedError("scope_changed")
			}
			if err := h.unifiedBindingLocalCurrent(b); err != nil {
				return err
			}
		}
		return h.unifiedTarget(q.Target, true)
	}
	var receipt taskapi.MutationReceipt
	switch q.Kind {
	case "session_input":
		receipt, e = h.messageQueue.UnifiedMutate(ctx, q.Target.SessionID, eligible, m)
	case "delegation_waiting":
		receipt, e = h.delegations.UnifiedMutate(ctx, eligible, m)
	case "controller_waiting_admission":
		receipt, e = h.dispatches.UnifiedMutate(ctx, q.Target.SessionID, eligible, m)
	default:
		e = unifiedError("unsupported")
	}
	if e != nil {
		return nil, e
	}
	if req.Operation == "queue_move" && s != nil {
		var data struct {
			Order []string `json:"order"`
		}
		if json.Unmarshal(receipt.Data, &data) != nil {
			return nil, unifiedError("unknown")
		}
		pending := []string{}
		for _, publicID := range data.Order {
			id := publicID
			for actual, public := range publicHandles {
				if public == publicID {
					id = actual
					break
				}
			}
			for _, a := range actorIDs {
				if id == a {
					pending = append(pending, id)
				}
			}
		}
		if e = s.ReorderWaiting(actorIDs, pending); e != nil {
			return nil, taskapi.Failure("unknown", "known_receipt", "lookup_original")
		}
	}
	return receipt, nil
}
