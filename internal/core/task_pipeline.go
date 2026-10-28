package core

import (
	"context"
	"encoding/json"
	"errors"
	contract "everything-go/contracts/task-api/pipeline-v7"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	p "everything-go/internal/taskpipeline"
	"strings"
	"time"
)

type pipelineInput struct {
	Manager           p.Identity    `json:"manager"`
	ID                string        `json:"id"`
	Expected          uint64        `json:"expected_revision"`
	Mission           *p.Mission    `json:"mission"`
	Watch             *p.Watch      `json:"watch"`
	Pipeline          *p.Pipeline   `json:"pipeline"`
	Checkpoint        *p.Checkpoint `json:"checkpoint"`
	Limit             int           `json:"limit"`
	Cursor            string        `json:"cursor"`
	After             uint64        `json:"after_sequence"`
	OriginalIntent    string        `json:"original_intent_id"`
	OriginalOperation string        `json:"original_operation"`
	Hash              string        `json:"expected_input_hash"`
}

var pipelineOps = []string{"capabilities", "mission_create", "mission_get", "mission_list", "mission_update", "mission_checkpoint", "mission_pause", "mission_cancel", "watch_create", "watch_get", "watch_list", "watch_update", "watch_pause", "watch_cancel", "pipeline_define", "pipeline_get", "pipeline_list", "pipeline_enable", "pipeline_pause", "pipeline_cancel", "event_history", "continuation_get", "continuation_ack", "continuation_receipt_get", "operation_get", "guard_status"}

func pipelineTarget(i p.Identity) jarvisTarget {
	return jarvisTarget{i.Authority, i.Session, i.Thread, i.Revision}
}
func pipelineIdentity(t jarvisTarget) p.Identity {
	return p.Identity{Authority: t.Authority, Session: t.SessionID, Thread: t.ThreadID, Revision: t.Revision}
}
func pipelineLocator(l p.Locator) unifiedLocator {
	round := l.RoundID
	if l.Kind != "delegation" {
		round = ""
	}
	return unifiedLocator{Kind: l.Kind, Authority: l.Authority, TaskID: l.TaskID, NativeID: l.NativeID, RoundID: round, SessionID: l.Session, RequestID: l.Request, ThreadID: l.Thread, Revision: l.Revision, RevisionKnown: true}
}
func (h *Hub) executePipeline(ctx context.Context, caller taskapi.BoundCaller, raw []byte) (taskapi.Response, bool) {
	var req taskapi.Request
	if json.Unmarshal(raw, &req) != nil || req.Type != "task_api_pipeline_request" {
		return taskapi.Response{}, false
	}
	op := req.Operation
	resp := taskapi.Response{Authority: h.cfg.InstanceID, Type: "task_api_pipeline_response", Version: contract.Version, CorrelationID: req.CorrelationID, Operation: &op}
	fail := func(e error) (taskapi.Response, bool) {
		var api *taskapi.APIError
		if errors.Is(e, p.ErrConflict) {
			api = taskapi.Failure("stale_revision", "known_none", "refresh_snapshot")
		} else if errors.Is(e, p.ErrScope) {
			api = taskapi.Failure("scope_changed", "known_none", "refresh_snapshot")
		} else if !errors.As(e, &api) {
			api = taskapi.Failure("unknown", "unknown", "lookup_original")
		}
		resp.Error = api
		return resp, true
	}
	if req.Version != contract.Version {
		return fail(unifiedError("unsupported"))
	}
	if e := contract.Validate(raw); e != nil {
		return fail(unifiedError("invalid_argument"))
	}
	if h.messageQueue == nil {
		return fail(unifiedError("unsupported"))
	}
	disclosures := []func(context.Context) error{}
	ctx = context.WithValue(ctx, unifiedDisclosureCollectorKey{}, &disclosures)
	bound, e := caller.Check(ctx)
	if e != nil {
		return fail(e)
	}
	if bound.Authority != h.cfg.InstanceID {
		return fail(unifiedError("wrong_authority"))
	}
	var in pipelineInput
	if e = json.Unmarshal(req.Input, &in); e != nil {
		return fail(unifiedError("invalid_argument"))
	}
	worker := h.unifiedBoundedWorker(bound)
	role := "native_agent"
	if h.dispatches != nil && bound.SourceSessionID != "" {
		if g, err := h.dispatches.Grant(ctx, bound.SourceSessionID); err == nil && g.Enabled && g.Local {
			role = "native_dispatcher"
		}
	}
	if bound.BindingKind == "paired_human" {
		role = "paired_human"
	}
	if worker {
		role = "bounded_worker"
	}
	if op == "capabilities" {
		allowed := pipelineOps
		if worker {
			allowed = []string{"capabilities", "mission_get", "mission_list", "event_history", "continuation_get", "continuation_receipt_get", "mission_checkpoint", "continuation_ack", "operation_get"}
		}
		resp.OK = true
		resp.Result = map[string]any{"kind": "capabilities", "schema_hash": contract.Hash(), "authority_instance_id": h.cfg.InstanceID, "role": role, "allowed_operations": allowed, "features": map[string]any{"missions": true, "exact_round_watch": true, "pipeline_dag": true, "server_automation": role == "native_dispatcher" || role == "paired_human", "speech": false, "remote_automation": false}, "limits": map[string]any{"max_records": 128, "max_nodes": 32, "max_edges": 64, "max_events_per_read": 100, "max_request_bytes": 262144, "max_response_bytes": 8388608}, "scope": map[string]any{"session_id": bound.SourceSessionID, "namespace_generation": bound.NamespaceGeneration}}
		return resp, true
	}
	manager, e := h.unifiedManager(bound, unifiedInput{Manager: pipelineTarget(in.Manager)})
	if e != nil {
		return fail(e)
	}
	if manager != in.Manager.Session {
		return fail(unifiedError("scope_changed"))
	}
	source, ok := h.registry.Get(manager)
	if !ok {
		return fail(unifiedError("scope_changed"))
	}
	sourceRevision := source.Snapshot().SourceRevision
	scope := p.Scope{Principal: bound.StableScopeID, Generation: bound.NamespaceGeneration, Manager: in.Manager}
	var inherited *p.Continuation
	if bound.BindingKind != "paired_human" && bound.SourceRequestID != "" {
		c, issuer, record, found, err := h.messageQueue.PipelineStore().ExecutionContext(ctx, bound.SourceSessionID, bound.SourceRequestID)
		if err != nil {
			return fail(err)
		}
		if found {
			if err = h.pipelineIssuerCurrent(ctx, issuer, record); err != nil {
				return fail(err)
			}
			switch op {
			case "mission_get", "event_history", "mission_checkpoint":
				if in.ID != record.ID {
					return fail(unifiedError("permission"))
				}
			case "continuation_get", "continuation_ack", "continuation_receipt_get":
				if in.ID != c.ID {
					return fail(unifiedError("permission"))
				}
			case "operation_get":
				in.OriginalIntent = "native:" + c.ID + ":" + in.OriginalIntent
			default:
				return fail(unifiedError("permission"))
			}
			scope = issuer.Scope
			inherited = &c
		}
	}
	if worker && inherited == nil {
		return fail(unifiedError("permission"))
	}
	projection := false
	switch op {
	case "mission_get", "watch_get", "pipeline_get", "event_history", "continuation_get", "continuation_receipt_get":
		if bound.BindingKind == "paired_human" {
			recordID := in.ID
			if op == "continuation_get" || op == "continuation_receipt_get" {
				cont, found, err := h.messageQueue.PipelineStore().ExactContinuation(ctx, in.ID)
				if err != nil {
					return fail(err)
				}
				if found {
					recordID = cont.RecordID
				}
			}
			if _, _, found, err := h.messageQueue.PipelineStore().Get(ctx, scope, recordID); err != nil {
				return fail(err)
			} else if !found {
				r, i, found, err := h.messageQueue.PipelineStore().NativeProjection(ctx, in.Manager, recordID)
				if err != nil {
					return fail(err)
				}
				if found {
					if err = h.pipelineIssuerCurrent(ctx, i, r); err != nil {
						return fail(err)
					}
					scope = i.Scope
					capturedIssuer, capturedRecord := i, r
					disclosures = append(disclosures, func(ctx context.Context) error { return h.pipelineIssuerCurrent(ctx, capturedIssuer, capturedRecord) })
					projection = true
				}
			}
		}
	}

	check := func(ctx context.Context) error {
		current, e := caller.Check(ctx)
		if e != nil {
			return e
		}
		if current != bound {
			return unifiedError("caller_unbound")
		}
		s, ok := h.registry.Get(manager)
		if !ok || s != source || s.Snapshot().SourceRevision != sourceRevision {
			return unifiedError("scope_changed")
		}
		return h.unifiedTarget(pipelineTarget(in.Manager), false)
	}
	if e = check(ctx); e != nil {
		return fail(e)
	}
	cid := ""
	if inherited != nil {
		cid = inherited.ID
	}
	store := h.messageQueue.PipelineStore().WithProof(pipelineNativeProvenance(bound, req), cid)
	disclose := func(r p.Record) error {
		observer := bound
		owner := manager
		if inherited != nil {
			issuer, current, e := store.Issuer(ctx, r.ID)
			if e != nil {
				return e
			}
			if e = h.pipelineIssuerCurrent(ctx, issuer, current); e != nil {
				return e
			}
			observer = pipelineObserver(issuer, current)
			owner = current.Manager.Session
		}
		return h.pipelineDisclosure(ctx, observer, owner, r)
	}
	var result any
	switch op {
	case "mission_list", "watch_list", "pipeline_list":
		rows, next, e := store.List(ctx, scope, strings.Split(op, "_")[0], in.Cursor, in.Limit)
		if bound.BindingKind == "paired_human" {
			items, cursor, err := store.ReadList(ctx, scope, strings.Split(op, "_")[0], in.Cursor, in.Limit)
			if err != nil {
				return fail(err)
			}
			rows = []p.Record{}
			next = cursor
			for _, item := range items {
				if item.Projection {
					if err = h.pipelineIssuerCurrent(ctx, item.Issuer, item.Record); err != nil {
						return fail(err)
					}
					captured := item
					disclosures = append(disclosures, func(ctx context.Context) error { return h.pipelineIssuerCurrent(ctx, captured.Issuer, captured.Record) })
				}
				rows = append(rows, item.Record)
			}
		}
		if e != nil {
			return fail(e)
		}
		for _, r := range rows {
			if e = disclose(r); e != nil {
				return fail(e)
			}
		}
		result = map[string]any{"kind": "records", "items": rows, "next_cursor": next, "complete": next == ""}
	case "mission_get", "watch_get", "pipeline_get":
		r, _, found, e := store.Get(ctx, scope, in.ID)
		if e != nil {
			return fail(e)
		}
		if !found {
			return fail(unifiedError("not_found"))
		}
		if e = disclose(r); e != nil {
			return fail(e)
		}
		if projection {
			r.AccessMode = "read_only_projection"
		}
		result = r
	case "event_history":
		r, _, f, e := store.Get(ctx, scope, in.ID)
		if e != nil {
			return fail(e)
		}
		if !f {
			return fail(unifiedError("not_found"))
		}
		if e = disclose(r); e != nil {
			return fail(e)
		}
		events, next, more, e := store.Events(ctx, scope, in.ID, in.After, in.Limit)
		if e != nil {
			return fail(e)
		}
		result = map[string]any{"kind": "events", "items": events, "next_sequence": next, "has_more": more}
	case "continuation_get":
		c, f, e := store.Continuation(ctx, scope, in.ID)
		if e != nil {
			return fail(e)
		}
		if !f {
			return fail(unifiedError("not_found"))
		}
		r, _, f, e := store.Get(ctx, scope, c.RecordID)
		if e != nil || !f {
			return fail(unifiedError("scope_changed"))
		}
		if e = disclose(r); e != nil {
			return fail(e)
		}
		if c.DefinitionRevision != r.DefinitionRevision {
			old, _, err := store.OriginalDefinition(ctx, c.RecordID, c.DefinitionRevision)
			if err != nil {
				return fail(err)
			}
			if err = disclose(old); err != nil {
				return fail(err)
			}
		}
		result = c
	case "continuation_receipt_get":
		c, found, err := store.Continuation(ctx, scope, in.ID)
		if err != nil {
			return fail(err)
		}
		if !found {
			return fail(unifiedError("not_found"))
		}
		if in.OriginalOperation != "continuation_ack" && in.OriginalOperation != "mission_checkpoint" {
			return fail(unifiedError("permission"))
		}
		receipt, found, err := store.Operation(ctx, scope, "native:"+c.ID+":"+in.OriginalIntent, in.OriginalOperation, in.Hash)
		if err != nil {
			return fail(err)
		}
		if !found {
			return fail(taskapi.Failure("not_found", "unknown", "lookup_original"))
		}
		expectedRequest := c.RequestID
		if c.OriginalReceipt != nil {
			expectedRequest = c.OriginalReceipt.Request
		}
		if receipt.Provenance == nil || receipt.Provenance.RequestID != expectedRequest || receipt.Provenance.Source != c.Action.Target || receipt.IntentID != in.OriginalIntent {
			return fail(unifiedError("scope_changed"))
		}
		r, _, found, err := store.Get(ctx, scope, c.RecordID)
		if err != nil {
			return fail(err)
		}
		if !found {
			return fail(unifiedError("not_found"))
		}
		if err = disclose(r); err != nil {
			return fail(err)
		}
		if c.DefinitionRevision != r.DefinitionRevision {
			old, _, err := store.OriginalDefinition(ctx, c.RecordID, c.DefinitionRevision)
			if err != nil {
				return fail(err)
			}
			if err = disclose(old); err != nil {
				return fail(err)
			}
		}
		result = receipt
	case "operation_get":
		receipt, f, e := store.Operation(ctx, scope, in.OriginalIntent, in.OriginalOperation, in.Hash)
		if e != nil {
			return fail(e)
		}
		if !f {
			return fail(taskapi.Failure("not_found", "unknown", "lookup_original"))
		}
		if receipt.Record != nil {
			if e = disclose(*receipt.Record); e != nil {
				return fail(e)
			}
		}
		result = receipt
	case "guard_status":
		result, e = store.ScopedGuard(ctx, scope)
		if e != nil {
			return fail(e)
		}
	default:
		if bound.BindingKind == "paired_human" && in.ID != "" {
			if _, _, own, err := store.Get(ctx, scope, in.ID); err != nil {
				return fail(err)
			} else if !own {
				if _, _, projected, err := store.NativeProjection(ctx, in.Manager, in.ID); err != nil {
					return fail(err)
				} else if projected {
					return fail(taskapi.Failure("read_only_projection", "known_none", "return_original_owner"))
				}
			}
		}
		if worker && op != "mission_checkpoint" && op != "continuation_ack" {
			return fail(unifiedError("permission"))
		}
		if op == "continuation_ack" {
			return h.pipelineAck(ctx, caller, bound, scope, req, in, check)
		}
		var definition *p.Record
		initial := []p.Fact{}
		issuer := p.Issuer{Scope: scope, Kind: bound.BindingKind, SourceRequest: bound.SourceRequestID, SourceGeneration: sourceRevision, Targets: map[string]uint64{}}
		if in.Mission != nil || in.Watch != nil || in.Pipeline != nil {
			definition = &p.Record{Manager: in.Manager, Mission: in.Mission, Watch: in.Watch, Pipeline: in.Pipeline}
			if in.Mission != nil {
				definition.Kind = "mission"
			} else if in.Watch != nil {
				definition.Kind = "watch"
			} else {
				definition.Kind = "pipeline"
			}
			if e = p.Validate(*definition); e != nil {
				return fail(taskapi.Failure(e.Error(), "known_none", "review_rule"))
			}
			if worker {
				return fail(unifiedError("permission"))
			}
			if e = h.pipelineAuthorize(ctx, bound, &issuer, *definition); e != nil {
				return fail(e)
			}
			for _, st := range p.Steps(*definition) {
				if fact, e := h.pipelineWorkRef(ctx, bound, manager, st); e != nil {
					return fail(e)
				} else if fact != nil {
					initial = append(initial, *fact)
				}
				if st.Source != nil {
					fact, e := h.pipelineFact(ctx, bound, manager, *st.Source)
					if e != nil {
						return fail(e)
					}
					if err := h.pipelineArtifactFact(ctx, st.Predicate, &fact); err != nil {
						return fail(err)
					}
					initial = append(initial, fact)
				}
			}
		}
		if op == "mission_checkpoint" {
			if in.Checkpoint == nil || bound.SourceRequestID == "" || in.Checkpoint.RequestID != bound.SourceRequestID {
				return fail(unifiedError("permission"))
			}
			r, _, found, e := store.Get(ctx, scope, in.ID)
			if e != nil || !found {
				return fail(unifiedError("not_found"))
			}
			if e = h.pipelineValidateCheckpoint(ctx, bound, scope, r, *in.Checkpoint); e != nil {
				return fail(e)
			}
		}
		h.delegationMu.Lock()
		receipt, e := store.Mutate(ctx, scope, issuer, func() string {
			if inherited != nil {
				return "native:" + inherited.ID + ":" + req.IdempotencyKey
			}
			return req.IdempotencyKey
		}(), op, unifiedHash(req), in.ID, in.Expected, definition, in.Checkpoint, initial, check, req.IdempotencyKey)
		h.delegationMu.Unlock()
		if e != nil {
			return fail(e)
		}
		result = receipt
	}
	if e = check(ctx); e != nil {
		return fail(e)
	}
	for _, disclose := range disclosures {
		if e = disclose(ctx); e != nil {
			return fail(e)
		}
	}
	resp.OK = true
	resp.Result = result
	bytes, e := json.Marshal(resp)
	if e != nil || len(bytes) > 8388608 {
		return fail(unifiedError("unsupported"))
	}
	return resp, true
}
func (h *Hub) pipelineAuthorize(ctx context.Context, c taskapi.VerifiedContext, issuer *p.Issuer, r p.Record) error {
	if h.unifiedBoundedWorker(c) {
		return unifiedError("permission")
	}
	if e := h.unifiedTarget(pipelineTarget(r.Manager), false); e != nil {
		return e
	}
	// Native issuance requires the actual dispatch role now. Paired issuance is
	// a real human policy receipt; any cross-session destination still needs role.
	needsRole := c.BindingKind != "paired_human"
	actions := []p.Action{}
	if r.Mission != nil && r.Mission.RecoveryAction != nil {
		actions = append(actions, *r.Mission.RecoveryAction)
	}
	for _, st := range p.Steps(r) {
		if st.Action != nil {
			actions = append(actions, *st.Action)
		}
	}
	if r.Watch != nil && r.Watch.Action != nil {
		actions = append(actions, *r.Watch.Action)
	}
	for _, a := range actions {
		if a.Target.Authority != h.cfg.InstanceID {
			return unifiedError("remote_automation_unsupported")
		}
		if e := h.unifiedTarget(pipelineTarget(a.Target), true); e != nil {
			return e
		}
		target, _ := h.registry.Get(a.Target.Session)
		issuer.Targets[a.Target.Session] = target.Snapshot().SourceRevision
		if a.Target.Session != r.Manager.Session {
			needsRole = true
		}
		if a.Kind == "delegation_continue" {
			if a.OriginalTask == nil {
				return unifiedError("original_parent_required")
			}
			resolved, e := h.resolveUnified(ctx, c, r.Manager.Session, pipelineLocator(*a.OriginalTask))
			if e != nil {
				return e
			}
			if resolved.Parent.SessionID != r.Manager.Session || resolved.Locator.Kind != "delegation" || resolved.Target.SessionID != a.Target.Session {
				return unifiedError("original_parent_required")
			}
		}
	}
	if needsRole {
		if h.dispatches == nil {
			return unifiedError("permission")
		}
		grant, e := h.dispatches.Grant(ctx, r.Manager.Session)
		if e != nil {
			return e
		}
		if !grant.Enabled || !grant.AllowsInstance(h.cfg.InstanceID, h.cfg.InstanceID) {
			return unifiedError("permission")
		}
		for _, a := range actions {
			if !grant.Allows(h.cfg.InstanceID, h.cfg.InstanceID, a.Target.Session) {
				return unifiedError("permission")
			}
		}
		issuer.GrantRevision = grant.Revision
	}
	return h.pipelineDisclosure(ctx, c, r.Manager.Session, r)
}
func (h *Hub) pipelineDisclosure(ctx context.Context, c taskapi.VerifiedContext, manager string, r p.Record) error {
	for _, st := range p.Steps(r) {
		if _, e := h.pipelineWorkRef(ctx, c, manager, st); e != nil {
			return e
		}
		if st.Source != nil {
			sourceCtx := context.WithValue(ctx, pipelineServiceReadKey{}, pipelineServiceReadProof{IssuerScope: c.StableScopeID, Generation: c.NamespaceGeneration, Manager: manager, Locator: *st.Source})
			resolved, e := h.resolveUnified(sourceCtx, c, manager, pipelineLocator(*st.Source))
			if e != nil {
				return e
			}
			if e = resolved.disclosureCurrent(ctx); e != nil {
				return e
			}
		}
	}
	return nil
}
func (h *Hub) pipelineFact(ctx context.Context, c taskapi.VerifiedContext, manager string, loc p.Locator) (p.Fact, error) {
	ctx = context.WithValue(ctx, pipelineServiceReadKey{}, pipelineServiceReadProof{IssuerScope: c.StableScopeID, Generation: c.NamespaceGeneration, Manager: manager, Locator: loc})
	if loc.Kind != "delegation" && loc.RoundID != loc.Request {
		return p.Fact{}, unifiedError("exact_request_scope_required")
	}
	f := p.Fact{Source: loc, Admission: "unknown", NativeStart: "unknown", Terminal: "unknown", Final: "pending", QA: "not_requested", ObservedAt: time.Now().UnixMilli()}
	r, e := h.resolveUnified(ctx, c, manager, pipelineLocator(loc))
	if e != nil {
		return f, e
	}
	if r.Locator.SessionID != loc.Session || r.Locator.RequestID != loc.Request || r.Locator.ThreadID != loc.Thread || r.Locator.Revision != loc.Revision {
		return f, unifiedError("scope_changed")
	}
	f.Store = r.Locator.Kind
	if f.Store == "request" {
		f.Store = "ordinary"
	}
	journal := h.apiJournals()[f.Store]
	if journal == nil {
		return f, unifiedError("source_unverified")
	}
	f.Sequence, e = journal.SourceRevision(ctx, loc.Session, loc.Request)
	if e != nil {
		return f, e
	}
	dto, e := h.unifiedTaskDTO(ctx, c, manager, r)
	if e != nil {
		return f, e
	}
	d := dto.(map[string]any)
	execution := d["execution"].(map[string]any)
	f.Admission, _ = execution["admission"].(string)
	f.Admission = pipelineAdmissionState(f.Admission)
	f.NativeStart, _ = execution["native_start"].(string)
	f.Terminal, _ = execution["terminal"].(string)
	if r.HasEntry {
		f.InputRevision = r.Entry.ContentRevision
		f.InputHash = r.Entry.EffectiveHash
		rev, hash, found, err := h.messageQueue.ConsumedInput(ctx, loc.Session, loc.Request, loc.Thread)
		if err != nil {
			return f, err
		}
		if found {
			f.ConsumedRevision = rev
			f.ConsumedHash = hash
			f.EvidenceStatus = "verified_native_ingress"
		} else if r.Entry.ContentRevision == 1 && f.NativeStart == "observed" {
			f.ConsumedRevision = 1
			f.ConsumedHash = r.Entry.PayloadHash
			f.EvidenceStatus = "original_unedited_native_ingress"
		} else {
			f.EvidenceStatus = "unverified_input_ingress"
		}
	}
	if final, ok := d["final"].(map[string]any); ok {
		f.Final = "sealed"
		f.SealID, _ = final["seal_id"].(string)
		f.FullHash, _ = final["full_hash"].(string)
		f.Bytes, _ = final["total_bytes"].(int)
	}
	if loc.Kind == "delegation" && h.delegations != nil {
		original, found, e := h.delegations.ByID(ctx, loc.RoundID)
		if e != nil {
			return f, e
		}
		if !found || original.ChildRequestID != loc.Request {
			return f, unifiedError("source_unverified")
		}
		if original.State == "terminal" {
			f.Terminal = original.TerminalStatus
		}
		f.QARound, f.QA, e = h.delegations.QAForRound(ctx, loc.RoundID)
		if e != nil {
			return f, e
		}
		if f.QARound != "" {
			f.QAOwnerRound = loc.RoundID
		}
	}
	// Old original-store rows may predate their adapter journal. The matching
	// MQ seal/admission sequence is a separate, explicitly named evidence domain.
	if f.Sequence == 0 {
		mqSeq, err := h.messageQueue.TaskJournal().SourceRevision(ctx, loc.Session, loc.Request)
		if err != nil {
			return f, err
		}
		if mqSeq > 0 {
			f.Store = "message_queue_exact_receipt"
			f.Sequence = mqSeq
		}
	}
	// A sequence of zero remains a legacy evidence gap, never fabricated as fresh.
	if e = r.disclosureCurrent(ctx); e != nil {
		return f, e
	}
	return f, nil
}
func (h *Hub) pipelineValidateCheckpoint(ctx context.Context, c taskapi.VerifiedContext, scope p.Scope, r p.Record, cp p.Checkpoint) error {
	if cp.RequestID != c.SourceRequestID || cp.Revision != r.Revision {
		return unifiedError("stale_revision")
	}
	if e := h.messageQueue.PipelineStore().CheckEventIDs(ctx, scope, r.ID, cp.Processed); e != nil {
		return e
	}
	if cp.Outcome == "done" && r.State != "done" {
		return unifiedError("required_steps_unsatisfied")
	}
	return nil
}
func (h *Hub) pipelineAck(ctx context.Context, caller taskapi.BoundCaller, c taskapi.VerifiedContext, scope p.Scope, req taskapi.Request, in pipelineInput, check func(context.Context) error) (taskapi.Response, bool) {
	op := req.Operation
	resp := taskapi.Response{Authority: h.cfg.InstanceID, Type: "task_api_pipeline_response", Version: contract.Version, CorrelationID: req.CorrelationID, Operation: &op}
	fail := func(code string) (taskapi.Response, bool) {
		resp.Error = taskapi.Failure(code, "known_none", "refresh_snapshot")
		return resp, true
	}
	if in.Checkpoint == nil || in.Checkpoint.RequestID != c.SourceRequestID {
		return fail("permission")
	}
	if e := check(ctx); e != nil {
		return fail("scope_changed")
	}
	if _, found, e := h.messageQueue.ExactNativeAcceptance(c.SourceSessionID, c.SourceRequestID); e != nil || !found {
		return fail("native_acceptance_unverified")
	}
	receipt, e := h.messageQueue.PipelineStore().WithProof(pipelineNativeProvenance(c, req)).Ack(ctx, scope, req.IdempotencyKey, "native:"+in.ID+":"+req.IdempotencyKey, unifiedHash(req), in.ID, in.Expected, *in.Checkpoint, c.SourceSessionID, c.SourceRequestID, check)
	if e != nil {
		if errors.Is(e, p.ErrConflict) {
			return fail("stale_revision")
		}
		if errors.Is(e, p.ErrScope) {
			return fail("scope_changed")
		}
		switch e.Error() {
		case "pipeline_definition_changed", "continuation_not_admitted", "unverified_checkpoint_event", "required_steps_unsatisfied", "native_operation_limit":
			return fail(e.Error())
		}
		resp.Error = taskapi.Failure("unknown", "unknown", "lookup_original")
		return resp, true
	}
	resp.OK = true
	resp.Result = receipt
	return resp, true
}
func (h *Hub) pipelineIssuerCurrent(ctx context.Context, i p.Issuer, r p.Record) error {
	manager, ok := h.registry.Get(r.Manager.Session)
	if !ok || manager.Snapshot().SourceRevision != i.SourceGeneration {
		return unifiedError("scope_changed")
	}
	if e := h.unifiedTarget(pipelineTarget(r.Manager), false); e != nil {
		return e
	}
	switch i.Kind {
	case "paired_human":
		device := strings.TrimPrefix(i.Scope.Principal, "device:")
		if device == i.Scope.Principal || !h.jarvisCredentialCurrent(device, i.Scope.Generation) {
			return unifiedError("caller_unbound")
		}
	case "native_tool", "mcp_process_binding":
		if i.GrantRevision == 0 {
			return unifiedError("permission")
		}
	default:
		return unifiedError("permission")
	}
	if i.GrantRevision != 0 {
		g, e := h.dispatches.Grant(ctx, r.Manager.Session)
		if e != nil {
			return e
		}
		if !g.Enabled || g.Revision != i.GrantRevision {
			return unifiedError("permission")
		}
	}
	for id, rev := range i.Targets {
		s, ok := h.registry.Get(id)
		if !ok || s.Snapshot().SourceRevision != rev {
			return unifiedError("scope_changed")
		}
	}
	return nil
}

// This context is explicitly a server automation observer, never a fabricated
// human/provider caller and never bound with an expired native callback.
func pipelineObserver(i p.Issuer, r p.Record) taskapi.VerifiedContext {
	return taskapi.VerifiedContext{Authority: r.Manager.Authority, StableScopeID: i.Scope.Principal, NamespaceGeneration: i.Scope.Generation, BindingKind: "automation_service", SourceSessionID: r.Manager.Session, SourceResumeID: r.Manager.Thread, SourceConfigRevision: r.Manager.Revision, SourceConfigKnown: true}
}
func (h *Hub) pipelineIdle(s *session.Session) bool {
	if s == nil || s.State() != session.Idle || s.QueueLen() != 0 || s.ActiveQueuedID() != "" {
		return false
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	for _, call := range h.voiceClients {
		if call.session == s {
			return false
		}
	}
	return true
}

// A server observation is restricted to the captured exact source. It cannot
// become a generic paired-human reader or a provider/native caller.
type pipelineServiceReadKey struct{}
type pipelineServiceReadProof struct {
	IssuerScope string
	Generation  uint64
	Manager     string
	Locator     p.Locator
}

func (h *Hub) pipelineServiceReadAllowed(ctx context.Context, c taskapi.VerifiedContext, r taskapi.IntentRecord) bool {
	if c.BindingKind != "automation_service" {
		return false
	}
	proof, ok := ctx.Value(pipelineServiceReadKey{}).(pipelineServiceReadProof)
	if !ok || proof.IssuerScope != c.StableScopeID || proof.Generation != c.NamespaceGeneration || proof.Manager != c.SourceSessionID {
		return false
	}
	return proof.Locator.TaskID == r.TaskID && proof.Locator.Request == r.RequestID && proof.Locator.Session == r.SessionID && proof.Locator.Authority == h.cfg.InstanceID
}

func (h *Hub) pipelineEffectCurrent(ctx context.Context, i p.Issuer, r p.Record) error {
	if e := h.pipelineIssuerCurrent(ctx, i, r); e != nil {
		return e
	}
	if e := h.pipelineDisclosure(ctx, pipelineObserver(i, r), r.Manager.Session, r); e != nil {
		return e
	}
	if r.Mission != nil && r.Mission.RecoveryAction != nil {
		if e := h.unifiedTarget(pipelineTarget(r.Mission.RecoveryAction.Target), true); e != nil {
			return e
		}
	}
	if r.Pipeline != nil && r.DefinitionRevision > 1 {
		if _, e := h.pipelineCurrentFacts(ctx, i, r); e != nil {
			return e
		}
	}

	a := p.Auth(r)
	if !a.Confirmed || a.Budget.DeadlineMS <= time.Now().UnixMilli() {
		return unifiedError("authorization_expired")
	}
	for _, step := range p.Steps(r) {
		if step.Action != nil {
			if e := h.unifiedTarget(pipelineTarget(step.Action.Target), true); e != nil {
				return e
			}
		}
	}
	if r.Watch != nil && r.Watch.Action != nil {
		return h.unifiedTarget(pipelineTarget(r.Watch.Action.Target), true)
	}
	return nil
}

func pipelineAdmissionState(original string) string {
	switch original {
	case "prepared":
		return "prepared"
	case "rejected":
		return "rejected"
	case "queued", "running", "completed", "failed", "cancelled", "provisioning", "terminal", "collecting", "uncertain":
		return "queued"
	}
	return "unknown"
}

// Only the authenticated native gateway supplies this provenance. A paired
// request never supplies a tool identity and cannot manufacture a native receipt.
func pipelineNativeProvenance(c taskapi.VerifiedContext, req taskapi.Request) *p.NativeProvenance {
	if c.BindingKind == "paired_human" || c.SourceRequestID == "" || (c.NativeTurnID == "" && c.ProcessGeneration == "") || c.ToolCallID == "" {
		return nil
	}
	return &p.NativeProvenance{Source: p.Identity{Authority: c.Authority, Session: c.SourceSessionID, Thread: c.SourceResumeID, Revision: c.SourceConfigRevision}, RequestID: c.SourceRequestID, NativeTurnID: c.NativeTurnID, ToolCallID: c.ToolCallID, ProcessGeneration: c.ProcessGeneration, CorrelationID: req.CorrelationID, ActorKind: c.BindingKind, Tool: "task_pipeline"}
}

func (h *Hub) pipelineArtifactFact(ctx context.Context, predicate p.Predicate, f *p.Fact) error {
	if predicate.ArtifactHash == "" || f.Final != "sealed" {
		return nil
	}
	exists, e := h.messageQueue.PipelineArtifactHash(ctx, f.Source.Session, f.Source.Request, f.SealID, predicate.ArtifactHash)
	if e != nil {
		return e
	}
	if exists {
		f.ArtifactHash = predicate.ArtifactHash
	}
	return nil
}
