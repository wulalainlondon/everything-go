package core

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	unified "everything-go/contracts/task-api/unified-v6"
	originalcontract "everything-go/contracts/task-api/v1"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type unifiedLocator struct {
	Kind          string `json:"kind"`
	Authority     string `json:"authority_instance_id"`
	TaskID        string `json:"task_id,omitempty"`
	NativeID      string `json:"native_id,omitempty"`
	RoundID       string `json:"round_id,omitempty"`
	SessionID     string `json:"session_id,omitempty"`
	RequestID     string `json:"request_id,omitempty"`
	ThreadID      string `json:"thread_id,omitempty"`
	Revision      uint64 `json:"config_revision,omitempty"`
	RevisionKnown bool   `json:"-"`
}

// Presence is semantic: a verified revision 0 must survive the response wire,
// while an unbound/omitted input revision must not be invented as known zero.
func (l unifiedLocator) MarshalJSON() ([]byte, error) {
	type plain unifiedLocator
	var revision *uint64
	if l.RevisionKnown || l.Revision != 0 {
		revision = &l.Revision
	}
	return json.Marshal(struct {
		plain
		Revision *uint64 `json:"config_revision,omitempty"`
	}{plain(l), revision})
}

func (l *unifiedLocator) UnmarshalJSON(b []byte) error {
	type plain unifiedLocator
	var v plain
	if e := json.Unmarshal(b, &v); e != nil {
		return e
	}
	var fields map[string]json.RawMessage
	if e := json.Unmarshal(b, &fields); e != nil {
		return e
	}
	*l = unifiedLocator(v)
	_, l.RevisionKnown = fields["config_revision"]
	return nil
}
func (h *Hub) unifiedBoundedWorker(c taskapi.VerifiedContext) bool {
	if c.BindingKind == "paired_human" {
		return false
	}
	source, ok := h.registry.Get(c.SourceSessionID)
	if !ok {
		return false
	}
	scope, e := h.TaskWorkerScope(source)
	if e != nil || scope != nil {
		return true
	}
	if h.delegations != nil {
		child, e := h.delegations.HasChild(context.Background(), source.ID)
		return e != nil || child
	}
	return false
}

type unifiedQueue struct {
	Kind   string       `json:"kind"`
	Target jarvisTarget `json:"target"`
}
type unifiedInput struct {
	Manager           jarvisTarget    `json:"manager"`
	Locator           unifiedLocator  `json:"locator"`
	Queue             unifiedQueue    `json:"queue"`
	Original          json.RawMessage `json:"original_request"`
	OriginalOperation string          `json:"original_operation"`
	OriginalKey       string          `json:"original_idempotency_key"`
	ExpectedQueue     uint64          `json:"expected_queue_revision"`
	ExpectedContent   uint64          `json:"expected_content_revision"`
	ExpectedHash      string          `json:"expected_content_hash"`
	Membership        string          `json:"membership_hash"`
	Item              string          `json:"item_id"`
	Relative          string          `json:"relative_to"`
	Placement         string          `json:"placement"`
	Content           string          `json:"content"`
	Images            []intakeImage   `json:"images"`
	Files             []intakeFile    `json:"files"`
	Direction         string          `json:"direction"`
	Delivery          string          `json:"delivery"`
	Source            jarvisTarget    `json:"source"`
	Target            jarvisTarget    `json:"target"`
	ReplyTo           string          `json:"reply_to_request_id"`
	ReplyHash         string          `json:"reply_to_hash"`
	OwnerRequest      string          `json:"owner_request_id"`
	SealID            string          `json:"seal_id"`
	Hash              string          `json:"expected_hash"`
	Offset            int64           `json:"offset"`
	Limit             int64           `json:"limit"`
	Path              string          `json:"path"`
	DocumentID        string          `json:"document_id"`
	GrantID           string          `json:"grant_id"`
	Consumer          jarvisTarget    `json:"consumer"`
	Purpose           string          `json:"purpose"`
	Expires           int64           `json:"expires_at"`
}
type unifiedResolved struct {
	ReadFence               func(context.Context) error
	LocalFence              func() error
	InitializedDelegation   *unifiedDelegationInitialization
	ControllerGrantRevision uint64
	Locator                 unifiedLocator
	Parent                  jarvisTarget
	Target                  jarvisTarget
	Entry                   messagequeue.Entry
	HasEntry                bool
	Canonical               *taskapi.IntentRecord
	State, Delivery         string
	ReplyHash               string
	CanEdit                 bool
}

func unifiedError(code string) error { return taskapi.Failure(code, "known_none", "refresh_snapshot") }
func unifiedHash(r taskapi.Request) string {
	var v any
	_ = json.Unmarshal(r.Input, &v)
	return taskapi.Digest(struct {
		Operation string `json:"operation"`
		Input     any    `json:"input"`
	}{r.Operation, v})
}
func (h *Hub) unifiedIdentity(id string) jarvisTarget {
	s, ok := h.registry.Get(id)
	if !ok {
		return jarvisTarget{}
	}
	return jarvisTarget{h.cfg.InstanceID, id, s.ResumeID(), s.SettingsSnapshot().ConfigRevision}
}
func (h *Hub) unifiedTarget(t jarvisTarget, write bool) error {
	s, ok := h.registry.Get(t.SessionID)
	if !ok || t.Authority != h.cfg.InstanceID || s.ResumeID() != t.ThreadID || s.SettingsSnapshot().ConfigRevision != t.Revision || s.Snapshot().Hidden || s.State() == session.Closed || !h.controllerInScope(s) {
		return unifiedError("scope_changed")
	}
	if write && !h.controls.MobileMayWrite(s.ID) {
		return unifiedError("permission")
	}
	if policy, e := h.PMConfiguration(s.ID); e != nil || policy != nil {
		return unifiedError("permission")
	}
	return nil
}
func (h *Hub) unifiedManager(c taskapi.VerifiedContext, in unifiedInput) (string, error) {
	if c.BindingKind == "paired_human" {
		if e := h.unifiedTarget(in.Manager, false); e != nil {
			return "", e
		}
		return in.Manager.SessionID, nil
	}
	if c.SourceSessionID == "" {
		return "", unifiedError("caller_unbound")
	}
	if in.Manager.SessionID != "" && (in.Manager.Authority != c.Authority || in.Manager.SessionID != c.SourceSessionID || in.Manager.ThreadID != c.SourceResumeID || in.Manager.Revision != c.SourceConfigRevision) {
		return "", unifiedError("permission")
	}
	return c.SourceSessionID, nil
}
func (h *Hub) serveUnifiedTask(w http.ResponseWriter, r *http.Request, device string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", 405)
		return
	}
	raw, e := io.ReadAll(http.MaxBytesReader(w, r.Body, jarvisMaxBytes))
	if e != nil {
		http.Error(w, "bounded request required", 400)
		return
	}
	principal := jarvisRequestPrincipal(r, device)
	tokenHash := sha256.Sum256([]byte(readIdentityToken(r)))
	generation := binary.BigEndian.Uint64(tokenHash[:8]) & ((1 << 53) - 1)
	caller, e := taskapi.BindCaller(r.Context(), apiVerifier{func() (taskapi.VerifiedContext, error) {
		if h.inboxDevice(r) != device || device == "" || jarvisRequestPrincipal(r, device) != principal {
			return taskapi.VerifiedContext{}, unifiedError("caller_unbound")
		}
		return taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:" + device, NamespaceGeneration: generation, InvocationGeneration: principal, BindingKind: "paired_human", Transport: "authenticated_ws", TaskSchemaHash: unified.Hash()}, nil
	}}, taskapi.Invocation{})
	if e != nil {
		http.Error(w, "paired principal changed", 403)
		return
	}
	response, handled := h.executeUnified(r.Context(), caller, raw)
	if !handled {
		op := "capabilities"
		response = taskapi.Response{Type: "task_api_unified_response", Version: unified.Version, Authority: h.cfg.InstanceID, CorrelationID: "invalid-request", Operation: &op, Error: taskapi.Failure("invalid_argument", "known_none", "refresh_snapshot")}
	}
	_ = json.NewEncoder(w).Encode(response)
}
func readIdentityToken(r *http.Request) string {
	return strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
}
func (h *Hub) executeUnified(ctx context.Context, caller taskapi.BoundCaller, raw []byte) (taskapi.Response, bool) {
	var req taskapi.Request
	if json.Unmarshal(raw, &req) != nil || req.Type != "task_api_unified_request" {
		return taskapi.Response{}, false
	}
	op := req.Operation
	resp := taskapi.Response{Type: "task_api_unified_response", Version: unified.Version, CorrelationID: req.CorrelationID, Operation: &op, Authority: h.cfg.InstanceID}
	fail := func(e error) (taskapi.Response, bool) {
		var api *taskapi.APIError
		if !errors.As(e, &api) {
			api = taskapi.Failure("unknown", "unknown", "lookup_original")
		}
		resp.Error = api
		return resp, true
	}
	if req.Version != unified.Version {
		return fail(unifiedError("unsupported"))
	}
	if unified.Validate(raw) != nil {
		return fail(unifiedError("invalid_argument"))
	}
	bound, e := caller.Check(ctx)
	if e != nil {
		return fail(e)
	}
	if bound.Authority != h.cfg.InstanceID {
		return fail(unifiedError("wrong_authority"))
	}
	if source, ok := h.registry.Get(bound.SourceSessionID); ok {
		scope, scopeErr := h.TaskWorkerScope(source)
		if scopeErr != nil {
			return fail(scopeErr)
		}
		legacyWorker := false
		if h.delegations != nil {
			legacyWorker, e = h.delegations.HasChild(ctx, source.ID)
			if e != nil {
				return fail(e)
			}
		}
		if scope != nil || legacyWorker {
			allowed := op == "operation_get" || op == "capabilities" || op == "read_document" || op == "read_result" || op == "get" || op == "resolve"
			if !allowed && op != "reply" {
				return fail(unifiedError("permission"))
			}
		}
	}
	var in unifiedInput
	if json.Unmarshal(req.Input, &in) != nil {
		return fail(unifiedError("invalid_argument"))
	}
	if op == "reply" && bound.SourceSessionID != "" && h.delegations != nil {
		child, e := h.delegations.HasChild(ctx, bound.SourceSessionID)
		if e != nil {
			return fail(e)
		}
		if child && (in.Direction != "worker_to_parent" || in.Delivery != "metadata") {
			return fail(unifiedError("permission"))
		}
	}
	if h.unifiedBoundedWorker(bound) && op == "operation_get" && in.OriginalOperation != "reply" {
		return fail(unifiedError("permission"))
	}
	var manager string
	if op != "capabilities" {
		manager, e = h.unifiedManager(bound, in)
		if e != nil {
			return fail(e)
		}
	}
	check := func(ctx context.Context) error {
		now, e := caller.Check(ctx)
		if e != nil {
			return e
		}
		if now != bound {
			return unifiedError("caller_unbound")
		}
		if op != "capabilities" {
			id, e := h.unifiedManager(now, in)
			if e != nil || id != manager {
				return unifiedError("scope_changed")
			}
		}
		return nil
	}
	disclosures := []func(context.Context) error{}
	ctx = context.WithValue(ctx, unifiedDisclosureCollectorKey{}, &disclosures)
	var result any
	switch op {
	case "capabilities":
		result = h.unifiedCapabilities(bound)
	case "dispatch", "continue", "cancel", "qa_bind", "qa_record", "interactions", "snapshot", "events":
		result, e = h.unifiedAdapter(ctx, caller, req, in)
	case "resolve", "get", "read_result":
		var resolved unifiedResolved
		resolved, e = h.resolveUnified(ctx, bound, manager, in.Locator)
		if e == nil {
			if op == "read_result" {
				result, e = h.unifiedReadResult(ctx, resolved, in)
			} else {
				result, e = h.unifiedTaskDTO(ctx, bound, manager, resolved)
			}
		}
	case "queue_snapshot", "queue_move", "queue_edit", "operation_get":
		result, e = h.unifiedQueueOperation(ctx, bound, manager, req, in, check)
	case "reply":
		result, e = h.unifiedReply(ctx, bound, manager, req, in, check)
	case "document_collect", "document_grant", "document_revoke", "read_document":
		result, e = h.unifiedDocument(ctx, bound, manager, req, in, check)
	default:
		e = unifiedError("unsupported")
	}
	if e != nil {
		return fail(e)
	}
	if e = check(ctx); e != nil {
		return fail(e)
	}
	// Native caller verification can itself wait for its original queue read.
	// Recheck every disclosed relation AFTER that verifier's final await.
	for _, disclose := range disclosures {
		if e = disclose(ctx); e != nil {
			return fail(e)
		}
	}
	resp.OK = true
	resp.Result = result
	encoded, encodeErr := json.Marshal(resp)
	if encodeErr != nil || len(encoded) > 8*1024*1024 {
		resp.OK = false
		resp.Result = nil
		return fail(taskapi.Failure("unsupported", "known_none", "reduce_range"))
	}
	return resp, true
}
func (h *Hub) unifiedCapabilities(c taskapi.VerifiedContext) map[string]any {
	ops := []string{"capabilities", "resolve", "get", "read_result", "operation_get", "dispatch", "continue", "cancel", "qa_bind", "qa_record", "interactions", "snapshot", "events", "queue_snapshot", "queue_edit", "queue_move", "reply", "document_collect", "document_grant", "document_revoke", "read_document"}
	if h.unifiedBoundedWorker(c) {
		ops = []string{"capabilities", "resolve", "get", "read_result", "read_document", "reply", "operation_get"}
	}
	queueKinds := []string{"session_input", "delegation_waiting", "controller_waiting_admission"}
	if h.unifiedBoundedWorker(c) {
		queueKinds = []string{}
	}
	features := map[string]string{}
	for i := 1; i <= 19; i++ {
		features[fmt.Sprintf("I%02d", i)] = "adapter_scoped"
	}
	role := "native_agent"
	if c.BindingKind == "paired_human" {
		role = "paired_human"
	}
	return map[string]any{"schema_hash": unified.Hash(), "allowed_operations": ops, "authority_instance_id": h.cfg.InstanceID, "role": role, "features": features, "queue_kinds": queueKinds, "reply_attachments": c.BindingKind == "paired_human", "queue_edit_attachments": c.BindingKind == "paired_human", "limits": map[string]any{"request_bytes": jarvisMaxBytes, "range_bytes": 32768, "document_bytes": 8 * 1024 * 1024, "queue_rows": 4096}, "scope": "original_owner_grants_required", "rollback": "disable_rc6_only_preserve_original_revisions_and_order"}
}
func unifiedOriginalCaller(ctx context.Context, caller taskapi.BoundCaller) (taskapi.BoundCaller, error) {
	return taskapi.BindCaller(ctx, apiVerifier{func() (taskapi.VerifiedContext, error) {
		c, e := caller.Check(ctx)
		if e != nil {
			return c, e
		}
		c.TaskSchemaHash = originalcontract.Hash()
		return c, nil
	}}, taskapi.Invocation{})
}
func (h *Hub) unifiedAdapter(ctx context.Context, caller taskapi.BoundCaller, req taskapi.Request, in unifiedInput) (any, error) {
	var original taskapi.Request
	if json.Unmarshal(in.Original, &original) != nil {
		return nil, unifiedError("invalid_argument")
	}
	allowed := map[string][]string{"dispatch": {"create_dispatch"}, "continue": {"append"}, "cancel": {"cancel"}, "qa_bind": {"append"}, "qa_record": {"append"}, "interactions": {"interactions"}, "snapshot": {"snapshot", "list"}, "events": {"events"}}
	ok := false
	for _, op := range allowed[req.Operation] {
		if original.Operation == op {
			ok = true
		}
	}
	if !ok {
		return nil, unifiedError("invalid_argument")
	}
	if req.Operation == "qa_bind" || req.Operation == "qa_record" {
		var v struct {
			Continuity struct {
				Action string `json:"action"`
			} `json:"continuity"`
		}
		if json.Unmarshal(original.Input, &v) != nil || req.Operation == "qa_bind" && v.Continuity.Action != "bind_qa" || req.Operation == "qa_record" && v.Continuity.Action != "record_review" {
			return nil, unifiedError("invalid_argument")
		}
	}
	bound, e := caller.Check(ctx)
	if e != nil {
		return nil, e
	}
	manager, e := h.unifiedManager(bound, in)
	if e != nil {
		return nil, e
	}
	adapted, e := unifiedOriginalCaller(ctx, caller)
	if e != nil {
		return nil, e
	}
	execute := func(raw []byte) taskapi.Response {
		if (req.Operation == "qa_bind" || req.Operation == "qa_record") && bound.BindingKind == "paired_human" {
			data, e := h.unifiedHumanQA(ctx, caller, req, in, original)
			op := original.Operation
			r := taskapi.Response{Type: "task_api_response", Version: original.Version, CorrelationID: original.CorrelationID, Operation: &op, OK: e == nil, Result: data}
			if e != nil {
				var api *taskapi.APIError
				if !errors.As(e, &api) {
					api = taskapi.Failure("unknown", "unknown", "lookup_original")
				}
				r.Error = api
				r.Result = nil
			}
			return r
		}
		b, _ := adapted.Check(ctx)
		parent, _ := h.registry.Get(b.SourceSessionID)
		if r, handled := h.executeNativeContinuity(ctx, adapted, parent, raw); handled {
			return r
		}
		if r, handled := h.executeInteractions(ctx, adapted, raw); handled {
			return r
		}
		return h.taskService.Execute(ctx, adapted, raw)
	}
	if req.IdempotencyKey == "" {
		r := execute(in.Original)
		if !r.OK {
			return nil, r.Error
		}
		return map[string]any{"adapter_response": r}, nil
	}
	if req.IdempotencyKey != original.IdempotencyKey {
		return nil, unifiedError("idempotency_conflict")
	}
	b, e := adapted.Check(ctx)
	if e != nil {
		return nil, e
	}
	var continuity struct {
		Continuity json.RawMessage `json:"continuity"`
	}
	_ = json.Unmarshal(original.Input, &continuity)
	var locator taskapi.Locator
	if len(continuity.Continuity) > 0 {
		validator, _ := originalcontract.New()
		if validator.Validate(in.Original, "Request") != nil {
			return nil, unifiedError("invalid_argument")
		}
		locator = taskapi.Locator{Authority: h.cfg.InstanceID, Path: "delegation", Operation: original.Operation, Key: original.IdempotencyKey}
		if b.BindingKind != "paired_human" {
			parent, ok := h.registry.Get(b.SourceSessionID)
			if !ok {
				return nil, unifiedError("permission")
			}
			if e = h.continuityCaller(parent, b.SourceRequestID, b.ToolCallID); e != nil {
				return nil, e
			}
		}
	} else {
		locator, e = h.Authorize(ctx, b, original)
		if e != nil {
			return nil, e
		}
	}
	j := h.apiJournals()[locator.Path]
	if j == nil {
		return nil, unifiedError("unsupported")
	}
	check := func(ctx context.Context) error {
		now, e := caller.Check(ctx)
		if e != nil {
			return e
		}
		if now != bound {
			return unifiedError("caller_unbound")
		}
		id, e := h.unifiedManager(now, in)
		if e != nil || id != manager {
			return unifiedError("scope_changed")
		}
		return nil
	}
	m := unifiedMutation(bound, manager, req, check)
	old, found, e := j.UnifiedFacadeRead(ctx, m)
	if e != nil {
		return nil, e
	}
	if found {
		return h.recoverUnifiedFacade(ctx, adapted, j, m, old)
	}
	fresh, e := j.UnifiedFacadeClaim(ctx, m, in.Original, locator)
	if e != nil {
		return nil, e
	}
	if !fresh {
		p, found, e := j.UnifiedFacadeRead(ctx, m)
		if e != nil {
			return nil, e
		}
		if !found {
			return nil, taskapi.Failure("unknown", "unknown", "lookup_original")
		}
		return h.recoverUnifiedFacade(ctx, adapted, j, m, p)
	}
	r := execute(in.Original)
	if !r.OK {
		_, saveErr := j.UnifiedFacadeFinish(ctx, m, r, locator)
		if saveErr != nil {
			return nil, saveErr
		}
		return nil, r.Error
	}
	return j.UnifiedFacadeFinish(ctx, m, r, locator)
}
func (h *Hub) recoverUnifiedFacade(ctx context.Context, caller taskapi.BoundCaller, j *taskapi.Journal, m taskapi.QueueMutation, p taskapi.FacadePointer) (any, error) {
	var original taskapi.Request
	if json.Unmarshal(p.OriginalRequest, &original) != nil {
		return nil, taskapi.Failure("unknown", "unknown", "lookup_original")
	}
	m.Hash = p.Hash
	bound, e := caller.Check(ctx)
	if e != nil {
		return nil, e
	}
	manager := bound.SourceSessionID
	if manager == "" {
		parts := strings.Split(m.Scope, "\x00")
		if len(parts) == 2 {
			manager = parts[1]
		}
	}
	if data, handled, e := h.unifiedContinuityRead(ctx, bound, manager, original); handled {
		if e != nil {
			return nil, e
		}
		if p.Receipt != nil && p.Receipt.State != "unknown" {
			return *p.Receipt, nil
		}
		op := original.Operation
		r := taskapi.Response{Type: "task_api_response", Version: original.Version, CorrelationID: original.CorrelationID, Operation: &op, OK: true, Result: data}
		return j.UnifiedFacadeFinish(ctx, m, r, p.Locator)
	}
	input, _ := json.Marshal(map[string]any{"original_receipt": p.Locator})
	raw, _ := json.Marshal(taskapi.Request{Type: "task_api_request", Version: originalcontract.Version, CorrelationID: "original-unified-recovery", Operation: "get", Input: input})
	r := h.taskService.Execute(ctx, caller, raw)
	if !r.OK {
		return nil, taskapi.Failure("unknown", "unknown", "lookup_original")
	}
	if p.Receipt != nil && p.Receipt.State != "unknown" {
		return *p.Receipt, nil
	}
	// Read the exact original receipt, not task existence/current/latest state.
	var observed struct {
		Locator   taskapi.Locator `json:"locator"`
		Operation string          `json:"operation"`
		Key       string          `json:"idempotency_key"`
		Hash      string          `json:"intent_hash"`
	}
	body, marshalErr := json.Marshal(r.Result)
	digest, hashErr := taskapi.IntentHash(original)
	if marshalErr != nil || hashErr != nil || json.Unmarshal(body, &observed) != nil || observed.Locator != p.Locator || observed.Operation != original.Operation || observed.Key != original.IdempotencyKey || observed.Hash != digest {
		return nil, taskapi.Failure("unknown", "unknown", "lookup_original")
	}
	r.Operation = &original.Operation
	return j.UnifiedFacadeFinish(ctx, m, r, p.Locator)
}
func (h *Hub) resolveUnified(ctx context.Context, c taskapi.VerifiedContext, manager string, l unifiedLocator) (unifiedResolved, error) {
	out := unifiedResolved{Locator: l, Delivery: "unknown"}
	if l.Kind != "task" && l.RoundID != "" && l.RoundID != l.NativeID {
		return out, unifiedError("stale_revision")
	}
	if l.Authority != h.cfg.InstanceID {
		return out, unifiedError("wrong_authority")
	}
	if l.Kind == "workspace" && l.TaskID == "" && l.SessionID != "" && l.RequestID != "" {
		r, e := h.messageQueue.TaskJournal().OriginalCommandForRequest(ctx, l.SessionID, l.RequestID)
		if e != nil {
			return out, e
		}
		if e = h.checkAPIRecord(ctx, c, r); e != nil {
			return out, e
		}
		var meta apiMetadata
		if json.Unmarshal(r.Metadata, &meta) != nil || meta.WorkspaceRoomID == "" || r.NativeID != l.NativeID {
			return out, unifiedError("permission")
		}
		l.TaskID = r.TaskID
		out.Locator.TaskID = r.TaskID
	}
	if l.Kind != "task" && l.Kind != "workspace" && l.Kind != "pm_v1" && l.Kind != "pm_v2" && l.TaskID != "" {
		r, e := h.apiRecord(ctx, c, l.TaskID)
		if e != nil {
			return out, e
		}
		if r.Path != l.Kind || r.NativeID != l.NativeID {
			return out, unifiedError("ambiguous_owner")
		}
	}
	if l.Kind == "task" || l.Kind == "pm_v1" || l.Kind == "pm_v2" || l.Kind == "workspace" {
		if l.TaskID == "" {
			return out, unifiedError("invalid_argument")
		}
		r, e := h.apiRecord(ctx, c, l.TaskID)
		if e != nil {
			return out, e
		}
		if l.Kind == "workspace" {
			var meta apiMetadata
			if json.Unmarshal(r.Metadata, &meta) != nil || meta.WorkspaceRoomID == "" {
				return out, unifiedError("permission")
			}
		}
		if (l.Kind == "pm_v1" || l.Kind == "pm_v2") && r.Path != l.Kind {
			return out, unifiedError("permission")
		}
		if r.Path == "controller" || r.Path == "delegation" {
			native := r.NativeID
			if l.RoundID != "" {
				if r.Path != "delegation" {
					return out, unifiedError("stale_revision")
				}
				round, found, e := h.delegations.Round(ctx, l.RoundID)
				if e != nil {
					return out, e
				}
				root, rootFound, e := h.delegations.Round(ctx, r.NativeID)
				if e != nil || !found || !rootFound || round.LogicalID != root.LogicalID {
					return out, unifiedError("stale_revision")
				}
				native = l.RoundID
			}
			ref := l
			ref.Kind = r.Path
			ref.NativeID = native
			ref.TaskID = ""
			resolved, e := h.resolveUnified(ctx, c, manager, ref)
			if e != nil {
				return out, e
			}
			resolved.Canonical = &r
			resolved.Locator.TaskID = l.TaskID
			return resolved, nil
		}
		if l.NativeID != "" && l.NativeID != r.NativeID {
			return out, unifiedError("ambiguous_owner")
		}
		out.Canonical = &r
		out.Locator.SessionID = r.SessionID
		out.Locator.RequestID = r.RequestID
		out.Locator.NativeID = r.NativeID
		out.Target = h.unifiedIdentity(r.SessionID)
		var meta apiMetadata
		_ = json.Unmarshal(r.Metadata, &meta)
		out.Parent = h.unifiedIdentity(meta.Caller.SourceSessionID)
		out.ReadFence, out.LocalFence, e = h.unifiedDisclosureFence(ctx, c, manager, out)
		if e != nil {
			return out, e
		}
		entry, found, e := h.messageQueue.EffectiveInput(ctx, r.SessionID, r.RequestID)
		if e != nil {
			return out, e
		}
		out.Entry = entry
		out.HasEntry = found
		out.State = "unknown"
		if found {
			out.State = string(entry.State)
			out.ReplyHash = entry.PayloadHash
			var payload queuedPayload
			if json.Unmarshal(entry.Payload, &payload) == nil && payload.ExpectedTarget != nil {
				out.Target = jarvisTarget{h.cfg.InstanceID, r.SessionID, payload.ExpectedTarget.ThreadID, payload.ExpectedTarget.Revision}
			}
		}
		if e := h.unifiedTarget(out.Target, false); e != nil {
			return out, e
		}
		if l.SessionID != "" && l.SessionID != r.SessionID || l.RequestID != "" && l.RequestID != r.RequestID || l.ThreadID != "" && l.ThreadID != out.Target.ThreadID || (l.RevisionKnown || l.Revision != 0) && l.Revision != out.Target.Revision {
			return out, unifiedError("scope_changed")
		}
		out.Locator.ThreadID = out.Target.ThreadID
		out.Locator.Revision = out.Target.Revision
		out.Locator.RevisionKnown = true
		out.CanEdit = r.ScopeID == c.StableScopeID && r.Generation == c.NamespaceGeneration
		return out, nil
	}
	switch l.Kind {
	case "controller":
		if h.dispatches == nil {
			return out, unifiedError("unsupported")
		}
		d, ok, e := h.dispatches.Get(ctx, l.NativeID)
		if e != nil {
			return out, e
		}
		if !ok {
			return out, unifiedError("not_found")
		}
		if d.InstanceID != h.cfg.InstanceID {
			return out, unifiedError("unsupported")
		}
		out.Parent = jarvisTarget{h.cfg.InstanceID, d.ParentID, d.ParentThreadID, d.ParentConfigRevision}
		out.Target = jarvisTarget{d.InstanceID, d.SessionID, d.ThreadID, d.ConfigRevision}
		if !d.ParentConfigKnown {
			record, lookupErr := h.dispatches.TaskJournal().OriginalCommandForRequest(ctx, d.SessionID, d.RequestID)
			var meta apiMetadata
			if lookupErr != nil || json.Unmarshal(record.Metadata, &meta) != nil || !meta.Caller.SourceConfigKnown || meta.Caller.SourceSessionID != d.ParentID || meta.Caller.SourceResumeID != d.ParentThreadID {
				return out, unifiedError("scope_changed")
			}
			out.Parent.Revision = meta.Caller.SourceConfigRevision
		}
		if manager != d.ParentID && manager != d.SessionID && c.SourceSessionID != d.SessionID {
			return out, unifiedError("permission")
		}
		g, e := h.dispatches.Grant(ctx, d.ParentID)
		if e != nil || !g.Allows(h.cfg.InstanceID, d.InstanceID, d.SessionID) {
			return out, unifiedError("permission")
		}
		out.ControllerGrantRevision = g.Revision
		out.Locator.SessionID = d.SessionID
		out.Locator.RequestID = d.RequestID
		out.State = d.State
		out.ReplyHash = d.IntentHash
		out.Delivery = d.DeliveryState
		out.CanEdit = manager == d.ParentID
	case "delegation":
		if h.delegations == nil {
			return out, unifiedError("unsupported")
		}
		d, ok, e := h.delegations.ByID(ctx, l.NativeID)
		if e != nil {
			return out, e
		}
		if !ok {
			return out, unifiedError("not_found")
		}
		if manager != d.ParentSessionID && manager != d.ChildSessionID && c.SourceSessionID != d.ChildSessionID {
			return out, unifiedError("permission")
		}
		thread, rev, bound, e := h.delegations.SourceBinding(ctx, d.ID)
		if e != nil || !bound {
			return out, unifiedError("scope_changed")
		}
		out.Parent = jarvisTarget{h.cfg.InstanceID, d.ParentSessionID, thread, rev}
		out.Target, out.InitializedDelegation, e = h.unifiedDelegationReadTarget(ctx, d, nil)
		if e != nil {
			return out, e
		}
		out.Locator.SessionID = d.ChildSessionID
		out.Locator.RequestID = d.ChildRequestID
		out.State = d.State
		out.ReplyHash = d.IntentHash
		out.Delivery = d.DeliveryState
		out.CanEdit = manager == d.ParentSessionID
	case "request":
		if l.SessionID == "" || l.RequestID == "" {
			return out, unifiedError("invalid_argument")
		}
		r, e := h.messageQueue.TaskJournal().OriginalCommandForRequest(ctx, l.SessionID, l.RequestID)
		if e == nil {
			if e = h.checkAPIRecord(ctx, c, r); e != nil {
				return out, e
			}
			out.Canonical = &r
			var meta apiMetadata
			_ = json.Unmarshal(r.Metadata, &meta)
			out.Parent = h.unifiedIdentity(meta.Caller.SourceSessionID)
			out.Target = h.unifiedIdentity(l.SessionID)
		} else {
			if !errors.Is(e, sql.ErrNoRows) {
				return out, e
			}
			if h.dispatches != nil {
				d, found, err := h.dispatches.UnifiedByExecution(ctx, h.cfg.InstanceID, l.SessionID, l.RequestID)
				if err != nil {
					return out, err
				}
				if found {
					if l.NativeID != "" && l.NativeID != d.ID {
						return out, unifiedError("ambiguous_owner")
					}
					ref := l
					ref.Kind = "controller"
					ref.NativeID = d.ID
					return h.resolveUnified(ctx, c, manager, ref)
				}
			}
			if h.delegations != nil {
				d, found, err := h.delegations.ByChildRequest(ctx, l.SessionID, l.RequestID)
				if err != nil {
					return out, err
				}
				if found {
					if l.NativeID != "" && l.NativeID != d.ID {
						return out, unifiedError("ambiguous_owner")
					}
					ref := l
					ref.Kind = "delegation"
					ref.NativeID = d.ID
					return h.resolveUnified(ctx, c, manager, ref)
				}
			}
			scope, generation, body, replyFound, readErr := h.messageQueue.UnifiedReplySource(ctx, l.SessionID, l.RequestID)
			if readErr != nil {
				return out, readErr
			}
			if replyFound {
				var reply struct {
					Source jarvisTarget `json:"source"`
					Target jarvisTarget `json:"target"`
				}
				if json.Unmarshal(body, &reply) != nil || (scope != c.StableScopeID+"\x00"+manager || generation != c.NamespaceGeneration) && (c.SourceSessionID != reply.Source.SessionID || c.StableScopeID != "session:"+reply.Source.SessionID) {
					return out, unifiedError("permission")
				}
				if reply.Target.SessionID != l.SessionID {
					return out, unifiedError("scope_changed")
				}
				out.Parent = reply.Source
				out.Target = reply.Target
			} else {
				links, e := h.messageQueue.LegacySourceLinks(ctx)
				if e != nil {
					return out, e
				}
				matches := 0
				for _, link := range links {
					if link.Source.SessionID == manager && link.Target.SessionID == l.SessionID && link.RequestID == l.RequestID && h.legacyReadAllowed(c, link) {
						matches++
						out.Parent = jarvisTarget{link.Source.InstanceID, link.Source.SessionID, link.Source.ThreadID, link.Source.Revision}
						out.Target = jarvisTarget{link.Target.InstanceID, link.Target.SessionID, link.Target.ThreadID, link.Target.Revision}
					}
				}
				if matches != 1 {
					return out, unifiedError("permission")
				}
			}
		}
		out.CanEdit = true
	default:
		return out, unifiedError("unsupported")
	}
	if out.Parent.SessionID != "" {
		if e := h.unifiedTarget(out.Parent, false); e != nil {
			return out, e
		}
	}
	if out.Target.ThreadID != "" {
		if e := h.unifiedTarget(out.Target, false); e != nil {
			return out, e
		}
	}
	if l.SessionID != "" && l.SessionID != out.Locator.SessionID || l.RequestID != "" && l.RequestID != out.Locator.RequestID || l.ThreadID != "" && l.ThreadID != out.Target.ThreadID || (l.RevisionKnown || l.Revision != 0) && l.Revision != out.Target.Revision {
		return out, unifiedError("scope_changed")
	}
	out.Locator.ThreadID = out.Target.ThreadID
	out.Locator.Revision = out.Target.Revision
	out.Locator.RevisionKnown = true
	var err error
	out.ReadFence, out.LocalFence, err = h.unifiedDisclosureFence(ctx, c, manager, out)
	if err != nil {
		return out, err
	}
	e, found, err := h.messageQueue.EffectiveInput(ctx, out.Locator.SessionID, out.Locator.RequestID)
	if err != nil {
		return out, err
	}
	out.Entry = e
	out.HasEntry = found
	if found {
		out.ReplyHash = e.PayloadHash
	}
	if found {
		out.State = string(e.State)
	}
	return out, nil
}
func (h *Hub) unifiedTaskDTO(ctx context.Context, c taskapi.VerifiedContext, manager string, r unifiedResolved) (result any, err error) {
	defer func() {
		if err == nil {
			if e := r.disclosureCurrent(ctx); e != nil {
				result = nil
				err = e
			}
		}
	}()
	data := map[string]any{}
	if r.Canonical != nil {
		d, e := h.apiTask(ctx, *r.Canonical)
		if e != nil {
			return nil, e
		}
		data = d
	}
	if effective, err := h.unifiedWorkEffective(ctx, messagequeue.WorkLink{Kind: r.Locator.Kind, Authority: h.cfg.InstanceID, SessionID: r.Locator.SessionID, RequestID: r.Locator.RequestID, NativeID: r.Locator.NativeID}); err != nil {
		return nil, err
	} else if effective != nil {
		data["effective_input"] = effective
	}
	execution := map[string]any{"admission": r.State, "queue": r.State, "native_start": "unknown", "terminal": "unknown", "source": "original_store"}
	if r.HasEntry {
		switch r.Entry.State {
		case messagequeue.Completed, messagequeue.Failed, messagequeue.Cancelled:
			execution["terminal"] = string(r.Entry.State)
		}
		if start, ok, e := h.messageQueue.NativeStart(r.Entry.SessionID, r.Entry.RequestID); e != nil {
			return nil, e
		} else if ok {
			execution["native_start"] = "observed"
			execution["start_proof"] = start
		}
	}
	h.unifiedDirectReplyFinal(ctx, r)
	var final any
	target, targetFound := h.registry.Get(r.Locator.SessionID)
	var seal messagequeue.TaskSeal
	var e error
	if targetFound {
		seal, e = h.jarvisResultSeal(ctx, target, r.Locator.RequestID, "")
	} else {
		e = unifiedError("scope_changed")
	}
	if e == nil {
		final = map[string]any{"owner_request_id": seal.RequestID, "seal_id": seal.ID, "full_hash": seal.Hash, "total_bytes": len([]byte(seal.Text)), "anchor": json.RawMessage(seal.Anchor), "verified_bytes": false}
	}
	routes := []any{}
	if r.Parent.SessionID != "" && r.Target.SessionID != "" && r.Target.ThreadID != "" {
		source, target, dir := r.Parent, r.Target, "parent_to_worker"
		if c.SourceSessionID == r.Target.SessionID || manager == r.Target.SessionID {
			source, target, dir = r.Target, r.Parent, "worker_to_parent"
		}
		if manager == r.Parent.SessionID || manager == r.Target.SessionID || c.SourceSessionID == r.Target.SessionID {
			hash := r.ReplyHash
			if hash != "" {
				routes = append(routes, map[string]any{"direction": dir, "source": source, "target": target, "reply_to_request_id": r.Locator.RequestID, "reply_to_hash": hash, "delivery": []string{"metadata", "queue"}})
			}
		}
	}
	var edit any
	if r.HasEntry && r.Entry.State == messagequeue.Queued && r.CanEdit {
		var payload queuedPayload
		if json.Unmarshal(r.Entry.Payload, &payload) != nil {
			return nil, unifiedError("invalid_argument")
		}
		userContent := payload.Content
		if r.Entry.ContentRevision > 1 {
			userContent = r.Entry.Content
		} else if r.Canonical != nil {
			var metadata apiMetadata
			if json.Unmarshal(r.Canonical.Metadata, &metadata) == nil && metadata.Intake != nil {
				userContent = metadata.Intake.Goal
			}
		}
		if r.Entry.ContentRevision <= 1 && r.Locator.Kind == "controller" {
			if d, ok, err := h.dispatches.Get(ctx, r.Locator.NativeID); err != nil {
				return nil, err
			} else if ok {
				userContent = d.Content
			}
		}
		if r.Entry.ContentRevision <= 1 && r.Locator.Kind == "delegation" {
			if d, ok, err := h.delegations.ByID(ctx, r.Locator.NativeID); err != nil {
				return nil, err
			} else if ok {
				userContent = d.Instruction
			}
		}
		if r.Entry.ContentRevision <= 1 && payload.UnifiedBinding != nil {
			_, _, body, found, err := h.messageQueue.UnifiedReplySource(ctx, r.Entry.SessionID, r.Entry.RequestID)
			if err != nil {
				return nil, err
			}
			if found {
				var reply struct {
					Content string `json:"content"`
				}
				if json.Unmarshal(body, &reply) != nil {
					return nil, unifiedError("unknown")
				}
				userContent = reply.Content
			}
		}
		edit = map[string]any{"queue": unifiedQueue{"session_input", r.Target}, "content": userContent, "content_revision": r.Entry.ContentRevision, "content_hash": r.Entry.EffectiveHash, "attachments_preserved": true}
	}
	if !r.HasEntry && r.CanEdit && (r.Locator.Kind == "controller" || r.Locator.Kind == "delegation") {
		q := unifiedQueue{"controller_waiting_admission", r.Target}
		if r.Locator.Kind == "delegation" {
			q = unifiedQueue{"delegation_waiting", h.unifiedIdentity(manager)}
		}
		view, e := h.unifiedQueueView(ctx, q, nil)
		if e != nil {
			return nil, e
		}
		for _, row := range view.Items {
			if row.ID == r.Locator.NativeID && row.State == "queued" {
				var content string
				if r.Locator.Kind == "controller" {
					d, ok, e := h.dispatches.Get(ctx, row.ID)
					if e != nil {
						return nil, e
					}
					if !ok {
						return nil, unifiedError("not_found")
					}
					content, e = h.dispatches.EffectiveContent(ctx, q.Target.SessionID, row.ID, d.Content)
					if e != nil {
						return nil, e
					}
				} else {
					d, ok, e := h.delegations.ByID(ctx, row.ID)
					if e != nil {
						return nil, e
					}
					if !ok {
						return nil, unifiedError("not_found")
					}
					content, e = h.delegations.EffectiveInstruction(ctx, row.ID, d.Instruction)
					if e != nil {
						return nil, e
					}
				}
				edit = map[string]any{"queue": q, "content": content, "content_revision": row.ContentRevision, "content_hash": row.ContentHash, "attachments_preserved": true}
			}
		}
	}
	return map[string]any{"locator": r.Locator, "owner": r.Target, "execution": execution, "final": final, "delivery": r.Delivery, "qa": "original_selected_round_only", "data": data, "reply_routes": routes, "edit": edit}, nil
}
func (h *Hub) unifiedReadResult(ctx context.Context, r unifiedResolved, in unifiedInput) (result any, err error) {
	defer func() {
		if err == nil {
			if e := r.disclosureCurrent(ctx); e != nil {
				result = nil
				err = e
			}
		}
	}()
	if in.OwnerRequest != r.Locator.RequestID {
		return nil, unifiedError("ambiguous_owner")
	}
	target, ok := h.registry.Get(r.Locator.SessionID)
	if !ok {
		return nil, unifiedError("scope_changed")
	}
	seal, e := h.jarvisResultSeal(ctx, target, in.OwnerRequest, in.SealID)
	if e != nil {
		return nil, e
	}
	if seal.Hash != in.Hash {
		return nil, unifiedError("stale_revision")
	}
	return unifiedChunk(r.Locator, seal.RequestID, seal.ID, seal.Hash, []byte(seal.Text), in)
}
func unifiedChunk(l unifiedLocator, request, seal, hash string, b []byte, in unifiedInput) (any, error) {
	if in.Offset < 0 || in.Offset > int64(len(b)) || in.Limit < 1 || in.Limit > 32768 {
		return nil, unifiedError("invalid_argument")
	}
	end := in.Offset + in.Limit
	if end > int64(len(b)) {
		end = int64(len(b))
	}
	chunk := b[in.Offset:end]
	return map[string]any{"locator": l, "owner_request_id": request, "seal_id": seal, "full_hash": hash, "total_bytes": len(b), "offset": in.Offset, "next_offset": end, "data_base64": base64.StdEncoding.EncodeToString(chunk), "chunk_hash": taskapi.RawDigest(chunk), "has_more": end < int64(len(b))}, nil
}
