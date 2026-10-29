package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	contract "everything-go/contracts/task-api/controller-results-v9"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	cr "everything-go/internal/controllerresults"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	"everything-go/internal/taskpipeline"
	"time"
)

func (h *Hub) observeControllerResult(ctx context.Context, r sessiondispatch.Record) (cr.Scope, bool) {
	if r.DeliveryState == "inbound" {
		return cr.Scope{}, false
	}
	if r.State != "completed" && r.State != "failed" && r.State != "cancelled" && r.State != "rejected" {
		return cr.Scope{}, false
	}
	original, e := h.controllerResultOriginalParent(ctx, r)
	if e != nil {
		return cr.Scope{}, false
	}
	r = original
	parent, ok := h.registry.Get(r.ParentID)
	if !ok {
		return cr.Scope{}, false
	}
	scope := cr.Scope{Parent: cr.Identity{h.cfg.InstanceID, r.ParentID, r.ParentThreadID, r.ParentConfigRevision}, Generation: parent.Snapshot().SourceRevision}
	if !r.ParentConfigKnown || parent.ResumeID() != r.ParentThreadID || parent.SettingsSnapshot().ConfigRevision != r.ParentConfigRevision {
		return scope, false
	}
	grant, grantErr := h.dispatches.Grant(ctx, r.ParentID)
	if grantErr != nil {
		return scope, false
	}
	scope.GrantRevision = grant.Revision
	target, ok := h.registry.Get(r.SessionID)
	verified := r.InstanceID == h.cfg.InstanceID && ok && target.ResumeID() == r.ThreadID && target.SettingsSnapshot().ConfigRevision == r.ConfigRevision
	request := r.RequestID
	turn := ""
	alias := false
	var entry messagequeue.Entry
	var found bool
	e = nil
	if r.InstanceID == h.cfg.InstanceID {
		entry, found, e = h.messageQueue.GetContext(ctx, r.SessionID, r.RequestID)
	}
	if e != nil || !found {
		verified = false
	} else if r.Mode == "steer" {
		if entry.State != messagequeue.Steered || !h.messageQueue.StrictSteerProven(entry) || entry.ActiveRequestID == "" {
			verified = false
		} else {
			request = entry.ActiveRequestID
			turn = entry.TurnID
			alias = true
		}
	}
	var native messagequeue.NativeReceipt
	var known bool
	if r.InstanceID == h.cfg.InstanceID {
		native, known, e = h.messageQueue.ExactNativeAcceptanceContext(ctx, r.SessionID, request)
	}
	quarantined := false
	if errors.Is(e, messagequeue.ErrNativeConflict) {
		quarantined = h.dispatches.Results().Quarantine(ctx, r.ID, "canonical_native_conflict") == nil
	}
	if e != nil || !known || native.ThreadID != r.ThreadID || (turn != "" && native.TurnID != turn) {
		verified = false
	}
	turn = native.TurnID
	var final *cr.Final
	if verified && r.State == "completed" {
		seal, e := h.jarvisResultSeal(ctx, target, request, "")
		if e == nil {
			final = &cr.Final{SealID: seal.ID, Hash: seal.Hash, Bytes: len([]byte(seal.Text)), Request: request}
		}
	}
	historical := r.DeliveryState == "delivered" || r.DeliveryState == "not_requested"
	_, legacyFound, e := h.messageQueue.GetContext(ctx, r.ParentID, "screturn_"+r.ID)
	if e != nil {
		return scope, false
	}
	_, e = h.dispatches.Results().Observe(ctx, cr.Observation{Scope: scope, Execution: cr.Execution{Target: cr.Identity{r.InstanceID, r.SessionID, r.ThreadID, r.ConfigRevision}, Request: request, Turn: turn}, Member: cr.Member{DispatchID: r.ID, Request: r.RequestID, Mode: r.Mode, IntentHash: r.IntentHash, AliasProven: alias, LegacyDelivery: r.DeliveryState, CreatedAt: r.CreatedAt}, State: r.State, Final: final, Verified: verified, Historical: historical, LegacyOutstanding: legacyFound && !historical, CompletedAt: r.TerminalAt})
	return scope, e == nil || quarantined
}
func (h *Hub) reconcileControllerResults(ctx context.Context) {
	records, e := h.dispatches.IndexRecords(ctx)
	if e != nil {
		return
	}
	scopes := map[string]cr.Scope{}
	for _, r := range records {
		h.projectControllerResultDelivery(ctx, &r)
		if scope, ok := h.observeControllerResult(ctx, r); ok {
			scopes[scope.Key()] = scope
		}
	}
	for _, scope := range scopes {
		h.reconcileControllerResultApplications(ctx, scope)
		h.reconcileControllerResultBatch(ctx, scope)
		h.publishControllerResultInvalidation(ctx, scope)
	}
}
func (h *Hub) controllerResultWaitReason(ctx context.Context, scope cr.Scope, c cr.Control, mqLocked bool) string {
	if c.Paused {
		return "paused"
	}
	if c.Mode != "resume_authorized" {
		return "notify_only"
	}
	if !cr.ControlAuthorized(scope, c) {
		return "scope_changed"
	}
	if c.Deadline <= crNow() || c.Issued >= c.MaxBatches || c.NoProgress >= c.MaxNoProgress {
		return "budget_exhausted"
	}
	if h.resultDisclosure(ctx, scope) != nil {
		return "scope_changed"
	}
	parent, ok := h.registry.Get(scope.Parent.Session)
	if !ok {
		return "scope_changed"
	}
	if !h.controls.MobileMayWrite(parent.ID) {
		return "control_owner"
	}
	if !h.controllerResultMaintenanceIdle(parent.ID, mqLocked) {
		return "audio_or_maintenance"
	}
	h.voiceMu.Lock()
	audio := false
	for _, call := range h.voiceClients {
		if call.session == parent {
			audio = true
		}
	}
	h.voiceMu.Unlock()
	if audio {
		return "audio_or_maintenance"
	}
	q, e := h.messageQueue.SnapshotContext(ctx, parent.ID)
	if e != nil {
		return "native_state_unknown"
	}
	if parent.QueueLen() != 0 {
		return "user_queue"
	}
	for _, entry := range q.Items {
		switch entry.State {
		case messagequeue.Queued, messagequeue.Running, messagequeue.Steering:
			return "user_queue"
		case messagequeue.Uncertain:
			return "native_state_unknown"
		case messagequeue.Completed, messagequeue.Failed, messagequeue.Cancelled, messagequeue.Steered:
		default:
			return "native_state_unknown"
		}
	}
	if parent.State() != session.Idle || parent.ActiveQueuedID() != "" {
		return "parent_busy"
	}
	views := h.runtimes.Snapshot("", []string{parent.ID})
	if len(views) != 1 {
		return "native_state_unknown"
	}
	if runtimePhaseActive(views[0].Phase) {
		return "parent_busy"
	}
	if views[0].Phase == "interrupted" || views[0].Phase == "" {
		return "native_state_unknown"
	}
	owned, e := h.messageQueue.PipelineStore().HasRecoveryOwner(ctx, parent.ID)
	if e != nil {
		return "native_state_unknown"
	}
	if owned {
		return "mission_owner"
	}
	if reader, ok := h.exec.(backend.QueuedSubmissionReader); ok {
		bounded, cancel := context.WithTimeout(ctx, 6*time.Second)
		defer cancel()
		e := reader.QueuedSubmissionReady(bounded, parent, "")
		if errors.Is(e, backend.ErrNativeSubmissionBusy) {
			return "parent_busy"
		}
		if e != nil {
			return "native_state_unknown"
		}
	}
	return "ready"
}
func (h *Hub) controllerResultBoundary(ctx context.Context, scope cr.Scope, mqLocked ...bool) error {
	status, e := h.dispatches.Results().Status(ctx, scope)
	if e != nil {
		return e
	}
	c := status.Control
	// A frozen batch already consumed its bounded authorization at Freeze; its
	// ORIGINAL admission is not a second effect. Unknown is never discounted.
	for _, b := range status.Batches {
		if b.State == "frozen" && b.ControlRevision == c.Revision && c.Issued > 0 {
			c.Issued--
			break
		}
	}
	if h.controllerResultWaitReason(ctx, scope, c, len(mqLocked) > 0 && mqLocked[0]) != "ready" {
		return cr.ErrScope
	}
	return nil
}
func (h *Hub) reconcileControllerResultBatch(ctx context.Context, scope cr.Scope) {
	store := h.dispatches.Results()
	status, e := store.Status(ctx, scope)
	if e != nil {
		return
	}
	// Original receipts, including UNKNOWN, always win over a fresh admission.
	for _, b := range status.Batches {
		if b.State == "completed" || b.State == "failed" || b.State == "cancelled" || b.State == "retired" {
			continue
		}
		entry, found, e := h.messageQueue.GetContext(ctx, scope.Parent.Session, b.Request)
		if e != nil {
			return
		}
		if found {
			if !h.controllerResultEntryMatches(scope, b, entry) {
				_ = store.SetBatchState(ctx, b.ID, "unknown", "")
				return
			}
			state := string(entry.State)
			if state == "uncertain" || state == "steering" || state == "steered" {
				state = "unknown"
			}
			native, known, err := h.messageQueue.ExactNativeAcceptanceContext(ctx, scope.Parent.Session, b.Request)
			if err != nil {
				state = "unknown"
			}
			turn := ""
			if known {
				turn = native.TurnID
			}
			if state == "completed" && (!known || turn == "") {
				state = "unknown"
			}
			if state == "completed" {
				parent, exists := h.registry.Get(scope.Parent.Session)
				if !exists {
					state = "unknown"
				} else if _, sealErr := h.jarvisResultSeal(ctx, parent, b.Request, ""); sealErr != nil {
					state = "unknown"
				}
			}
			_ = store.SetBatchState(ctx, b.ID, state, turn)
			return
		}
		if b.State != "frozen" {
			return
		}
		current, e := store.Control(ctx, scope)
		if e != nil {
			return
		}
		if !cr.ControlAuthorized(scope, current) || current.Revision != b.ControlRevision || current.Paused || current.Mode != "resume_authorized" {
			// Frozen+positive original MQ absence means dispatch-only intent; retire
			// ONLY before writer admission. Unknown/queued receipts never take this path.
			h.messageQueueMu.Lock()
			_, exists, err := h.messageQueue.GetContext(ctx, scope.Parent.Session, b.Request)
			if err == nil && !exists {
				_ = store.SetBatchState(ctx, b.ID, "retired", "")
			}
			h.messageQueueMu.Unlock()
			return
		}
	}
	if h.controllerResultBoundary(ctx, scope) != nil {
		return
	}
	batch, _, e := store.Freeze(ctx, scope, func() error { return nil })
	if e != nil || batch.ID == "" {
		return
	}
	if h.controllerResultFactsCurrent(ctx, scope, batch.Versions) != nil {
		return
	}
	// Original MQ writer mutex serializes human input/pause and the final check.
	// No dispatch SQL transaction is held while entering MQ or invoking a provider.
	parent, _ := h.registry.Get(scope.Parent.Session)
	client := &Client{hub: h, deviceID: "controller-result-v2", controllerResultAdmission: &messagequeue.ControllerResultAdmission{Batch: batch.ID, Scope: scope.Key(), Session: scope.Parent.Session, Request: batch.Request}, send: make(chan []byte, 8), quit: make(chan struct{}), ctx: ctx, controllerResultFence: func() error {
		c, e := store.Control(ctx, scope)
		if e != nil {
			return e
		}
		if !cr.ControlAuthorized(scope, c) || c.Revision != batch.ControlRevision || c.Paused || c.Mode != "resume_authorized" || c.Deadline <= crNow() {
			return cr.ErrScope
		}
		status, err := store.Status(ctx, scope)
		if err != nil {
			return err
		}
		active := false
		for _, v := range status.Batches {
			if v.ID == batch.ID && v.State == "frozen" && v.PayloadHash == batch.PayloadHash {
				active = true
			}
		}
		if !active {
			return cr.ErrScope
		}
		if err = h.controllerResultFactsCurrent(ctx, scope, batch.Versions); err != nil {
			return err
		}
		return h.controllerResultBoundary(ctx, scope, true)
	}}
	if h.enqueueChatMessageExpected(client, clientproto.Command{Kind: "message", SessionID: scope.Parent.Session, RequestID: batch.Request, Content: batch.Payload, MessagePurpose: "result_return"}, &dispatchTargetExpectation{ThreadID: scope.Parent.Thread, Revision: scope.Parent.Revision}) {
		_ = store.SetBatchState(ctx, batch.ID, "queued", "")
	} else {
		// Enqueue false is not positive absence. Preserve original key and reconcile
		// only that receipt; never build a new batch to make the retry pass.
		_ = store.SetBatchState(ctx, batch.ID, "unknown", "")
	}
	_ = parent
}
func crNow() int64 { return controllerResultClock() }

var controllerResultClock = func() int64 { return timeNowMillis() }

func timeNowMillis() int64 { return time.Now().UnixMilli() }

// Only metadata invalidations go over connected clients. Reads subscribe to an
// exact scope and identity epoch; revocation retires the subscription, not rows.
type controllerResultSubscription struct {
	Scope      cr.Scope
	Principal  string
	Generation uint64
	High       uint64
}

func (h *Hub) subscribeControllerResults(ctx context.Context, c *Client, raw []byte, r taskapi.Response) {
	if !r.OK {
		return
	}
	var req taskapi.Request
	if json.Unmarshal(raw, &req) != nil || req.Type != "task_api_controller_results_request" || (req.Operation != "read_dispatch_inbox" && req.Operation != "dispatch_result_events" && req.Operation != "get_dispatch_return_status") {
		return
	}
	var in controllerResultInput
	if json.Unmarshal(req.Input, &in) != nil {
		return
	}
	caller, e := h.taskHuman(c)
	if e != nil {
		return
	}
	b, e := caller.Check(ctx)
	if e != nil {
		return
	}
	scope, e := h.resultScope(ctx, b, in.Manager)
	if e != nil {
		return
	}
	high := uint64(0)
	encoded, _ := json.Marshal(r.Result)
	var observed struct {
		High uint64 `json:"high_water"`
	}
	if json.Unmarshal(encoded, &observed) == nil {
		high = observed.High
	}
	// Use the response snapshot watermark, never a later store query: a commit
	// between snapshot and subscription must generate the next invalidation.
	c.controllerResultSubscriptions.Store(scope.Key(), controllerResultSubscription{scope, b.StableScopeID, b.NamespaceGeneration, high})
}
func (h *Hub) publishControllerResultInvalidation(ctx context.Context, scope cr.Scope) {
	h.mu.Lock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	for _, c := range clients {
		v, ok := c.controllerResultSubscriptions.Load(scope.Key())
		if !ok {
			continue
		}
		sub := v.(controllerResultSubscription)
		caller, e := h.taskHuman(c)
		if e != nil {
			c.controllerResultSubscriptions.Delete(scope.Key())
			continue
		}
		b, e := caller.Check(ctx)
		if e != nil || b.StableScopeID != sub.Principal || b.NamespaceGeneration != sub.Generation {
			c.controllerResultSubscriptions.Delete(scope.Key())
			continue
		}
		if h.resultDisclosure(ctx, sub.Scope) != nil {
			// This socket still owns the original paired identity. Retire only its
			// already-known subscription; disclose no watermark, membership or body.
			c.enqueueEvent(map[string]any{"type": "controller_results_retired", "api_version": contract.Version, "manager": sub.Scope.Parent, "scope_hash": sub.Scope.Key(), "reason": "scope_changed"})
			c.controllerResultSubscriptions.Delete(scope.Key())
			continue
		}
		status, e := h.dispatches.Results().Status(ctx, scope)
		if e != nil || status.HighWater <= sub.High {
			continue
		}
		c.enqueueEvent(map[string]any{"type": "controller_results_invalidated", "api_version": contract.Version, "manager": scope.Parent, "scope_hash": scope.Key(), "high_water": status.HighWater})
		sub.High = status.HighWater
		c.controllerResultSubscriptions.Store(scope.Key(), sub)
	}
}

// Called with an explicit lock state only by the original MQ admission fence.
func (h *Hub) controllerResultMaintenanceIdle(id string, locked bool) bool {
	if h.toolRepairRunning.Load() {
		return false
	}
	if !locked {
		h.messageQueueMu.Lock()
		defer h.messageQueueMu.Unlock()
	}
	return h.maintenanceHolds[id] == nil
}

// A legacy receipt may truthfully project delivered ONLY once its exact v2
// coverage has a formal original parent final. Never create a synthetic seal.
func (h *Hub) projectControllerResultDelivery(ctx context.Context, r *sessiondispatch.Record) {
	if r.DeliveryState != "pending" {
		return
	}
	b, v, covered, e := h.dispatches.Results().Link(ctx, r.ID)
	if e != nil || !covered || b.State != "completed" || v.Version == 0 {
		return
	}
	p, ok := h.registry.Get(r.ParentID)
	if !ok || p.ResumeID() != r.ParentThreadID || !r.ParentConfigKnown || p.SettingsSnapshot().ConfigRevision != r.ParentConfigRevision {
		return
	}
	scope := cr.Scope{Parent: cr.Identity{h.cfg.InstanceID, p.ID, p.ResumeID(), p.SettingsSnapshot().ConfigRevision}, Generation: p.Snapshot().SourceRevision}
	same, e := h.dispatches.Results().LinkScopeCurrent(ctx, r.ID, scope)
	if e != nil || !same {
		return
	}
	if _, e = h.jarvisResultSeal(ctx, p, b.Request, ""); e != nil {
		return
	}
	r.DeliveryState = "delivered"
	_ = h.dispatches.Put(ctx, *r)
}

func (h *Hub) controllerResultEntryMatches(scope cr.Scope, b cr.Batch, e messagequeue.Entry) bool {
	if e.SessionID != b.Parent.Session || e.RequestID != b.Request || b.Parent != scope.Parent || cr.TextHash(b.Payload) != b.PayloadHash {
		return false
	}
	p, ok := h.registry.Get(e.SessionID)
	if !ok {
		return false
	}
	expected := &dispatchTargetExpectation{ThreadID: b.Parent.Thread, Revision: b.Parent.Revision}
	intent, _ := json.Marshal(queuedPayload{Content: b.Payload, MessagePurpose: "result_return", ExpectedTarget: expected})
	hash := sha256.Sum256(intent)
	if e.PayloadHash != hex.EncodeToString(hash[:]) {
		return false
	}
	config := session.ConfigurationFrom(p.SettingsSnapshot())
	full, _ := json.Marshal(queuedPayload{Content: b.Payload, MessagePurpose: "result_return", ExpectedTarget: expected, Configuration: &config})
	proof, found, err := h.messageQueue.ControllerResultInput(context.Background(), e.SessionID, e.RequestID)
	if err != nil || !found || proof.Batch != b.ID || proof.Scope != scope.Key() || proof.IntentHash != e.PayloadHash || !bytes.Equal(proof.Payload, full) {
		return false
	}
	if len(e.Payload) == 0 {
		return e.State == messagequeue.Completed || e.State == messagequeue.Failed || e.State == messagequeue.Cancelled
	}
	return bytes.Equal(e.Payload, full)
}
func (h *Hub) controllerResultFactsCurrent(ctx context.Context, scope cr.Scope, versions []cr.Version) error {
	for _, v := range versions {
		page, e := h.dispatches.Results().Members(ctx, scope, v.ID, "", 1)
		if e != nil {
			return e
		}
		g := page.Group
		if g.Version != v.Version || g.FactHash != v.Hash || g.State == "conflicted" || g.State == "pending_final" || g.State == "historical_unverified" {
			return cr.ErrScope
		}
		target, ok := h.registry.Get(g.Execution.Target.Session)
		if !ok || h.unifiedTarget(resultTarget(g.Execution.Target), false) != nil {
			return cr.ErrScope
		}
		native, known, e := h.messageQueue.ExactNativeAcceptanceContext(ctx, target.ID, g.Execution.Request)
		if e != nil || !known || native.ThreadID != g.Execution.Target.Thread || native.TurnID != g.Execution.Turn {
			return cr.ErrScope
		}
		if g.State == "final" {
			seal, e := h.jarvisResultSeal(ctx, target, g.Execution.Request, "")
			if e != nil || g.Final == nil || seal.ID != g.Final.SealID || seal.Hash != g.Final.Hash {
				return cr.ErrScope
			}
		} else {
			entry, found, e := h.messageQueue.GetContext(ctx, target.ID, g.Execution.Request)
			if e != nil || !found || string(entry.State) != g.State {
				return cr.ErrScope
			}
		}
	}
	return nil
}

// Upgrade only a read projection from the immutable ORIGINAL TaskAPI journal;
// never guess a revision from the current registry or rewrite the legacy row.
func (h *Hub) controllerResultOriginalParent(ctx context.Context, r sessiondispatch.Record) (sessiondispatch.Record, error) {
	if r.ParentConfigKnown {
		return r, nil
	}
	original, e := h.dispatches.TaskJournal().OriginalCommandForRequest(ctx, r.SessionID, r.RequestID)
	if e != nil {
		return r, e
	}
	var m apiMetadata
	if json.Unmarshal(original.Metadata, &m) != nil || !m.Caller.SourceConfigKnown || m.Caller.SourceSessionID != r.ParentID || m.Caller.SourceResumeID != r.ParentThreadID || m.Caller.SourceRequestID != r.OriginRequestID || m.Caller.Authority != h.cfg.InstanceID || original.NativeID != r.ID {
		return r, cr.ErrScope
	}
	if m.Caller.BindingKind != "native_tool" && m.Caller.BindingKind != "mcp_process_binding" {
		return r, cr.ErrScope
	}
	r.ParentConfigRevision = m.Caller.SourceConfigRevision
	r.ParentConfigKnown = true
	return r, nil
}

func (h *Hub) reconcileControllerResultApplications(ctx context.Context, scope cr.Scope) {
	manager := taskpipeline.Identity{Authority: scope.Parent.Authority, Session: scope.Parent.Session, Thread: scope.Parent.Thread, Revision: scope.Parent.Revision}
	apps, e := h.messageQueue.PipelineStore().ControllerApplications(ctx, manager, scope.Generation)
	if e != nil {
		return
	}
	for _, app := range apps {
		if h.pipelineIssuerCurrent(ctx, app.Issuer, app.Definition) != nil {
			continue
		}
		f := app.Fact
		if !taskpipeline.PredicateMet(taskpipeline.Predicate{Kind: "success_sealed"}, f) {
			continue
		}
		target := cr.Identity{f.Source.Authority, f.Source.Session, f.Source.Thread, f.Source.Revision}
		g, found, e := h.dispatches.Results().MatchFact(ctx, scope, target, f.Source.Request, f.SealID, f.FullHash)
		if e != nil || !found {
			continue
		}
		native, known, e := h.messageQueue.ExactNativeAcceptanceContext(ctx, app.Processing.Session, app.ProcessingRequest)
		if e != nil || !known || native.ThreadID != app.Processing.Thread || (app.ProcessingTurn != "" && native.TurnID != app.ProcessingTurn) || h.unifiedTarget(pipelineTarget(app.Processing), false) != nil {
			continue
		}
		if app.Final != nil {
			processing, ok := h.registry.Get(app.Processing.Session)
			if !ok {
				continue
			}
			seal, e := h.jarvisResultSeal(ctx, processing, app.ProcessingRequest, app.Final.SealID)
			if e != nil || seal.Hash != app.Final.FullHash {
				continue
			}
		}
		proof := cr.MissionApplication{RecordID: app.RecordID, DefinitionRevision: app.DefinitionRevision, CheckpointRevision: app.CheckpointRevision, EventID: app.EventID, ContinuationID: app.ContinuationID, IntentID: app.IntentID, InputHash: app.InputHash, Processing: cr.Identity{app.Processing.Authority, app.Processing.Session, app.Processing.Thread, app.Processing.Revision}, ProcessingRequest: app.ProcessingRequest, GroupVersion: g.Version, FactHash: g.FactHash}
		_ = h.dispatches.Results().ApplyMission(ctx, scope, g.ID, proof)
	}
}

// The controller pause can only restrict an existing mission's recovery of
// these exact receipt facts; default notify-only never creates new authority.
func (h *Hub) controllerResultMissionFence(ctx context.Context, manager taskpipeline.Identity, generation uint64, facts []taskpipeline.Fact) error {
	if h.dispatches == nil {
		return nil
	} // No controller ledger/control exists; retain original mission behavior.
	scope := cr.Scope{Parent: cr.Identity{manager.Authority, manager.Session, manager.Thread, manager.Revision}, Generation: generation}
	control, e := h.dispatches.Results().Control(ctx, scope)
	if e != nil {
		return e
	}
	if control.Revision == 0 || !control.Paused {
		return nil
	}
	for _, f := range facts {
		if !taskpipeline.PredicateMet(taskpipeline.Predicate{Kind: "success_sealed"}, f) {
			continue
		}
		_, found, e := h.dispatches.Results().MatchFact(ctx, scope, cr.Identity{f.Source.Authority, f.Source.Session, f.Source.Thread, f.Source.Revision}, f.Source.Request, f.SealID, f.FullHash)
		if e != nil {
			return e
		}
		if found {
			return cr.ErrScope
		}
	}
	return nil
}
