package core

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"fmt"
	"strings"
)

func batchCovered(r protocol.AsyncBatchRequest) []protocol.AsyncCoveredQuestion {
	out := []protocol.AsyncCoveredQuestion{}
	for _, q := range r.Selected {
		out = append(out, protocol.AsyncCoveredQuestion{RequestID: q.RequestID, QuestionRevision: q.QuestionRevision})
	}
	return out
}
func (h *Hub) projectQuestion(p protocol.UserInputRequestPayload) protocol.UserInputRequestPayload {
	if p.Kind != "request_user_input_async" || p.QuestionRevision != "" {
		return p
	}
	s, ok := h.registry.Get(p.SessionID)
	if !ok {
		return p
	}
	source := s.Snapshot()
	p.NativeThreadID = source.ResumeID
	p.ConfigRevision = source.ConfigRevision
	p.QuestionRevision = backend.AsyncQuestionRevisionForConfiguration(p, p.NativeThreadID, session.ConfigurationFrom(source))
	return p
}
func (h *Hub) questionClock() (uint64, error) {
	if h.messageQueue == nil {
		return 0, fmt.Errorf("queue_unavailable")
	}
	return h.messageQueue.NextQuestionRevision()
}
func (h *Hub) batchReceipt(v messagequeue.QuestionBatch) protocol.AsyncBatchReceipt {
	phase := v.Phase
	if phase == "reserved" {
		phase = "unknown"
	}
	out := protocol.AsyncBatchReceipt{Type: "user_input_batch_receipt", Authority: h.cfg.InstanceID, SessionID: v.Request.SessionID, RequestID: v.Request.RequestID, Phase: phase, KnownNoEffect: phase == "rejected", ErrorCode: v.Error, Covered: batchCovered(v.Request), Generation: h.cfg.InstanceID}
	if e, ok, err := h.messageQueue.Get(v.Request.SessionID, v.Request.RequestID); err == nil && ok {
		out.QueueState = string(e.State)
	}
	return out
}
func (h *Hub) sendBatchReceipt(c *Client, v messagequeue.QuestionBatch) {
	h.interactionProjectionMu.Lock()
	defer h.interactionProjectionMu.Unlock()
	out := h.batchReceipt(v)
	revision, err := h.questionClock()
	if err != nil {
		h.queueError(c, clientproto.Command{SessionID: v.Request.SessionID, RequestID: v.Request.RequestID}, "question_receipt_unavailable", "Original question receipt remains pending; query its original intent")
		return
	}
	out.Revision = revision
	c.enqueueEvent(out)
}
func (h *Hub) replyBatch(c *Client, cmd clientproto.Command, status bool) {
	h.asyncBatchAdmissionMu.Lock()
	defer h.asyncBatchAdmissionMu.Unlock()
	req := protocol.AsyncBatchRequest{Authority: cmd.BatchAuthority, SessionID: cmd.SessionID, RequestID: cmd.RequestID, ThreadID: cmd.ExpectedNativeThreadID, Selected: cmd.Selected}
	if cmd.ExpectedConfigRevision != nil {
		req.ConfigRevision = *cmd.ExpectedConfigRevision
	}
	owner := h.pairedTaskDevice(c)
	identity := c.readIdentity.Load()
	var generation uint64
	if identity != nil {
		generation = jarvisTokenGeneration(identity.token)
	}
	unknown := func(code string) {
		h.sendBatchReceipt(c, messagequeue.QuestionBatch{Request: req, Phase: "unknown", Error: code})
	}
	s, ok := h.registry.Get(req.SessionID)
	if h.messageQueue == nil || owner == "" || identity == nil || !ok || !h.controllerInScope(s) || s.Snapshot().Hidden || s.State() == session.Closed || req.Authority != h.cfg.InstanceID {
		h.queueError(c, cmd, "batch_scope_unavailable", "Original paired question scope unavailable")
		return
	}
	if !strings.HasPrefix(req.RequestID, "ui_async_batch_") || len(req.RequestID) < 24 || len(req.RequestID) > 110 {
		h.queueError(c, cmd, "batch_intent_invalid", "Exact batch intent required")
		return
	}
	if status {
		old, found, err := h.messageQueue.QuestionBatch(req.SessionID, req.RequestID)
		if err != nil || !found {
			unknown("batch_not_found")
			return
		}
		if old.Owner != owner || old.OwnerGeneration != generation {
			unknown("batch_owner_changed")
			return
		}
		h.sendBatchReceipt(c, old)
		return
	}
	raw, err := json.Marshal(req)
	if err != nil || len(raw) > 64<<10 {
		unknown("batch_size_invalid")
		return
	}
	record := messagequeue.QuestionBatch{Request: req, Hash: backend.AsyncContentHash(string(raw)), Owner: owner, OwnerGeneration: generation, Phase: "reserved"}
	old, created, err := h.messageQueue.ClaimQuestionBatch(record)
	if err != nil {
		unknown("batch_intent_conflict")
		return
	}
	if !created {
		if old.Owner != owner || old.OwnerGeneration != generation {
			unknown("batch_owner_changed")
			return
		}
		h.sendBatchReceipt(c, old)
		return
	}
	reject := func(code string) {
		if _, found, e := h.messageQueue.Get(req.SessionID, req.RequestID); e != nil || found {
			v, _, e := h.messageQueue.QuestionBatch(req.SessionID, req.RequestID)
			if e == nil {
				h.sendBatchReceipt(c, v)
			}
			return
		}
		if e := h.messageQueue.RejectQuestionBatch(req.SessionID, req.RequestID, record.Hash, code); e != nil {
			unknown("batch_outcome_unknown")
			return
		}
		record.Phase = "rejected"
		record.Error = code
		h.sendBatchReceipt(c, record)
	}
	if len(req.Selected) < 1 || len(req.Selected) > 32 || cmd.ExpectedConfigRevision == nil || req.ThreadID == "" || s.ResumeID() != req.ThreadID || s.Snapshot().ConfigRevision != req.ConfigRevision || s.Backend() != backend.Codex || h.intakeWritePolicySnapshot(s, s.Snapshot()) != nil {
		reject("batch_source_changed")
		return
	}
	admission := cmd
	admission.Kind = "message"
	admission.MessagePurpose = "question_reply"
	if h.rejectToolMaintenanceWrite(c, admission) || h.rejectPMCommand(c, admission) {
		reject("batch_write_not_allowed")
		return
	}
	native, supported := h.exec.(backend.NativeAsyncBatch)
	if !supported {
		reject("batch_unsupported")
		return
	}
	content, err := native.PrepareAsyncBatch(s, req)
	if err != nil {
		reject(err.Error())
		return
	}
	binding := &backend.AsyncBatchBinding{OwnerDevice: owner, OwnerGeneration: generation, Cwd: s.Cwd(), SourceRevision: s.Snapshot().SourceRevision, Request: req, Configuration: session.ConfigurationFrom(s.Snapshot()), ContentHash: backend.AsyncContentHash(content)}
	admission.AsyncBatch = binding
	admission.AsyncCovers = batchCovered(req)
	admission.Content = content
	admission.Images = nil
	admission.Files = nil
	active := h.sessionHasActiveTurn(s)
	if !h.enqueueChatMessageExpectedInheritedMode(c, admission, &dispatchTargetExpectation{ThreadID: req.ThreadID, Revision: req.ConfigRevision}, nil, active) {
		reject("batch_admission_rejected")
		return
	}
	if active {
		admission.Kind = "promote_queued_message"
		h.promoteQueuedMessage(c, admission)
	}
}

func (h *Hub) pendingQuestionAdmissions(c *Client, sid string) []protocol.AsyncAdmission {
	out := []protocol.AsyncAdmission{}
	if h.messageQueue == nil || h.pairedTaskDevice(c) == "" {
		return out
	}
	// New single-answer records freeze the same descriptor. Legacy receipts
	// lacking it are deliberately not used to suppress a different question.
	if ir, ok := h.exec.(interactionResponder); ok {
		for _, question := range ir.PendingInteractions(sid) {
			s, exists := h.registry.Get(question.SessionID)
			if !exists || !h.eventSessionAllowed(s) || h.intakeWritePolicySnapshot(s, s.Snapshot()) != nil {
				continue
			}
			question = h.projectQuestion(question)
			if batch, found, e := h.messageQueue.QuestionCoverage(question.SessionID, question.RequestID, question.QuestionRevision); e == nil && found && (batch.Phase == "admitted" || batch.Phase == "consumed") {
				out = append(out, protocol.AsyncAdmission{SessionID: question.SessionID, RequestID: question.RequestID, QuestionRevision: question.QuestionRevision, ReplyRequestID: batch.Request.RequestID, State: batch.Phase})
				continue
			}

			if question.Kind != "request_user_input_async" || strings.HasPrefix(question.RequestID, "ui_async_batch_") {
				continue
			}
			full, found, e := h.messageQueue.Get(question.SessionID, question.RequestID)
			if e != nil || !found {
				continue
			}
			var frozen queuedPayload
			if json.Unmarshal(full.Payload, &frozen) != nil {
				continue
			}
			for _, q := range frozen.AsyncCovers {
				out = append(out, protocol.AsyncAdmission{SessionID: question.SessionID, RequestID: q.RequestID, QuestionRevision: q.QuestionRevision, ReplyRequestID: question.RequestID, State: "admitted"})
			}
		}
	}
	return out
}

func (h *Hub) asyncBatchSendGuard(s *session.Session, b *backend.AsyncBatchBinding) error {
	if b == nil || b.Request.Authority != h.cfg.InstanceID || b.Request.SessionID != s.ID || s.ResumeID() != b.Request.ThreadID || s.Cwd() != b.Cwd || s.Snapshot().SourceRevision != b.SourceRevision || session.ConfigurationFrom(s.Snapshot()) != b.Configuration || !h.jarvisCredentialCurrent(b.OwnerDevice, b.OwnerGeneration) || !h.eventSessionAllowed(s) || h.intakeWritePolicySnapshot(s, s.Snapshot()) != nil {
		return fmt.Errorf("batch_authority_or_source_changed")
	}
	return nil
}
