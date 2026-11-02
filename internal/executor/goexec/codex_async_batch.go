package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"fmt"
	"strings"
	"time"
)

func (c *Codex) PrepareAsyncBatch(s *session.Session, r protocol.AsyncBatchRequest) (string, error) {
	content, release, err := c.ReserveAsyncBatch(s, r)
	if release != nil {
		release()
	}
	return content, err
}
func (c *Codex) ReserveAsyncBatch(s *session.Session, r protocol.AsyncBatchRequest) (string, func(), error) {
	c.asyncMu.Lock()
	c.interMu.Lock()
	content, err := c.prepareAsyncBatchLocked(s, r)
	release := func() { c.interMu.Unlock(); c.asyncMu.Unlock() }
	if err != nil {
		release()
		return "", nil, err
	}
	return content, release, nil
}
func (c *Codex) prepareAsyncBatchLocked(s *session.Session, r protocol.AsyncBatchRequest) (string, error) {
	if len(r.Selected) < 1 || len(r.Selected) > 32 || r.SessionID != s.ID || r.ThreadID == "" || s.ResumeID() != r.ThreadID || r.ConfigRevision != s.Snapshot().ConfigRevision {
		return "", fmt.Errorf("batch_source_changed")
	}
	c.loadAsyncResolvedLocked(r.ThreadID)
	if c.asyncResolved["unavailable:"+r.ThreadID] {
		return "", fmt.Errorf("question_history_unavailable")
	}
	seen := map[string]bool{}
	replies := []nativeQuestionReply{}
	for _, selected := range r.Selected {
		if seen[selected.RequestID] {
			return "", fmt.Errorf("duplicate_question")
		}
		seen[selected.RequestID] = true
		ci, ok := c.interactions[selected.RequestID]
		if !ok || ci.nativeThread != r.ThreadID || ci.payload.SessionID != s.ID || ci.payload.Kind != "request_user_input_async" || ci.payload.ConfigRevision != r.ConfigRevision || ci.nativeConfig == nil || *ci.nativeConfig != session.ConfigurationFrom(s.Snapshot()) || c.asyncResolved[selected.RequestID] || ci.payload.IsBlocking == nil || *ci.payload.IsBlocking {
			return "", fmt.Errorf("question_not_pending")
		}
		if ci.payload.ExpiresAt > 0 && ci.payload.ExpiresAt <= time.Now().UnixMilli() {
			return "", fmt.Errorf("question_expired")
		}
		if selected.QuestionRevision != backend.AsyncQuestionRevisionForConfiguration(ci.payload, r.ThreadID, *ci.nativeConfig) {
			return "", fmt.Errorf("question_revision_changed")
		}
		if len(selected.Answers) != len(ci.payload.Questions) {
			return "", fmt.Errorf("answer_mapping_invalid")
		}
		for _, q := range ci.payload.Questions {
			answer, ok := selected.Answers[q.QuestionID].(string)
			if !ok || strings.TrimSpace(answer) == "" {
				return "", fmt.Errorf("explicit_answer_required")
			}
			if asyncRequestID(r.ThreadID, q.QuestionID) != selected.RequestID {
				return "", fmt.Errorf("question_route_changed")
			}
			replies = append(replies, nativeQuestionReply{Answer: answer, Question: q.Text, QuestionItemID: q.QuestionID})
		}
	}
	raw, err := json.Marshal(replies)
	if err != nil {
		return "", err
	}
	if len(raw) > 64<<10 {
		return "", fmt.Errorf("batch_too_large")
	}
	return asyncReplyOpen + "\n" + string(raw) + "\n" + asyncReplyClose, nil
}
func (c *Codex) validateAsyncInput(ctx context.Context, s *session.Session, id, content string) (string, error) {
	if !strings.HasPrefix(id, "ui_async_batch_") {
		return c.validateAsyncReply(s, id, content)
	}
	b := backend.AsyncBatchFrom(ctx)
	if b == nil || b.Request.RequestID != id || b.Configuration != session.ConfigurationFrom(s.Snapshot()) || b.Cwd != s.Cwd() || b.SourceRevision != s.Snapshot().SourceRevision || b.ContentHash != backend.AsyncContentHash(content) {
		return "", fmt.Errorf("batch_binding_missing_or_changed")
	}
	original, err := c.PrepareAsyncBatch(s, b.Request)
	if err != nil {
		return "", err
	}
	if original != content {
		return "", fmt.Errorf("batch_envelope_changed")
	}
	return b.Request.ThreadID, nil
}

// Keep exact native descriptors reserved through the original writer's bytes,
// not through the remote ACK. Canonical observations may proceed after Write.
func (c *Codex) reserveAsyncInput(ctx context.Context, s *session.Session, id, text string) (func(), error) {
	if !strings.HasPrefix(id, "ui_async_batch_") {
		return nil, nil
	}
	b := backend.AsyncBatchFrom(ctx)
	if b == nil || b.Request.RequestID != id || b.Configuration != session.ConfigurationFrom(s.Snapshot()) || b.Cwd != s.Cwd() || b.SourceRevision != s.Snapshot().SourceRevision || b.ContentHash != backend.AsyncContentHash(text) {
		return nil, fmt.Errorf("batch_binding_missing_or_changed")
	}
	original, release, err := c.ReserveAsyncBatch(s, b.Request)
	if err != nil {
		return nil, err
	}
	if original != text || b.Configuration != session.ConfigurationFrom(s.Snapshot()) || b.Cwd != s.Cwd() || b.SourceRevision != s.Snapshot().SourceRevision {
		release()
		return nil, fmt.Errorf("batch_envelope_or_source_changed")
	}
	return release, nil
}
func (c *Codex) reserveAsyncStart(st *codexState, snap session.Snapshot, requestID string, input []map[string]any) (func(), error) {
	if !strings.HasPrefix(requestID, "ui_async_batch_") {
		return nil, nil
	}
	st.mu.Lock()
	b, current := st.asyncBatch, st.reqID == requestID
	st.mu.Unlock()
	c.mu.Lock()
	s := c.threadToSession[snap.ResumeID]
	c.mu.Unlock()
	if !current || s == nil || s.ID != snap.ID || len(input) != 1 {
		return nil, fmt.Errorf("batch_start_binding_changed")
	}
	text, _ := input[0]["text"].(string)
	return c.reserveAsyncInput(backend.WithAsyncBatch(context.Background(), b), s, requestID, text)
}
