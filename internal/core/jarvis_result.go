package core

import (
	"context"
	"encoding/json"
	"errors"

	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
)

// Reuse the original canonical queue/history/native guards. A raw DB seal is
// never enough to expose a result or authorize human resolution.
func (h *Hub) jarvisResultSeal(ctx context.Context, s *session.Session, request, id string, materialize ...bool) (messagequeue.TaskSeal, error) {
	missing := errors.New("canonical native terminal result unavailable")
	entry, found, err := h.messageQueue.GetContext(ctx, s.ID, request)
	if err != nil || !found || entry.State != messagequeue.Completed {
		return messagequeue.TaskSeal{}, missing
	}
	if conflict, err := h.messageQueue.NativeConflictContext(ctx, s.ID, request); err != nil || conflict {
		return messagequeue.TaskSeal{}, missing
	}
	thread, turn, nativeKnown := h.messageQueue.NativeExecutionContext(ctx, s.ID, request)
	if s.Backend() == backend.Codex && (!nativeKnown || !h.nativeCorrelationKnown(s, thread, request, turn)) {
		return messagequeue.TaskSeal{}, missing
	}
	valid := func(seal messagequeue.TaskSeal) bool {
		var anchor map[string]any
		if json.Unmarshal(seal.Anchor, &anchor) != nil || anchor["instance_id"] != h.cfg.InstanceID || anchor["session_id"] != s.ID || anchor["request_id"] != request || anchor["source_message_id"] == nil {
			return false
		}
		message, ok := anchor["source_message_id"].(string)
		if !ok || message == "" {
			return false
		}
		execution, ok := anchor["execution"].(map[string]any)
		if !ok || execution["backend"] != s.Backend() {
			return false
		}
		if s.Backend() == backend.Claude {
			proof, found, err := h.messageQueue.ProviderEvidenceContext(ctx, s.ID, request)
			return err == nil && found && proof.Backend == backend.Claude && proof.Status == "succeeded" && h.claudeAnchorID(s, proof) != "" && anchor["source_message_id"] == h.claudeAnchorID(s, proof) && execution["provider_session_id"] == proof.ConversationID && execution["provider_token"] == proof.Token && seal.Hash == jarvisTextHash(proof.Text)
		}
		return h.nativeSealKnown(s, anchor, request, thread, turn)
	}
	if seal, err := h.messageQueue.TaskSeal(ctx, s.ID, request, id); err == nil {
		if valid(seal) {
			return seal, nil
		}
		return messagequeue.TaskSeal{}, missing
	}
	// A writer-held carry check may only read an already sealed native result.
	if len(materialize) > 0 && !materialize[0] {
		return messagequeue.TaskSeal{}, missing
	}
	// An original inbox assignment uses the existing queue, not a second Task API
	// intake journal. Materialize its final through the original session read model.
	finals, _ := h.sessionTaskFinals(s)
	projected := h.projectSessionTask(s, entry, "", finals)
	if projected.State != "completed" || projected.APIAxes["final"] != "sealed" || projected.SourceMessageID == "" {
		return messagequeue.TaskSeal{}, missing
	}
	text := ""
	execution := map[string]string{}
	if s.Backend() == backend.Codex {
		final := finals[request]
		if final == nil {
			return messagequeue.TaskSeal{}, missing
		}
		finalThread, _ := final["source_thread_id"].(string)
		if finalThread != "" && finalThread != thread {
			return messagequeue.TaskSeal{}, missing
		}
		text, _ = final["content"].(string)
		execution = map[string]string{"backend": "codex", "native_thread_id": thread, "native_turn_id": turn}
	} else if s.Backend() == backend.Claude {
		proof, found, err := h.messageQueue.ProviderEvidenceContext(ctx, s.ID, request)
		if err != nil || !found {
			return messagequeue.TaskSeal{}, missing
		}
		text = proof.Text
		execution = map[string]string{"backend": "claude", "provider_session_id": proof.ConversationID, "token_kind": proof.TokenKind, "provider_token": proof.Token}
	} else {
		return messagequeue.TaskSeal{}, missing
	}
	hash := jarvisTextHash(text)
	sealID := "seal_" + hash
	if id != "" && id != sealID {
		return messagequeue.TaskSeal{}, missing
	}
	anchor, _ := json.Marshal(map[string]any{"instance_id": h.cfg.InstanceID, "session_id": s.ID, "request_id": request, "source_message_id": projected.SourceMessageID, "execution": execution})
	seal := messagequeue.TaskSeal{ID: sealID, SessionID: s.ID, RequestID: request, Hash: hash, Text: text, Anchor: anchor}
	if !valid(seal) {
		return messagequeue.TaskSeal{}, missing
	}
	return h.messageQueue.SealTask(ctx, seal)
}
