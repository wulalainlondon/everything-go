package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

type humanIntakeContextKey struct{}

func humanIntakeSeal(ctx context.Context) *messagequeue.HumanSeal {
	seal, _ := ctx.Value(humanIntakeContextKey{}).(*messagequeue.HumanSeal)
	return seal
}
func (h *Hub) humanIntakeThreadMatches(ctx context.Context, s *session.Session, thread string) bool {
	if s.ResumeID() == thread {
		return true
	}
	seal := humanIntakeSeal(ctx)
	return seal != nil && seal.Binding.Session == s.ID && seal.Binding.Thread == thread && h.messageQueue.HumanThreadCompatible(seal.Binding, s.ResumeID())
}

// The native outbox uses the ORIGINAL typed intake service and paired verifier.
// The private seal carrier only joins its API record and queue reservation in
// the same SQLite commit. It is neither a wire grant nor an ordinary fallback.
func (h *Hub) enqueueHumanIntake(c *Client, cmd clientproto.Command, raw json.RawMessage) bool {
	if cmd.HumanSeal == nil || cmd.HumanSeal.Operation.Kind != "seal" {
		return false
	}
	var envelope struct {
		Request taskapi.Request `json:"request"`
	}
	if json.Unmarshal(raw, &envelope) != nil || envelope.Request.Operation != "intake" || envelope.Request.IdempotencyKey != cmd.RequestID {
		return false
	}
	var input intakeInput
	if json.Unmarshal(envelope.Request.Input, &input) != nil || input.Action != "submit" || input.RequestID != cmd.RequestID || input.Target.Instance != cmd.HumanSeal.Binding.Authority || input.Target.Session != cmd.SessionID || input.Target.Revision != cmd.HumanSeal.Binding.Config || input.Goal != cmd.Content || input.MediaBatchID != cmd.MediaBatchID || len(input.Images) != len(cmd.Images) || len(input.Files) != len(cmd.Files) {
		return false
	}
	thread := ""
	if input.Target.Thread != nil {
		thread = *input.Target.Thread
	}
	if thread != cmd.HumanSeal.Binding.Thread {
		return false
	}
	for i, v := range input.Images {
		a := cmd.Images[i]
		if v.AttachmentID != a.AttachmentID || v.SHA256 != a.SHA256 || v.SizeBytes != a.SizeBytes || v.Data != a.Data || v.MediaType != a.MediaType {
			return false
		}
	}
	for i, v := range input.Files {
		a := cmd.Files[i]
		if v.AttachmentID != a.AttachmentID || v.SHA256 != a.SHA256 || v.SizeBytes != a.SizeBytes || v.Content != a.Content || v.MediaType != a.MediaType || v.Name != a.Name || a.RemotePath != "" {
			return false
		}
	}
	ctx := context.WithValue(c.ctx, humanIntakeContextKey{}, cmd.HumanSeal)
	response := h.executeMediaTaskAPI(ctx, c, raw)
	if response.OK {
		return true
	}
	// Preserve the original typed refusal for diagnostics; no second submission.
	return false
}
