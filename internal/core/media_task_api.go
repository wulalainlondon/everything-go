package core

import (
	"context"
	"encoding/json"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/taskapi"
)

// Explicit per-request media dialect; the connection's existing immutable hello
// selector is never changed. Only representation changes, no caller authority.
func (h *Hub) handleMediaTaskAPI(ctx context.Context, c *Client, raw []byte) {
	var envelope struct {
		Type    string          `json:"type"`
		Hash    string          `json:"schema_hash"`
		Request json.RawMessage `json:"request"`
	}
	err := json.Unmarshal(raw, &envelope)
	var req taskapi.Request
	_ = json.Unmarshal(envelope.Request, &req)
	op := req.Operation
	response := taskapi.Response{Type: "task_api_response", Version: contract.Version, CorrelationID: req.CorrelationID, Operation: &op, Error: taskapi.Failure("invalid_argument", "known_none", "correct_input")}
	parsed, pe := taskapiParseMediaEnvelope(raw)
	if err != nil || pe != nil || !parsed || len(raw) > mediaControlMaxBytes || envelope.Type != "media_task_api_request" || envelope.Hash != contract.MediaRefsHash() || h.media == nil || h.media.err != nil || (op != "intake" && op != "get" && op != "capabilities") {
		c.enqueueEvent(response)
		return
	}
	caller, e := taskapi.BindCaller(ctx, apiVerifier{func() (taskapi.VerifiedContext, error) {
		b, e := h.taskHuman(c)
		if e != nil {
			return taskapi.VerifiedContext{}, e
		}
		v, e := b.Check(ctx)
		if e != nil {
			return v, e
		}
		v.TaskSchemaHash = contract.MediaRefsHash()
		return v, nil
	}}, taskapi.Invocation{})
	if e != nil {
		response.Error = taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
		c.enqueueEvent(response)
		return
	}
	response = h.taskService.Execute(ctx, caller, envelope.Request)
	c.enqueueEvent(response)
}
func taskapiParseMediaEnvelope(raw []byte) (bool, error) {
	v, e := contract.Parse(raw)
	if e != nil {
		return false, e
	}
	m, ok := v.(map[string]any)
	if !ok || len(m) != 3 {
		return false, nil
	}
	_, a := m["type"]
	_, b := m["schema_hash"]
	_, c := m["request"]
	return a && b && c, nil
}

// Bounds cover the original raw UTF-8 frame, not a re-marshaled subset.
func (h *Hub) validateMediaControl(c *Client, raw []byte) bool {
	v, e := contract.Parse(raw)
	if e == nil {
		e = contract.ValidateWireNumbers(v)
	}
	if len(raw) <= mediaControlMaxBytes && e == nil {
		return true
	}
	var header struct{ Type, RequestID, SessionID, MediaBatchID string }
	var m map[string]json.RawMessage
	_ = json.Unmarshal(raw, &m)
	_ = json.Unmarshal(m["type"], &header.Type)
	_ = json.Unmarshal(m["request_id"], &header.RequestID)
	_ = json.Unmarshal(m["session_id"], &header.SessionID)
	_ = json.Unmarshal(m["media_batch_id"], &header.MediaBatchID)
	c.enqueueEvent(map[string]any{"type": "error", "code": "media_control_invalid", "command_type": header.Type, "request_id": header.RequestID, "session_id": header.SessionID, "authority_instance_id": h.cfg.InstanceID, "known_effect": "none", "message": "Media control exceeds its finite budget or has ambiguous JSON"})
	return false
}
