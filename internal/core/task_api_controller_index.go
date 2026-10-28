package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
)

// This is a derived read index of an existing canonical controller receipt, not
// a synthetic TaskAPI acceptance or mutation namespace. The original scd stays.
func (h *Hub) controllerIndexRecord(ctx context.Context, d sessiondispatch.Record) (taskapi.IntentRecord, error) {
	if !d.ParentConfigKnown || d.ParentThreadID == "" || d.RequestID == "" || d.InstanceID != h.cfg.InstanceID {
		return taskapi.IntentRecord{}, unifiedError("source_unverified")
	}
	source, ok := h.registry.Get(d.ParentID)
	if !ok || source.ResumeID() != d.ParentThreadID || source.SettingsSnapshot().ConfigRevision != d.ParentConfigRevision {
		return taskapi.IntentRecord{}, unifiedError("scope_changed")
	}
	meta := apiMetadata{SourceThread: d.ParentThreadID, SourceRevision: d.ParentConfigRevision, Transport: "original_controller_read_index", Caller: taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + d.ParentID, BindingKind: "controller_origin", SourceSessionID: d.ParentID, SourceRequestID: d.OriginRequestID, SourceResumeID: d.ParentThreadID, SourceConfigRevision: d.ParentConfigRevision, SourceConfigKnown: true}}
	meta.Request = taskapi.Request{Operation: "create_dispatch", Input: json.RawMessage(`{}`)}
	raw, _ := json.Marshal(meta)
	return taskapi.IntentRecord{TaskID: apiID(h.cfg.InstanceID, "controller", d.ID), NativeID: d.ID, SessionID: d.SessionID, RequestID: controllerIndexExecution(d), ScopeID: "session:" + d.ParentID, Path: "controller", Hash: d.IntentHash, Metadata: raw, CreatedAt: d.CreatedAt}, nil
}
func (h *Hub) controllerIndexGet(ctx context.Context, c taskapi.VerifiedContext, id string) (taskapi.IntentRecord, error) {
	parts, e := apiRef(id)
	if e != nil || parts[1] != "controller" || h.dispatches == nil {
		return taskapi.IntentRecord{}, unifiedError("not_found")
	}
	d, found, e := h.dispatches.Get(ctx, parts[2])
	if e != nil {
		return taskapi.IntentRecord{}, e
	}
	if !found {
		return taskapi.IntentRecord{}, unifiedError("not_found")
	}
	r, e := h.controllerIndexRecord(ctx, d)
	if e != nil {
		return r, e
	}
	return r, h.checkAPIRecord(ctx, c, r)
}

func controllerIndexExecution(d sessiondispatch.Record) string {
	if d.ExecutionRequestID != "" {
		return d.ExecutionRequestID
	}
	return d.RequestID
}
