package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/taskapi"
	"os"
	"path/filepath"
)

// Default-off, operator-owned acceptance scratch; never a model tool or caller.
type NativeResponseCapture struct {
	Directory, SourceSession, SourceThread string
	SourceRevision                         uint64
	Authority, ConfigPath, ConfigHash      string
	Operations                             map[string]bool
	Verifier                               *NativeArtifactVerifier
	Tasks                                  map[string]string // exact owned task -> expected immutable final hash
}

func (h *Hub) SetNativeResponseCapture(c *NativeResponseCapture) { h.nativeCapture.Store(c) }

// Called by the original MCP/native response owner before model consumption.
// No request body, credential/header/token/env/history/reasoning is persisted.
// Only the public projection is stored; digest covers the original structured
// response serialization. It is explicitly NOT raw HTTP/provider-wire proof.
func (h *Hub) CaptureTaskResponse(ctx context.Context, c backend.TaskCaller, raw json.RawMessage, response taskapi.Response) error {
	return h.capturePublicResponse(ctx, c, raw, response, true)
}
func (h *Hub) capturePublicResponse(ctx context.Context, c backend.TaskCaller, raw json.RawMessage, response taskapi.Response, verify bool) error {
	p := h.nativeCapture.Load()
	if p == nil {
		return nil
	}
	if !h.captureConfigCurrent(p) {
		return os.ErrPermission
	}
	if c.Session == nil || c.Validate == nil || !c.Validate() || c.Session.ID != p.SourceSession || c.ThreadID != p.SourceThread || c.Session.ResumeID() != p.SourceThread || c.Session.SettingsSnapshot().ConfigRevision != p.SourceRevision || c.Session.ActiveQueuedID() != c.RequestID || c.CallID == "" {
		return nil
	}
	var request taskapi.Request
	if json.Unmarshal(raw, &request) != nil || request.Operation != "get" && request.Operation != "read_result" && request.Operation != "capabilities" && request.Operation != "artifact_capabilities" && request.Operation != "read_artifact_bytes" {
		return nil
	}
	if p.Operations != nil && !p.Operations[request.Operation] {
		return nil
	}
	if !response.OK {
		return nil
	}
	bodyBytes, _ := json.Marshal(response.Result)
	var body map[string]any
	if json.Unmarshal(bodyBytes, &body) != nil {
		return nil
	}
	if request.Operation != "capabilities" && request.Operation != "artifact_capabilities" {
		id, _ := body["task_id"].(string)
		wanted, selected := p.Tasks[id]
		if !selected {
			return nil
		}
		if request.Operation == "read_artifact_bytes" && (body["full_hash"] != wanted || body["artifact_id"] != "result.txt" || body["seal_id"] != "seal_"+wanted) {
			return nil
		}
		if request.Operation == "read_result" && body["final_hash"] != wanted {
			return nil
		}
	}
	fields := []string{"task_id", "task_ref", "axes", "revision", "definition_revision", "seal_id", "final_hash", "receipt_ids", "offset", "next_offset", "has_more", "terminal_status", "text", "api_versions", "schema_hash", "allowed_operations", "paths", "mutation_authority_instance_id", "artifact_id", "owner_request_id", "owner_round_id", "total_bytes", "full_hash", "chunk_hash", "data_base64", "max_range_bytes", "artifact_owner", "reported_paths"}
	public := map[string]any{}
	for _, field := range fields {
		if value, ok := body[field]; ok {
			public[field] = value
		}
	}
	if !c.Validate() || ctx.Err() != nil {
		return nil
	}
	original, _ := json.Marshal(response)
	digest := sha256.Sum256(original)
	record := map[string]any{"evidence_kind": "original_native_structured_public_projection", "raw_wire": false, "source_session_id": c.Session.ID, "source_native_thread_id": c.ThreadID, "source_config_revision": p.SourceRevision, "source_request_id": c.RequestID, "native_call_id": c.CallID, "correlation_id": request.CorrelationID, "operation": request.Operation, "api_version": response.Version, "upstream_structured_response_sha256": hex.EncodeToString(digest[:]), "public_result": public}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	identity := sha256.Sum256([]byte(c.RequestID + "\x00" + c.CallID + "\x00" + request.CorrelationID))
	if err := os.MkdirAll(p.Directory, 0700); err != nil {
		return err
	}
	canonical, err := filepath.EvalSymlinks(p.Directory)
	if err != nil || canonical != filepath.Clean(p.Directory) {
		return os.ErrPermission
	}
	file, err := os.OpenFile(filepath.Join(p.Directory, hex.EncodeToString(identity[:])+".json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(append(data, '\n'))
	if err == nil && verify && p.Verifier != nil && request.Operation == "read_artifact_bytes" && body["offset"] == float64(0) {
		err = h.verifyOriginalArtifact(ctx, c, request, p)
	}
	return err
}

func captureCallerCurrent(p *NativeResponseCapture, c backend.TaskCaller) bool {
	return c.Session != nil && c.Validate != nil && c.Validate() && c.Session.ID == p.SourceSession && c.ThreadID == p.SourceThread && c.Session.ResumeID() == p.SourceThread && c.Session.SettingsSnapshot().ConfigRevision == p.SourceRevision && c.Session.ActiveQueuedID() == c.RequestID && c.CallID != ""
}
