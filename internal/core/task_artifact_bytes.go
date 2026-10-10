package core

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	artifact "everything-go/contracts/task-api/artifacts-v3"
	"everything-go/internal/taskapi"
)

// The immutable seal is the original result owner. No filesystem path, URL,
// inbox broadcast, model artifact assertion or second artifact ledger exists.
func (h *Hub) executeArtifactBytes(ctx context.Context, caller taskapi.BoundCaller, raw []byte) (taskapi.Response, bool) {
	var request taskapi.Request
	if json.Unmarshal(raw, &request) != nil || request.Operation != "read_artifact_bytes" && request.Operation != "artifact_capabilities" {
		return taskapi.Response{}, false
	}
	op := request.Operation
	response := taskapi.Response{Type: "task_api_response", Version: artifact.Version, CorrelationID: request.CorrelationID, Operation: &op}
	fail := func(code string) (taskapi.Response, bool) {
		response.OK = false
		response.Result = nil
		response.Error = taskapi.Failure(code, "known_none", "read_capabilities")
		return response, true
	}
	if request.Version != artifact.Version {
		response.RequestVersion = &request.Version
		return fail("unsupported")
	}
	if artifact.Validate(raw, "Request") != nil {
		return fail("invalid_argument")
	}
	bound, err := caller.Check(ctx)
	if err != nil {
		return fail("caller_unbound")
	}
	if bound.Authority != h.cfg.InstanceID {
		return fail("wrong_authority")
	}
	if bound.SourceSessionID != "" {
		child, err := h.delegations.HasChild(ctx, bound.SourceSessionID)
		if err != nil || child {
			return fail("permission")
		}
	}
	if request.Operation == "artifact_capabilities" {
		response.OK = true
		response.Result = map[string]any{"api_versions": []string{artifact.Version}, "schema_hash": artifact.Hash(), "allowed_operations": []string{"artifact_capabilities", "read_artifact_bytes"}, "mutation_authority_instance_id": h.cfg.InstanceID, "max_range_bytes": 32768, "artifact_owner": "immutable_sealed_final", "reported_paths": "unsupported"}
		return response, true
	}
	var in struct {
		TaskID     string `json:"task_id"`
		SealID     string `json:"seal_id"`
		ArtifactID string `json:"artifact_id"`
		Hash       string `json:"expected_hash"`
		Offset     int64  `json:"offset"`
		Limit      int64  `json:"limit"`
	}
	if json.Unmarshal(request.Input, &in) != nil {
		return fail("invalid_argument")
	}
	// The original scoped authorization runs BEFORE any sealed body access.
	getInput, _ := json.Marshal(map[string]any{"task_id": in.TaskID})
	if _, err := h.Authorize(ctx, bound, taskapi.Request{Operation: "get", Input: getInput}); err != nil {
		return fail("permission")
	}
	record, err := h.apiRecord(ctx, bound, in.TaskID)
	if err != nil {
		return fail("permission")
	}
	if record.Path != "delegation" || in.ArtifactID != "result.txt" {
		return fail("unsupported")
	}
	original, found, err := h.delegations.ByChildRequest(ctx, record.SessionID, record.RequestID)
	if err != nil || !found {
		return fail("result_not_ready")
	}
	if _, err := caller.Check(ctx); err != nil {
		return fail("caller_unbound")
	}
	seal, verified := h.canonicalReviewSeal(ctx, original)
	if !verified || seal.ID != in.SealID || seal.Hash != in.Hash {
		return fail("stale_revision")
	}
	// Owner-generated result.txt is precisely the immutable sealed UTF-8 body.
	data := []byte(seal.Text)
	if in.Offset > int64(len(data)) {
		return fail("invalid_argument")
	}
	end := in.Offset + in.Limit
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	chunk := data[in.Offset:end]
	chunkDigest := sha256.Sum256(chunk)
	if _, err := caller.Check(ctx); err != nil {
		return fail("caller_unbound")
	}
	response.OK = true
	response.Result = map[string]any{"task_id": record.TaskID, "seal_id": seal.ID, "artifact_id": "result.txt", "owner_request_id": record.RequestID, "total_bytes": len(data), "offset": in.Offset, "next_offset": end, "has_more": end < int64(len(data)), "full_hash": seal.Hash, "chunk_hash": hex.EncodeToString(chunkDigest[:]), "data_base64": base64.StdEncoding.EncodeToString(chunk)}
	return response, true
}
