package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/pipeline-v7"
	base "everything-go/contracts/task-api/v1"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	"everything-go/internal/taskpipeline"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPipelineIntentActualGoGolden(t *testing.T) {
	inputs := []string{`{"manager":{"authority_instance_id":"本機<>&\u2028\u2029","session_id":"原對話","thread_id":"native","config_revision":0},"10":"ten","2":"two","\ue000":"private-use","💡":"astral"}`, `{"nested":{"negative":-3,"maximum":9007199254740991,"normal":1000},"text":"繁體中文 <>&\u2028\u2029","zero":0}`}
	vectors := []any{}
	for _, input := range inputs {
		parsed, e := base.Parse([]byte(input))
		if e != nil {
			t.Fatal(e)
		}
		if e = base.ValidateWireNumbers(parsed); e != nil {
			t.Fatal(e)
		}
		req := taskapi.Request{Operation: "mission_create", Input: json.RawMessage(input)}
		actual := unifiedHash(req)
		if len(actual) != 64 {
			t.Fatal(actual)
		}
		var decoded any
		json.Unmarshal(req.Input, &decoded)
		vectors = append(vectors, map[string]any{"operation": req.Operation, "input": decoded, "expected_hash": actual})
	}
	bad := []byte(`{"x":9007199254740992}`)
	v, e := base.Parse(bad)
	if e == nil && base.ValidateWireNumbers(v) == nil {
		t.Fatal("unsafe integer accepted")
	}
	if directory := os.Getenv("PIPELINE_WIRE_OUT"); directory != "" {
		path := filepath.Join(directory, "HASH_GOLDEN_WIP_R1.json")
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		if e = json.NewEncoder(f).Encode(map[string]any{"status": "WIP_ACTUAL_GO_FUNCTION_NOT_FINAL_SOURCE", "schema_hash": contract.Hash(), "function": "core.unifiedHash (same rc7 mutation path)", "algorithm": "SHA256(encoding/json Marshal struct{operation,input}); excludes correlation/idempotency key", "vectors": vectors, "unsafe_integer": "9007199254740992 rejected by production pre-hash wire validator"}); e != nil {
			t.Fatal(e)
		}
	}
}

func TestPipelineActualHTTPWirePacket(t *testing.T) {
	directory := os.Getenv("PIPELINE_WIRE_OUT")
	if directory == "" {
		t.Skip("artifact exporter only; production route covered by non-export tests")
	}
	packet := filepath.Join(directory, "wire-wip-r1")
	if e := os.Mkdir(packet, 0700); e != nil {
		t.Fatal(e)
	}
	h, call := jarvisFixture(t)
	s := h.registry.Create("wire-manager", "Synthetic wire manager", h.cfg.RootDir, "codex", "m", "read-only", "wire-thread")
	release := s.HoldQueue()
	defer release()
	defer s.CloseAndWait()
	manager := pipelineIdentity(h.unifiedIdentity(s.ID))
	entries := []any{}
	invoke := func(name, op, key string, input any) taskapi.Response {
		req := pipelineRequest(t, op, key, input)
		raw, _ := json.Marshal(req)
		w := call("POST", "task-api", req, "phone")
		resp := decodePipeline(t, w.Body.Bytes())
		requestPath := name + ".request.json"
		responsePath := name + ".response.json"
		if e := os.WriteFile(filepath.Join(packet, requestPath), raw, 0600); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(filepath.Join(packet, responsePath), w.Body.Bytes(), 0600); e != nil {
			t.Fatal(e)
		}
		entries = append(entries, map[string]any{"name": name, "operation": op, "request_path": requestPath, "request_sha256": taskapi.RawDigest(raw), "request_bytes": len(raw), "response_path": responsePath, "response_sha256": taskapi.RawDigest(w.Body.Bytes()), "response_bytes": w.Body.Len(), "http_status": w.Code, "expected_schema_valid": true, "ok": resp.OK})
		return resp
	}
	if r := invoke("capabilities", "capabilities", "", map[string]any{}); !r.OK {
		t.Fatal(r.Error)
	}
	instruction := "依原目標接續；繁體中文<>&\u2028\u2029"
	action := &taskpipeline.Action{Kind: "session_resume", Target: manager, Instruction: instruction, InstructionHash: taskapi.RawDigest([]byte(instruction)), Disclosure: "references_only"}
	mission := taskpipeline.Mission{Goal: "公開合成工程主線", Completion: "exact native final and required steps", Steps: []taskpipeline.Step{{ID: "execute", Title: "原actor接續", Required: true, Predicate: taskpipeline.Predicate{Kind: "success_sealed"}, Action: action}}, Authorization: taskpipeline.Authorization{Confirmed: true, SourceRef: "synthetic-paired-human-request", Budget: taskpipeline.Budget{DeadlineMS: time.Now().Add(time.Hour).UnixMilli(), MaxEffects: 1, MaxModelTurns: 1, MaxNoProgress: 1}}}
	input := map[string]any{"manager": manager, "mission": mission}
	result := invoke("mission-create", "mission_create", "r_11111111111111111111111111111111", input)
	if !result.OK {
		t.Fatal(result.Error)
	}
	body, _ := json.Marshal(result.Result)
	var receipt taskpipeline.Receipt
	json.Unmarshal(body, &receipt)
	id := receipt.Record.ID
	if r := invoke("mission-get", "mission_get", "", map[string]any{"manager": manager, "id": id}); !r.OK {
		t.Fatal(r.Error)
	}
	invoke("mission-list", "mission_list", "", map[string]any{"manager": manager, "limit": 20, "cursor": ""})
	hash := unifiedHash(taskapi.Request{Operation: "mission_create", Input: mustPipelineJSON(input)})
	if r := invoke("operation-original", "operation_get", "", map[string]any{"manager": manager, "original_intent_id": "r_11111111111111111111111111111111", "original_operation": "mission_create", "expected_input_hash": hash}); !r.OK {
		t.Fatal(r.Error)
	}
	if r := invoke("operation-unknown", "operation_get", "", map[string]any{"manager": manager, "original_intent_id": "r_22222222222222222222222222222222", "original_operation": "mission_create", "expected_input_hash": hash}); r.OK || r.Error.Acceptance != "unknown" {
		t.Fatal(r)
	}
	h.reconcileTaskPipelines(context.Background())
	pending, e := h.messageQueue.PipelineStore().Pending(context.Background(), 10)
	if e != nil || len(pending) != 1 {
		t.Fatal(pending, e)
	}
	cid := pending[0].ID
	if r := invoke("continuation-get", "continuation_get", "", map[string]any{"manager": manager, "id": cid}); !r.OK {
		t.Fatal(r.Error)
	}
	invoke("event-history", "event_history", "", map[string]any{"manager": manager, "id": id, "after_sequence": 0, "limit": 100})
	invoke("guard-status", "guard_status", "", map[string]any{"manager": manager})
	if r := invoke("wrong-revision", "mission_pause", "r_33333333333333333333333333333333", map[string]any{"manager": manager, "id": id, "expected_revision": 999}); r.OK || r.Error.Acceptance != "known_none" {
		t.Fatal(r)
	}
	current, _, _, e := h.messageQueue.PipelineStore().Get(context.Background(), taskpipeline.Scope{Principal: "device:phone", Generation: jarvisTokenGeneration("jarvis-token"), Manager: manager}, id)
	if e != nil {
		t.Fatal(e)
	}
	if r := invoke("mission-pause", "mission_pause", "r_44444444444444444444444444444444", map[string]any{"manager": manager, "id": id, "expected_revision": current.Revision}); !r.OK {
		t.Fatal(r.Error)
	}
	// The native issuer below is an isolated canonical-store fixture, not a
	// production provider invocation. The actual parent disclosure uses HTTP.
	h.dispatches, e = sessiondispatch.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer h.dispatches.Close()
	grant, e := h.dispatches.SetGrant(context.Background(), s.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	if e != nil {
		t.Fatal(e)
	}
	native := pipelineProjectionFixture(t, h, manager, "session:"+s.ID, "native_tool", "native-fixture-rule", grant.Revision)
	_ = pipelineProjectionFixture(t, h, manager, "device:other-private-fixture", "paired_human", "must-not-disclose", 0)
	if r := invoke("native-read-projection", "mission_get", "", map[string]any{"manager": manager, "id": native.ID}); !r.OK {
		t.Fatal(r.Error)
	}
	invoke("mixed-owner-native-page-one", "mission_list", "", map[string]any{"manager": manager, "limit": 1, "cursor": ""})
	if r := invoke("native-mutation-denied", "mission_pause", "r_55555555555555555555555555555555", map[string]any{"manager": manager, "id": native.ID, "expected_revision": native.Revision}); r.OK || r.Error.Acceptance != "known_none" {
		t.Fatal(r)
	}
	wrong := manager
	wrong.Thread += "foreign"
	if r := invoke("native-foreign-thread-denied", "mission_get", "", map[string]any{"manager": wrong, "id": native.ID}); r.OK {
		t.Fatal(r)
	}
	if _, e = h.dispatches.SetGrant(context.Background(), s.ID, sessiondispatch.Grant{Enabled: false}, grant.Revision); e != nil {
		t.Fatal(e)
	}
	if r := invoke("native-revoked-mixed-page-denied", "mission_list", "", map[string]any{"manager": manager, "limit": 50, "cursor": ""}); r.OK {
		t.Fatal(r)
	}
	manifest := map[string]any{"status": "WIP_ACTUAL_PAIRED_HTTP_SQLITE_NOT_FINAL_SOURCE", "schema_hash": contract.Hash(), "source_commit": os.Getenv("PIPELINE_SOURCE_COMMIT"), "source_tree_hash": os.Getenv("PIPELINE_SOURCE_TREE"), "raw_test_refs": map[string]string{"log": os.Getenv("PIPELINE_RAW_LOG"), "receipt": os.Getenv("PIPELINE_RAW_RECEIPT")}, "source_qualified": false, "effects": map[string]any{"production": 0, "synthetic_original_queue_admissions": 1, "provider_model_calls": 0}, "entries": entries}
	raw, _ := json.MarshalIndent(manifest, "", "  ")
	if e := os.WriteFile(filepath.Join(packet, "manifest.json"), append(raw, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
}
