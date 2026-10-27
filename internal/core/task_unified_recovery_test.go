package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"everything-go/internal/delegation"
	"everything-go/internal/taskapi"
	"testing"
)

func TestTaskUnifiedOriginalOperationGetSettlesOnlyCanonicalPositiveAndContinuity(t *testing.T) {
	h, call, a, b := unifiedFixtureSessions(t)
	release := b.HoldQueue()
	defer release()
	manager := h.unifiedIdentity(a.ID)
	originalInput := map[string]any{"goal": "once original", "instruction": "isolated receipt recovery", "scope": map[string]any{"workspace_roots": []string{h.cfg.RootDir}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "acceptance": []any{}, "dependencies": []any{}, "workspace": map[string]any{"cwd": h.cfg.RootDir, "input_artifact_ids": []any{}}, "existing_target": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": b.ID, "expected_native_thread_id": b.ResumeID(), "expected_config_revision": b.SettingsSnapshot().ConfigRevision}}
	originalRequest := map[string]any{"type": "task_api_request", "api_version": "1.0.0-rc.1", "correlation_id": "original-recovery", "operation": "create_dispatch", "idempotency_key": "recover-once", "input": originalInput}
	r, _ := unifiedHTTP(t, call, manager, "dispatch", "recover-once", map[string]any{"original_request": originalRequest})
	initial := unifiedResult(t, r)
	var m taskapi.QueueMutation
	var j *taskapi.Journal
	var locator taskapi.Locator
	var originalBytes []byte
	for _, candidate := range h.apiJournals() {
		err := candidate.Transaction(t.Context(), func(tx *sql.Tx) error {
			var raw []byte
			err := tx.QueryRow("SELECT scope,generation,input_hash,original_locator,original_request FROM unified_facade_pointers WHERE operation='dispatch' AND intent='recover-once'").Scan(&m.Scope, &m.Generation, &m.Hash, &raw, &originalBytes)
			if err != nil {
				return err
			}
			return json.Unmarshal(raw, &locator)
		})
		if err == nil {
			j = candidate
			break
		}
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
	}
	if j == nil {
		t.Fatal("original facade pointer missing")
	}
	originalCount := 0
	if err := j.Transaction(t.Context(), func(tx *sql.Tx) error {
		return tx.QueryRow("SELECT COUNT(*) FROM task_api_intents").Scan(&originalCount)
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		var count int
		err := j.Transaction(t.Context(), func(tx *sql.Tx) error { return tx.QueryRow("SELECT COUNT(*) FROM task_api_intents").Scan(&count) })
		if err != nil || count != originalCount {
			t.Error("recovery replayed original acceptance", originalCount, count, err)
		}
	}()
	m.Operation = "dispatch"
	m.Intent = "recover-once"
	m.Revalidate = func(context.Context) error { return nil }
	// Inject only the wrapper-observation crash window after the original owner
	// durably accepted. The actual original dispatch/locator remains untouched.
	unknown := taskapi.MutationReceipt{Intent: m.Intent, Hash: m.Hash, State: "unknown", Data: json.RawMessage(`{"adapter_response":{"ok":false},"original_receipt_locator":{}}`)}
	rawUnknown, _ := json.Marshal(unknown)
	if err := j.Transaction(t.Context(), func(tx *sql.Tx) error {
		_, e := tx.Exec("UPDATE unified_facade_pointers SET receipt=? WHERE scope=? AND generation=? AND operation=? AND intent=?", rawUnknown, m.Scope, m.Generation, m.Operation, m.Intent)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	lookup := map[string]any{"original_operation": "dispatch", "original_idempotency_key": m.Intent}
	var wrongOriginal map[string]any
	if e := json.Unmarshal(originalBytes, &wrongOriginal); e != nil {
		t.Fatal(e)
	}
	wrongOriginal["input"].(map[string]any)["goal"] = "same task pointer, different original intent"
	replaceOriginal := func(raw []byte) {
		t.Helper()
		if err := j.Transaction(t.Context(), func(tx *sql.Tx) error {
			_, e := tx.Exec("UPDATE unified_facade_pointers SET original_request=? WHERE scope=? AND generation=? AND operation=? AND intent=?", raw, m.Scope, m.Generation, m.Operation, m.Intent)
			return e
		}); err != nil {
			t.Fatal(err)
		}
	}
	replaceOriginal(mustJSON(t, wrongOriginal))
	r, _ = unifiedHTTP(t, call, manager, "operation_get", "", lookup)
	if r["ok"] != false {
		t.Fatal("same canonical task with different original hash settled", r)
	}
	still, _, err := j.UnifiedFacadeRead(t.Context(), m)
	if err != nil || still.Receipt.State != "unknown" {
		t.Fatal("hash mismatch erased uncertainty", still, err)
	}
	replaceOriginal(originalBytes)
	r, _ = unifiedHTTP(t, call, manager, "operation_get", "", lookup)
	settled := unifiedResult(t, r)
	if settled["state"] != "applied" || settled["intent_id"] != initial["intent_id"] || settled["input_hash"] != initial["input_hash"] {
		t.Fatal("exact original not recovered", r)
	}
	if err := j.Transaction(t.Context(), func(tx *sql.Tx) error {
		var saved []byte
		e := tx.QueryRow("SELECT receipt FROM unified_facade_observations WHERE scope=? AND generation=? AND operation=? AND intent=? AND receipt_hash=?", m.Scope, m.Generation, m.Operation, m.Intent, taskapi.RawDigest(rawUnknown)).Scan(&saved)
		if e == nil && string(saved) != string(rawUnknown) {
			t.Fatal("unknown observation changed")
		}
		return e
	}); err != nil {
		t.Fatal(err)
	}
	r, _ = unifiedHTTP(t, call, manager, "operation_get", "", lookup)
	if taskapi.Digest(unifiedResult(t, r)) != taskapi.Digest(settled) {
		t.Fatal("settled receipt changed")
	}
	// An exact missing original key cannot become known-none or run the writer.
	missing := m
	missing.Intent = "missing-original"
	missing.Hash = taskapi.RawDigest([]byte("missing-facade-hash"))
	missingLocator := locator
	missingLocator.Key = "missing-original"
	if _, e := j.UnifiedFacadeClaim(t.Context(), missing, mustJSON(t, map[string]any{"type": "task_api_request", "api_version": "1.0.0-rc.1", "operation": "create_dispatch", "idempotency_key": "missing-original", "input": originalInput}), missingLocator); e != nil {
		t.Fatal(e)
	}
	if _, e := j.UnifiedFacadeFinish(t.Context(), missing, taskapi.Response{OK: false, Error: taskapi.Failure("unknown", "unknown", "lookup_original")}, missingLocator); e != nil {
		t.Fatal(e)
	}
	r, _ = unifiedHTTP(t, call, manager, "operation_get", "", map[string]any{"original_operation": "dispatch", "original_idempotency_key": missing.Intent})
	if r["ok"] != false {
		t.Fatal("missing original invented receipt", r)
	}
	p, _, e := j.UnifiedFacadeRead(t.Context(), missing)
	if e != nil || p.Receipt.State != "unknown" {
		t.Fatal("missing lookup erased uncertainty", p, e)
	}
	// Canonical continuity commands have the same hash/callback requirements even
	// though their original result lives on delegation's existing command journal.
	original := taskapi.Request{Type: "task_api_request", Version: "1.0.0-rc.1", Operation: "append", IdempotencyKey: "continuity-original", Input: mustJSON(t, map[string]any{"continuity": map[string]any{"action": "bind_qa", "arguments": map[string]string{"owner_delegation_id": "seed-owner", "qa_delegation_id": "seed-qa"}}})}
	digest, _ := taskapi.IntentHash(original)
	command := delegation.ContinuityCommand{Parent: a.ID, Key: original.IdempotencyKey, Hash: digest, Action: "bind_qa", Request: "isolated-original", Call: "isolated-call"}
	if _, e := h.delegations.ReserveContinuityCommand(t.Context(), command); e != nil {
		t.Fatal(e)
	}
	if e := h.delegations.FinishContinuityCommand(t.Context(), command, "completed", []byte(`{"seeded_canonical_continuity":true}`), ""); e != nil {
		t.Fatal(e)
	}
	cm := m
	cm.Operation = "qa_bind"
	cm.Intent = original.IdempotencyKey
	cm.Hash = taskapi.RawDigest([]byte("continuity-facade-hash"))
	cl := taskapi.Locator{Authority: h.cfg.InstanceID, Path: "delegation", Operation: "append", Key: original.IdempotencyKey}
	if _, e := j.UnifiedFacadeClaim(t.Context(), cm, mustJSON(t, original), cl); e != nil {
		t.Fatal(e)
	}
	if _, e := j.UnifiedFacadeFinish(t.Context(), cm, taskapi.Response{OK: false, Error: taskapi.Failure("unknown", "unknown", "lookup_original")}, cl); e != nil {
		t.Fatal(e)
	}
	r, _ = unifiedHTTP(t, call, manager, "operation_get", "", map[string]any{"original_operation": "qa_bind", "original_idempotency_key": cm.Intent})
	if unifiedResult(t, r)["state"] != "applied" {
		t.Fatal("continuity original not recovered", r)
	}
}
func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, e := json.Marshal(value)
	if e != nil {
		t.Fatal(e)
	}
	return raw
}
