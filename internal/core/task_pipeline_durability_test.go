package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	contract8 "everything-go/contracts/task-api/pipeline-v8"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/taskapi"
	p "everything-go/internal/taskpipeline"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func pipelineRequest8(t *testing.T, op, key string, input any) map[string]any {
	t.Helper()
	v := map[string]any{"type": "task_api_pipeline_request", "api_version": contract8.Version, "correlation_id": "synthetic-durable-" + op, "operation": op, "input": input}
	if key != "" {
		v["idempotency_key"] = key
	}
	raw, _ := json.Marshal(v)
	if e := contract8.Validate(raw); e != nil {
		t.Fatal(e, string(raw))
	}
	return v
}
func decodePipeline8(t *testing.T, raw []byte) taskapi.Response {
	t.Helper()
	if e := contract8.ValidateResponse(raw); e != nil {
		t.Fatal(e, string(raw))
	}
	var r taskapi.Response
	if e := json.Unmarshal(raw, &r); e != nil {
		t.Fatal(e)
	}
	return r
}
func notifySourceFixture(t *testing.T) (pipelineSourceFixture, p.Record, p.Issuer) {
	f := sourceWriterFixture(t)
	w := p.Watch{Source: f.source, Destination: f.manager, Predicate: p.Predicate{Kind: "success_sealed"}, Mode: "notify_only", CatchUp: true, Authorization: p.Authorization{SourceRef: "synthetic exact human intent", Budget: p.Budget{DeadlineMS: time.Now().Add(time.Hour).UnixMilli(), MaxNoProgress: 1}}}
	response := decodePipeline8(t, f.call("POST", "task-api", pipelineRequest8(t, "watch_create", "r_20020020020020020020020020020001", map[string]any{"manager": f.manager, "watch": w}), "phone").Body.Bytes())
	if !response.OK {
		t.Fatal(response.Error)
	}
	var r p.Receipt
	raw, _ := json.Marshal(response.Result)
	json.Unmarshal(raw, &r)
	i, record, e := f.h.messageQueue.PipelineStore().Issuer(context.Background(), r.Record.ID)
	if e != nil {
		t.Fatal(e)
	}
	return f, record, i
}
func TestPipelineDurabilityOriginalSourcesVectorPagesGapAndOffline(t *testing.T) {
	f, r, i := notifySourceFixture(t)
	ctx := context.Background()
	h := f.h
	store := h.messageQueue.PipelineStore()
	// More than one original store page, interleaved independent transactions.
	for _, j := range []*taskapi.Journal{h.messageQueue.TaskJournal(), h.delegations.TaskJournal()} {
		if e := j.Transaction(ctx, func(tx *sql.Tx) error {
			for k := 0; k < 205; k++ {
				if e := taskapi.ChangeTx(tx, "other-synthetic", "not-observed", "bounded-original-event"); e != nil {
					return e
				}
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
	}
	facts, vector, e := h.pipelineSourceBatch(ctx, i, r)
	if e != nil || len(vector) != 2 || len(facts) != 0 {
		t.Fatal(len(facts), vector, e)
	}
	for _, c := range vector {
		if c.State != "partial" || c.After >= c.Watermark {
			t.Fatal(c)
		}
	}
	if e = store.Observe(ctx, r.ID, r.Revision, facts, func(context.Context) error { return nil }, vector...); e != nil {
		t.Fatal(e)
	}
	// Bounded pages advance only consumed sequence, surviving a new Store wrapper.
	for k := 0; k < 4; k++ {
		current, _, _, _ := store.Get(ctx, i.Scope, r.ID)
		facts, vector, e = h.pipelineSourceBatch(ctx, i, current)
		if e != nil {
			t.Fatal(e)
		}
		if e = store.Observe(ctx, current.ID, current.Revision, facts, func(context.Context) error { return nil }, vector...); e != nil {
			t.Fatal(e)
		}
	}
	saved, e := store.SourceCursors(ctx, i.Scope, r.ID)
	if e != nil || len(saved) != 2 {
		t.Fatal(saved, e)
	}
	for _, c := range saved {
		if c.After != c.Watermark || c.State != "current" || c.StoreIdentity == "" {
			t.Fatal(c)
		}
	}
	// A fresh definition binds the same original exact work, then loses old journal
	// pages. Actual original Prune produces cursor_expired, not synthetic flags.
	w := *r.Watch
	w.CatchUp = false
	next := p.Record{Kind: "watch", Manager: f.manager, Watch: &w}
	created, e := store.Mutate(ctx, i.Scope, i, "new-gap-watch", "watch_create", "new-gap-hash", "", 0, &next, nil, nil, func(context.Context) error { return nil })
	if e != nil {
		t.Fatal(e)
	}
	r = *created.Record
	j := h.delegations.TaskJournal()
	if e = j.Prune(ctx, time.Now().Add(90*24*time.Hour)); e != nil {
		t.Fatal(e)
	}
	facts, vector, e = h.pipelineSourceBatch(ctx, i, r)
	if e != nil {
		t.Fatal(e)
	}
	gap := false
	for _, c := range vector {
		if c.Store == "delegation" {
			gap = c.State == "snapshot_recovered" && c.After == c.Watermark && c.Floor > 0
		}
	}
	if !gap {
		t.Fatal(vector, facts)
	}
	if e = store.Observe(ctx, r.ID, r.Revision, facts, func(context.Context) error { return nil }, vector...); e != nil {
		t.Fatal(e)
	}
	// Source store unavailable: do not skip or consume it as a proved empty page.
	current, _, _, _ := store.Get(ctx, i.Scope, r.ID)
	facts, vector, e = h.pipelineSourceBatch(ctx, i, current)
	if e != nil {
		t.Fatal(e)
	}
	if e = store.Observe(ctx, r.ID, current.Revision, facts, func(context.Context) error { return nil }, vector...); e != nil {
		t.Fatal(e)
	}
	saved, _ = store.SourceCursors(ctx, i.Scope, r.ID)
	if e = h.delegations.Close(); e != nil {
		t.Fatal(e)
	}
	current, _, _, _ = store.Get(ctx, i.Scope, r.ID)
	facts, vector, e = h.pipelineSourceBatch(ctx, i, current)
	if e != nil || len(facts) != 0 {
		t.Fatal(e, facts)
	}
	for _, c := range vector {
		for _, old := range saved {
			if c.Key == old.Key && c.After != old.After {
				t.Fatal("offline source consumed", c, old)
			}
		}
		if c.State != "offline" {
			t.Fatal(c)
		}
	}
}
func TestPipelineDurabilityHTTPReceiptsNativeZeroNoReplayAndWrongScope(t *testing.T) {
	f, r, i := notifySourceFixture(t)
	ctx := context.Background()
	store := f.h.messageQueue.PipelineStore()
	facts, vector, err := f.h.pipelineSourceBatch(ctx, i, r)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Observe(ctx, r.ID, r.Revision, facts, func(context.Context) error { return nil }, vector...); err != nil {
		t.Fatal(err)
	}
	pendingNotices, err := store.PendingNotifications(ctx, 10)
	if err != nil || len(pendingNotices) != 1 {
		t.Fatal(pendingNotices, err)
	}
	if err = store.DeliverNotification(ctx, pendingNotices[0], func(context.Context) error { return errors.New("isolated temporary delivery fault") }); err == nil {
		t.Fatal("missing delivery failure")
	}
	if err = store.NotificationFailed(ctx, pendingNotices[0], "visual_delivery_unavailable"); err != nil {
		t.Fatal(err)
	}
	entries := []any{}
	directory := os.Getenv("PIPELINE_DURABILITY_WIRE_OUT")
	if directory != "" {
		if e := os.Mkdir(directory, 0700); e != nil {
			t.Fatal(e)
		}
	}
	invoke := func(name, op, key string, input any) taskapi.Response {
		req := pipelineRequest8(t, op, key, input)
		raw, _ := json.Marshal(req)
		w := f.call("POST", "task-api", req, "phone")
		resp := decodePipeline8(t, w.Body.Bytes())
		if directory != "" {
			rp := name + ".request.json"
			sp := name + ".response.json"
			if e := os.WriteFile(filepath.Join(directory, rp), raw, 0600); e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(filepath.Join(directory, sp), w.Body.Bytes(), 0600); e != nil {
				t.Fatal(e)
			}
			entries = append(entries, map[string]any{"name": name, "operation": op, "request_path": rp, "request_sha256": taskapi.RawDigest(raw), "request_bytes": len(raw), "response_path": sp, "response_sha256": taskapi.RawDigest(w.Body.Bytes()), "response_bytes": w.Body.Len(), "http_status": w.Code, "expected_schema_valid": true, "ok": resp.OK})
		}
		return resp
	}
	invoke("capabilities", "capabilities", "", map[string]any{})
	status := invoke("source-status", "source_status", "", map[string]any{"manager": f.manager, "id": r.ID})
	if !status.OK {
		t.Fatal(status.Error)
	}
	result := invoke("notification-list", "notification_list", "", map[string]any{"manager": f.manager, "id": r.ID, "cursor": "", "limit": 10})
	if !result.OK {
		t.Fatal(result.Error)
	}
	notices, _, e := store.Notifications(ctx, i.Scope, r.ID, "", 10)
	if e != nil || len(notices) != 1 || notices[0].State != "failed" {
		t.Fatal(notices, e)
	}
	n := notices[0]
	retryInput := map[string]any{"manager": f.manager, "id": r.ID, "notification_id": n.ID, "expected_revision": n.Revision}
	retryKey := "r_20020020020020020020020020020004"
	if !invoke("notification-retry", "notification_retry", retryKey, retryInput).OK {
		t.Fatal("retry failed")
	}
	retryHash := unifiedHash(taskapi.Request{Operation: "notification_retry", Input: mustPipelineJSON(retryInput)})
	if !invoke("retry-original", "operation_get", "", map[string]any{"manager": f.manager, "original_intent_id": retryKey, "original_operation": "notification_retry", "expected_input_hash": retryHash}).OK {
		t.Fatal("retry receipt missing")
	}
	f.h.reconcilePipelineNotifications(ctx)
	notices, _, _ = store.Notifications(ctx, i.Scope, r.ID, "", 10)
	n = notices[0]
	if n.State != "inbox_committed" {
		t.Fatal(n)
	}
	invoke("notification-inbox", "notification_list", "", map[string]any{"manager": f.manager, "id": r.ID, "cursor": "", "limit": 10})
	input := map[string]any{"manager": f.manager, "id": r.ID, "notification_id": n.ID, "expected_revision": n.Revision}
	key := "r_20020020020020020020020020020002"
	ack := invoke("notification-displayed", "notification_ack", key, input)
	if !ack.OK {
		t.Fatal(ack.Error)
	}
	hash := unifiedHash(taskapi.Request{Operation: "notification_ack", Input: mustPipelineJSON(input)})
	original := invoke("display-original", "operation_get", "", map[string]any{"manager": f.manager, "original_intent_id": key, "original_operation": "notification_ack", "expected_input_hash": hash})
	if !original.OK {
		t.Fatal(original.Error)
	}
	unknown := invoke("display-unknown", "operation_get", "", map[string]any{"manager": f.manager, "original_intent_id": "r_ffffffffffffffffffffffffffffffff", "original_operation": "notification_ack", "expected_input_hash": hash})
	if unknown.OK || unknown.Error.Acceptance != "unknown" {
		t.Fatal(unknown)
	}
	invoke("trigger-policy", "trigger_policy_get", "", map[string]any{"manager": f.manager})
	policyInput := map[string]any{"manager": f.manager, "expected_revision": 0, "enabled": false}
	policyKey := "r_20020020020020020020020020020003"
	disabled := invoke("trigger-disable", "trigger_policy_set", policyKey, policyInput)
	if !disabled.OK {
		t.Fatal(disabled.Error)
	}
	phash := unifiedHash(taskapi.Request{Operation: "trigger_policy_set", Input: mustPipelineJSON(policyInput)})
	if !invoke("trigger-original", "operation_get", "", map[string]any{"manager": f.manager, "original_intent_id": policyKey, "original_operation": "trigger_policy_set", "expected_input_hash": phash}).OK {
		t.Fatal("original policy unavailable")
	}
	foreign := f.manager
	foreign.Revision++
	wrong := invoke("wrong-revision", "notification_list", "", map[string]any{"manager": foreign, "id": r.ID, "cursor": "", "limit": 10})
	if wrong.OK {
		t.Fatal(wrong)
	}
	// Source revocation discloses no notification fact/hash/locator or cursor.
	hidden := true
	f.child.SetMeta(nil, &hidden)
	revoked := invoke("revoked-source", "source_status", "", map[string]any{"manager": f.manager, "id": r.ID})
	if revoked.OK {
		t.Fatal("revoked source vector leaked", revoked)
	}
	// A reply metadata mutation, start notice or visual receipt is never a model start.
	pending, _ := store.Pending(ctx, 32)
	if len(pending) != 0 {
		t.Fatal(pending)
	}
	if directory != "" {
		manifest := map[string]any{"status": "WIP_ACTUAL_ISOLATED_HTTP_NOT_RUNTIME_QUALIFICATION", "schema_hash": contract8.Hash(), "entries": entries}
		raw, _ := json.MarshalIndent(manifest, "", "  ")
		if e := os.WriteFile(filepath.Join(directory, "manifest.json"), raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
}

func TestPipelineDurabilityDisablePreservesAdmittedNativeFinalAndSourceReadback(t *testing.T) {
	f, r, i, c := definitionCarryFixture(t, false)
	ctx := context.Background()
	store := f.h.messageQueue.PipelineStore()
	disabled := decodePipeline8(t, f.call("POST", "task-api", pipelineRequest8(t, "trigger_policy_set", "r_20020020020020020020020020020005", map[string]any{"manager": f.manager, "expected_revision": 0, "enabled": false}), "phone").Body.Bytes())
	if !disabled.OK {
		t.Fatal(disabled.Error)
	}
	bound, found, e := f.h.delegations.ByID(ctx, c.OriginalReceipt.NativeID)
	if e != nil || !found {
		t.Fatal(e)
	}
	bound.Result = "original native final after global own-scope disable"
	correctionSeal(t, f.h, bound, false)
	f.h.Emit(protocol.NewDone(f.child.ID, bound.ChildRequestID))
	if e = f.h.reconcileDelegations(ctx); e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entry, found, _ := f.h.messageQueue.GetContext(ctx, f.child.ID, bound.ChildRequestID)
		if found && entry.State == messagequeue.Completed {
			break
		}
		time.Sleep(time.Millisecond)
	}
	f.h.reconcileTaskPipelines(ctx)
	original, found, e := store.ExactContinuation(ctx, c.ID)
	if e != nil || !found || original.State != "checkpointed" {
		t.Fatal("disable blocked original admitted final", original, e)
	}
	current, _, found, e := store.Get(ctx, i.Scope, r.ID)
	if e != nil || !found {
		t.Fatal(e)
	}
	for _, v := range current.Steps {
		if v.ID == "C" && v.ContinuationID != "" {
			t.Fatal("disabled new frontier admitted", v)
		}
	}
	status := decodePipeline8(t, f.call("POST", "task-api", pipelineRequest8(t, "source_status", "", map[string]any{"manager": f.manager, "id": r.ID}), "phone").Body.Bytes())
	if !status.OK {
		t.Fatal("disabled source read unavailable", status.Error)
	}
	// Original admitted tuple/seal is available despite the control switch. No
	// historical immutable receipt is rewritten into a new unissued effect.
	seal, e := f.h.pipelineAdmittedFact(ctx, original, true)
	if e != nil || seal.SealID == "" {
		t.Fatal(seal, e)
	}
}

func TestPipelineDurabilityActualGoGolden(t *testing.T) {
	directory := os.Getenv("PIPELINE_DURABILITY_GOLDEN_OUT")
	if directory == "" {
		return
	}
	inputs := []string{`{"manager":{"authority_instance_id":"本機<>&\u2028\u2029","session_id":"原對話","thread_id":"native","config_revision":0},"10":"ten","2":"two","\ue000":"private-use","💡":"astral"}`, `{"nested":{"negative":-3,"maximum":9007199254740991,"normal":1000},"text":"繁體中文 <>&\u2028\u2029","zero":0}`}
	vectors := []any{}
	for _, input := range inputs {
		var value any
		if e := json.Unmarshal([]byte(input), &value); e != nil {
			t.Fatal(e)
		}
		req := taskapi.Request{Operation: "trigger_policy_set", Input: json.RawMessage(input)}
		vectors = append(vectors, map[string]any{"operation": req.Operation, "input": value, "expected_hash": unifiedHash(req)})
	}
	out := map[string]any{"status": "WIP_ACTUAL_GO_FUNCTION_NOT_SOURCE_QUALIFIED", "schema_hash": contract8.Hash(), "function": "core.unifiedHash (actual rc8 mutation path)", "algorithm": "SHA256(encoding/json Marshal struct{operation,input}); excludes api_version/correlation/idempotency key", "vectors": vectors}
	raw, _ := json.MarshalIndent(out, "", "  ")
	f, e := os.OpenFile(filepath.Join(directory, "HASH_GOLDEN_RC8_R1.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if _, e = f.Write(raw); e != nil {
		t.Fatal(e)
	}
}

func TestPipelineDurabilityPartialSourcePageBlocksExistingReadyClaim(t *testing.T) {
	f := sourceWriterFixture(t)
	r, _, c := f.ready(t, "session_resume", "r_20020020020020020020020020020006")
	ctx := context.Background()
	for _, j := range []*taskapi.Journal{f.h.messageQueue.TaskJournal(), f.h.delegations.TaskJournal()} {
		if e := j.Transaction(ctx, func(tx *sql.Tx) error {
			for k := 0; k < 205; k++ {
				if e := taskapi.ChangeTx(tx, "synthetic-unrelated", "unchanged", "finite interleave"); e != nil {
					return e
				}
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
	}
	f.h.reconcileTaskPipelines(ctx)
	current, found, e := f.h.messageQueue.PipelineStore().ExactContinuation(ctx, c.ID)
	if e != nil || !found || current.State != "ready" || current.OriginalReceipt != nil {
		t.Fatal("partial page admitted an unchecked ready claim", current, e)
	}
	for k := 0; k < 5; k++ {
		f.h.reconcileTaskPipelines(ctx)
	}
	current, found, e = f.h.messageQueue.PipelineStore().ExactContinuation(ctx, c.ID)
	if e != nil || !found || current.OriginalReceipt == nil {
		t.Fatal("source pages did not resume original same key", current, e)
	}
	if current.ID != c.ID || current.RecordID != r.ID || current.RequestID != c.RequestID {
		t.Fatal("source retry minted new intent", current, c)
	}
}
func TestPipelineDurabilityStoreEpochGapIsExplicitAndRestartKeepsIdentity(t *testing.T) {
	f := sourceWriterFixture(t)
	r, i, c := f.ready(t, "session_resume", "r_20020020020020020020020020020007")
	ctx := context.Background()
	store := f.h.messageQueue.PipelineStore()
	facts, vector, e := f.h.pipelineSourceBatch(ctx, i, r)
	if e != nil {
		t.Fatal(e)
	}
	current, _, _, _ := store.Get(ctx, i.Scope, r.ID)
	if e = store.Observe(ctx, r.ID, current.Revision, facts, func(context.Context) error { return nil }, vector...); e != nil {
		t.Fatal(e)
	}
	epoch, e := f.h.delegations.TaskJournal().Identity(ctx)
	if e != nil {
		t.Fatal(e)
	}
	again, e := f.h.delegations.TaskJournal().Identity(ctx)
	if e != nil || again != epoch {
		t.Fatal("ordinary reopen changed original epoch", epoch, again, e)
	}
	// A genuine replacement epoch cannot silently adopt the old cursor/claim.
	if e = f.h.delegations.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		_, e := tx.Exec(`UPDATE task_api_journal_identity SET identity='synthetic-replacement-epoch' WHERE id=1`)
		return e
	}); e != nil {
		t.Fatal(e)
	}
	f.h.reconcileTaskPipelines(ctx)
	saved, found, e := store.ExactContinuation(ctx, c.ID)
	if e != nil || !found || saved.State != "ready" || saved.OriginalReceipt != nil {
		t.Fatal("new store identity admitted old claim", saved, e)
	}
	response := decodePipeline8(t, f.call("POST", "task-api", pipelineRequest8(t, "source_status", "", map[string]any{"manager": f.manager, "id": r.ID}), "phone").Body.Bytes())
	if !response.OK {
		t.Fatal(response.Error)
	}
	raw, _ := json.Marshal(response.Result)
	var status struct {
		Items    []p.SourceCursor `json:"items"`
		Complete bool             `json:"complete"`
	}
	json.Unmarshal(raw, &status)
	if status.Complete || len(status.Items) != 2 {
		t.Fatal("gap hidden by cursor count or old epoch mixed", status)
	}
	gap := false
	for _, v := range status.Items {
		if v.Store == "delegation" {
			gap = v.State == "evidence_gap" && v.Reason == "source_store_identity_changed"
		}
	}
	if !gap {
		t.Fatal(status)
	}
	all, e := store.SourceCursors(ctx, i.Scope, r.ID)
	if e != nil || len(all) != 3 {
		t.Fatal("original cursor audit lost", all, e)
	}
}
