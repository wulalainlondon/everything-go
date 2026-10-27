package core

import (
	"encoding/base64"
	"encoding/json"
	unified "everything-go/contracts/task-api/unified-v6"
	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type unifiedWire struct {
	Kind   string `json:"kind"`
	Path   string `json:"relative_path"`
	Hash   string `json:"sha256"`
	Status int    `json:"response_http_status"`
	Bytes  int    `json:"bytes"`
	Valid  bool   `json:"expected_schema_valid"`
}

func unifiedHTTP(t *testing.T, call func(string, string, any, string) *httptest.ResponseRecorder, manager jarvisTarget, op, key string, input map[string]any) (map[string]any, []byte) {
	t.Helper()
	if input == nil {
		input = map[string]any{}
	}
	if op != "capabilities" {
		input["manager"] = manager
	}
	req := map[string]any{"type": "task_api_unified_request", "api_version": unified.Version, "correlation_id": "fixture-" + op, "operation": op, "input": input}
	if key != "" {
		req["idempotency_key"] = key
	}
	w := call("POST", "task-api", req, "phone")
	if w.Code != 200 {
		t.Fatalf("HTTP%d %s", w.Code, w.Body.String())
	}
	if e := unified.ValidateResponse(w.Body.Bytes()); e != nil {
		t.Fatalf("invalid response %s: %v", w.Body.String(), e)
	}
	var out map[string]any
	if e := json.Unmarshal(w.Body.Bytes(), &out); e != nil {
		t.Fatal(e)
	}
	return out, append([]byte(nil), w.Body.Bytes()...)
}
func unifiedResult(t *testing.T, r map[string]any) map[string]any {
	t.Helper()
	if r["ok"] != true {
		t.Fatal(r)
	}
	return r["result"].(map[string]any)
}
func unifiedFixtureSessions(t *testing.T) (*Hub, func(string, string, any, string) *httptest.ResponseRecorder, *session.Session, *session.Session) {
	t.Helper()
	root := t.TempDir()
	pair := governance.NewPairing(filepath.Join(root, "pairing.json"))
	if err := pair.Claim("jarvis-token", "phone"); err != nil {
		t.Fatal(err)
	}
	h := NewHub(session.NewRegistry(), Config{InstanceID: "unified-fixture", RootDir: root, DataDir: root}, pair, 0)
	h.SetExecutor(&scopeFixtureExecutor{fakeExec: &fakeExec{sink: h}})
	t.Cleanup(func() { h.messageQueue.Close(); h.delegations.Close(); h.dispatches.Close() })
	call := func(method, path string, input any, device string) *httptest.ResponseRecorder {
		var body []byte
		if text, ok := input.(string); ok {
			body = []byte(text)
		} else {
			body, _ = json.Marshal(input)
		}
		r := httptest.NewRequest(method, "/api/jarvis/v1/"+path, strings.NewReader(string(body)))
		r.Header.Set("Authorization", "Bearer jarvis-token")
		r.Header.Set("X-Bridge-Device-ID", device)
		w := httptest.NewRecorder()
		h.ServeJarvisAPI(w, r)
		return w
	}
	a := h.registry.Create("s_a", "A", h.cfg.RootDir, "codex", "m", "read-only", "")
	a.SetResumeID("thread-a")
	b := h.registry.Create("s_b", "B", h.cfg.RootDir, "codex", "m", "read-only", "")
	b.SetResumeID("thread-b")
	t.Cleanup(func() { a.Close(); b.Close() })
	_, e := h.dispatches.SetGrant(t.Context(), a.ID, sessiondispatch.Grant{Enabled: true, Local: true, Sessions: []string{h.cfg.InstanceID + ":" + b.ID}}, 0)
	if e != nil {
		t.Fatal(e)
	}
	return h, call, a, b
}
func seedUnifiedController(t *testing.T, h *Hub, a, b *session.Session, id string) unifiedLocator {
	t.Helper()
	_, e := h.dispatches.Create(t.Context(), sessiondispatch.Record{ID: id, ParentID: a.ID, ParentThreadID: a.ResumeID(), ParentConfigKnown: true, ParentConfigRevision: a.SettingsSnapshot().ConfigRevision, OriginRequestID: "origin-" + id, ToolCallID: "original-call-" + id, IntentHash: taskapi.RawDigest([]byte(id)), InstanceID: h.cfg.InstanceID, SessionID: b.ID, ThreadID: b.ResumeID(), ConfigRevision: b.SettingsSnapshot().ConfigRevision, RequestID: "r_" + id, Content: "original " + id, Mode: "queue"})
	if e != nil {
		t.Fatal(e)
	}
	return unifiedLocator{Kind: "controller", Authority: h.cfg.InstanceID, NativeID: id}
}
func TestTaskUnifiedHTTPQueueEditReplyDocumentsAndFiniteWire(t *testing.T) {
	h, call, a, b := unifiedFixtureSessions(t)
	manager := h.unifiedIdentity(a.ID)
	loc := seedUnifiedController(t, h, a, b, "scd_original")
	entries := []unifiedWire{}
	outDir := os.Getenv("TASK_UNIFIED_HTTP_FIXTURE_DIR")
	capture := func(name, kind string, raw []byte) {
		if outDir == "" {
			return
		}
		if e := os.MkdirAll(outDir, 0700); e != nil {
			t.Fatal(e)
		}
		p := filepath.Join(outDir, name+".json")
		f, e := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = f.Write(raw); e != nil {
			t.Fatal(e)
		}
		f.Close()
		entries = append(entries, unifiedWire{kind, filepath.Base(p), taskapi.RawDigest(raw), 200, len(raw), true})
	}
	r, raw := unifiedHTTP(t, call, manager, "capabilities", "", nil)
	capture("capabilities", "capabilities", raw)
	if unifiedResult(t, r)["schema_hash"] != unified.Hash() {
		t.Fatal(r)
	}
	r, raw = unifiedHTTP(t, call, manager, "resolve", "", map[string]any{"locator": loc})
	capture("resolve-original-scd", "task", raw)
	resolved := unifiedResult(t, r)
	if resolved["locator"].(map[string]any)["native_id"] != "scd_original" || resolved["edit"] == nil {
		t.Fatal("lost native identity/edit", resolved)
	}
	q := unifiedQueue{"controller_waiting_admission", h.unifiedIdentity(b.ID)}
	r, raw = unifiedHTTP(t, call, manager, "queue_snapshot", "", map[string]any{"queue": q})
	capture("queue-before", "queue", raw)
	view := unifiedResult(t, r)
	row := view["items"].([]any)[0].(map[string]any)
	edit := map[string]any{"locator": loc, "expected_queue_revision": view["revision"], "expected_content_revision": row["content_revision"], "expected_content_hash": row["content_hash"], "content": "新修訂繁體內容"}
	r, raw = unifiedHTTP(t, call, manager, "queue_edit", "original-edit", edit)
	capture("edit-applied", "receipt", raw)
	receipt := unifiedResult(t, r)
	if receipt["state"] != "applied" {
		t.Fatal(receipt)
	}
	r, raw = unifiedHTTP(t, call, manager, "operation_get", "", map[string]any{"queue": q, "original_operation": "queue_edit", "original_idempotency_key": "original-edit"})
	capture("edit-original-lookup", "receipt", raw)
	if taskapi.Digest(unifiedResult(t, r)) != taskapi.Digest(receipt) {
		t.Fatal("receipt changed")
	}
	r, raw = unifiedHTTP(t, call, manager, "get", "", map[string]any{"locator": loc})
	capture("get-effective", "task", raw)
	task := unifiedResult(t, r)
	if task["edit"].(map[string]any)["content"] != "新修訂繁體內容" {
		t.Fatal(task)
	}
	original, _, _ := h.dispatches.Get(t.Context(), "scd_original")
	if original.Content != "original scd_original" {
		t.Fatal("original overwritten")
	}
	// Admission copies the effective version before original actor submission.
	holdAdmission := b.HoldQueue()
	defer holdAdmission()
	admitted := h.submitLocalSessionDispatch(t.Context(), original)
	if admitted.State != "queued" {
		t.Fatal("original admission failed", admitted)
	}
	inherited, ok, e := h.messageQueue.EffectiveInput(t.Context(), b.ID, admitted.RequestID)
	if e != nil || !ok || inherited.ContentRevision != 2 || inherited.Content != "新修訂繁體內容" {
		t.Fatal("version lost at native queue", inherited, e)
	}
	r, _ = unifiedHTTP(t, call, manager, "get", "", map[string]any{"locator": loc})
	task = unifiedResult(t, r)
	// Metadata reports never wake the parent or answer human-only questions.
	routes := task["reply_routes"].([]any)
	route := routes[0].(map[string]any)
	reply := map[string]any{"locator": loc, "direction": route["direction"], "source": route["source"], "target": route["target"], "reply_to_request_id": route["reply_to_request_id"], "reply_to_hash": route["reply_to_hash"], "delivery": "metadata", "content": "原對話補充"}
	r, raw = unifiedHTTP(t, call, manager, "reply", "original-report", reply)
	capture("reply-metadata", "receipt", raw)
	unifiedResult(t, r)
	r, _ = unifiedHTTP(t, call, h.unifiedIdentity(b.ID), "get", "", map[string]any{"locator": loc})
	workerTask := unifiedResult(t, r)
	if workerTask["reply_routes"].([]any)[0].(map[string]any)["direction"] != "worker_to_parent" {
		t.Fatal("no B to A route", r)
	}
	// Queue reply uses original actor, hold it only in this isolated test.
	release := b.HoldQueue()
	defer release()
	reply["delivery"] = "queue"
	reply["content"] = ""
	reply["images"] = []intakeImage{{Data: base64.StdEncoding.EncodeToString([]byte("image-fixture")), MediaType: "image/png"}}
	r, raw = unifiedHTTP(t, call, manager, "reply", "original-media-reply", reply)
	capture("reply-pure-media-queued", "receipt", raw)
	rr := unifiedResult(t, r)
	d := rr["data"].(map[string]any)
	rid := d["request_id"].(string)
	entry, found, e := h.messageQueue.Get(b.ID, rid)
	if e != nil || !found || entry.State != messagequeue.Queued {
		t.Fatal(entry, e)
	}
	r, _ = unifiedHTTP(t, call, manager, "reply", "original-media-reply", reply)
	if taskapi.Digest(unifiedResult(t, r)) != taskapi.Digest(rr) {
		t.Fatal("replayed reply")
	}
	if b.QueueLen() != 2 {
		t.Fatal("new executor", b.QueueLen())
	}
	r, raw = unifiedHTTP(t, call, manager, "get", "", map[string]any{"locator": unifiedLocator{Kind: "request", Authority: h.cfg.InstanceID, SessionID: b.ID, RequestID: rid}})
	capture("reply-original-get", "task", raw)
	unifiedResult(t, r)
	// Exact terminal seal is seeded isolated engineering evidence, not live proof.

	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: b.ID, RequestID: "r_scd_original", ThreadID: b.ResumeID(), TurnID: "fixture-native-turn"})
	if _, _, err := h.messageQueue.Transition(b.ID, "r_scd_original", []messagequeue.State{messagequeue.Queued}, messagequeue.Completed, "", "", ""); err != nil {
		t.Fatal(err)
	}
	anchor, _ := json.Marshal(map[string]any{"instance_id": h.cfg.InstanceID, "session_id": b.ID, "request_id": "r_scd_original", "source_message_id": "final-r_scd_original", "execution": map[string]string{"backend": "codex", "native_thread_id": b.ResumeID(), "native_turn_id": "fixture-native-turn"}})
	if _, err := h.messageQueue.SealTask(t.Context(), messagequeue.TaskSeal{ID: "seal_fixture", SessionID: b.ID, RequestID: "r_scd_original", Text: "result", Hash: jarvisTextHash("result"), Anchor: anchor}); err != nil {
		t.Fatal(err)
	}
	seal, e := h.messageQueue.TaskSeal(t.Context(), b.ID, "r_scd_original", "seal_fixture")
	if e != nil {
		t.Fatal(e)
	}
	r, raw = unifiedHTTP(t, call, manager, "get", "", map[string]any{"locator": loc})
	capture("get-seeded-terminal-final", "task", raw)
	if unifiedResult(t, r)["final"] == nil {
		t.Fatal("seeded final ref missing")
	}
	r, raw = unifiedHTTP(t, call, manager, "read_result", "", map[string]any{"locator": loc, "owner_request_id": seal.RequestID, "seal_id": seal.ID, "expected_hash": seal.Hash, "offset": 0, "limit": 32768})
	capture("exact-result-bytes", "chunk", raw)
	chunk := unifiedResult(t, r)
	if chunk["full_hash"] != seal.Hash {
		t.Fatal(chunk)
	}
	body := []byte("唯讀交接\n")
	path := "handoff.txt"
	if e = os.WriteFile(filepath.Join(h.cfg.RootDir, path), body, 0600); e != nil {
		t.Fatal(e)
	}
	r, raw = unifiedHTTP(t, call, manager, "document_collect", "original-collect", map[string]any{"locator": loc, "owner_request_id": seal.RequestID, "seal_id": seal.ID, "path": path, "expected_hash": taskapi.RawDigest(body)})
	capture("document-collected", "receipt", raw)
	doc := unifiedResult(t, r)["data"].(map[string]any)
	docID := doc["document_id"].(string)
	r, raw = unifiedHTTP(t, call, manager, "document_grant", "original-grant", map[string]any{"locator": loc, "document_id": docID, "consumer": h.unifiedIdentity(b.ID), "purpose": "read-only handoff", "expires_at": time.Now().Add(time.Hour).UnixMilli()})
	capture("document-granted", "receipt", raw)
	grant := unifiedResult(t, r)["data"].(map[string]any)["grant_id"].(string)
	if e = os.WriteFile(filepath.Join(h.cfg.RootDir, path), []byte("changed current file"), 0600); e != nil {
		t.Fatal(e)
	}
	r, raw = unifiedHTTP(t, call, manager, "read_document", "", map[string]any{"locator": loc, "document_id": docID, "expected_hash": taskapi.RawDigest(body), "offset": 0, "limit": 32768})
	capture("immutable-document-bytes", "chunk", raw)
	if unifiedResult(t, r)["data_base64"] != base64.StdEncoding.EncodeToString(body) {
		t.Fatal("read mutable file")
	}
	r, raw = unifiedHTTP(t, call, manager, "document_revoke", "original-revoke", map[string]any{"locator": loc, "grant_id": grant})
	capture("document-revoked", "receipt", raw)
	unifiedResult(t, r)
	r, raw = unifiedHTTP(t, call, h.unifiedIdentity(b.ID), "read_document", "", map[string]any{"locator": loc, "document_id": docID, "grant_id": grant, "expected_hash": taskapi.RawDigest(body), "offset": 0, "limit": 32768})
	capture("revoked-document-error", "error", raw)
	if r["ok"] != false {
		t.Fatal("revoked read", r)
	}
	bad := loc
	bad.Authority = "foreign"
	r, raw = unifiedHTTP(t, call, manager, "get", "", map[string]any{"locator": bad})
	capture("wrong-authority-error", "error", raw)
	if r["ok"] != false {
		t.Fatal(r)
	}

	originalInput := map[string]any{"goal": "wrapper original", "instruction": "isolated original instruction", "scope": map[string]any{"workspace_roots": []string{h.cfg.RootDir}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "acceptance": []any{}, "dependencies": []any{}, "workspace": map[string]any{"cwd": h.cfg.RootDir, "input_artifact_ids": []any{}}, "existing_target": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": b.ID, "expected_native_thread_id": b.ResumeID(), "expected_config_revision": b.SettingsSnapshot().ConfigRevision}}
	originalRequest := map[string]any{"type": "task_api_request", "api_version": "1.0.0-rc.1", "correlation_id": "original-wrapper", "operation": "create_dispatch", "idempotency_key": "original-wrapper-id", "input": originalInput}
	r, raw = unifiedHTTP(t, call, manager, "dispatch", "original-wrapper-id", map[string]any{"original_request": originalRequest})
	capture("dispatch-uniform-receipt", "receipt", raw)
	wrapped := unifiedResult(t, r)
	r, raw = unifiedHTTP(t, call, manager, "operation_get", "", map[string]any{"original_operation": "dispatch", "original_idempotency_key": "original-wrapper-id"})
	capture("dispatch-original-operation-lookup", "receipt", raw)
	if taskapi.Digest(unifiedResult(t, r)) != taskapi.Digest(wrapped) {
		t.Fatal("wrapper receipt lost")
	}
	if outDir != "" {
		manifest := map[string]any{"schema_hash": unified.Hash(), "source_commit": os.Getenv("TASK_UNIFIED_SOURCE_COMMIT"), "source_tree_hash": os.Getenv("TASK_UNIFIED_SOURCE_TREE_HASH"), "raw_test_refs": []string{os.Getenv("TASK_UNIFIED_RAW_LOG"), os.Getenv("TASK_UNIFIED_RAW_RECEIPT")}, "scope": "real isolated paired HTTP and original SQLite stores; no provider model/native device effects; explicitly seeded engineering terminal seal", "entries": entries}
		b, _ := json.MarshalIndent(manifest, "", "  ")
		f, e := os.OpenFile(filepath.Join(outDir, "manifest.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		f.Write(append(b, '\n'))
	}
}
func TestTaskUnifiedCollectorRefusesSymlinkOutsideAndLarge(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "private")
	os.WriteFile(outside, []byte("private"), 0600)
	os.Symlink(outside, filepath.Join(root, "link"))
	for _, path := range []string{"link", "../private", outside} {
		if _, _, e := readUnifiedDocumentSnapshot(root, path, 8); e == nil {
			t.Fatal("path escaped", path)
		}
	}
	os.WriteFile(filepath.Join(root, "large"), []byte(strings.Repeat("x", 9)), 0600)
	if _, _, e := readUnifiedDocumentSnapshot(root, "large", 8); e == nil {
		t.Fatal("large")
	}
}
func TestTaskUnifiedMalformedHTTPHasClosedError(t *testing.T) {
	h, call, a, _ := unifiedFixtureSessions(t)
	_ = h
	_ = a
	for _, raw := range []string{`{`, `{"type":"unrelated"}`, `{"type":"task_api_unified_request","api_version":"1.0.0-rc.6","correlation_id":"x","operation":"capabilities","input":{"x":1}}`} {
		w := call("POST", "task-api", raw, "phone")
		if e := unified.ValidateResponse(w.Body.Bytes()); e != nil {
			t.Fatal(w.Body.String(), e)
		}
		var r map[string]any
		json.Unmarshal(w.Body.Bytes(), &r)
		if r["ok"] != false {
			t.Fatal(r)
		}
	}
}

func TestTaskUnifiedSessionQueueCrossParentActualActorAndRevisionProjection(t *testing.T) {
	h, call, a, b := unifiedFixtureSessions(t)
	c := h.registry.Create("s_c", "Private parent", h.cfg.RootDir, "codex", "m", "read-only", "")
	c.SetResumeID("thread-c")
	defer c.Close()
	if _, e := h.dispatches.SetGrant(t.Context(), c.ID, sessiondispatch.Grant{Enabled: true, Local: true, Sessions: []string{h.cfg.InstanceID + ":" + b.ID}}, 0); e != nil {
		t.Fatal(e)
	}
	locA := seedUnifiedController(t, h, a, b, "scd_a")
	_ = seedUnifiedController(t, h, c, b, "scd_c")
	release := b.HoldQueue()
	defer release()
	ran := make(chan string, 3)
	for _, id := range []string{"scd_a", "scd_c"} {
		d, _, _ := h.dispatches.Get(t.Context(), id)
		d.State = "queued"
		if e := h.dispatches.Put(t.Context(), d); e != nil {
			t.Fatal(e)
		}
		config := session.ConfigurationFrom(b.SettingsSnapshot())
		p := queuedPayload{Content: d.Content, ExpectedTarget: &dispatchTargetExpectation{b.ResumeID(), b.SettingsSnapshot().ConfigRevision}, Configuration: &config}
		raw, _ := json.Marshal(p)
		if _, _, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: b.ID, RequestID: d.RequestID, Content: p.Content, Payload: raw}); e != nil {
			t.Fatal(e)
		}
		rid := d.RequestID
		if !b.SubmitNamed(rid, func() { ran <- rid; b.EndTurn() }) {
			t.Fatal("actor submit")
		}
	}
	manager := h.unifiedIdentity(a.ID)
	q := unifiedQueue{"session_input", h.unifiedIdentity(b.ID)}
	r, _ := unifiedHTTP(t, call, manager, "queue_snapshot", "", map[string]any{"queue": q})
	v := unifiedResult(t, r)
	items := v["items"].([]any)
	first, second := items[0].(map[string]any), items[1].(map[string]any)
	if !first["can_move"].(bool) || !second["can_move"].(bool) {
		t.Fatal(v)
	}
	if second["locator"] != nil || second["content_hash"] != "" || strings.Contains(second["item_id"].(string), "scd_c") {
		t.Fatal("private original leaked", second)
	}
	move := map[string]any{"queue": q, "expected_queue_revision": v["revision"], "membership_hash": v["membership_hash"], "item_id": second["item_id"], "relative_to": first["item_id"], "placement": "before"}
	r, _ = unifiedHTTP(t, call, manager, "queue_move", "cross-parent-move", move)
	receipt := unifiedResult(t, r)
	r, _ = unifiedHTTP(t, call, manager, "queue_move", "cross-parent-move", move)
	if taskapi.Digest(unifiedResult(t, r)) != taskapi.Digest(receipt) {
		t.Fatal("lost ACK new effect")
	}
	r, _ = unifiedHTTP(t, call, manager, "get", "", map[string]any{"locator": locA})
	task := unifiedResult(t, r)
	edit := task["edit"].(map[string]any)
	r, _ = unifiedHTTP(t, call, manager, "queue_snapshot", "", map[string]any{"queue": q})
	v = unifiedResult(t, r)
	r, _ = unifiedHTTP(t, call, manager, "queue_edit", "edit-after-rank", map[string]any{"locator": locA, "expected_queue_revision": v["revision"], "expected_content_revision": edit["content_revision"], "expected_content_hash": edit["content_hash"], "content": "有效修訂"})
	unifiedResult(t, r)
	effective, e := h.unifiedWorkEffective(t.Context(), messagequeue.WorkLink{Kind: "controller", NativeID: "scd_a", SessionID: b.ID, RequestID: "r_scd_a"})
	if e != nil || effective["content_revision"] != uint64(2) || effective["body_preview"] != "有效修訂" {
		t.Fatal(effective, e)
	}
	if got := b.WaitingIDs(); len(got) != 2 || got[0] != "r_scd_c" {
		t.Fatal("actor not ranked", got)
	}
	release()
	for _, want := range []string{"r_scd_c", "r_scd_a"} {
		select {
		case got := <-ran:
			if got != want {
				t.Fatal(got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("dequeue timeout")
		}
	}
}
func TestTaskUnifiedControllerBarrierScopeCASAndZeroRevision(t *testing.T) {
	h, call, a, b := unifiedFixtureSessions(t)
	manager := h.unifiedIdentity(a.ID)
	first := seedUnifiedController(t, h, a, b, "one")
	seedUnifiedController(t, h, a, b, "barrier")
	seedUnifiedController(t, h, a, b, "three")
	d, _, _ := h.dispatches.Get(t.Context(), "barrier")
	d.Mode = "steer"
	if e := h.dispatches.Put(t.Context(), d); e != nil {
		t.Fatal(e)
	}
	q := unifiedQueue{"controller_waiting_admission", h.unifiedIdentity(b.ID)}
	r, _ := unifiedHTTP(t, call, manager, "queue_snapshot", "", map[string]any{"queue": q})
	v := unifiedResult(t, r)
	rows := v["items"].([]any)
	r, _ = unifiedHTTP(t, call, manager, "queue_move", "barrier-move", map[string]any{"queue": q, "expected_queue_revision": v["revision"], "membership_hash": v["membership_hash"], "item_id": rows[2].(map[string]any)["item_id"], "relative_to": rows[0].(map[string]any)["item_id"], "placement": "before"})
	if r["ok"] != false {
		t.Fatal("crossed barrier", r)
	}
	// A supplied zero is an exact compare, never omitted as unknown.
	identity := h.unifiedIdentity(b.ID)
	b.SetConfigRevision(b.SettingsSnapshot().ConfigRevision + 1)
	raw, _ := json.Marshal(first)
	var l map[string]any
	json.Unmarshal(raw, &l)
	l["config_revision"] = 0
	l["thread_id"] = identity.ThreadID
	r, _ = unifiedHTTP(t, call, manager, "get", "", map[string]any{"locator": l})
	if r["ok"] != false {
		t.Fatal("zero accepted stale", r)
	}
}
func TestTaskUnifiedUnverifiedRawSealNeverClaimsFinalOrCollection(t *testing.T) {
	h, call, a, b := unifiedFixtureSessions(t)
	loc := seedUnifiedController(t, h, a, b, "unanchored")
	raw := messagequeue.TaskSeal{ID: "raw", SessionID: b.ID, RequestID: "r_unanchored", Hash: taskapi.RawDigest([]byte("fake")), Text: "fake", Anchor: json.RawMessage(`{}`)}
	if _, e := h.messageQueue.SealTask(t.Context(), raw); e != nil {
		t.Fatal(e)
	}
	manager := h.unifiedIdentity(a.ID)
	r, _ := unifiedHTTP(t, call, manager, "get", "", map[string]any{"locator": loc})
	if unifiedResult(t, r)["final"] != nil {
		t.Fatal("raw seal became verified final", r)
	}
	r, _ = unifiedHTTP(t, call, manager, "read_result", "", map[string]any{"locator": loc, "owner_request_id": raw.RequestID, "seal_id": raw.ID, "expected_hash": raw.Hash, "offset": 0, "limit": 100})
	if r["ok"] != false {
		t.Fatal("raw seal bytes", r)
	}
	loc.TaskID = "ta1_fabricated"
	r, _ = unifiedHTTP(t, call, manager, "get", "", map[string]any{"locator": loc})
	if r["ok"] != false {
		t.Fatal("scd promoted fake ta1", r)
	}
}
