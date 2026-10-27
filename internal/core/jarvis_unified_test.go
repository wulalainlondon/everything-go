package core

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	"everything-go/internal/workspace"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type unifiedResetFixture struct {
	*fakeExec
	resetCalls  int
	exportError bool
	resetError  bool
	body        string
	beforeReset func(context.Context) error
}

func (f *unifiedResetFixture) ExportContext(_ context.Context, s *session.Session) (backend.ContextExport, error) {
	if f.exportError {
		return backend.ContextExport{}, errors.New("history missing page")
	}
	return backend.ContextExport{Provider: "fixture", ThreadID: s.ResumeID(), SourceGeneration: "fixture-v1", Complete: true, Messages: []map[string]any{{"content": f.body, "role": "user"}}}, nil
}
func (f *unifiedResetFixture) ResetContext(ctx context.Context, s *session.Session, old string) (string, error) {
	if f.beforeReset != nil {
		if e := f.beforeReset(ctx); e != nil {
			return "", e
		}
	}
	if e := backend.CheckContextResetGuard(ctx); e != nil {
		return "", e
	}
	f.resetCalls++
	if f.resetError {
		return "", errors.New("timeout")
	}
	return "awaiting_initialization", s.DetachReservedContext(old)
}
func unifiedBase(h *Hub, s *session.Session) map[string]any {
	return map[string]any{"authority_instance_id": h.cfg.InstanceID, "manager": map[string]any{"authority_instance_id": h.cfg.InstanceID, "session_id": s.ID, "thread_id": s.ResumeID(), "config_revision": s.SettingsSnapshot().ConfigRevision}}
}
func decodeUnified(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var v map[string]any
	if e := json.Unmarshal(raw, &v); e != nil {
		t.Fatal(string(raw), e)
	}
	return v
}
func TestUnifiedHTTPWorklistScopeCASAndNoDispatch(t *testing.T) {
	h, call := jarvisFixture(t)
	s := h.registry.Create("s_unified", "Jarvis", h.cfg.RootDir, "codex", "m", "read-only", "native")
	base := unifiedBase(h, s)
	base["intent_id"] = "r_record_original"
	base["expected_revision"] = 0
	base["item_id"] = "note1"
	base["kind"] = "discussion_note"
	base["title"] = "僅記錄需求"
	base["body"] = "text"
	base["recording_state"] = "recorded"
	base["source_refs"] = []string{"r_user_original"}
	base["links"] = []any{}
	w := call("POST", "worklist/item", base, "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	v := decodeUnified(t, w.Body.Bytes())
	if v["dispatched"] != false || v["recorded"] != true {
		t.Fatal(v)
	}
	lookup := unifiedBase(h, s)
	lookup["intent_id"] = "r_record_original"
	w = call("POST", "worklist/get", lookup, "phone")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "僅記錄需求") {
		t.Fatal(w.Code, w.Body.String())
	}
	base["title"] = "mutated"
	if w = call("POST", "worklist/item", base, "phone"); w.Code != 409 {
		t.Fatal(w.Code)
	}
	snap, _ := h.messageQueue.Snapshot(s.ID)
	if len(snap.Items) > 0 {
		t.Fatal("metadata executed")
	}
	s.SetMeta(nil, func() *bool { v := true; return &v }())
	if w = call("POST", "worklist/get", lookup, "phone"); w.Code != 403 {
		t.Fatal("hidden source not revoked", w.Code)
	}
}
func TestUnifiedHTTPContextCompleteHistoryBeforeResetAndOriginalRecovery(t *testing.T) {
	for _, mode := range []string{"direct", "handoff", "history-failed", "reset-unknown", "boundary-race"} {
		t.Run(mode, func(t *testing.T) {
			h, call := jarvisFixture(t)
			s := h.registry.Create("s_unified", "Jarvis", h.cfg.RootDir, "codex", "m", "read-only", "old-thread")
			f := &unifiedResetFixture{fakeExec: &fakeExec{sink: h}, body: strings.Repeat("原正文", 10000)}
			h.SetExecutor(f)
			base := unifiedBase(h, s)
			base["intent_id"] = "r_reset_original"
			base["mode"] = "direct"
			if mode == "handoff" {
				base["mode"] = "handoff"
				base["handoff"] = "User reviewed handoff"
				base["handoff_revision"] = 1
				boundary := call("POST", "context/boundary", unifiedBase(h, s), "phone")
				if boundary.Code != 200 {
					t.Fatal(boundary.Code, boundary.Body.String())
				}
				base["handoff_boundary_hash"] = decodeUnified(t, boundary.Body.Bytes())["handoff_boundary_hash"]
				base["seed_request_id"] = "r_" + strings.Repeat("1", 32)
			}
			if mode == "history-failed" {
				f.exportError = true
			}
			w := call(http.MethodPost, "context/prepare", base, "phone")
			if mode == "history-failed" {
				if w.Code != 409 || f.resetCalls != 0 || s.ResumeID() != "old-thread" {
					t.Fatal(w.Code, f.resetCalls, s.ResumeID())
				}
				return
			}
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			prepared := decodeUnified(t, w.Body.Bytes())["reset"].(map[string]any)
			commit := unifiedBase(h, s)
			commit["intent_id"] = "r_reset_original"
			commit["expected_revision"] = prepared["revision"]
			commit["confirmed_boundary_hash"] = prepared["boundary_hash"]
			commit["confirmed_handoff_hash"] = prepared["handoff_hash"]
			if mode == "reset-unknown" {
				f.resetError = true
			}
			if mode == "boundary-race" {
				f.body = "new source text"
			}
			w = call(http.MethodPost, "context/commit", commit, "phone")
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			result := decodeUnified(t, w.Body.Bytes())["reset"].(map[string]any)
			if mode == "boundary-race" {
				if f.resetCalls != 0 || result["phase"] != "blocked" || s.ResumeID() != "old-thread" {
					t.Fatal(result, f.resetCalls)
				}
				return
			}
			if mode == "reset-unknown" {
				if result["phase"] != "unknown" {
					t.Fatal(result)
				}
				w = call(http.MethodPost, "context/commit", commit, "phone")
				if w.Code != 200 || f.resetCalls != 1 {
					t.Fatal("unknown reset replay", w.Code, f.resetCalls)
				}
				if !h.contextAdmissionBlocked(s) {
					t.Fatal("unknown generation accepted input")
				}
				return
			}
			expected := "completed"
			if mode == "handoff" {
				expected = "awaiting_seed"
			}
			if result["phase"] != expected || f.resetCalls != 1 || s.ResumeID() != "" {
				t.Fatal(result, f.resetCalls, s.ResumeID())
			}
			if ids := s.ResumeIDs(); len(ids) != 1 || ids[0] != "old-thread" {
				t.Fatal("oldhistory lost", ids)
			}
			lookup := unifiedBase(h, s)
			lookup["manager"].(map[string]any)["thread_id"] = "old-thread"
			lookup["intent_id"] = "r_reset_original"
			lookup["include_archive"] = true
			lookup["archive_limit"] = 15999
			w = call(http.MethodPost, "context/get", lookup, "phone")
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			page := decodeUnified(t, w.Body.Bytes())["archive_page"].(map[string]any)
			if len([]byte(page["text"].(string))) > 15999 || page["complete"] != false {
				t.Fatal(page)
			}
			if len(h.registry.List()) != 1 {
				t.Fatal("new session manufactured")
			}
		})
	}
}
func TestUnifiedContextAdmissionBarrierAlsoBlocksActorAndNoNewClear(t *testing.T) {
	h, call := jarvisFixture(t)
	s := h.registry.Create("s_unified", "Jarvis", h.cfg.RootDir, "codex", "m", "read-only", "old")
	f := &unifiedResetFixture{fakeExec: &fakeExec{sink: h}, body: "old"}
	h.SetExecutor(f)
	_, _, _ = h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: "pending", Payload: []byte(`{}`)})
	in := unifiedBase(h, s)
	in["intent_id"] = "r_reset_original"
	in["mode"] = "direct"
	w := call("POST", "context/prepare", in, "phone")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "queue_or_send_unsettled") || f.resetCalls != 0 {
		t.Fatal(w.Code, w.Body.String(), f.resetCalls)
	}
}

func TestUnifiedPreparedContextCancelIsMetadataOnly(t *testing.T) {
	h, call := jarvisFixture(t)
	s := h.registry.Create("s_unified", "Jarvis", h.cfg.RootDir, "codex", "m", "read-only", "old")
	f := &unifiedResetFixture{fakeExec: &fakeExec{sink: h}, body: "old"}
	h.SetExecutor(f)
	in := unifiedBase(h, s)
	in["intent_id"] = "r_reset_original"
	in["mode"] = "direct"
	w := call("POST", "context/prepare", in, "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	reset := decodeUnified(t, w.Body.Bytes())["reset"].(map[string]any)
	cancel := unifiedBase(h, s)
	cancel["intent_id"] = "r_reset_original"
	cancel["expected_revision"] = reset["revision"]
	w = call("POST", "context/cancel", cancel, "phone")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "cancelled") || f.resetCalls != 0 || s.ResumeID() != "old" {
		t.Fatal(w.Code, w.Body.String(), f.resetCalls)
	}
	if active, e := h.messageQueue.ContextBarrier(context.Background(), s.ID); e != nil || active {
		t.Fatal(active, e)
	}
}
func TestUnifiedOriginalMetadataReceiptCannotCrossManager(t *testing.T) {
	h, call := jarvisFixture(t)
	a := h.registry.Create("s_unified_a", "A", h.cfg.RootDir, "codex", "m", "read-only", "a")
	b := h.registry.Create("s_unified_b", "B", h.cfg.RootDir, "codex", "m", "read-only", "b")
	in := unifiedBase(h, a)
	in["intent_id"] = "r_record_original"
	in["expected_revision"] = 0
	in["item_id"] = "note"
	in["kind"] = "discussion_note"
	in["title"] = "A private note"
	in["body"] = "body"
	in["recording_state"] = "recorded"
	in["source_refs"] = []string{}
	in["links"] = []any{}
	if w := call("POST", "worklist/item", in, "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	get := unifiedBase(h, b)
	get["intent_id"] = "r_record_original"
	w := call("POST", "worklist/get", get, "phone")
	if strings.Contains(w.Body.String(), "private note") {
		t.Fatal("crossmanager receipt leak", w.Body.String())
	}
	sub := unifiedBase(h, a)
	sub["intent_id"] = "r_subscribe_original"
	sub["expected_revision"] = 0
	sub["enabled"] = true
	sub["push_schema_version"] = 1
	if w := call("POST", "worklist/notifications", sub, "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	get["intent_id"] = "r_subscribe_original"
	w = call("POST", "worklist/get", get, "phone")
	if strings.Contains(w.Body.String(), "subscription") {
		t.Fatal("crossmanager subscription leak")
	}
}

func TestUnifiedContextTerminalDelegationWithoutReturnIsSettled(t *testing.T) {
	h, _ := newTestHub(t)
	store, e := delegation.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	h.delegations = store
	s := h.registry.Create("parent", "Parent", t.TempDir(), "codex", "m", "read-only", "parent-thread")
	child := h.registry.Create("child", "Child", s.Cwd(), "codex", "m", "read-only", "child-thread")
	record := delegation.Record{ID: "d_original", ParentSessionID: s.ID, ChildSessionID: child.ID, ChildRequestID: "r_child", ParentRequestID: "r_return", ChildName: "Child", Cwd: s.Cwd()}
	if e = store.Create(context.Background(), record); e != nil {
		t.Fatal(e)
	}
	if blockers := h.contextBlockers(s); !strings.Contains(strings.Join(blockers, ","), "child_execution_unsettled") {
		t.Fatal("running child missed", blockers)
	}
	if _, e = store.Complete(context.Background(), record.ChildSessionID, record.ChildRequestID, "completed", "done", "", nil); e != nil {
		t.Fatal(e)
	}
	if e = store.MarkDeliveryQueued(context.Background(), record.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = store.MarkDelivered(context.Background(), s.ID, record.ParentRequestID); e != nil {
		t.Fatal(e)
	}
	if blockers := h.contextBlockers(s); strings.Contains(strings.Join(blockers, ","), "child_execution_unsettled") {
		t.Fatal("terminal child treated as running", blockers)
	}
}

func TestUnifiedBoundedSummaryPagesAndExactOriginalDetail(t *testing.T) {
	h, call := jarvisFixture(t)
	s := h.registry.Create("s_bounded", "Jarvis", h.cfg.RootDir, "codex", "m", "read-only", "native")
	body := strings.Repeat("原始正文<>&", 1300)
	for i := 0; i < 30; i++ {
		in := unifiedBase(h, s)
		in["intent_id"] = fmt.Sprintf("r_bounded_original_%02d", i)
		in["item_id"] = fmt.Sprintf("bounded_%02d", i)
		in["kind"] = "discussion_note"
		in["title"] = "完整原始記錄"
		in["body"] = body
		in["recording_state"] = "recorded"
		in["source_refs"] = []string{"r_original"}
		in["links"] = []any{}
		w := call("POST", "worklist/item", in, "phone")
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}

	store, err := sessiondispatch.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	h.dispatches = store
	if _, err = store.SetGrant(context.Background(), s.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0); err != nil {
		t.Fatal(err)
	}
	target := h.registry.Create("s_bounded_target", "Target", h.cfg.RootDir, "codex", "m", "read-only", "target-native")
	for i := 0; i < 10; i++ {
		request := fmt.Sprintf("r_source_%02d", i)
		if _, _, err = h.messageQueue.Enqueue(messagequeue.Entry{SessionID: target.ID, RequestID: request, Content: body, Payload: []byte("{}")}); err != nil {
			t.Fatal(err)
		}
		created, createErr := store.Create(context.Background(), sessiondispatch.Record{ID: fmt.Sprintf("source_%02d", i), OriginRequestID: request, ToolCallID: request, ParentID: s.ID, ParentThreadID: s.ResumeID(), ParentConfigKnown: true, ParentConfigRevision: s.SettingsSnapshot().ConfigRevision, InstanceID: h.cfg.InstanceID, SessionID: target.ID, ThreadID: target.ResumeID(), ConfigRevision: target.SettingsSnapshot().ConfigRevision, RequestID: request, IntentHash: request, Content: body, State: "completed"})
		if createErr != nil || !created {
			t.Fatal(created, createErr)
		}
	}
	cursor := ""
	seen := map[string]bool{}
	for page := 0; page < 5; page++ {
		in := unifiedBase(h, s)
		in["cursor"] = cursor
		in["limit"] = 30
		w := call("POST", "worklist/snapshot", in, "phone")
		if w.Code != 200 || w.Body.Len() > 240<<10 {
			t.Fatal(w.Code, w.Body.Len(), w.Body.String())
		}
		v := decodeUnified(t, w.Body.Bytes())
		rows := v["items"].([]any)
		if len(rows) > 8 {
			t.Fatal("unbounded page", len(rows))
		}
		for _, row := range rows {
			item := row.(map[string]any)["item"].(map[string]any)
			id := item["item_id"].(string)
			if seen[id] {
				t.Fatal("duplicate keyset", id)
			}
			seen[id] = true
			if item["body_truncated"] != true || item["body_hash"] != jarvisTextHash(body) || len([]rune(item["body"].(string))) != 240 {
				t.Fatal(item)
			}
		}
		cursor = v["cursor"].(string)
	}
	if len(seen) != 40 || cursor != "" {
		t.Fatal("lost page", len(seen), cursor)
	}
	for _, key := range []string{"item_id", "intent_id", "source_detail"} {
		in := unifiedBase(h, s)
		if key == "source_detail" {
			in["item_id"] = "controller_source_00"
		} else if key == "item_id" {
			in[key] = "bounded_00"
		} else {
			in[key] = "r_bounded_original_00"
		}
		w := call("POST", "worklist/get", in, "phone")
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		item := decodeUnified(t, w.Body.Bytes())["items"].([]any)[0].(map[string]any)["item"].(map[string]any)
		if item["body"] != body || item["body_hash"] != jarvisTextHash(body) || item["body_truncated"] == true {
			t.Fatal("detail is not exact original")
		}
	}
	in := unifiedBase(h, s)
	in["intent_id"] = "r_invalid_summary_save"
	in["item_id"] = "bounded_00"
	in["expected_revision"] = 1
	in["kind"] = "discussion_note"
	in["title"] = "bad"
	in["body"] = "preview"
	in["body_truncated"] = true
	in["recording_state"] = "recorded"
	if w := call("POST", "worklist/item", in, "phone"); w.Code != 409 {
		t.Fatal("summary overwrite accepted", w.Code)
	}
	projection := jarvisWorkProjectionSummary(map[string]any{"goal": strings.Repeat("secret", 100000), "definition": strings.Repeat("x", 100000), "axes": map[string]string{"execution": "unknown"}, "revision": 1})
	raw, _ := json.Marshal(projection)
	if len(raw) > 2048 || strings.Contains(string(raw), "secret") || projection["axes"] == nil {
		t.Fatal("unbounded canonical definition", projection)
	}
	w := httptest.NewRecorder()
	jarvisWorkJSON(w, map[string]any{"body": strings.Repeat("x", 250<<10)})
	if w.Code != 413 || json.Valid(w.Body.Bytes()) {
		t.Fatal("oversize did not fail before JSON write", w.Code)
	}
}

func TestUnifiedFrozenSnapshotDoesNotMixMetadataGenerations(t *testing.T) {
	h, call := jarvisFixture(t)
	s := h.registry.Create("s_freeze", "Jarvis", h.cfg.RootDir, "codex", "m", "read-only", "native")
	for i := 0; i < 9; i++ {
		in := unifiedBase(h, s)
		in["intent_id"] = fmt.Sprintf("r_freeze_original_%02d", i)
		in["item_id"] = fmt.Sprintf("note_%02d", i)
		in["kind"] = "discussion_note"
		in["title"] = "old"
		in["body"] = "old body"
		in["recording_state"] = "recorded"
		if w := call("POST", "worklist/item", in, "phone"); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	first := call("POST", "worklist/snapshot", unifiedBase(h, s), "phone")
	if first.Code != 200 {
		t.Fatal(first.Code, first.Body.String())
	}
	f := decodeUnified(t, first.Body.Bytes())
	update := unifiedBase(h, s)
	update["intent_id"] = "r_freeze_update_08"
	update["item_id"] = "note_08"
	update["expected_revision"] = 1
	update["kind"] = "discussion_note"
	update["title"] = "new"
	update["body"] = "new body"
	update["recording_state"] = "recorded"
	if w := call("POST", "worklist/item", update, "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	page := unifiedBase(h, s)
	page["cursor"] = f["cursor"]
	w := call("POST", "worklist/snapshot", page, "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	v := decodeUnified(t, w.Body.Bytes())
	item := v["items"].([]any)[0].(map[string]any)["item"].(map[string]any)
	if item["body"] != "old body" || item["revision"] != float64(1) || v["freeze_id"] != f["freeze_id"] || jarvisHash(v["watermarks"]) != jarvisHash(f["watermarks"]) {
		t.Fatal("mixed frozen generation", v)
	}
	if err := h.messageQueue.TaskJournal().Transaction(context.Background(), func(tx *sql.Tx) error {
		_, e := tx.Exec(`UPDATE task_api_freezes SET expires_at=0 WHERE id=?`, f["freeze_id"])
		return e
	}); err != nil {
		t.Fatal(err)
	}
	if w = call("POST", "worklist/snapshot", page, "phone"); w.Code != 409 || !strings.Contains(w.Body.String(), "cursor_expired") {
		t.Fatal("expired freeze silently refreshed", w.Code, w.Body.String())
	}
}
func TestUnifiedHandoffBoundaryIsDurablyRejectedBeforePrepareEffect(t *testing.T) {
	h, call := jarvisFixture(t)
	s := h.registry.Create("s_handoff_boundary", "Jarvis", h.cfg.RootDir, "codex", "m", "read-only", "old")
	f := &unifiedResetFixture{fakeExec: &fakeExec{sink: h}, body: "summary source"}
	h.SetExecutor(f)
	w := call("POST", "context/boundary", unifiedBase(h, s), "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	boundary := decodeUnified(t, w.Body.Bytes())["boundary_hash"]
	f.body = "new result after document import"
	in := unifiedBase(h, s)
	in["intent_id"] = "r_handoff_stale_original"
	in["mode"] = "handoff"
	in["handoff"] = "reviewed old document"
	in["handoff_revision"] = 1
	in["handoff_boundary_hash"] = boundary
	in["seed_request_id"] = "r_" + strings.Repeat("9", 32)
	w = call("POST", "context/prepare", in, "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	v := decodeUnified(t, w.Body.Bytes())
	if v["phase"] != "rejected" || v["no_effect"] != true || f.resetCalls != 0 || s.ResumeID() != "old" {
		t.Fatal(v)
	}
	get := unifiedBase(h, s)
	get["intent_id"] = in["intent_id"]
	w = call("POST", "context/get", get, "phone")
	if w.Code != 200 || decodeUnified(t, w.Body.Bytes())["reset"].(map[string]any)["phase"] != "rejected" {
		t.Fatal("original rejection not durable", w.Code, w.Body.String())
	}
}
func TestUnifiedContextGuardSurvivesBlockedOriginalStoreAndRevocation(t *testing.T) {
	h, call := jarvisFixture(t)
	s := h.registry.Create("s_guard", "Jarvis", h.cfg.RootDir, "codex", "m", "read-only", "old")
	f := &unifiedResetFixture{fakeExec: &fakeExec{sink: h}, body: "old"}
	h.SetExecutor(f)
	in := unifiedBase(h, s)
	in["intent_id"] = "r_guard_original"
	in["mode"] = "direct"
	w := call("POST", "context/prepare", in, "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	prepared := decodeUnified(t, w.Body.Bytes())["reset"].(map[string]any)
	f.beforeReset = func(ctx context.Context) error {
		locked := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- h.messageQueue.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error { close(locked); <-release; return nil })
		}()
		<-locked
		readStarted := make(chan struct{})
		readDone := make(chan error, 1)
		go func() {
			close(readStarted)
			_, _, e := h.messageQueue.ContextArchive(ctx, jarvisTestPrincipal("phone", "jarvis-token"), "r_guard_original")
			readDone <- e
		}()
		<-readStarted
		if e := h.pairing.Unclaim("jarvis-token"); e != nil {
			close(release)
			return e
		}
		close(release)
		if e := <-done; e != nil {
			return e
		}
		return <-readDone
	}
	commit := unifiedBase(h, s)
	commit["intent_id"] = in["intent_id"]
	commit["expected_revision"] = prepared["revision"]
	commit["confirmed_boundary_hash"] = prepared["boundary_hash"]
	w = call("POST", "context/commit", commit, "phone")
	if w.Code != 200 || f.resetCalls != 0 || s.ResumeID() != "old" {
		t.Fatal("stale principal reached effect", w.Code, w.Body.String(), f.resetCalls)
	}
}
func jarvisTestPrincipal(device, token string) string {
	digest := sha256.Sum256([]byte(token))
	return device + "\x00" + strconv.FormatUint(binary.BigEndian.Uint64(digest[:8])&((1<<53)-1), 10)
}
func TestUnifiedDelegationStartRequiresActualSourceAndExecutionBindings(t *testing.T) {
	h, call := jarvisFixture(t)
	parent := h.registry.Create("s_start_parent", "Parent", h.cfg.RootDir, "codex", "m", "read-only", "parent-thread")
	child := h.registry.Create("s_dg_start_child", "Child", h.cfg.RootDir, "codex", "m", "read-only", "child-thread")
	store, e := delegation.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	h.delegations = store
	record := delegation.Record{ID: "d_start_original", ParentSessionID: parent.ID, ChildSessionID: child.ID, ChildRequestID: "r_child_original", ParentRequestID: "r_return_original", ChildName: "Child", Cwd: parent.Cwd(), SourceThread: parent.ResumeID(), SourceRevision: parent.SettingsSnapshot().ConfigRevision, ExpectedThread: child.ResumeID(), ExpectedRevision: child.SettingsSnapshot().ConfigRevision}
	if e = store.CreateBounded(context.Background(), record); e != nil {
		t.Fatal(e)
	}
	policy := unifiedBase(h, parent)
	policy["intent_id"] = "r_policy_start_original"
	policy["expected_revision"] = 0
	policy["enabled"] = true
	policy["push_schema_version"] = 1
	if w := call("POST", "worklist/notifications", policy, "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	sub, _, e := h.messageQueue.CurrentNoticePolicy(context.Background(), jarvisTestPrincipal("phone", "jarvis-token"), parent.ID)
	if e != nil {
		t.Fatal(e)
	}
	fact := messagequeue.NativeStart{SessionID: child.ID, RequestID: record.ChildRequestID, ThreadID: child.ResumeID()}
	if !h.startRelationRelevant(context.Background(), sub, fact) {
		t.Fatal("actual bound original not recognized")
	}
	parent.SetConfigRevision(parent.SettingsSnapshot().ConfigRevision + 1)
	policy = unifiedBase(h, parent)
	policy["intent_id"] = "r_policy_start_new_revision"
	policy["expected_revision"] = 1
	policy["enabled"] = true
	policy["push_schema_version"] = 1
	if w := call("POST", "worklist/notifications", policy, "phone"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	sub, _, e = h.messageQueue.CurrentNoticePolicy(context.Background(), jarvisTestPrincipal("phone", "jarvis-token"), parent.ID)
	if e != nil {
		t.Fatal(e)
	}
	if h.startRelationRelevant(context.Background(), sub, fact) {
		t.Fatal("old source revision allowed notice")
	}
	parent.SetConfigRevision(record.SourceRevision)
	child.SetConfigRevision(record.ExpectedRevision + 1)
	// Restore original policy tuple directly as a fresh user-owned policy.
	raw, _ := json.Marshal(jarvisNoticePolicy{jarvisWorkScope: jarvisWorkScope{Authority: h.cfg.InstanceID, Manager: jarvisTarget{Authority: h.cfg.InstanceID, SessionID: parent.ID, ThreadID: parent.ResumeID(), Revision: parent.SettingsSnapshot().ConfigRevision}}, Enabled: true, PushSchemaVersion: 1})
	sub.Input = raw
	if _, e = h.messageQueue.SetNoticePolicy(context.Background(), sub, "r_policy_start_target_changed", 2); e != nil {
		t.Fatal(e)
	}
	if h.startRelationRelevant(context.Background(), sub, fact) {
		t.Fatal("changed target execution binding allowed notice")
	}
}

func TestUnifiedManagerOrdinaryOriginalRequestsRemainReadable(t *testing.T) {
	h, call := jarvisFixture(t)
	s := h.registry.Create("s_original_manager", "Jarvis", h.cfg.RootDir, "codex", "m", "read-only", "native")
	digest := sha256.Sum256([]byte("jarvis-token"))
	generation := binary.BigEndian.Uint64(digest[:8]) & ((1 << 53) - 1)
	thread := s.ResumeID()
	for i := 0; i < 2; i++ {
		rid := fmt.Sprintf("r_original_manager_%d", i)
		goal := fmt.Sprintf("原總管訊息 %d", i)
		input, _ := json.Marshal(intakeInput{Action: "submit", SessionID: s.ID, RequestID: rid, Goal: goal, Target: intakeTarget{Instance: h.cfg.InstanceID, Session: s.ID, Thread: &thread, Revision: s.SettingsSnapshot().ConfigRevision}})
		request := taskapi.Request{Operation: "intake", IdempotencyKey: rid, Input: input}
		caller := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:phone", NamespaceGeneration: generation, Transport: "authenticated_ws", BindingKind: "paired_human"}
		metadata, _ := json.Marshal(apiMetadata{Request: request, Caller: caller, Intake: &intakeMetadata{Goal: goal, Source: map[string]any{"session_id": s.ID, "request_id": rid}}})
		command := taskapi.AuthorizedCommand{Caller: caller, Request: request, Locator: taskapi.Locator{Authority: h.cfg.InstanceID, Path: "ordinary"}, Namespace: taskapi.Namespace{Authority: h.cfg.InstanceID, StableScopeID: caller.StableScopeID, Generation: generation, Path: "ordinary", Operation: "intake"}, IntentHash: jarvisTextHash(rid)}
		record := taskapi.IntentRecord{ReceiptID: "tr_" + rid, TaskID: apiID(h.cfg.InstanceID, "ordinary", s.ID+"/"+rid), NativeID: s.ID + "/" + rid, SessionID: s.ID, RequestID: rid, Metadata: metadata}
		if _, created, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: rid, Content: goal, Payload: []byte(`{}`), API: &messagequeue.APIAdmission{Command: command, Record: record}}); e != nil || !created {
			t.Fatal(created, e)
		}
	}
	w := call("POST", "worklist/snapshot", unifiedBase(h, s), "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	v := decodeUnified(t, w.Body.Bytes())
	rows := v["items"].([]any)
	if len(rows) != 2 {
		t.Fatal("ordinary manager requests omitted", len(rows), w.Body.String())
	}
	for _, raw := range rows {
		row := raw.(map[string]any)
		item := row["item"].(map[string]any)
		if !strings.HasPrefix(item["item_id"].(string), "intake_") || len(row["executions"].([]any)) != 1 {
			t.Fatal(row)
		}
	}
	if v["coverage"].(map[string]any)["all_tasks"] != false {
		t.Fatal("partial coverage advertised complete")
	}
}

// Optional cross-repository packet export: real paired HTTP routing and original
// SQLite, synthetic read-only provider. No WS/model/device/effect proof is implied.
func TestUnifiedExportCrossRepositoryWirePacket(t *testing.T) {
	dir := os.Getenv("JARVIS_WIRE_FIXTURE_DIR")
	if dir == "" {
		t.Skip("explicit report-only export not requested")
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	h, call := jarvisFixture(t)
	s := h.registry.Create("s_wire_original", "Jarvis fixture", h.cfg.RootDir, "codex", "m", "read-only", "native-original")
	f := &unifiedResetFixture{fakeExec: &fakeExec{sink: h}, body: "isolated original transcript"}
	h.SetExecutor(f)
	outputs := map[string]any{}
	rawResponses := map[string]json.RawMessage{}
	export := func(name, path string, input any) {
		w := call("POST", path, input, "phone")
		if w.Code != 200 {
			t.Fatal(name, w.Code, w.Body.String())
		}
		var value any
		if e := json.Unmarshal(w.Body.Bytes(), &value); e != nil {
			t.Fatal(e)
		}
		outputs[name] = value
		rawResponses[name] = append([]byte(nil), w.Body.Bytes()...)
	}
	note := unifiedBase(h, s)
	note["intent_id"] = "r_wire_original_note"
	note["item_id"] = "wire-note"
	note["kind"] = "discussion_note"
	note["title"] = "跨倉庫真接線樣本"
	note["body"] = strings.Repeat("原始文字", 120)
	note["recording_state"] = "recorded"
	note["links"] = []any{}
	note["source_refs"] = []string{"r_user_fixture"}
	export("mutation", "worklist/item", note)
	export("snapshot", "worklist/snapshot", unifiedBase(h, s))
	detail := unifiedBase(h, s)
	detail["item_id"] = "wire-note"
	export("detail", "worklist/get", detail)
	lookup := unifiedBase(h, s)
	lookup["intent_id"] = note["intent_id"]
	export("original_mutation", "worklist/get", lookup)
	export("context_status", "context/status", unifiedBase(h, s))
	export("context_boundary", "context/boundary", unifiedBase(h, s))
	prepare := unifiedBase(h, s)
	prepare["intent_id"] = "r_wire_original_prepare"
	prepare["mode"] = "direct"
	export("context_prepared", "context/prepare", prepare)
	get := unifiedBase(h, s)
	get["intent_id"] = prepare["intent_id"]
	export("context_get", "context/get", get)
	reject := unifiedBase(h, s)
	reject["intent_id"] = "r_wire_rejected_handoff"
	reject["mode"] = "handoff"
	reject["handoff"] = "older reviewed summary"
	reject["handoff_revision"] = 1
	reject["handoff_boundary_hash"] = strings.Repeat("a", 64)
	reject["seed_request_id"] = "r_" + strings.Repeat("8", 32)
	export("context_rejected", "context/prepare", reject)

	// The workspace variant traverses authenticated WS room/task creation,
	// original queue admission, then the paired Jarvis HTTP read adapter.
	wf := workspaceSetup(t)
	room := wf.room("Isolated wire room")
	agent := wf.agent(room, "Isolated wire agent")
	started := make(chan struct{}, 1)
	wf.h.exec.(*workspaceFixtureExecutor).onSend = func(_ *session.Session, _, _ string) {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	result := wf.call(workspace.Command{Action: "create_task", RoomID: room.ID, Data: wireData(workspace.TaskInput{Title: "Isolated original workspace task", Instruction: "Synthetic executor only; no native provider", AgentID: agent.ID})})
	var task workspace.Task
	if e := json.Unmarshal(result.Entity.Body, &task); e != nil {
		t.Fatal(e)
	}
	select {
	case <-started:
	case <-wf.ctx.Done():
		t.Fatal("workspace original admission not observed", wf.ctx.Err())
	}
	record, e := wf.h.messageQueue.TaskJournal().OriginalCommandForRequest(wf.ctx, task.SessionID, task.RequestID)
	if e != nil {
		t.Fatal(e)
	}
	manager := wf.h.registry.Create("s_wire_workspace_manager", "Jarvis fixture", wf.h.cfg.RootDir, "codex", "fixture-model", "read-only", "manager-native")
	target, found := wf.h.registry.Get(task.SessionID)
	if !found {
		t.Fatal("original workspace target absent")
	}
	workspaceCall := func(name, path string, input any) {
		raw, _ := json.Marshal(input)
		r := httptest.NewRequest("POST", "/api/jarvis/v1/"+path, strings.NewReader(string(raw)))
		r.Header.Set("Authorization", "Bearer fixture-workspace-device-token")
		r.Header.Set("X-Bridge-Device-ID", "workspace-device")
		w := httptest.NewRecorder()
		wf.h.ServeJarvisAPI(w, r)
		if w.Code != 200 {
			t.Fatal(name, w.Code, w.Body.String())
		}
		var value any
		if e := json.Unmarshal(w.Body.Bytes(), &value); e != nil {
			t.Fatal(e)
		}
		outputs[name] = value
		rawResponses[name] = append([]byte(nil), w.Body.Bytes()...)
	}
	linked := unifiedBase(wf.h, manager)
	linked["intent_id"] = "r_wire_workspace_original"
	linked["item_id"] = "workspace-original"
	linked["kind"] = "execution"
	linked["title"] = "原協作區工作"
	linked["body"] = "original authenticated workspace receipt"
	linked["recording_state"] = "recorded"
	linked["links"] = []messagequeue.WorkLink{{Kind: "workspace", Authority: wf.h.cfg.InstanceID, SessionID: task.SessionID, ThreadID: target.ResumeID(), ConfigRevision: target.SettingsSnapshot().ConfigRevision, RequestID: task.RequestID, NativeID: record.NativeID}}
	workspaceCall("workspace_mutation", "worklist/item", linked)
	workspaceCall("workspace_snapshot", "worklist/snapshot", unifiedBase(wf.h, manager))
	workspaceDetail := unifiedBase(wf.h, manager)
	workspaceDetail["item_id"] = "workspace-original"
	workspaceCall("workspace_detail", "worklist/get", workspaceDetail)
	packet := map[string]any{"sourceCommit": os.Getenv("JARVIS_WIRE_FIXTURE_SOURCE"), "evidence": "verified paired HTTP + authenticated original workspace WS/queue + real SQLite; synthetic provider only; no native/model/provider-reset/device/external effects", "outputs": outputs, "schemaHashes": map[string]string{"legacy": "674aada21ce809c5bec8324b192163c4f3c4acf7f5b57f39042eacd9982d23fa", "media": "ca3bac5e7c66b75bd141379035a49d060ca4e16996c08b230094be9d7487c018"}, "providerResetCalls": f.resetCalls}
	raw, e := json.MarshalIndent(packet, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "WIRE_PACKET.json"), raw, 0600); e != nil {
		t.Fatal(e)
	}

	cases := []map[string]any{}
	for name, body := range rawResponses {
		kind := "snapshot"
		switch name {
		case "snapshot", "workspace_snapshot":
			kind = "snapshot"
		case "detail", "original_mutation", "workspace_detail":
			kind = "detail"
		case "context_status":
			kind = "context_status"
		case "context_boundary":
			kind = "context_boundary"
		case "context_prepared", "context_get", "context_rejected":
			kind = "context_reply"
		default:
			kind = "work_mutation"
		}
		relative := name + ".json"
		if e = os.WriteFile(filepath.Join(dir, relative), body, 0600); e != nil {
			t.Fatal(e)
		}
		sum := sha256.Sum256(body)
		cases = append(cases, map[string]any{"name": name, "kind": kind, "relative_path": relative, "sha256": fmt.Sprintf("%x", sum), "responseHttpStatus": 200})
	}
	sort.Slice(cases, func(i, j int) bool { return cases[i]["name"].(string) < cases[j]["name"].(string) })
	manifest, _ := json.MarshalIndent(map[string]any{"sourceCommit": os.Getenv("JARVIS_WIRE_FIXTURE_SOURCE"), "evidence": packet["evidence"], "cases": cases, "providerResetCalls": f.resetCalls}, "", "  ")
	if e = os.WriteFile(filepath.Join(dir, "WIRE_MANIFEST.json"), manifest, 0600); e != nil {
		t.Fatal(e)
	}
	if f.resetCalls != 0 {
		t.Fatal("wire packet export issued provider reset")
	}
}

func TestUnifiedFrozenDuplicateExecutionNoticeChannelsDoNotMixPages(t *testing.T) {
	h, call := jarvisFixture(t)
	manager := h.registry.Create("s_channels_manager", "Manager", h.cfg.RootDir, "codex", "m", "read-only", "manager-native")
	target := h.registry.Create("s_channels_target", "Target", h.cfg.RootDir, "codex", "m", "read-only", "target-native")
	store, e := sessiondispatch.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	h.dispatches = store
	ctx := context.Background()
	if _, e = store.SetGrant(ctx, manager.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0); e != nil {
		t.Fatal(e)
	}
	record := sessiondispatch.Record{ID: "channels-original", ParentID: manager.ID, ParentThreadID: manager.ResumeID(), ParentConfigKnown: true, ParentConfigRevision: manager.SettingsSnapshot().ConfigRevision, OriginRequestID: "r_channels_origin", ToolCallID: "call-original", InstanceID: h.cfg.InstanceID, SessionID: target.ID, ThreadID: target.ResumeID(), ConfigRevision: target.SettingsSnapshot().ConfigRevision, RequestID: "r_channels_original", IntentHash: "original", Content: "Original", State: "running"}
	if _, e = store.Create(ctx, record); e != nil {
		t.Fatal(e)
	}
	if _, _, e = h.messageQueue.Enqueue(messagequeue.Entry{SessionID: target.ID, RequestID: record.RequestID, Payload: []byte(`{}`)}); e != nil {
		t.Fatal(e)
	}
	if _, _, e = h.messageQueue.Transition(target.ID, record.RequestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Running, "", "", ""); e != nil {
		t.Fatal(e)
	}
	if e = h.messageQueue.RecordNativeAcceptance(target.ID, record.RequestID, target.ResumeID(), "turn-channels"); e != nil {
		t.Fatal(e)
	}
	fact := messagequeue.NativeStart{SessionID: target.ID, RequestID: record.RequestID, Provider: "codex", ThreadID: target.ResumeID(), Token: "turn-channels", TokenKind: "turn_id", ProofKind: "live_turn_start_in_progress", ObservedAt: time.Now().UnixMilli()}
	if _, e = h.messageQueue.RecordNativeStart(fact); e != nil {
		t.Fatal(e)
	}
	fact, _, _ = h.messageQueue.NativeStart(target.ID, record.RequestID)
	link := messagequeue.WorkLink{Kind: "controller", Authority: h.cfg.InstanceID, SessionID: target.ID, ThreadID: target.ResumeID(), ConfigRevision: target.SettingsSnapshot().ConfigRevision, RequestID: record.RequestID, NativeID: record.ID}
	for _, id := range []string{"a", "b"} {
		in := unifiedBase(h, manager)
		in["intent_id"] = "r_channels_metadata_" + id
		in["item_id"] = id
		in["kind"] = "execution"
		in["title"] = "Original shared execution"
		in["body"] = "source owned"
		in["recording_state"] = "recorded"
		in["links"] = []messagequeue.WorkLink{link}
		if w := call("POST", "worklist/item", in, "phone"); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	raw, _ := json.Marshal(fact)
	push := messagequeue.StartPush{Scope: jarvisTestPrincipal("phone", "jarvis-token"), Manager: manager.ID, EventID: fact.EventID, Payload: raw, ExpiresAt: fact.ObservedAt + 180000}
	if e = h.messageQueue.PrepareStartPush(ctx, push); e != nil {
		t.Fatal(e)
	}
	first := unifiedBase(h, manager)
	first["limit"] = 1
	w := call("POST", "worklist/snapshot", first, "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	a := decodeUnified(t, w.Body.Bytes())
	if claimed, e := h.messageQueue.ClaimStartPush(ctx, push); e != nil || !claimed {
		t.Fatal(claimed, e)
	}
	if e = h.messageQueue.FinishStartPush(ctx, push, "unknown", json.RawMessage(`{"state":"unknown"}`)); e != nil {
		t.Fatal(e)
	}
	next := unifiedBase(h, manager)
	next["limit"] = 1
	next["cursor"] = a["cursor"]
	w = call("POST", "worklist/snapshot", next, "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	b := decodeUnified(t, w.Body.Bytes())
	if a["freeze_id"] != b["freeze_id"] || jarvisHash(a["notices"]) != jarvisHash(b["notices"]) || jarvisHash(a["push_receipts"]) != jarvisHash(b["push_receipts"]) {
		t.Fatal("same event changed across frozen pages", a, b)
	}
	if len(b["push_receipts"].([]any)) != 1 || b["push_receipts"].([]any)[0].(map[string]any)["status"] != "prepared" {
		t.Fatal("live receipt leaked into frozen snapshot", b)
	}
}
