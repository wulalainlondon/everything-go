package core

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"net/http"
	"strings"
	"testing"
)

type unifiedResetFixture struct {
	*fakeExec
	resetCalls  int
	exportError bool
	resetError  bool
	body        string
}

func (f *unifiedResetFixture) ExportContext(_ context.Context, s *session.Session) (backend.ContextExport, error) {
	if f.exportError {
		return backend.ContextExport{}, errors.New("history missing page")
	}
	return backend.ContextExport{Provider: "fixture", ThreadID: s.ResumeID(), SourceGeneration: "fixture-v1", Complete: true, Messages: []map[string]any{{"content": f.body, "role": "user"}}}, nil
}
func (f *unifiedResetFixture) ResetContext(_ context.Context, s *session.Session, old string) (string, error) {
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
