package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/nativewatch"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func waitContext(t *testing.T, h *Hub, s *session.Session, predicate func(*protocol.ContextObservation) bool) *protocol.ContextObservation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		o := h.contextObservation(s.Snapshot())
		if predicate(o) {
			return o
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("context did not settle: %+v", h.contextObservation(s.Snapshot()))
	return nil
}
func TestContextNativeRootProducerLedgerMarshalAndRestart(t *testing.T) {
	h, _ := newTestHub(t)
	h.messageQueue.Close()
	dir := t.TempDir()
	store, e := messagequeue.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	h.messageQueue = store
	s := h.registry.Create("context-root", "Root", t.TempDir(), "codex", "", "", "")
	s.SetResumeID("native-root")
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	appendRows := func(text string) {
		t.Helper()
		f, e := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		_, e = f.WriteString(text)
		if e == nil {
			e = f.Sync()
		}
		f.Close()
		if e != nil {
			t.Fatal(e)
		}
	}
	usage := func(stamp time.Time, n int) string {
		return fmt.Sprintf(`{"timestamp":%q,"type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"total_tokens":%d},"model_context_window":1000}}}`+"\n", stamp.UTC().Format(time.RFC3339Nano), n)
	}
	appendRows(`{"type":"session_meta","payload":{"id":"native-root","source":"exec"}}` + "\n" + `{"type":"event_msg","payload":{"type":"task_started","turn_id":"native-turn"}}` + "\n" + usage(time.Now().Add(-10*time.Minute), 900))
	watcher := nativewatch.NewTurnWatcher()
	watcher.Track(nativewatch.NativeSession{ID: s.ID, ResumeID: s.ResumeID(), Backend: "codex", Path: path})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); watcher.Run(ctx, func(nativewatch.TurnActivity) {}, h.observeNativeContext) }()
	defer func() { cancel(); <-done }()
	stages := []map[string]any{}
	capture := func(label string) {
		t.Helper()
		var summary protocol.SessionSummary
		for _, v := range h.sessionSummaries() {
			if v.ID == s.ID {
				summary = v
			}
		}
		if summary.ContextObservation == nil {
			t.Fatal("real session summary missing telemetry")
		}
		raw, e := marshalEvent(h.client.SessionsList([]protocol.SessionSummary{summary}), "i1")
		if e != nil {
			t.Fatal(e)
		}
		var result map[string]any
		if e = json.Unmarshal(raw, &result); e != nil {
			t.Fatal(e)
		}
		event, e := marshalEvent(protocol.ContextObservationEvent{Type: "context_observation", SessionID: s.ID, AuthorityInstanceID: "i1", ContextObservation: *summary.ContextObservation}, "i1")
		if e != nil {
			t.Fatal(e)
		}
		var envelope struct {
			SessionID string                      `json:"session_id"`
			Context   protocol.ContextObservation `json:"context_observation"`
		}
		if e = json.Unmarshal(event, &envelope); e != nil {
			t.Fatal(e)
		}
		if envelope.Context.SessionID != s.ID || envelope.Context.AuthorityInstanceID != "i1" || envelope.SessionID == s.ID {
			t.Fatalf("nested raw/outer scoped identity incorrect: %s", event)
		}
		stages = append(stages, map[string]any{"stage": label, "sessions_list": json.RawMessage(raw), "event": json.RawMessage(event)})
	}
	waitContext(t, h, s, func(o *protocol.ContextObservation) bool { return o.Usage.Status == "stale" })
	capture("native_timestamp_stale")
	row := fmt.Sprintf(`{"timestamp":%q,"type":"compacted","payload":{"message":"synthetic private summary not exported"}}`+"\n", time.Now().UTC().Format(time.RFC3339Nano))
	appendRows(row)
	waitContext(t, h, s, func(o *protocol.ContextObservation) bool {
		return o.Compactions.Completed != nil && *o.Compactions.Completed == 1 && o.Usage.Status == "invalidated"
	})
	capture("confirmed_compaction_one")
	appendRows(row + usage(time.Now(), 0))
	waitContext(t, h, s, func(o *protocol.ContextObservation) bool {
		return o.Compactions.Completed != nil && *o.Compactions.Completed == 1 && o.Usage.Status == "fresh" && o.Usage.UsedTokens != nil && *o.Usage.UsedTokens == 0
	})
	capture("replay_dedup_and_fresh_known_zero")
	old := s.Snapshot()
	s.SetResumeID("replacement")
	capture("replacement_unknown")
	h.Emit(backend.ContextTelemetry{SessionID: s.ID, Backend: "codex", NativeGeneration: old.ResumeID, SourceRevision: old.SourceRevision, EventID: "foreign-late", Completed: true})
	if o := h.contextObservation(s.Snapshot()); o.Compactions.Completed != nil || o.Usage.Status != "unknown" {
		t.Fatal("late old generation polluted new source")
	}
	s.SetResumeID(old.ResumeID)
	capture("return_original_generation")
	h.messageQueue.Close()
	h.messageQueue, e = messagequeue.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	capture("database_restart_restores_one")
	if o := h.contextObservation(s.Snapshot()); o.Compactions.Completed == nil || *o.Compactions.Completed != 1 {
		t.Fatal("restart lost persisted count")
	}
	if p := os.Getenv("CONTEXT_WIRE_FIXTURE"); p != "" {
		v := map[string]any{"schema": 1, "fixture": "isolated synthetic native root JSONL producer to real Hub/SQLite/sessionSummary/marshalEvent; no provider/model/network", "stages": stages}
		b, _ := json.MarshalIndent(v, "", "  ")
		if e = os.WriteFile(p, append(b, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
}
func TestContextScopePartialUnsupportedAndReadFailure(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("claude-context", "Claude", t.TempDir(), "claude", "", "", "")
	s.SetResumeID("uuid")
	zero := 0
	max := 100
	h.Emit(backend.ContextTelemetry{SessionID: s.ID, Backend: "claude", NativeGeneration: s.ResumeID(), SourceRevision: s.Snapshot().SourceRevision, EventID: "native-result", Origin: "claude_native_assistant_usage", MaxOrigin: "model_config", UsedTokens: &zero, MaxTokens: &max})
	o := h.contextObservation(s.Snapshot())
	if o.Usage.Status != "fresh" || o.Usage.UsedTokens == nil || *o.Usage.UsedTokens != 0 || o.Compactions.Supported || o.Compactions.Completed != nil || o.Compactions.Coverage != "unsupported" {
		t.Fatalf("Claude unknown/zero unsupported: %+v", o)
	}
	h.Emit(backend.ContextTelemetry{SessionID: s.ID, Backend: "claude", NativeGeneration: s.ResumeID(), SourceRevision: s.Snapshot().SourceRevision, EventID: "unqualified-completion", Completed: true})
	if o = h.contextObservation(s.Snapshot()); o.Compactions.Completed != nil {
		t.Fatal("unqualified Claude completion counted")
	}
	h.messageQueue.Close()
	o = h.contextObservation(s.Snapshot())
	if o.Compactions.Coverage != "unavailable" || o.Compactions.Completed != nil || o.Usage.Status != "unknown" {
		t.Fatal("read failure looked healthy")
	}
}
func TestContextLiveEventExcludesEnrollmentAndProbe(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("normal", "Normal", t.TempDir(), "codex", "", "", "")
	s.SetResumeID("thread")
	ordinary := newTestClient(h)
	provisional := newTestClient(h)
	provisional.enrollmentOnly = true
	probe := newTestClient(h)
	probe.inventoryProbe = true
	h.Emit(backend.ContextTelemetry{SessionID: s.ID, Backend: "codex", NativeGeneration: s.ResumeID(), SourceRevision: s.Snapshot().SourceRevision, EventID: "item", Completed: true})
	if len(ordinary.send) != 1 || len(provisional.send) != 0 || len(probe.send) != 0 {
		t.Fatal("telemetry leaked to provisional client/probe or was not delivered")
	}
}

func TestContextScopeRevisionInvalidatesCapacityWithoutResettingPhysicalCount(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("config-scope", "Scope", t.TempDir(), "codex", "old-model", "", "")
	s.SetResumeID("thread")
	used, max := 20, 100
	h.Emit(backend.ContextTelemetry{SessionID: s.ID, Backend: "codex", NativeGeneration: s.ResumeID(), SourceRevision: s.Snapshot().SourceRevision, EventID: "usage", Origin: "codex_native_token_usage", MaxOrigin: "codex_native_token_usage", UsedTokens: &used, MaxTokens: &max})
	s.SetModel("new-model")
	o := h.contextObservation(s.Snapshot())
	if o.Usage.Status != "invalidated" {
		t.Fatal("old model capacity remained fresh under new configuration")
	}
}
