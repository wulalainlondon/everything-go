package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateIsIdempotent(t *testing.T) {
	r := NewRegistry()
	a := r.Create("s1", "first", "/p", "claude", "", "", "")
	b := r.Create("s1", "second", "/q", "codex", "", "", "")
	if a != b {
		t.Fatal("Create on an existing id must return the same session")
	}
	if a.Name() != "first" {
		t.Fatalf("idempotent Create must not overwrite fields, got name %q", a.Name())
	}
}

func TestPruneCodexSessions(t *testing.T) {
	r := NewRegistry()
	r.Create("daily", "Daily work", "/Users/test/project", "codex", "", "", "")
	r.Create("evaluation", "<recommended_plugins>\nnoise", "/Users/test/project", "codex", "", "", "")
	r.Create("claude", "<recommended_plugins>\nkeep", "/tmp", "claude", "", "", "")

	removed := r.PruneCodexSessions(func(_ string, name string) bool {
		return strings.HasPrefix(name, "<recommended_plugins>")
	})
	if removed != 1 {
		t.Fatalf("removed=%d want 1", removed)
	}
	if _, ok := r.Get("evaluation"); ok {
		t.Fatal("excluded Codex session remains")
	}
	if _, ok := r.Get("daily"); !ok {
		t.Fatal("daily Codex session was removed")
	}
	if _, ok := r.Get("claude"); !ok {
		t.Fatal("non-Codex session was removed")
	}
}

func TestDeleteClosesWorker(t *testing.T) {
	r := NewRegistry()
	s := r.Create("s1", "n", "/p", "claude", "", "", "")
	r.Delete("s1")
	if _, ok := r.Get("s1"); ok {
		t.Fatal("deleted session must be gone from the registry")
	}
	if s.State() != Closed {
		t.Fatalf("Delete must Close the session, state=%s", s.State())
	}
	if s.Submit(func() {}) {
		t.Fatal("a deleted/closed session must reject new turns")
	}
}

// Restart behavior: sessions saved by one registry are restored field-for-field
// by a fresh registry attaching the same store file.
func TestPersistRestartRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")

	r1 := NewRegistry()
	r1.AttachStore(NewStore(path))
	s := r1.Create("s1", "my session", "/work", "codex", "gpt-5", "danger", "")
	s.SetResumeID("thread-abc")
	r1.Persist()

	// "Restart": a brand-new registry restores from the same file.
	r2 := NewRegistry()
	r2.AttachStore(NewStore(path))
	got, ok := r2.Get("s1")
	if !ok {
		t.Fatal("session not restored after restart")
	}
	snap := got.Snapshot()
	if snap.Name != "my session" || snap.Cwd != "/work" || snap.Backend != "codex" ||
		snap.Model != "gpt-5" || snap.Sandbox != "danger" || snap.ResumeID != "thread-abc" {
		t.Fatalf("restored session lost fields: %+v", snap)
	}
	if len(snap.HistoricalResumeIDs) != 0 {
		t.Fatalf("new session unexpectedly has historical ids: %+v", snap.HistoricalResumeIDs)
	}
	if got.State() != Idle {
		t.Fatalf("restored session should be Idle, got %s", got.State())
	}
}

func TestAttachStoreCollapsesDuplicateResumeIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "saved_sessions.json")
	initial := `{
  "jl_native": {
    "name": "native alias",
    "resume_id": "thread-dup",
    "last_used": 300,
    "cwd": "/work",
    "backend": "codex"
  },
  "s_old": {
    "name": "old app row",
    "resume_id": "thread-dup",
    "last_used": 100,
    "cwd": "/work",
    "backend": "codex"
  },
  "s_new": {
    "name": "canonical app row",
    "resume_id": "thread-dup",
    "last_used": 200,
    "cwd": "/work",
    "backend": "codex"
  },
  "s_other": {
    "name": "other",
    "resume_id": "thread-other",
    "last_used": 50,
    "cwd": "/work",
    "backend": "codex"
  }
}`
	if err := os.WriteFile(path, []byte(initial), 0o644); err != nil {
		t.Fatal(err)
	}

	r := NewRegistry()
	r.AttachStore(NewStore(path))

	if _, ok := r.Get("s_new"); !ok {
		t.Fatal("most-recent explicit app session should be canonical")
	}
	if _, ok := r.Get("s_old"); ok {
		t.Fatal("older duplicate app session survived migration")
	}
	if _, ok := r.Get("jl_native"); ok {
		t.Fatal("native watcher alias must not replace an explicit app session")
	}
	if got := len(r.List()); got != 2 {
		t.Fatalf("registry has %d sessions after dedupe, want 2", got)
	}

	var persisted map[string]json.RawMessage
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if _, ok := persisted["s_old"]; ok {
		t.Fatal("older duplicate remained persisted")
	}
	if _, ok := persisted["jl_native"]; ok {
		t.Fatal("native duplicate remained persisted")
	}
}

func TestFindByResumeIDReturnsCanonicalSession(t *testing.T) {
	r := NewRegistry()
	want := r.Create("s1", "one", "/work", "codex", "", "", "thread-1")
	if got, ok := r.FindByResumeID("thread-1"); !ok || got != want {
		t.Fatalf("FindByResumeID = (%v, %v), want (%v, true)", got, ok, want)
	}
	if _, ok := r.FindByResumeID(""); ok {
		t.Fatal("empty resume id must never match")
	}
}

func TestStorePreservesPythonOnlyFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "saved_sessions.json")
	initial := `{
  "s1": {
    "name": "old",
    "resume_id": "uuid-old",
    "claude_uuid": "uuid-old",
    "last_used": 100,
    "cwd": "/old",
    "backend": "claude",
    "model": "",
    "sandbox": "danger-full-access",
    "image_dir": "/images",
    "parent_session_id": "parent",
    "forked_at": 123.5,
    "historical_resume_ids": ["uuid-prev"],
    "latest_source_line": "line-9",
    "recent_request_ids": ["r1"]
  }
}`
	if err := os.WriteFile(path, []byte(initial), 0o644); err != nil {
		t.Fatal(err)
	}

	r := NewRegistry()
	r.AttachStore(NewStore(path))
	s, ok := r.Get("s1")
	if !ok {
		t.Fatal("session not restored")
	}
	s.SetName("new")
	s.SetResumeID("uuid-new")
	r.Persist()

	var raw map[string]map[string]any
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	got := raw["s1"]
	if got["name"] != "new" || got["resume_id"] != "uuid-new" || got["claude_uuid"] != "uuid-new" {
		t.Fatalf("known fields not updated: %+v", got)
	}
	if got["image_dir"] != "/images" || got["parent_session_id"] != "parent" ||
		got["latest_source_line"] != "line-9" {
		t.Fatalf("python-only scalar fields were not preserved: %+v", got)
	}
	hist, ok := got["historical_resume_ids"].([]any)
	if !ok || len(hist) != 2 || hist[0] != "uuid-prev" || hist[1] != "uuid-old" {
		t.Fatalf("historical_resume_ids not preserved: %+v", got["historical_resume_ids"])
	}
	recent, ok := got["recent_request_ids"].([]any)
	if !ok || len(recent) != 1 || recent[0] != "r1" {
		t.Fatalf("recent_request_ids not preserved: %+v", got["recent_request_ids"])
	}
}

func TestNativeWatcherCannotRollLogicalSessionBackToArchivedThread(t *testing.T) {
	r := NewRegistry()
	s := r.Create("jl_x_old-thread", "logical", "/work", "codex", "", "", "old-thread")
	s.SetResumeID("new-thread")

	got, _ := r.UpsertExternal(
		"jl_x_old-thread", "old native title", "/old", "codex", "old-thread", 300,
	)
	if got != s {
		t.Fatalf("UpsertExternal returned %v, want same logical session", got)
	}
	snap := s.Snapshot()
	if snap.ResumeID != "new-thread" {
		t.Fatalf("native watcher rolled active resume id backwards: %+v", snap)
	}
	if len(snap.HistoricalResumeIDs) != 1 || snap.HistoricalResumeIDs[0] != "old-thread" {
		t.Fatalf("old thread was not retained as history: %+v", snap.HistoricalResumeIDs)
	}
	if snap.Name != "logical" || snap.Cwd != "/work" {
		t.Fatalf("archived rollout overwrote logical metadata: %+v", snap)
	}
}

func TestResumeGenerationHistoryPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	r1 := NewRegistry()
	r1.AttachStore(NewStore(path))
	s := r1.Create("logical", "long session", "/work", "codex", "", "", "thread-a")
	s.SetResumeID("thread-b")
	s.SetResumeID("thread-c")
	r1.Persist()

	r2 := NewRegistry()
	r2.AttachStore(NewStore(path))
	restored, ok := r2.Get("logical")
	if !ok {
		t.Fatal("logical session missing after restart")
	}
	snap := restored.Snapshot()
	if snap.ResumeID != "thread-c" || strings.Join(snap.HistoricalResumeIDs, ",") != "thread-a,thread-b" {
		t.Fatalf("generation chain did not survive restart: %+v", snap)
	}
}

func TestStoreLoadsPythonFloatLastUsed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "saved_sessions.json")
	initial := `{
  "jl_c_float": {
    "name": "float time",
    "resume_id": "uuid-float",
    "claude_uuid": "uuid-float",
    "last_used": 1779885009.376,
    "cwd": "~",
    "backend": "claude",
    "model": "",
    "sandbox": "danger-full-access"
  }
}`
	if err := os.WriteFile(path, []byte(initial), 0o644); err != nil {
		t.Fatal(err)
	}
	r := NewRegistry()
	r.AttachStore(NewStore(path))
	got, ok := r.Get("jl_c_float")
	if !ok {
		t.Fatal("Python float last_used entry was skipped")
	}
	snap := got.Snapshot()
	if snap.Name != "float time" || snap.LastActivity == 0 || snap.CreatedAt == 0 {
		t.Fatalf("float last_used was not restored correctly: %+v", snap)
	}
}

func TestStoreKeepsExternalSessionsAddedAfterLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "saved_sessions.json")
	if err := os.WriteFile(path, []byte(`{"s1":{"name":"one","last_used":200,"cwd":"/one","backend":"claude"}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	r := NewRegistry()
	r.AttachStore(NewStore(path))

	var raw map[string]map[string]any
	data, _ := os.ReadFile(path)
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	raw["external"] = map[string]any{"name": "external", "last_used": 200, "cwd": "/x", "backend": "claude"}
	data, _ = json.MarshalIndent(raw, "", "  ")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	if s, ok := r.Get("s1"); ok {
		s.SetName("one-updated")
	}
	r.Persist()

	data, _ = os.ReadFile(path)
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["external"]; !ok {
		t.Fatalf("external session added after Go Load was deleted: %+v", raw)
	}
}
