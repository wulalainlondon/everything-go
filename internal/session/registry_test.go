package session

import (
	"encoding/json"
	"os"
	"path/filepath"
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
	if got.State() != Idle {
		t.Fatalf("restored session should be Idle, got %s", got.State())
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
	if !ok || len(hist) != 1 || hist[0] != "uuid-prev" {
		t.Fatalf("historical_resume_ids not preserved: %+v", got["historical_resume_ids"])
	}
	recent, ok := got["recent_request_ids"].([]any)
	if !ok || len(recent) != 1 || recent[0] != "r1" {
		t.Fatalf("recent_request_ids not preserved: %+v", got["recent_request_ids"])
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
