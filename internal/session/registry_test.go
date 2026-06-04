package session

import (
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
