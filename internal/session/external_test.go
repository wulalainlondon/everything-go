package session

import "testing"

func TestRegistryUpsertExternalCreatesAndUpdates(t *testing.T) {
	r := NewRegistry()
	s, changed := r.UpsertExternal("jl_c_123", "First", "/repo", "claude", "resume-1", 100)
	if !changed || s == nil {
		t.Fatalf("create changed=%v session=%v", changed, s)
	}
	snap := s.Snapshot()
	if snap.ID != "jl_c_123" || snap.Name != "First" || snap.Cwd != "/repo" ||
		snap.Backend != "claude" || snap.ResumeID != "resume-1" || int64(snap.LastActivity) != 100 {
		t.Fatalf("bad snapshot after create: %+v", snap)
	}

	s2, changed := r.UpsertExternal("jl_c_123", "Second", "/repo2", "claude", "resume-1", 120)
	if !changed || s2 != s {
		t.Fatalf("update changed=%v same=%v", changed, s2 == s)
	}
	snap = s.Snapshot()
	if snap.Name != "Second" || snap.Cwd != "/repo2" || int64(snap.LastActivity) != 120 {
		t.Fatalf("bad snapshot after update: %+v", snap)
	}
}

func TestRegistryUpsertExternalDedupesBridgeSessionByResumeID(t *testing.T) {
	r := NewRegistry()
	bridge := r.Create("s_live", "Live", "/work", "codex", "", "danger-full-access", "thread-1")
	last := int64(nowSeconds()) + 10
	got, changed := r.UpsertExternal("jl_x_thread", "External", "/other", "codex", "thread-1", last)
	// Dedupe must resolve to the existing bridge session (not create a second one)...
	if got != bridge {
		t.Fatalf("expected dedupe to the bridge session, gotBridge=%v", got == bridge)
	}
	// ...but a pure activity bump on an already-known bridge session must NOT be
	// reported as "changed": the CLI rewrites the transcript constantly, and
	// broadcasting sessions_list on every write floods (OOMs) the app. Structural
	// changes (name/cwd/backend) still count — see registry.UpsertExternal.
	if changed {
		t.Fatalf("pure activity update on a known bridge session should not be 'changed'")
	}
	if ids := r.List(); len(ids) != 1 {
		t.Fatalf("expected one session after dedupe, got %d", len(ids))
	}
	snap := bridge.Snapshot()
	if snap.Name != "Live" || snap.Cwd != "/work" || int64(snap.LastActivity) != last {
		t.Fatalf("bridge-owned fields should be preserved while activity updates: %+v", snap)
	}
}
