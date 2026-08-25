package runtimejournal

import "testing"

func TestPerDeviceReadAndRestart(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.Update("s1", "running", "r1", 0, "", "")
	completed, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	if got := s.Snapshot("phone", []string{"s1"})[0].Unread; got != 1 {
		t.Fatalf("phone unread=%d", got)
	}
	s.Ack("desktop", "s1", completed.Revision, true)
	if got := s.Snapshot("phone", []string{"s1"})[0].Unread; got != 1 {
		t.Fatalf("desktop consumed phone unread=%d", got)
	}
	if got := s.Snapshot("desktop", []string{"s1"})[0].Unread; got != 0 {
		t.Fatalf("desktop unread=%d", got)
	}
	reloaded := New(dir)
	if got := reloaded.Snapshot("phone", []string{"s1"})[0].Unread; got != 1 {
		t.Fatalf("restart lost phone cursor: %d", got)
	}
	reloaded.Ack("phone", "s1", completed.Revision, true)
	if got := reloaded.Snapshot("phone", []string{"s1"})[0].Unread; got != 0 {
		t.Fatalf("phone read did not clear: %d", got)
	}
}

func TestTerminalHistoryIsBounded(t *testing.T) {
	s := New(t.TempDir())
	for i := 0; i < 200; i++ {
		s.Update("s1", "running", "r", 0, "", "")
		s.Update("s1", "completed", "r", 0, "completed", "")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if got := len(s.records["s1"].Terminals); got != 128 {
		t.Fatalf("terminals=%d", got)
	}
}

func TestRecoverAllStaleRunsAtStartupBoundary(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.Update("active", "running", "r1", 0, "", "")
	s.Update("done", "completed", "r2", 0, "completed", "")

	reloaded := New(dir)
	recovered := reloaded.RecoverAllStale()
	if len(recovered) != 1 || recovered[0].SessionID != "active" || recovered[0].Phase != "interrupted" {
		t.Fatalf("recovered=%+v", recovered)
	}
	if got := reloaded.Snapshot("phone", []string{"done"})[0].Phase; got != "completed" {
		t.Fatalf("completed session changed to %q", got)
	}
	if again := reloaded.RecoverAllStale(); len(again) != 0 {
		t.Fatalf("startup recovery was not idempotent: %+v", again)
	}
}

func TestProgressIsRevisionedDedupedAndPersists(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	initial, _ := s.Update("s1", "running", "r1", 0, "", "")
	resuming, changed := s.Progress("s1", "r1", "resuming_thread", "Loading conversation")
	if !changed || resuming.Revision <= initial.Revision || resuming.Stage != "resuming_thread" {
		t.Fatalf("resuming progress=%+v changed=%v initial=%+v", resuming, changed, initial)
	}
	duplicate, changed := s.Progress("s1", "r1", "resuming_thread", "Loading conversation")
	if changed || duplicate.Revision != resuming.Revision {
		t.Fatalf("duplicate advanced revision: %+v changed=%v", duplicate, changed)
	}
	waiting, changed := s.Progress("s1", "r1", "waiting_model", "Codex accepted the turn")
	if !changed || waiting.Revision <= resuming.Revision {
		t.Fatalf("waiting progress=%+v changed=%v", waiting, changed)
	}
	regressed, changed := s.Progress("s1", "r1", "submitting_turn", "late RPC update")
	if changed || regressed.Stage != "waiting_model" || regressed.Revision != waiting.Revision {
		t.Fatalf("out-of-order stage regressed runtime: %+v changed=%v", regressed, changed)
	}
	reloaded := New(dir).Snapshot("phone", []string{"s1"})[0]
	if reloaded.Stage != "waiting_model" || reloaded.StageMessage != "Codex accepted the turn" || reloaded.StageStartedAt == 0 {
		t.Fatalf("restart lost stage: %+v", reloaded)
	}
}
