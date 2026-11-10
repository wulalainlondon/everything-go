package session

import (
	"context"
	"path/filepath"
	"testing"
)

func TestInitializationBindChecksOriginalSourceAtomically(t *testing.T) {
	for _, change := range []string{"config", "hidden", "closed", "cancel", "transport"} {
		t.Run(change, func(t *testing.T) {
			s := NewRegistry().Create("s", "name", "/", "codex", "m", "read-only", "")
			snap := s.Snapshot()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch change {
			case "config":
				s.SetConfigRevision(9)
			case "hidden":
				hidden := true
				s.SetMeta(nil, &hidden)
			case "closed":
				s.Close()
			case "cancel":
				cancel()
			}
			if s.BindInitializedThread(ctx, snap, "old-ack", func() bool { return change != "transport" }) || s.ResumeID() != "" {
				t.Fatal("stale initialization bound")
			}
		})
	}
}

func TestInitializationContextGenerationPersistsOnlyExplicitReset(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "sessions.json")
	r := NewRegistry()
	r.AttachStore(NewStore(filename))
	s := r.Create("s", "name", "/", "codex", "m", "read-only", "")
	if !s.BindInitializedThread(context.Background(), s.Snapshot(), "old", func() bool { return true }) {
		t.Fatal("original binding refused")
	}
	s.SetConfigRevision(9)
	s.SetEffort("high")
	if s.Snapshot().ContextGeneration != 0 {
		t.Fatal("configuration changed explicit context generation")
	}
	release, err := s.ReserveContextGeneration("r_reset", "old", s.Snapshot().SourceRevision)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.DetachReservedContext("old"); err != nil {
		t.Fatal(err)
	}
	release()
	if s.Snapshot().ContextGeneration != 1 {
		t.Fatal("explicit reset not recorded")
	}
	if err = r.PersistDurably(); err != nil {
		t.Fatal(err)
	}
	reopened := NewRegistry()
	reopened.AttachStore(NewStore(filename))
	restored, ok := reopened.Get("s")
	if !ok || restored == nil || restored.Snapshot().ContextGeneration != 1 || restored.ResumeID() != "" {
		t.Fatal("explicit generation lost on reopen")
	}
	restored.ClearResumeIDs()
	if restored.Snapshot().ContextGeneration != 2 {
		t.Fatal("clear did not retire acknowledged generation")
	}
}
