package session

import "testing"

func TestProviderDefaultEffortReceiptsCannotBeForgedOrSurviveSourceABA(t *testing.T) {
	for _, change := range []string{"nil", "forged", "otherSession", "sourceABA", "futureConfig", "cwd", "resetReserved", "closed"} {
		t.Run(change, func(t *testing.T) {
			s := NewRegistry().Create("original", "Original", t.TempDir(), "codex", "model", "read-only", "")
			high := "high"
			_, _, p := s.ApplyProviderRuntimeMetadata(nil, &high, 1)
			if p == nil {
				t.Fatal("missing receipt")
			}
			switch change {
			case "nil":
				p = nil
			case "forged":
				p = &DefaultEffortTransition{}
			case "otherSession":
				other := NewRegistry().Create("other", "Other", t.TempDir(), "codex", "model", "read-only", "")
				if e := other.WithDefaultEffortTransition(p, func(Snapshot, Snapshot, string) error { t.Error("foreign receipt invoked"); return nil }); e == nil {
					t.Fatal("foreign session accepted")
				}
				return
			case "sourceABA":
				s.SetModel("other")
				s.SetModel("model")
			case "futureConfig":
				cfg := ConfigurationFrom(s.SettingsSnapshot())
				_, _ = s.SetFutureConfiguration(cfg, nil)
			case "cwd":
				s.mu.Lock()
				s.cwd = "different"
				s.mu.Unlock()
			case "resetReserved":
				release, e := s.ReserveContextGeneration("original-reset", s.ResumeID(), s.Snapshot().SourceRevision)
				if e != nil {
					t.Fatal(e)
				}
				defer release()
			case "closed":
				s.Close()
			}
			if e := s.WithDefaultEffortTransition(p, func(Snapshot, Snapshot, string) error { t.Error("retired receipt invoked"); return nil }); e == nil {
				t.Fatal("retired receipt accepted")
			}
		})
	}
}

func TestMediaHostDefaultEffortEpochZeroCannotBecomeRoleReceipt(t *testing.T) {
	s := NewRegistry().Create("media-original", "Media", t.TempDir(), "codex", "gpt-6.1-sol", "read-only", "")
	high := "high"
	_, _, proof := s.ApplyProviderMediaRuntimeMetadata(nil, &high, 0)
	if proof == nil {
		t.Fatal("media ordinary receipt absent")
	}
	if e := s.WithDefaultEffortTransition(proof, func(Snapshot, Snapshot, string) error { t.Fatal("epoch zero acquired role receipt"); return nil }); e == nil {
		t.Fatal("old role API accepted ordinary proof")
	}
	if e := s.WithMediaDefaultEffortTransition(proof, func(Snapshot, Snapshot, string) error { return nil }); e != nil {
		t.Fatal(e)
	}
	s.SetResumeID("unproved")
	if e := s.WithMediaDefaultEffortTransition(proof, func(Snapshot, Snapshot, string) error { t.Fatal("ABA proof accepted"); return nil }); e == nil {
		t.Fatal("changed source adopted")
	}
}
