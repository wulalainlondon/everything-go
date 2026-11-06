package session

import (
	"testing"
	"time"
)

func TestQueuedConfigurationWitnessOwnActorAndSourceFence(t *testing.T) {
	for _, change := range []string{"none", "wrong_request", "model_ABA", "thread_ABA", "hidden", "closed", "new_active_request"} {
		t.Run(change, func(t *testing.T) {
			s := NewRegistry().Create("original", "Original", t.TempDir(), "codex", "model", "read-only", "thread")
			defer s.Close()
			before := s.SettingsSnapshot()
			config := ConfigurationFrom(before)
			config.Model = "next"
			future, err := s.SetFutureConfiguration(config, nil)
			if err != nil {
				t.Fatal(err)
			}
			config = ConfigurationFrom(future)
			finished := make(chan error, 1)
			if !s.SubmitNamedWhenReady("original-request", func() bool { return true }, func() {
				if err := s.ActivateQueuedConfiguration(config); err != nil {
					finished <- err
					return
				}
				request := "original-request"
				switch change {
				case "wrong_request":
					request = "other"
				case "model_ABA":
					s.SetModel("foreign")
					s.SetModel("next")
				case "thread_ABA":
					s.SetResumeID("foreign")
					s.SetResumeID("thread")
				case "hidden":
					v := true
					s.SetMeta(nil, &v)
				case "closed":
					s.Close()
				case "new_active_request":
					s.mu.Lock()
					s.activeQueuedID = "other"
					s.mu.Unlock()
				}
				called := false
				err := s.WithQueuedConfigurationActivation(request, func(a, b Snapshot, c Configuration) error {
					called = true
					if a.SourceRevision != before.SourceRevision || b.SourceRevision != before.SourceRevision+1 || c != config || b.ConfigRevision != future.ConfigRevision {
						t.Error("not exact original activation")
					}
					return nil
				})
				if change == "none" {
					if err != nil || !called {
						t.Error("own activation unavailable", err)
					}
				} else if err == nil || called {
					t.Error("retired or foreign activation accepted", change)
				}
				finished <- nil
			}) {
				t.Fatal("actor not admitted")
			}
			select {
			case err := <-finished:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("actor stalled")
			}
		})
	}
}
