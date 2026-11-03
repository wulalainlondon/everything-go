package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"testing"
	"time"
)

// Native start/resume defaults precede application of the durable input's
// settings. They must not retire that input's recovery identity.
func TestQueuedCodexInitializationPreservesAdmittedConfiguration(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "cold-resume"}[existing], func(t *testing.T) {
			c := NewCodex(&capSink{}, "codex")
			c.appServerMode = "daemon"
			thread := ""
			if existing {
				thread = "original-thread"
			}
			s := session.NewRegistry().Create("queued", "New Session", t.TempDir(), backend.Codex, "gpt-6.1-sol", "danger-full-access", thread)
			s.SetEffort("medium")
			admitted := session.ConfigurationFrom(s.Snapshot())
			revision := s.Snapshot().SourceRevision
			w := &toolTestWriter{c: c}
			c.rpc.setWriter(w)
			settings := 0
			w.reply = func(method string, params json.RawMessage) (any, error) {
				var p map[string]any
				json.Unmarshal(params, &p)
				switch method {
				case "thread/start":
					if existing {
						t.Error("cold resume forked a new thread")
					}
					if p["model"] != "gpt-6.1-sol" {
						t.Error("model changed")
					}
				case "thread/resume":
					if !existing || p["threadId"] != "original-thread" || p["excludeTurns"] != true {
						t.Errorf("unexpected resume: %s", params)
					}
				case "thread/settings/update":
					settings++
					if p["model"] != "gpt-6.1-sol" || p["effort"] != "medium" {
						t.Errorf("admitted settings lost: %s", params)
					}
					return map[string]any{}, nil
				default:
					t.Errorf("unexpected RPC %s", method)
				}
				return map[string]any{"thread": map[string]any{"id": "original-thread", "model": "gpt-6.1-sol", "reasoningEffort": "high"}}, nil
			}
			done := make(chan error, 1)
			s.SubmitNamed("r_original", func() {
				defer s.EndTurn()
				if err := s.ActivateQueuedConfiguration(admitted); err != nil {
					done <- err
					return
				}
				if err := c.ensureThread(s, c.state(s.ID), 0); err != nil {
					done <- err
					return
				}
				want := revision
				if !existing {
					want++
				}
				if got := s.Snapshot(); session.ConfigurationFrom(got) != admitted || got.SourceRevision != want {
					t.Errorf("initialization changed admission: %+v", got)
				}
				done <- c.UpdateSessionSettings(context.Background(), s)
			})
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("initialization timeout")
			}
			s.CloseAndWait()
			if settings != 1 {
				t.Fatalf("settings updates=%d", settings)
			}
		})
	}
}
