package core

import (
	"context"
	"strings"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
)

type ownedFinalFixtureExec struct {
	*fakeExec
	final *backend.ExactProviderFinal
	calls int
}

func (e *ownedFinalFixtureExec) ExactProviderFinalForRequest(*session.Session, string, string, string) (*backend.ExactProviderFinal, error) {
	e.calls++
	return e.final, nil
}

func TestOwnedFinalUsesCanonicalSealOnlyAfterOriginalAcceptance(t *testing.T) {
	for _, mode := range []string{"valid", "wrong-session", "wrong-request", "wrong-thread", "wrong-turn", "wrong-source", "missing-subject", "missing-pages", "missing-native", "not-completed"} {
		t.Run(mode, func(t *testing.T) {
			h, fe := newTestHub(t)
			s := h.registry.Create("owned", "target", t.TempDir(), backend.Codex, "", "read-only", "thread")
			state := messagequeue.Completed
			if mode == "not-completed" {
				state = messagequeue.Queued
			}
			taskEntry(t, h, s.ID, "request", "owner", state)
			useNativeCorrelationFixture(h, fe, "thread", "request", "turn")
			if mode != "missing-native" {
				if err := h.messageQueue.RecordNativeAcceptance(s.ID, "request", "thread", "turn"); err != nil {
					t.Fatal(err)
				}
			}
			f := &backend.ExactProviderFinal{SessionID: s.ID, RequestID: "request", ThreadID: "thread", TurnID: "turn", ConfigRevision: s.SettingsSnapshot().ConfigRevision, SourceRevision: s.Snapshot().SourceRevision, SourceMessageID: "codex:thread:item:native-final", Text: "original exact public final", SubjectSHA256: strings.Repeat("a", 64), SubjectBeforeSHA256: strings.Repeat("d", 64), SubjectAfterSHA256: strings.Repeat("e", 64), TransportEpoch: 1, TurnResponseSHA256: strings.Repeat("b", 64), ItemPageSHA256: []string{strings.Repeat("c", 64)}}
			switch mode {
			case "wrong-session":
				f.SessionID = "other"
			case "wrong-request":
				f.RequestID = "other"
			case "wrong-thread":
				f.ThreadID = "other"
			case "wrong-turn":
				f.TurnID = "other"
			case "wrong-source":
				f.SourceRevision++
			case "missing-subject":
				f.SubjectSHA256 = ""
			case "missing-pages":
				f.ItemPageSHA256 = nil
			}
			ex := &ownedFinalFixtureExec{fakeExec: fe, final: f}
			h.SetExecutor(ex)
			seal, err := h.jarvisResultSeal(context.Background(), s, "request", "")
			if mode == "valid" {
				if err != nil || seal.Text != f.Text || seal.Hash != jarvisTextHash(f.Text) || seal.ID != "seal_"+jarvisTextHash(f.Text) {
					t.Fatal(seal, err)
				}
				again, err := h.jarvisResultSeal(context.Background(), s, "request", seal.ID)
				if err != nil || again.Hash != seal.Hash || ex.calls != 1 {
					t.Fatal(again, err, ex.calls)
				}
			} else {
				if err == nil || seal.Text != "" {
					t.Fatal("unproven final was sealed", seal, err)
				}
				if mode == "missing-native" || mode == "not-completed" {
					if ex.calls != 0 {
						t.Fatal("provider called before original acceptance/terminal guard")
					}
				}
				if _, err := h.messageQueue.TaskSeal(context.Background(), s.ID, "request", ""); err == nil {
					t.Fatal("negative result persisted seal")
				}
			}
		})
	}
}
