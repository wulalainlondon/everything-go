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
	final             *backend.ExactProviderFinal
	calls             int
	closeBeforeReturn bool
}

func (e *ownedFinalFixtureExec) ExactProviderFinalForRequest(s *session.Session, _ string, _ string, _ string) (*backend.ExactProviderFinal, error) {
	e.calls++
	if e.closeBeforeReturn {
		s.Close()
	}
	return e.final, nil
}

func TestOwnedFinalUsesCanonicalSealOnlyAfterOriginalAcceptance(t *testing.T) {
	for _, mode := range []string{"closed-before-seal", "valid", "wrong-session", "wrong-request", "wrong-thread", "wrong-turn", "wrong-source", "missing-subject", "missing-pages", "missing-native", "not-completed"} {
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
			ex := &ownedFinalFixtureExec{fakeExec: fe, final: f, closeBeforeReturn: mode == "closed-before-seal"}
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
				if wrong, err := h.jarvisResultSeal(context.Background(), s, "request", "seal_wrong"); err == nil || wrong.Text != "" || ex.calls != 1 {
					t.Fatal("wrong existing seal ID triggered fallback", wrong, err, ex.calls)
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

type fileFirstOwnedFixture struct {
	*ownedFinalFixtureExec
	proof *workspaceFixtureHistory
}

func (e *fileFirstOwnedFixture) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.proof, true
}
func TestOwnedFinalFileFirstAndMismatchNeverFallsThrough(t *testing.T) {
	for _, mode := range []string{"valid", "seal-mismatch", "thread-mismatch", "turn-mismatch", "carry-only"} {
		t.Run(mode, func(t *testing.T) {
			h, fe := newTestHub(t)
			s := h.registry.Create("file-first", "target", t.TempDir(), backend.Codex, "", "read-only", "thread")
			taskEntry(t, h, s.ID, "request", "owner", messagequeue.Completed)
			p := &workspaceFixtureHistory{correlationHistoryFixture: correlationHistoryFixture{Owners: map[string]string{}}, finals: map[string][]map[string]any{}}
			p.add(s, "request", "turn", "original file final")
			useNativeCorrelationFixture(h, fe, "thread", "request", "turn")
			if err := h.messageQueue.RecordNativeAcceptance(s.ID, "request", "thread", "turn"); err != nil {
				t.Fatal(err)
			}
			ex := &fileFirstOwnedFixture{ownedFinalFixtureExec: &ownedFinalFixtureExec{fakeExec: fe}, proof: p}
			h.SetExecutor(ex)
			id := ""
			switch mode {
			case "seal-mismatch":
				id = "seal_wrong"
			case "thread-mismatch":
				p.finals["thread"][0]["source_thread_id"] = "other"
			case "turn-mismatch":
				p.finals["thread"][0]["source_turn_id"] = "other"
			}
			allow := mode != "carry-only"
			seal, err := h.jarvisResultSeal(context.Background(), s, "request", id, allow)
			if ex.calls != 0 {
				t.Fatal("existing/conflicting file or carry-only reached provider")
			}
			if mode == "valid" {
				if err != nil || seal.Text != "original file final" {
					t.Fatal(seal, err)
				}
			} else if err == nil {
				t.Fatal("mismatch accepted", seal)
			}
		})
	}
}
