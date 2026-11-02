package core

import (
	"everything-go/internal/backend"
	"everything-go/internal/runtimejournal"
	"everything-go/internal/session"
	"testing"
)

type readinessProvider struct {
	*accessProvider
	value string
}

func (p *readinessProvider) RuntimeToolReadiness(*session.Session, string) string { return p.value }

type readinessExecutor struct {
	fakeExec
	p *readinessProvider
}

func (e *readinessExecutor) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.p, true
}
func TestReadinessHasItsOwnRevisionAtUnchangedRuntimeProgress(t *testing.T) {
	h, _ := newTestHub(t)
	h.registry.Create("source", "One", t.TempDir(), "codex", "", "", "thread")
	p := &readinessProvider{accessProvider: &accessProvider{}, value: "ready"}
	h.exec = &readinessExecutor{p: p}
	v := runtimejournal.View{SessionID: "source", Revision: 2, Phase: "running", ActiveRequestID: "original", ReadEpoch: "this-boot"}
	first := h.runtimeEvent(v)
	p.value = "observing"
	second := h.runtimeEvent(v)
	duplicate := h.runtimeEvent(v)
	if first.ToolReadiness != "ready" || second.ToolReadiness != "observing" || second.ToolReadinessRevision <= first.ToolReadinessRevision || duplicate.ToolReadinessRevision != second.ToolReadinessRevision || second.Revision != first.Revision {
		t.Fatal(first, second, duplicate)
	}
	p.value = "ready"
	newer := h.runtimeEvent(v)
	if newer.ToolReadinessRevision <= second.ToolReadinessRevision {
		t.Fatal("legitimate newer sample dropped")
	}
}
