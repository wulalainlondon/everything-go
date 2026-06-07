package core

import (
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// treeProv is a history provider that also builds an agent tree.
type treeProv struct {
	gotResume string
	total     int
	tree      []*protocol.AgentNode
}

func (p *treeProv) LoadHistory(string, history.Opts) (*history.Result, error) {
	return &history.Result{Kind: "snapshot"}, nil
}
func (p *treeProv) ResumableSessions(int) ([]history.ResumableSession, error) { return nil, nil }
func (p *treeProv) BuildAgentTree(resumeID string) (int, []*protocol.AgentNode) {
	p.gotResume = resumeID
	return p.total, p.tree
}

type treeExec struct {
	fakeExec
	prov *treeProv
}

func (e *treeExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.prov, true
}
func (e *treeExec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.prov}
}

// plainProv supports history but NOT agent trees (Codex/Ollama parity).
type plainProv struct{}

func (plainProv) LoadHistory(string, history.Opts) (*history.Result, error) {
	return &history.Result{Kind: "snapshot"}, nil
}
func (plainProv) ResumableSessions(int) ([]history.ResumableSession, error) { return nil, nil }

type plainExec struct {
	fakeExec
	prov plainProv
}

func (e *plainExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.prov, true
}
func (e *plainExec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.prov}
}

func TestGetAgentTreeRoutesToBackend(t *testing.T) {
	h, _ := newTestHub(t)
	pid := "p1"
	prov := &treeProv{total: 1, tree: []*protocol.AgentNode{{
		AgentID: "A", AgentType: "explorer", PromptID: &pid,
		ToolCalls: []protocol.AgentToolCall{}, Children: []*protocol.AgentNode{},
	}}}
	h.SetExecutor(&treeExec{fakeExec: fakeExec{sink: h}, prov: prov})
	h.registry.Create("s1", "S", t.TempDir(), "claude", "", "", "resume-xyz")

	c := newTestClient(h)
	route(h, c, `{"type":"get_agent_tree","session_id":"s1"}`)

	ev := waitForType(t, c, "agent_tree")
	if ev["session_id"] != "s1" || ev["resume_id"] != "resume-xyz" {
		t.Fatalf("agent_tree envelope wrong: %+v", ev)
	}
	if ev["total_agents"].(float64) != 1 {
		t.Fatalf("total_agents = %v, want 1", ev["total_agents"])
	}
	tree, _ := ev["tree"].([]any)
	if len(tree) != 1 {
		t.Fatalf("tree len = %d, want 1", len(tree))
	}
	if prov.gotResume != "resume-xyz" {
		t.Fatalf("backend received resume %q, want resume-xyz", prov.gotResume)
	}
}

// No session, empty resume id, or a backend without the capability → no event
// (parity with Python's silent return True). The client stays quiet.
func TestGetAgentTreeNoOps(t *testing.T) {
	t.Run("unknown session", func(t *testing.T) {
		h, _ := newTestHub(t)
		h.SetExecutor(&treeExec{fakeExec: fakeExec{sink: h}, prov: &treeProv{}})
		c := newTestClient(h)
		h.handleAgentTree(c, "ghost")
		assertNoEvent(t, c)
	})
	t.Run("empty resume id", func(t *testing.T) {
		h, _ := newTestHub(t)
		h.SetExecutor(&treeExec{fakeExec: fakeExec{sink: h}, prov: &treeProv{}})
		h.registry.Create("s1", "S", t.TempDir(), "claude", "", "", "") // no resume
		c := newTestClient(h)
		h.handleAgentTree(c, "s1")
		assertNoEvent(t, c)
	})
	t.Run("backend without capability", func(t *testing.T) {
		h, _ := newTestHub(t)
		h.SetExecutor(&plainExec{fakeExec: fakeExec{sink: h}})
		h.registry.Create("s1", "S", t.TempDir(), "codex", "", "", "resume-1")
		c := newTestClient(h)
		h.handleAgentTree(c, "s1")
		assertNoEvent(t, c)
	})
}

// assertNoEvent fails if the client receives any frame within a short window.
func assertNoEvent(t *testing.T, c *Client) {
	t.Helper()
	select {
	case data := <-c.send:
		t.Fatalf("expected no event, got %s", string(data))
	default:
	}
}
