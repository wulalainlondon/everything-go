package goexec

import (
	"os"
	"path/filepath"
	"testing"
)

// writeJSONLLines writes newline-terminated JSONL lines to path.
func writeJSONLLines(t *testing.T, path string, lines ...string) {
	t.Helper()
	var buf []byte
	for _, l := range lines {
		buf = append(buf, l...)
		buf = append(buf, '\n')
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

// agentTreeFixture mirrors real Claude data: every subagent has a single user
// row whose promptId is the main turn that spawned it. Two agents (A, B) belong
// to the latest turn p2; one (C) belongs to the older turn p1 and so is filtered.
//
//	<projects>/proj/<resume>.jsonl          main transcript (turns p1, p2)
//	<projects>/proj/<resume>/subagents/     agent-A/B/C.jsonl + meta
func agentTreeFixture(t *testing.T) (*Claude, string) {
	t.Helper()
	projects := t.TempDir()
	proj := filepath.Join(projects, "proj")
	subagents := filepath.Join(proj, "resume-1", "subagents")
	if err := os.MkdirAll(subagents, 0o755); err != nil {
		t.Fatal(err)
	}

	writeJSONLLines(t, filepath.Join(proj, "resume-1.jsonl"),
		`{"type":"user","promptId":"p1","timestamp":"2026-06-01T00:00:00Z","message":{"content":"first turn"}}`,
		`{"type":"user","promptId":"p2","timestamp":"2026-06-01T01:00:00Z","message":{"content":"second turn"}}`,
	)
	writeJSONLLines(t, filepath.Join(subagents, "agent-A.jsonl"),
		`{"type":"user","promptId":"p2","timestamp":"2026-06-01T01:00:01Z","message":{"content":[{"type":"text","text":"explore the repo"}]}}`,
		`{"type":"assistant","timestamp":"2026-06-01T01:00:02Z","message":{"content":[{"type":"tool_use","name":"Grep"},{"type":"text","text":"scanning"}]}}`,
		`{"type":"assistant","timestamp":"2026-06-01T01:00:05Z","message":{"content":[{"type":"text","text":"A final answer"}]}}`,
	)
	writeJSONLLines(t, filepath.Join(subagents, "agent-A.meta.json"), `{"agentType":"explorer"}`)
	writeJSONLLines(t, filepath.Join(subagents, "agent-B.jsonl"),
		`{"type":"user","promptId":"p2","timestamp":"2026-06-01T01:00:03Z","message":{"content":[{"type":"text","text":"run the tests"}]}}`,
		`{"type":"assistant","timestamp":"2026-06-01T01:00:04Z","message":{"content":[{"type":"text","text":"B done"}]}}`,
	)
	writeJSONLLines(t, filepath.Join(subagents, "agent-C.jsonl"),
		`{"type":"user","promptId":"p1","timestamp":"2026-06-01T00:00:01Z","message":{"content":[{"type":"text","text":"old turn agent"}]}}`,
		`{"type":"assistant","timestamp":"2026-06-01T00:00:02Z","message":{"content":[{"type":"text","text":"C done"}]}}`,
	)

	c := NewClaude(&capSink{}, "claude")
	c.projectsDir = projects
	return c, "resume-1"
}

func TestBuildAgentTreeLatestTurnAndFields(t *testing.T) {
	c, resume := agentTreeFixture(t)

	total, tree := c.BuildAgentTree(resume)
	if total != 3 {
		t.Fatalf("total_agents = %d, want 3 (A,B,C)", total)
	}
	// Latest turn is p2 → A and B survive (both p2 roots); C (p1) is dropped.
	if len(tree) != 2 {
		t.Fatalf("expected 2 roots from the latest turn, got %d: %+v", len(tree), tree)
	}
	for _, n := range tree {
		if n.PromptID == nil || *n.PromptID != "p2" {
			t.Fatalf("filtered tree must only contain latest-turn (p2) agents, got %v", n.PromptID)
		}
		if n.AgentID == "C" {
			t.Fatal("agent C belongs to the older turn and must be filtered out")
		}
	}

	a := tree[0] // discovery order: agent-A sorts before agent-B
	if a.AgentID != "A" {
		t.Fatalf("first root = %s, want A (discovery order)", a.AgentID)
	}
	if a.AgentType != "explorer" {
		t.Errorf("agent A type = %q, want explorer (from meta.json)", a.AgentType)
	}
	if a.Description != "explore the repo" {
		t.Errorf("agent A description = %q", a.Description)
	}
	if a.OutputPreview != "A final answer" {
		t.Errorf("agent A output_preview = %q (want last assistant text)", a.OutputPreview)
	}
	if len(a.ToolCalls) != 1 || a.ToolCalls[0].Name != "Grep" {
		t.Errorf("agent A tool_calls = %+v, want one Grep", a.ToolCalls)
	}
	if a.StartTS == nil || a.EndTS == nil || a.DurationMS == nil || *a.DurationMS <= 0 {
		t.Errorf("agent A timing missing: start=%v end=%v dur=%v", a.StartTS, a.EndTS, a.DurationMS)
	}
	if len(a.Children) != 0 {
		t.Errorf("agent A should be a leaf, got %d children", len(a.Children))
	}
	// agent-B has no meta.json → type defaults to unknown.
	if tree[1].AgentType != "unknown" {
		t.Errorf("agent B type = %q, want unknown (no meta.json)", tree[1].AgentType)
	}
}

// TestBuildAgentTreeNesting exercises parent-linking: a child whose only prompt
// (pChild) is re-issued as a non-first prompt inside a later-sorted parent. The
// parent's first prompt (pZroot) is a main turn, so it roots; the child nests.
func TestBuildAgentTreeNesting(t *testing.T) {
	projects := t.TempDir()
	subagents := filepath.Join(projects, "proj", "resume-n", "subagents")
	if err := os.MkdirAll(subagents, 0o755); err != nil {
		t.Fatal(err)
	}
	writeJSONLLines(t, filepath.Join(projects, "proj", "resume-n.jsonl"),
		`{"type":"user","promptId":"pZroot","timestamp":"2026-06-01T01:00:00Z","message":{"content":"turn"}}`,
	)
	// agent-child sorts before agent-parent; its sole prompt is pChild.
	writeJSONLLines(t, filepath.Join(subagents, "agent-child.jsonl"),
		`{"type":"user","promptId":"pChild","timestamp":"2026-06-01T01:00:02Z","message":{"content":[{"type":"text","text":"child"}]}}`,
		`{"type":"assistant","timestamp":"2026-06-01T01:00:03Z","message":{"content":[{"type":"text","text":"child done"}]}}`,
	)
	// agent-parent: first prompt pZroot (a main turn → root); also owns pChild.
	writeJSONLLines(t, filepath.Join(subagents, "agent-parent.jsonl"),
		`{"type":"user","promptId":"pZroot","timestamp":"2026-06-01T01:00:01Z","message":{"content":[{"type":"text","text":"parent"}]}}`,
		`{"type":"user","promptId":"pChild","timestamp":"2026-06-01T01:00:02Z","message":{"content":"spawn child"}}`,
		`{"type":"assistant","timestamp":"2026-06-01T01:00:04Z","message":{"content":[{"type":"text","text":"parent done"}]}}`,
	)

	c := NewClaude(&capSink{}, "claude")
	c.projectsDir = projects

	total, tree := c.BuildAgentTree("resume-n")
	if total != 2 {
		t.Fatalf("total_agents = %d, want 2", total)
	}
	if len(tree) != 1 || tree[0].AgentID != "parent" {
		t.Fatalf("expected single root 'parent', got %+v", tree)
	}
	if len(tree[0].Children) != 1 || tree[0].Children[0].AgentID != "child" {
		t.Fatalf("parent should own one child 'child', got %+v", tree[0].Children)
	}
	child := tree[0].Children[0]
	if child.ParentAgentID == nil || *child.ParentAgentID != "parent" {
		t.Errorf("child parent_agent_id = %v, want parent", child.ParentAgentID)
	}
}

func TestBuildAgentTreeNoSubagentDir(t *testing.T) {
	projects := t.TempDir()
	proj := filepath.Join(projects, "proj")
	if err := os.MkdirAll(proj, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proj, "lonely.jsonl"),
		[]byte(`{"type":"user","promptId":"p1","message":{"content":"hi"}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := NewClaude(&capSink{}, "claude")
	c.projectsDir = projects

	total, tree := c.BuildAgentTree("lonely")
	if total != 0 || len(tree) != 0 || tree == nil {
		t.Fatalf("no subagents → want (0, empty non-nil slice), got (%d, %v)", total, tree)
	}
}

func TestBuildAgentTreeUnknownResume(t *testing.T) {
	c := NewClaude(&capSink{}, "claude")
	c.projectsDir = t.TempDir()
	total, tree := c.BuildAgentTree("ghost")
	if total != 0 || tree == nil || len(tree) != 0 {
		t.Fatalf("unknown resume → want (0, empty), got (%d, %v)", total, tree)
	}
}
