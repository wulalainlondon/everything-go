package goexec

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"everything-go/internal/protocol"
)

// BuildAgentTree reconstructs the subagent hierarchy for a Claude session from
// the on-disk transcripts under <dir>/<resume_id>/subagents/agent-*.jsonl.
// Mirrors ClaudeHistory._build_agent_tree_sync (bridge/backends/claude_history.py).
// Satisfies the core's agentTreeBuilder capability; Codex/Ollama don't implement
// it, so get_agent_tree on those backends is a no-op (parity with Python, whose
// only build_agent_tree lives on the Claude backend).
//
// Deliberate parity gap: Python keeps an mtime-keyed LRU cache (_AGENT_TREE_CACHE)
// to skip re-scanning unchanged transcripts. Go scans fresh each call — simpler,
// and the trees are small (one turn's subagents). No cache.
func (c *Claude) BuildAgentTree(resumeID string) (int, []*protocol.AgentNode) {
	empty := []*protocol.AgentNode{}
	mainPath := c.findSessionFile(resumeID)
	if mainPath == "" {
		return 0, empty
	}
	subagentDir := filepath.Join(filepath.Dir(mainPath), resumeID, "subagents")
	if fi, err := os.Stat(subagentDir); err != nil || !fi.IsDir() {
		return 0, empty
	}

	mainPromptIDs, latestPromptID := scanMainJSONL(mainPath)

	// Task-tool subagents sit flat under subagents/; their hierarchy is rebuilt
	// from promptId parentage (the original behaviour).
	taskTotal, tree := buildFlatTaskTree(subagentDir, mainPromptIDs)

	// Workflow-tool runs land one level deeper, under
	// subagents/workflows/<wf_id>/agent-*.jsonl. Their agents share a single turn
	// promptId and carry no Task-style parentage, so the flat ReadDir above never
	// sees them (it skips the workflows/ directory). Group each run under a
	// synthetic node so the orchestration reads as "one workflow, N agents"
	// instead of N indistinguishable siblings. Deliberate divergence from Python,
	// which predates the Workflow tool and has no workflows/ handling.
	wfTotal, wfTree := buildWorkflowGroups(filepath.Join(subagentDir, "workflows"))
	tree = append(tree, wfTree...)

	total := taskTotal + wfTotal
	if total == 0 {
		return 0, empty
	}

	// Filter to the latest conversation turn only (parity with Python). Synthetic
	// workflow nodes carry their run's shared promptId, so a workflow that ran in
	// the latest turn survives the same filter.
	if latestPromptID != "" {
		var filtered []*protocol.AgentNode
		for _, n := range tree {
			if n.PromptID != nil && *n.PromptID == latestPromptID {
				filtered = append(filtered, n)
			}
		}
		if len(filtered) > 0 {
			tree = filtered
		}
	}
	if tree == nil {
		tree = empty
	}
	return total, tree
}

// buildFlatTaskTree reconstructs the Task-tool subagent hierarchy from the flat
// agent-*.jsonl files directly under dir, linking parents via promptId. Returns
// the agent count and the unfiltered root list. Nested directories (e.g.
// workflows/) are skipped — buildWorkflowGroups handles those.
func buildFlatTaskTree(dir string, mainPromptIDs map[string]bool) (int, []*protocol.AgentNode) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, nil
	}

	agents := map[string]*protocol.AgentNode{}
	var order []string                       // discovery order, for deterministic output
	subagentPromptIDs := map[string]string{} // any promptId → owning agent_id

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".jsonl") || !strings.HasPrefix(name, "agent-") {
			continue
		}
		agentID := name[len("agent-") : len(name)-len(".jsonl")]
		if agentID == "" {
			continue
		}

		agentType := readAgentType(filepath.Join(dir, "agent-"+agentID+".meta.json"))
		scan := scanAgentJSONL(filepath.Join(dir, name))

		for _, pid := range scan.promptIDs {
			subagentPromptIDs[pid] = agentID
		}

		agents[agentID] = nodeFromScan(agentID, agentType, scan)
		order = append(order, agentID)
	}

	if len(agents) == 0 {
		return 0, nil
	}

	// Determine parent_agent_id: an agent whose first prompt is in the main
	// transcript is a root; otherwise its parent is whoever owns that promptId.
	for _, aid := range order {
		node := agents[aid]
		if node.PromptID == nil {
			continue
		}
		fp := *node.PromptID
		if mainPromptIDs[fp] {
			continue
		}
		if parent, ok := subagentPromptIDs[fp]; ok && parent != aid {
			p := parent
			node.ParentAgentID = &p
		}
	}

	// Group children under parents (preserving discovery order).
	childrenMap := map[string][]string{}
	var rootNodes []string
	for _, aid := range order {
		p := agents[aid].ParentAgentID
		if p != nil {
			if _, ok := agents[*p]; ok {
				childrenMap[*p] = append(childrenMap[*p], aid)
				continue
			}
		}
		rootNodes = append(rootNodes, aid)
	}

	// Attach children depth-first with a visited guard (cycle-safe, mirrors the
	// BFS+visited in Python).
	visited := map[string]bool{}
	var build func(aid string) *protocol.AgentNode
	build = func(aid string) *protocol.AgentNode {
		n := agents[aid]
		if visited[aid] {
			return n
		}
		visited[aid] = true
		n.Children = []*protocol.AgentNode{}
		for _, child := range childrenMap[aid] {
			if !visited[child] {
				n.Children = append(n.Children, build(child))
			}
		}
		return n
	}
	var tree []*protocol.AgentNode
	for _, aid := range rootNodes {
		tree = append(tree, build(aid))
	}
	return len(agents), tree
}

// buildWorkflowGroups scans subagents/workflows/<wf_id>/ and returns one
// synthetic group node per workflow run, with that run's agents as leaf
// children. The group's promptId/timing are derived from its children: the
// shared turn promptId (so the latest-turn filter applies) and the min-start /
// max-end span. Returns the total real-agent count (group nodes excluded) and
// the group nodes. A missing workflows/ directory yields (0, nil).
func buildWorkflowGroups(workflowsDir string) (int, []*protocol.AgentNode) {
	dirs, err := os.ReadDir(workflowsDir)
	if err != nil {
		return 0, nil
	}

	total := 0
	var groups []*protocol.AgentNode
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		wfDir := filepath.Join(workflowsDir, d.Name())
		files, err := os.ReadDir(wfDir)
		if err != nil {
			continue
		}

		var children []*protocol.AgentNode
		var startTS, endTS *int64
		var groupPromptID *string
		for _, f := range files {
			name := f.Name()
			if f.IsDir() || !strings.HasSuffix(name, ".jsonl") || !strings.HasPrefix(name, "agent-") {
				continue
			}
			agentID := name[len("agent-") : len(name)-len(".jsonl")]
			if agentID == "" {
				continue
			}
			agentType := readAgentType(filepath.Join(wfDir, "agent-"+agentID+".meta.json"))
			child := nodeFromScan(agentID, agentType, scanAgentJSONL(filepath.Join(wfDir, name)))
			children = append(children, child)

			if child.StartTS != nil && (startTS == nil || *child.StartTS < *startTS) {
				v := *child.StartTS
				startTS = &v
			}
			if child.EndTS != nil && (endTS == nil || *child.EndTS > *endTS) {
				v := *child.EndTS
				endTS = &v
			}
			if groupPromptID == nil && child.PromptID != nil {
				v := *child.PromptID
				groupPromptID = &v
			}
		}
		if len(children) == 0 {
			continue
		}
		total += len(children)

		var duration *int64
		if startTS != nil && endTS != nil {
			d := *endTS - *startTS
			duration = &d
		}
		groups = append(groups, &protocol.AgentNode{
			AgentID:       d.Name(),
			AgentType:     "workflow",
			Description:   d.Name(),
			PromptID:      groupPromptID,
			ParentAgentID: nil,
			StartTS:       startTS,
			EndTS:         endTS,
			DurationMS:    duration,
			ToolCalls:     []protocol.AgentToolCall{},
			OutputPreview: "",
			Children:      children,
		})
	}
	return total, groups
}

// nodeFromScan builds a leaf AgentNode from a single agent's transcript scan.
func nodeFromScan(agentID, agentType string, scan agentScan) *protocol.AgentNode {
	var duration *int64
	if scan.startTS != nil && scan.endTS != nil {
		d := *scan.endTS - *scan.startTS
		duration = &d
	}
	var firstPID *string
	if scan.firstPromptID != "" {
		fp := scan.firstPromptID
		firstPID = &fp
	}
	return &protocol.AgentNode{
		AgentID:       agentID,
		AgentType:     agentType,
		Description:   scan.description,
		PromptID:      firstPID,
		ParentAgentID: nil,
		StartTS:       scan.startTS,
		EndTS:         scan.endTS,
		DurationMS:    duration,
		ToolCalls:     scan.toolCalls,
		OutputPreview: scan.outputPreview,
		Children:      []*protocol.AgentNode{},
	}
}

// scanMainJSONL collects every user promptId in the main transcript and the
// promptId of the most recent non-sidechain user turn.
func scanMainJSONL(path string) (mainPromptIDs map[string]bool, latestPromptID string) {
	mainPromptIDs = map[string]bool{}
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	var latestTS int64
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var r treeRow
		if json.Unmarshal(line, &r) != nil {
			continue
		}
		if r.Type != "user" {
			continue
		}
		if r.PromptID != "" {
			mainPromptIDs[r.PromptID] = true
			if !r.IsSidechain {
				ts := parseISOms(r.Timestamp)
				if ts >= latestTS {
					latestTS = ts
					latestPromptID = r.PromptID
				}
			}
		}
	}
	return
}

type agentScan struct {
	firstPromptID string
	promptIDs     []string
	startTS       *int64
	endTS         *int64
	description   string
	toolCalls     []protocol.AgentToolCall
	outputPreview string
}

// scanAgentJSONL reads one subagent transcript in a single pass.
func scanAgentJSONL(path string) agentScan {
	s := agentScan{toolCalls: []protocol.AgentToolCall{}}
	f, err := os.Open(path)
	if err != nil {
		return s
	}
	defer f.Close()

	firstUserFound := false
	var lastAssistantContent json.RawMessage
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var r treeRow
		if json.Unmarshal(line, &r) != nil {
			continue
		}

		if ts := parseISOms(r.Timestamp); ts != 0 {
			if s.startTS == nil {
				v := ts
				s.startTS = &v
			}
			v := ts
			s.endTS = &v
		}

		switch r.Type {
		case "user":
			if r.PromptID != "" {
				s.promptIDs = append(s.promptIDs, r.PromptID)
			}
			if !firstUserFound {
				firstUserFound = true
				s.firstPromptID = r.PromptID
				s.description = truncRunes(firstText(r.Message.Content), 150)
			}
		case "assistant":
			lastAssistantContent = r.Message.Content
			if len(s.toolCalls) < 50 {
				var recTS *int64
				if ts := parseISOms(r.Timestamp); ts != 0 {
					v := ts
					recTS = &v
				}
				for _, b := range parseBlocks(r.Message.Content) {
					if b.Type == "tool_use" && len(s.toolCalls) < 50 {
						s.toolCalls = append(s.toolCalls, protocol.AgentToolCall{Name: b.Name, TS: recTS})
					}
				}
			}
		}
	}
	if lastAssistantContent != nil {
		s.outputPreview = truncRunes(lastText(lastAssistantContent), 200)
	}
	return s
}

// treeRow is the subset of a transcript row read for tree building.
type treeRow struct {
	Type        string `json:"type"`
	PromptID    string `json:"promptId"`
	IsSidechain bool   `json:"isSidechain"`
	Timestamp   string `json:"timestamp"`
	Message     struct {
		Content json.RawMessage `json:"content"`
	} `json:"message"`
}

// readAgentType pulls agentType from agent-<id>.meta.json (default "unknown").
func readAgentType(metaPath string) string {
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return "unknown"
	}
	var meta struct {
		AgentType string `json:"agentType"`
	}
	if json.Unmarshal(data, &meta) != nil || meta.AgentType == "" {
		return "unknown"
	}
	return meta.AgentType
}

// firstText returns the first text block (or string content) of a message.
func firstText(content json.RawMessage) string {
	var str string
	if json.Unmarshal(content, &str) == nil {
		return str
	}
	for _, b := range parseBlocks(content) {
		if b.Type == "text" {
			return b.Text
		}
	}
	return ""
}

// lastText returns the last text block (or string content) of a message.
func lastText(content json.RawMessage) string {
	var str string
	if json.Unmarshal(content, &str) == nil {
		return str
	}
	last := ""
	for _, b := range parseBlocks(content) {
		if b.Type == "text" {
			last = b.Text
		}
	}
	return last
}

// truncRunes truncates to n runes (Python's text[:n] semantics, CJK-safe).
func truncRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
