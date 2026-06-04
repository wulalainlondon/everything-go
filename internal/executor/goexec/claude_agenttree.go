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

	entries, err := os.ReadDir(subagentDir)
	if err != nil {
		return 0, empty
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
		agentPath := filepath.Join(subagentDir, name)

		agentType := readAgentType(filepath.Join(subagentDir, "agent-"+agentID+".meta.json"))
		scan := scanAgentJSONL(agentPath)

		for _, pid := range scan.promptIDs {
			subagentPromptIDs[pid] = agentID
		}

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

		agents[agentID] = &protocol.AgentNode{
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
		order = append(order, agentID)
	}

	if len(agents) == 0 {
		return 0, empty
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

	// Filter to the latest conversation turn only (parity with Python).
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
	return len(agents), tree
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
