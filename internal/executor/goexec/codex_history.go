package goexec

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/history"
)

func (c *Codex) LoadHistory(resumeID string, opts history.Opts) (*history.Result, error) {
	path := c.findCodexSessionFile(resumeID)
	if path == "" {
		return history.Slice(nil, opts), nil
	}
	if fi, err := os.Stat(path); err == nil && !fi.IsDir() {
		key := history.FileKey{Path: path, MtimeNS: fi.ModTime().UnixNano(), Size: fi.Size()}
		cacheName := "codex:" + resumeID
		if messages, ok := history.DefaultCache().Load(cacheName, key); ok {
			return history.Slice(messages, opts), nil
		}
		messages, err := c.readCodexHistory(path, resumeID)
		if err != nil {
			return history.Slice(nil, opts), nil
		}
		history.DefaultCache().SaveAsync(cacheName, key, messages)
		return history.Slice(messages, opts), nil
	}
	messages, err := c.readCodexHistory(path, resumeID)
	if err != nil {
		return history.Slice(nil, opts), nil
	}
	return history.Slice(messages, opts), nil
}

func (c *Codex) ResumableSessions(limit int) ([]history.ResumableSession, error) {
	limit = history.ClampLimit(limit)
	paths := c.codexRolloutFiles()
	sort.Slice(paths, func(i, j int) bool {
		return filepath.Base(paths[i]) > filepath.Base(paths[j])
	})
	out := make([]history.ResumableSession, 0, min(limit, len(paths)))
	seen := map[string]bool{}
	for _, path := range paths {
		if len(out) >= limit {
			break
		}
		uid := codexRolloutUID(filepath.Base(path))
		if uid == "" || seen[uid] {
			continue
		}
		seen[uid] = true
		cwd, name := c.codexCwdAndName(path, uid)
		lastUsed := codexTimestampFromFilename(filepath.Base(path))
		if lastUsed == 0 {
			if fi, err := os.Stat(path); err == nil {
				lastUsed = fi.ModTime().Unix()
			}
		}
		out = append(out, history.ResumableSession{
			ID: uid, Name: sanitizeCodexSessionName(name, uid[:min(8, len(uid))]),
			ClaudeUUID: uid, LastUsed: lastUsed, Cwd: cwd, Backend: backend.Codex,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastUsed > out[j].LastUsed })
	return out, nil
}

// TranscriptPath returns the on-disk .jsonl rollout file for a resume id, used
// by fork_session to copy a session's history. Satisfies the core's
// transcriptLocator capability. .gz files are excluded because fork.go copies
// raw bytes directly into a new .jsonl; decompressing is out of scope.
func (c *Codex) TranscriptPath(resumeID string) (string, bool) {
	if p := c.findCodexSessionFile(resumeID); p != "" && !strings.HasSuffix(p, ".gz") {
		return p, true
	}
	return "", false
}

func (c *Codex) findCodexSessionFile(resumeID string) string {
	if resumeID == "" {
		return ""
	}
	for _, path := range c.codexRolloutFiles() {
		if codexRolloutUID(filepath.Base(path)) == resumeID {
			return path
		}
	}
	return ""
}

func (c *Codex) codexRolloutFiles() []string {
	var out []string
	_ = filepath.WalkDir(c.sessionsRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, "rollout-") && (strings.HasSuffix(name, ".jsonl") || strings.HasSuffix(name, ".jsonl.gz")) {
			out = append(out, path)
		}
		return nil
	})
	return out
}

func (c *Codex) readCodexHistory(path, resumeID string) ([]map[string]any, error) {
	r, closeFn, err := openCodexRollout(path)
	if err != nil {
		return nil, err
	}
	defer closeFn()

	type rec struct {
		lineNo int
		row    codexHistoryRow
	}
	var records []rec
	toolOutputs := map[string]string{}
	var messages []map[string]any
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		var row codexHistoryRow
		if json.Unmarshal(sc.Bytes(), &row) != nil {
			continue
		}
		if row.Type != "response_item" {
			continue
		}
		records = append(records, rec{lineNo: lineNo, row: row})
		if id, out, ok := codexResponseToolOutput(row.Payload); ok {
			toolOutputs[id] = out
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}

	for _, rc := range records {
		if tool, ok := normalizeCodexResponseTool(rc.row.Payload, toolOutputs[codexPayloadCallID(rc.row.Payload)]); ok {
			messages = append(messages, history.CompleteMsg(
				"codex", resumeID, "codex:"+resumeID+":line:"+itoa(rc.lineNo),
				"assistant", firstNonEmpty(tool.Command, tool.Name), parseCodexISOms(rc.row.Timestamp), []map[string]any{tool.historyBlock()},
			))
			continue
		}
		payload := parseCodexHistoryPayload(rc.row.Payload)
		if payload.Type != "message" {
			continue
		}
		role := payload.Role
		if role != "user" && role != "assistant" {
			continue
		}
		if role == "assistant" && payload.Phase == "commentary" {
			continue
		}
		text := extractCodexText(payload.Content)
		if text == "" || (role == "user" && isCodexBootstrapText(text)) {
			continue
		}
		ts := parseCodexISOms(rc.row.Timestamp)
		messages = append(messages, history.CompleteMsg(
			"codex", resumeID, "codex:"+resumeID+":line:"+itoa(rc.lineNo),
			role, text, ts, []map[string]any{{"type": "text", "text": text}},
		))
	}
	return messages, nil
}

type codexHistoryRow struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

type codexHistoryPayload struct {
	Type    string          `json:"type"`
	Role    string          `json:"role"`
	Phase   string          `json:"phase"`
	Content json.RawMessage `json:"content"`
	Cwd     string          `json:"cwd"`
}

func parseCodexHistoryPayload(raw json.RawMessage) codexHistoryPayload {
	var p codexHistoryPayload
	_ = json.Unmarshal(raw, &p)
	return p
}

func codexPayloadCallID(raw json.RawMessage) string {
	var p struct {
		CallID  string `json:"call_id"`
		CallID2 string `json:"callId"`
		ID      string `json:"id"`
	}
	_ = json.Unmarshal(raw, &p)
	return firstNonEmpty(p.CallID, p.CallID2, p.ID)
}

func (c *Codex) codexCwdAndName(path, uid string) (string, string) {
	cwd := "~"
	name := uid[:min(8, len(uid))]
	r, closeFn, err := openCodexRollout(path)
	if err != nil {
		return cwd, name
	}
	defer closeFn()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	for sc.Scan() {
		var row codexHistoryRow
		if json.Unmarshal(sc.Bytes(), &row) != nil {
			continue
		}
		payload := parseCodexHistoryPayload(row.Payload)
		switch {
		case row.Type == "session_meta" && payload.Cwd != "":
			cwd = payload.Cwd
		case row.Type == "response_item" && payload.Role == "user":
			text := extractCodexText(payload.Content)
			if text != "" && !isCodexBootstrapText(text) {
				name = text
				return cwd, name
			}
		}
	}
	return cwd, name
}

func openCodexRollout(path string) (io.Reader, func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, func() {}, err
	}
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, func() {}, err
		}
		return gz, func() { _ = gz.Close(); _ = f.Close() }, nil
	}
	return f, func() { _ = f.Close() }, nil
}

func codexRolloutUID(name string) string {
	name = strings.TrimSuffix(strings.TrimSuffix(name, ".gz"), ".jsonl")
	if len(name) < 36 {
		return ""
	}
	return name[len(name)-36:]
}

func codexTimestampFromFilename(name string) int64 {
	if len(name) < len("rollout-2006-01-02T15-04-05") {
		return 0
	}
	raw := name[len("rollout-") : len("rollout-")+len("2006-01-02T15-04-05")]
	t, err := time.Parse("2006-01-02T15-04-05", raw)
	if err != nil {
		return 0
	}
	return t.UTC().Unix()
}

func parseCodexISOms(value string) int64 {
	if value == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339Nano, strings.ReplaceAll(value, "Z", "+00:00"))
	if err != nil {
		return 0
	}
	return t.UnixMilli()
}

func extractCodexText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return cleanCodexText(s)
	}
	var items []struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &items) == nil {
		var parts []string
		for _, it := range items {
			if strings.TrimSpace(it.Text) != "" {
				parts = append(parts, it.Text)
			}
		}
		return cleanCodexText(strings.Join(parts, "\n"))
	}
	return ""
}

func cleanCodexText(text string) string {
	for {
		start := strings.Index(strings.ToLower(text), "<turn_aborted>")
		if start < 0 {
			break
		}
		end := strings.Index(strings.ToLower(text[start:]), "</turn_aborted>")
		if end < 0 {
			break
		}
		end += start + len("</turn_aborted>")
		text = text[:start] + text[end:]
	}
	return strings.TrimSpace(text)
}

func isCodexBootstrapText(text string) bool {
	stripped := strings.TrimLeft(text, " \t\r\n")
	return (strings.HasPrefix(stripped, "<environment_context>") && strings.Contains(stripped, "</environment_context>") && strings.Contains(stripped, "<cwd>")) ||
		(strings.HasPrefix(stripped, "# AGENTS.md instructions") && (strings.Contains(stripped, "<environment_context>") || strings.Contains(stripped, "<INSTRUCTIONS>")))
}

func sanitizeCodexSessionName(raw, fallback string) string {
	s := strings.Join(strings.Fields(raw), " ")
	s = strings.Trim(s, "`'\"[]{}()<>")
	if s == "" {
		return fallback
	}
	if len(s) > 80 {
		s = strings.TrimSpace(s[:80])
	}
	return s
}
