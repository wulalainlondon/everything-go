package goexec

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/history"
	"everything-go/internal/session"
)

type captureWriter struct{ bytes.Buffer }

func TestCodexInvalidateLiveThreadsClearsAllSessionRoutes(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	reg := session.NewRegistry()
	s1 := reg.Create("s1", "one", "/tmp", "codex", "", "", "")
	s2 := reg.Create("s2", "two", "/tmp", "codex", "", "", "")
	st1 := c.state(s1.ID)
	st2 := c.state(s2.ID)
	st1.threadID = "thread-1"
	st1.currentTurnID = "turn-1"
	st2.threadID = "thread-2"
	st2.currentTurnID = "turn-2"
	c.threadToSession["thread-1"] = s1
	c.threadToSession["thread-2"] = s2

	c.invalidateLiveThreads()

	if len(c.threadToSession) != 0 {
		t.Fatalf("threadToSession not cleared: %+v", c.threadToSession)
	}
	if st1.threadID != "" || st1.currentTurnID != "" {
		t.Fatalf("state1 not invalidated: %+v", st1)
	}
	if st2.threadID != "" || st2.currentTurnID != "" {
		t.Fatalf("state2 not invalidated: %+v", st2)
	}
}

func TestCodexDetectsStaleThreadErrors(t *testing.T) {
	cases := []string{
		"Unknown session: abc",
		"{'message':'thread not found: old-thread'}",
	}
	for _, msg := range cases {
		if !isStaleThreadError(errors.New(msg)) {
			t.Fatalf("expected stale thread error for %q", msg)
		}
	}
	if isStaleThreadError(errors.New("permission denied")) {
		t.Fatal("unrelated errors must not be treated as stale thread")
	}
}

func TestCodexInputIncludesFilesAndImages(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	reg := session.NewRegistry()
	cwd := t.TempDir()
	s := reg.Create("s1", "codex", cwd, "codex", "", "", "")
	st := c.state(s.ID)
	png := base64.StdEncoding.EncodeToString([]byte("fake-png"))

	input := c.codexInput(
		s,
		"r1",
		"hello",
		[]backend.ImageAttachment{{Data: png, MediaType: "image/png"}},
		[]backend.FileAttachment{{Name: "a.txt", Content: "file body"}},
		st,
	)

	if len(input) != 2 {
		t.Fatalf("want text + image input, got %+v", input)
	}
	text, _ := input[0]["text"].(string)
	if !strings.Contains(text, "hello") || !strings.Contains(text, "[File: a.txt]\nfile body") {
		t.Fatalf("file content not folded into text: %q", text)
	}
	if input[1]["type"] != "localImage" {
		t.Fatalf("image input should be localImage: %+v", input[1])
	}
	path, _ := input[1]["path"].(string)
	if filepath.Ext(path) != ".png" {
		t.Fatalf("image path extension = %q, want .png", filepath.Ext(path))
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("image temp file missing: %v", err)
	}

	c.cleanupTempImages(st)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("image temp file should be removed, stat err=%v", err)
	}
}

func TestCodexRequestUserInputWaitsForFrontendResponse(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "thread-1"
	c.threadToSession["thread-1"] = s
	w := &captureWriter{}
	c.rpc.setWriter(w)

	raw := json.RawMessage(`{
		"threadId":"thread-1",
		"itemId":"ask_1",
		"questions":[{
			"id":"choice",
			"question":"Pick one",
			"options":[{"id":"yes","label":"Yes"}]
		}]
	}`)
	c.handleServerRequest(77, "item/tool/requestUserInput", raw)
	if w.Len() != 0 {
		t.Fatalf("requestUserInput should not write a JSON-RPC response before frontend answer: %s", w.String())
	}
	pending := c.PendingInteractions("s1")
	if len(pending) != 1 || pending[0].ToolUseID != "ask_1" || pending[0].Questions[0].QuestionID != "choice" {
		t.Fatalf("bad pending interaction: %+v", pending)
	}
	if !c.RespondUserInput("ask_1", map[string]any{"choice": "yes"}, false) {
		t.Fatal("RespondUserInput should match by tool_use_id")
	}

	var reply struct {
		ID     int `json:"id"`
		Result struct {
			Answers   map[string]any `json:"answers"`
			Cancelled bool           `json:"cancelled"`
		} `json:"result"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(w.Bytes()), &reply); err != nil {
		t.Fatalf("bad JSON-RPC reply: %v raw=%s", err, w.String())
	}
	if reply.ID != 77 || reply.Result.Answers["choice"] != "yes" || reply.Result.Cancelled {
		t.Fatalf("bad JSON-RPC response: %+v", reply)
	}
	if got := c.PendingInteractions("s1"); len(got) != 0 {
		t.Fatalf("pending interactions should be empty after response: %+v", got)
	}
}

func TestCodexExtractedAskUserQuestionEmitsFallbackInteraction(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)
	st.accumulatedText = "Need input:\n```json\n{\"type\":\"ask_user_question\",\"header\":\"Confirm\",\"questions\":[{\"id\":\"go\",\"question\":\"Proceed?\",\"options\":[{\"id\":\"yes\",\"label\":\"Yes\"}]}]}\n```"
	w := &captureWriter{}
	c.rpc.setWriter(w)

	c.emitExtractedAskUserQuestion(s, st)

	pending := c.PendingInteractions("s1")
	if len(pending) != 1 || pending[0].Header != "Confirm" || pending[0].Questions[0].QuestionID != "go" {
		t.Fatalf("bad extracted AskUserQuestion interaction: %+v", pending)
	}
	if !c.RespondUserInput(pending[0].RequestID, map[string]any{"go": "yes"}, false) {
		t.Fatal("fallback interaction should resolve by request_id")
	}
	if w.Len() != 0 {
		t.Fatalf("fallback AskUserQuestion must not write JSON-RPC response: %s", w.String())
	}
	var sawRequest, sawResolved bool
	for _, ev := range sink.events {
		switch ev.(type) {
		case backend.UserInputRequest:
			sawRequest = true
		case backend.InteractionResolved:
			sawResolved = true
		}
	}
	if !sawRequest || !sawResolved {
		t.Fatalf("missing interaction events request=%v resolved=%v events=%+v", sawRequest, sawResolved, sink.events)
	}
}

func TestCodexApprovalEventIncludesEnvironmentID(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "thread-1"
	st.reqID = "r1"
	c.threadToSession["thread-1"] = s
	w := &captureWriter{}
	c.rpc.setWriter(w)

	c.handleServerRequest(42, "item/permissions/requestApproval", json.RawMessage(`{
		"threadId":"thread-1",
		"environmentId":"env_abc",
		"permission":{"name":"network"}
	}`))

	var sawEnv bool
	for _, ev := range sink.events {
		if tr, ok := ev.(backend.ToolResult); ok && strings.Contains(tr.Output, "env_abc") {
			sawEnv = true
		}
	}
	if !sawEnv {
		t.Fatalf("approval environment id not emitted in tool result: %+v", sink.events)
	}
	var reply struct {
		ID     int `json:"id"`
		Result struct {
			Decision string `json:"decision"`
		} `json:"result"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(w.Bytes()), &reply); err != nil {
		t.Fatalf("bad approval reply: %v raw=%s", err, w.String())
	}
	if reply.ID != 42 || reply.Result.Decision != "accept" {
		t.Fatalf("bad approval response: %+v", reply)
	}
}

func TestCodexHostedToolCallEmitsTraceAndSafeError(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "thread-1"
	st.reqID = "r1"
	c.threadToSession["thread-1"] = s
	w := &captureWriter{}
	c.rpc.setWriter(w)

	c.handleServerRequest(99, "item/tool/call", json.RawMessage(`{
		"threadId":"thread-1",
		"tool":{"name":"web_search"},
		"input":{"q":"codex"}
	}`))

	var sawStart bool
	for _, ev := range sink.events {
		if ts, ok := ev.(backend.ToolStart); ok && ts.Name == "web_search" {
			sawStart = true
		}
	}
	if !sawStart {
		t.Fatalf("hosted tool call did not emit tool_start: %+v", sink.events)
	}
	var reply struct {
		ID    int `json:"id"`
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(w.Bytes()), &reply); err != nil {
		t.Fatalf("bad hosted tool reply: %v raw=%s", err, w.String())
	}
	if reply.ID != 99 || reply.Error.Code != -32000 || !strings.Contains(reply.Error.Message, "web_search") {
		t.Fatalf("bad hosted tool error response: %+v", reply)
	}
}

func TestCodexTokenUsageUpdatesAutoCompactThreshold(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "thread-1"
	c.threadToSession["thread-1"] = s

	c.dispatch(json.RawMessage(`{
		"method":"thread/tokenUsage/updated",
		"params":{
			"threadId":"thread-1",
			"tokenUsage":{
				"last":{"totalTokens":800},
				"modelContextWindow":1000
			}
		}
	}`))

	st.mu.Lock()
	used, maxCtx := st.contextUsed, st.contextMax
	st.mu.Unlock()
	if used != 800 || maxCtx != 1000 {
		t.Fatalf("token usage not recorded: used=%d max=%d", used, maxCtx)
	}
	if !c.shouldAutoCompact(st) {
		t.Fatal("expected auto compact at threshold")
	}
}

func TestCodexCompactNotificationFinishesCompact(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "thread-1"
	st.compactActive = true
	st.compactDone = make(chan struct{})
	done := st.compactDone
	c.threadToSession["thread-1"] = s

	c.dispatch(json.RawMessage(`{
		"method":"thread/compacted",
		"params":{"threadId":"thread-1"}
	}`))

	select {
	case <-done:
	default:
		t.Fatal("compact notification did not close compactDone")
	}
	st.mu.Lock()
	active := st.compactActive
	st.mu.Unlock()
	if active {
		t.Fatal("compactActive should be false after thread/compacted")
	}
}

func TestCodexCompactCommandFailureEmitsSessionCommandFailed(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)

	c.runCompactCommand(s, st, "compact_s1")

	if sink.count(func(e any) bool {
		ev, ok := e.(backend.SessionCommandFailed)
		return ok && ev.RequestID == "compact_s1" && strings.Contains(ev.Message, "no codex thread")
	}) != 1 {
		t.Fatalf("compact failed command event missing: %+v", sink.events)
	}
}

func TestCodexGzipRolloutRegistersAndLoadsHistory(t *testing.T) {
	uid := "123e4567-e89b-12d3-a456-426614174000"
	root := t.TempDir()
	day := filepath.Join(root, "2026", "06", "04")
	if err := os.MkdirAll(day, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(day, "rollout-2026-06-04T01-27-37-"+uid+".jsonl.gz")
	records := []map[string]any{
		{"timestamp": "2026-06-04T01:27:37.210Z", "type": "session_meta", "payload": map[string]any{"id": uid, "cwd": "/tmp/codex137"}},
		{"timestamp": "2026-06-04T01:27:38.000Z", "type": "response_item", "payload": map[string]any{
			"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "檢查壓縮 rollout"}},
		}},
		{"timestamp": "2026-06-04T01:27:39.000Z", "type": "response_item", "payload": map[string]any{
			"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "可以讀取"}},
		}},
	}
	writeGzipJSONL(t, path, records)

	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	sessions, err := c.ResumableSessions(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].ClaudeUUID != uid || sessions[0].Name != "檢查壓縮 rollout" || sessions[0].Cwd != "/tmp/codex137" {
		t.Fatalf("bad resumable sessions: %+v", sessions)
	}
	hist, err := c.LoadHistory(uid, history.Opts{Limit: 10, Mode: "snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hist.Messages) != 2 || hist.Messages[0]["content"] != "檢查壓縮 rollout" || hist.Messages[1]["content"] != "可以讀取" {
		t.Fatalf("bad codex history: %+v", hist.Messages)
	}
}

func TestCodexHistoryStripsTurnAbortedNotice(t *testing.T) {
	uid := "123e4567-e89b-12d3-a456-426614174001"
	root := t.TempDir()
	day := filepath.Join(root, "2026", "05", "18")
	if err := os.MkdirAll(day, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(day, "rollout-2026-05-18T10-52-00-"+uid+".jsonl")
	records := []map[string]any{
		{"timestamp": "2026-05-18T02:52:00.000Z", "type": "session_meta", "payload": map[string]any{"id": uid, "cwd": "/tmp/project"}},
		{"timestamp": "2026-05-18T02:52:01.000Z", "type": "response_item", "payload": map[string]any{
			"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "打包taildrop給我\n<turn_aborted>\nThe user interrupted.\n</turn_aborted>\n我什麼都沒做"}},
		}},
	}
	writeJSONL(t, path, records)

	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	hist, err := c.LoadHistory(uid, history.Opts{Limit: 10, Mode: "snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hist.Messages) != 1 {
		t.Fatalf("want one history message, got %+v", hist.Messages)
	}
	content, _ := hist.Messages[0]["content"].(string)
	if !strings.Contains(content, "打包taildrop給我") || !strings.Contains(content, "我什麼都沒做") || strings.Contains(content, "turn_aborted") {
		t.Fatalf("turn_aborted wrapper not stripped: %q", content)
	}
}

func writeJSONL(t *testing.T, path string, records []map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	for _, rec := range records {
		raw, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(raw)
		buf.WriteByte('\n')
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeGzipJSONL(t *testing.T, path string, records []map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	for _, rec := range records {
		raw, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = gz.Write(raw)
		_, _ = gz.Write([]byte("\n"))
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}
