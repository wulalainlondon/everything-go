package goexec

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type captureWriter struct{ bytes.Buffer }

type rpcCaptureWriter struct{ writes chan []byte }

func (w *rpcCaptureWriter) Write(p []byte) (int, error) {
	copyOfP := append([]byte(nil), p...)
	w.writes <- copyOfP
	return len(p), nil
}

func TestCodexMultiAgentLifecycleBuildsLiveTreeWithoutFinishingRootTurn(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "gpt-5.6-sol", "", "")
	st := c.state(s.ID)
	st.threadID = "root"
	st.turnActive = true
	st.turnDone = make(chan struct{})
	c.threadToSession["root"] = s
	s.SetResumeID("root")
	c.dispatch(json.RawMessage(`{"method":"item/started","params":{"threadId":"root","turnId":"t1","item":{"type":"collabAgentToolCall","id":"i1","tool":"spawnAgent","status":"inProgress","senderThreadId":"root","receiverThreadIds":["child"],"prompt":"inspect tests","model":"gpt-5.6-sol","agentsStates":{"child":{"status":"running","message":null}}}}}`))
	c.dispatch(json.RawMessage(`{"method":"thread/started","params":{"thread":{"id":"child","parentThreadId":"root","agentNickname":"Scout","agentRole":"explorer"}}}`))
	c.dispatch(json.RawMessage(`{"method":"item/agentMessage/delta","params":{"threadId":"child","turnId":"ct","itemId":"m","delta":"found it","phase":"final"}}`))
	c.dispatch(json.RawMessage(`{"method":"turn/completed","params":{"threadId":"child","turn":{"id":"ct","status":"completed"}}}`))
	st.mu.Lock()
	active := st.turnActive
	st.mu.Unlock()
	if !active {
		t.Fatal("child completion finished root turn")
	}
	total, tree := c.BuildAgentTree("root")
	if total != 1 || len(tree) != 1 || tree[0].AgentID != "child" || tree[0].EndTS == nil || !strings.Contains(tree[0].OutputPreview, "found it") {
		t.Fatalf("bad tree total=%d tree=%+v", total, tree)
	}
	if sink.count(func(e any) bool { _, ok := e.(protocol.TextChunk); return ok }) != 0 {
		t.Fatal("child output leaked into root assistant text")
	}
}

func TestCodexRootTurnCompletionFinishesTheOwningSession(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "gpt-5.6-sol", "", "")
	st := c.state(s.ID)
	st.threadID = "root"
	st.turnActive = true
	st.turnDone = make(chan struct{})
	c.threadToSession["root"] = s

	c.dispatch(json.RawMessage(`{"method":"turn/completed","params":{"threadId":"root","turn":{"id":"t1","status":"completed"}}}`))

	select {
	case <-st.turnDone:
	case <-time.After(time.Second):
		t.Fatal("root turn/completed did not release the owning session")
	}
	st.mu.Lock()
	active, turnErr := st.turnActive, st.turnErr
	st.mu.Unlock()
	if active || turnErr != "" {
		t.Fatalf("root completion left stale state active=%v err=%q", active, turnErr)
	}
}

func TestCodexCompletedImageToolEmitsMediaWithoutBase64(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	reg := session.NewRegistry()
	dir := filepath.Join(t.TempDir(), ".codex", "generated_images", "thread")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	imagePath := filepath.Join(dir, "generated.png")
	if err := os.WriteFile(imagePath, []byte("png"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "root"
	st.reqID = "r1"
	c.threadToSession["root"] = s

	params, err := json.Marshal(map[string]any{
		"threadId": "root",
		"itemId":   "image-tool",
		"item":     map[string]any{"id": "image-tool", "type": "dynamicToolCall"},
		"output": []map[string]any{
			{"type": "input_image", "image_url": "data:image/png;base64,VERY_LARGE_BASE64"},
			{"type": "input_text", "text": "Generated images are saved as " + imagePath},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	message, err := json.Marshal(map[string]any{"method": "item/completed", "params": json.RawMessage(params)})
	if err != nil {
		t.Fatal(err)
	}
	c.dispatch(message)

	var media protocol.Media
	foundMedia := false
	var result protocol.ToolResult
	for _, event := range sink.events {
		switch e := event.(type) {
		case protocol.Media:
			media, foundMedia = e, true
		case protocol.ToolResult:
			result = e
		}
	}
	if !foundMedia || media.Path != imagePath || media.SessionID != "s1" || media.RequestID != "r1" {
		t.Fatalf("bad generated media event: %+v", media)
	}
	if strings.Contains(result.Output, "VERY_LARGE_BASE64") || !strings.Contains(result.Output, imagePath) {
		t.Fatalf("tool output was not compacted: %q", result.Output)
	}
}

func TestCodexMcpAndDynamicToolElicitationsReturnProtocolResponses(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "root"
	st.reqID = "r1"
	c.threadToSession["root"] = s
	w := &captureWriter{}
	c.rpc.setWriter(w)
	c.handleServerRequest(7, "mcpServer/elicitation/request", json.RawMessage(`{"threadId":"root","turnId":"t","serverName":"github","mode":"url","message":"Authorize","url":"https://example.test/auth","elicitationId":"e1"}`))
	if got := c.PendingInteractions("s1"); len(got) != 1 || got[0].Kind != "mcp_url" || !strings.Contains(got[0].Questions[0].Text, "https://example.test/auth") {
		t.Fatalf("bad MCP interaction: %+v", got)
	}
	if !c.RespondUserInput("e1", map[string]any{"action": "accept"}, false) {
		t.Fatal("MCP response not matched")
	}
	var mcpReply map[string]any
	if json.Unmarshal(bytes.TrimSpace(w.Bytes()), &mcpReply) != nil || mcpReply["id"].(float64) != 7 {
		t.Fatalf("bad MCP reply %s", w.String())
	}
	w.Reset()
	c.handleServerRequest(8, "item/tool/call", json.RawMessage(`{"threadId":"root","turnId":"t","callId":"call1","namespace":"custom","tool":"lookup","arguments":{"q":"x"}}`))
	if !c.RespondUserInput("call1", map[string]any{"result": "ok"}, false) {
		t.Fatal("dynamic tool response not matched")
	}
	var toolReply struct {
		ID     int `json:"id"`
		Result struct {
			Success bool `json:"success"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"contentItems"`
		} `json:"result"`
	}
	if json.Unmarshal(bytes.TrimSpace(w.Bytes()), &toolReply) != nil || toolReply.ID != 8 || !toolReply.Result.Success || len(toolReply.Result.Content) != 1 || toolReply.Result.Content[0].Text != "ok" {
		t.Fatalf("bad tool reply %s", w.String())
	}
}

func TestCodexLiveDiffNotificationIsForwarded(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "root"
	st.reqID = "r1"
	c.threadToSession["root"] = s
	c.dispatch(json.RawMessage(`{"method":"turn/diff/updated","params":{"threadId":"root","turnId":"t","diff":"--- a/x\n+++ b/x"}}`))
	if sink.count(func(e any) bool {
		d, ok := e.(protocol.CodexLiveDiff)
		return ok && strings.Contains(d.Diff, "+++ b/x")
	}) != 1 {
		t.Fatalf("missing live diff: %+v", sink.events)
	}
}

func TestCodexPostTurnGoalReconcileEmitsLatestState(t *testing.T) {
	tests := []struct {
		name       string
		result     string
		wantUpdate bool
	}{
		{
			name:       "complete",
			result:     `{"goal":{"threadId":"thread-1","objective":"ship","status":"complete","tokenBudget":null,"tokensUsed":42,"timeUsedSeconds":9,"createdAt":1,"updatedAt":2}}`,
			wantUpdate: true,
		},
		{name: "cleared", result: `{"goal":null}`, wantUpdate: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sink := &capSink{}
			c := NewCodex(sink, "codex")
			reg := session.NewRegistry()
			s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
			st := c.state(s.ID)
			st.threadID = "thread-1"
			writer := &rpcCaptureWriter{writes: make(chan []byte, 1)}
			c.rpc.setWriter(writer)

			done := make(chan struct{})
			go func() {
				c.reconcileGoalAfterTurn(s, st)
				close(done)
			}()

			var req struct {
				ID     int            `json:"id"`
				Method string         `json:"method"`
				Params map[string]any `json:"params"`
			}
			select {
			case raw := <-writer.writes:
				if err := json.Unmarshal(raw, &req); err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("goal reconcile request not written")
			}
			if req.Method != "thread/goal/get" || req.Params["threadId"] != "thread-1" {
				t.Fatalf("bad goal reconcile request: %+v", req)
			}
			c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":%s}`, req.ID, tc.result)))

			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("goal reconcile did not finish")
			}

			if tc.wantUpdate {
				if sink.count(func(e any) bool {
					goal, ok := e.(backend.GoalUpdate)
					return ok && goal.Goal.Status == "complete" && goal.Goal.TokensUsed == 42
				}) != 1 {
					t.Fatalf("complete goal update missing: %+v", sink.events)
				}
			} else if sink.count(func(e any) bool {
				_, ok := e.(backend.GoalCleared)
				return ok
			}) != 1 {
				t.Fatalf("goal cleared event missing: %+v", sink.events)
			}
		})
	}
}

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

func TestCodexTurnLivenessPolicy(t *testing.T) {
	now := time.Unix(10_000, 0)
	warnAfter := 5 * time.Minute
	abortAfter := 30 * time.Minute
	tests := []struct {
		name            string
		last            time.Time
		warned          bool
		waitingForInput bool
		want            codexLivenessAction
	}{
		{name: "recent activity", last: now.Add(-time.Minute), want: codexLivenessNone},
		{name: "warn once", last: now.Add(-6 * time.Minute), want: codexLivenessWarn},
		{name: "do not repeat warning", last: now.Add(-6 * time.Minute), warned: true, want: codexLivenessNone},
		{name: "abort", last: now.Add(-31 * time.Minute), warned: true, want: codexLivenessAbort},
		{name: "user input may wait indefinitely", last: now.Add(-time.Hour), waitingForInput: true, want: codexLivenessNone},
		{name: "uninitialized", want: codexLivenessNone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := codexLivenessActionAt(now, tc.last, tc.warned, tc.waitingForInput, warnAfter, abortAfter)
			if got != tc.want {
				t.Fatalf("action=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestCodexTurnDeadlineWaitsIndefinitelyForUserInput(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.turnTimeout = 20 * time.Millisecond
	c.interactions["ui_1"] = codexInteraction{payload: backend.UserInputPayload{
		RequestID: "ui_1",
		SessionID: "s1",
		Status:    "pending",
	}}

	// Model the runTurn deadline branch: the timer has fired, but a pending
	// interaction must re-arm it instead of allowing the turn to be aborted.
	timer := time.NewTimer(time.Millisecond)
	<-timer.C
	if !c.deferTurnDeadlineForInput("s1", timer) {
		t.Fatal("pending user input should defer the turn deadline")
	}
	select {
	case <-timer.C:
		// Re-arming is the expected behavior. Repeating this branch for as long
		// as the interaction stays pending makes the wait unbounded.
	case <-time.After(time.Second):
		t.Fatal("deferred turn deadline was not re-armed")
	}
	if !c.deferTurnDeadlineForInput("s1", timer) {
		t.Fatal("unanswered input should continue deferring every deadline")
	}

	c.interMu.Lock()
	delete(c.interactions, "ui_1")
	c.interMu.Unlock()
	if c.deferTurnDeadlineForInput("s1", timer) {
		t.Fatal("resolved input must restore the ordinary turn deadline")
	}
	timer.Stop()
}

func TestCodexStateActivityResetsStallWarning(t *testing.T) {
	st := newCodexState()
	st.stallWarned = true
	now := time.Unix(123, 0)
	st.touch(now)
	if !st.lastEventAt.Equal(now) || st.stallWarned {
		t.Fatalf("touch did not reset liveness state: %+v", st)
	}
}

func TestCodexTurnParamsForwardsSupportedEffort(t *testing.T) {
	input := []map[string]any{{"type": "text", "text": "hello"}}
	for _, effort := range []string{"low", "medium", "high", "xhigh", "max", "ultra"} {
		params := codexTurnParams("thread-1", input, effort)
		if params["effort"] != effort {
			t.Fatalf("effort %q not forwarded: %+v", effort, params)
		}
	}
	for _, effort := range []string{"", "auto", "invalid"} {
		params := codexTurnParams("thread-1", input, effort)
		if _, ok := params["effort"]; ok {
			t.Fatalf("effort %q should use model default: %+v", effort, params)
		}
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

func TestCodexLiveToolStartNormalizesFunctionCall(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "thread-1"
	st.reqID = "r1"
	c.threadToSession["thread-1"] = s

	c.dispatch(json.RawMessage(`{
		"method":"item/started",
		"params":{
			"threadId":"thread-1",
			"item":{
				"id":"call_1",
				"type":"function_call",
				"name":"exec_command",
				"arguments":"{\"cmd\":\"pwd\",\"workdir\":\"/tmp\"}"
			}
		}
	}`))

	var got backend.ToolStart
	for _, ev := range sink.events {
		if ts, ok := ev.(backend.ToolStart); ok {
			got = ts
			break
		}
	}
	if got.ToolUseID != "call_1" || got.Name != "Bash" || got.Command != "pwd" {
		t.Fatalf("bad normalized tool_start: %+v", got)
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

func TestCodexResumableSessionsRejectsSubagentsAndKeepsUserForks(t *testing.T) {
	root := t.TempDir()
	day := filepath.Join(root, "2026", "07", "20")
	if err := os.MkdirAll(day, 0o755); err != nil {
		t.Fatal(err)
	}
	parentID := "019f541f-63b6-7e53-a4e2-dd36d875a7c2"
	subagentID := "019f7ddf-17c7-7a53-a5c5-e6e81bd52d7b"
	legacySubagentID := "019f7333-b3ed-7762-9776-c0948bf285bd"
	userForkID := "019f7ddf-318d-7360-ac6b-617bebd1c195"
	writeJSONL(t, filepath.Join(day, "rollout-2026-07-20T12-53-20-"+subagentID+".jsonl"), []map[string]any{
		{"type": "session_meta", "payload": map[string]any{
			"id": subagentID, "cwd": "/repo", "thread_source": "subagent", "parent_thread_id": parentID,
			"source": map[string]any{"subagent": map[string]any{"depth": 1}},
		}},
		{"type": "response_item", "payload": map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"text": "worker task"}}}},
	})
	writeJSONL(t, filepath.Join(day, "rollout-2026-07-20T12-53-40-"+legacySubagentID+".jsonl"), []map[string]any{
		{"type": "session_meta", "payload": map[string]any{
			"id": legacySubagentID, "cwd": "/repo", "source": map[string]any{"subagent": map[string]any{"depth": 1}},
		}},
		{"type": "response_item", "payload": map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"text": "legacy worker task"}}}},
	})
	writeJSONL(t, filepath.Join(day, "rollout-2026-07-20T12-54-00-"+userForkID+".jsonl"), []map[string]any{
		{"type": "session_meta", "payload": map[string]any{
			"id": userForkID, "cwd": "/repo", "forked_from_id": parentID, "thread_source": "user", "source": "exec",
		}},
		{"type": "response_item", "payload": map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"text": "user fork"}}}},
	})

	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	sessions, err := c.ResumableSessions(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].ClaudeUUID != userForkID || sessions[0].Name != "user fork" {
		t.Fatalf("want only user fork, got %+v", sessions)
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

func TestCodexHistoryReplaysToolBlocks(t *testing.T) {
	uid := "123e4567-e89b-12d3-a456-426614174002"
	root := t.TempDir()
	day := filepath.Join(root, "2026", "06", "06")
	if err := os.MkdirAll(day, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(day, "rollout-2026-06-06T00-00-00-"+uid+".jsonl")
	records := []map[string]any{
		{"timestamp": "2026-06-06T00:00:00.000Z", "type": "response_item", "payload": map[string]any{
			"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "run tools"}},
		}},
		{"timestamp": "2026-06-06T00:00:01.000Z", "type": "response_item", "payload": map[string]any{
			"type": "function_call", "name": "exec_command", "arguments": `{"cmd":"pwd"}`, "call_id": "call_1",
		}},
		{"timestamp": "2026-06-06T00:00:02.000Z", "type": "response_item", "payload": map[string]any{
			"type": "function_call_output", "call_id": "call_1", "output": "/tmp\n",
		}},
		{"timestamp": "2026-06-06T00:00:03.000Z", "type": "response_item", "payload": map[string]any{
			"type": "custom_tool_call", "name": "apply_patch", "input": "*** Begin Patch\n*** Add File: x.txt\n+hi\n*** End Patch\n", "call_id": "call_2",
		}},
		{"timestamp": "2026-06-06T00:00:04.000Z", "type": "response_item", "payload": map[string]any{
			"type": "custom_tool_call_output", "call_id": "call_2", "output": "Success\n",
		}},
	}
	writeJSONL(t, path, records)

	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	hist, err := c.LoadHistory(uid, history.Opts{Limit: 20, Mode: "snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hist.Messages) != 3 {
		t.Fatalf("want user + two tool messages, got %+v", hist.Messages)
	}
	bash := hist.Messages[1]["blocks"].([]map[string]any)[0]
	if bash["name"] != "Bash" || bash["command"] != "pwd" || bash["output"] != "/tmp\n" {
		t.Fatalf("bad bash block: %+v", bash)
	}
	patch := hist.Messages[2]["blocks"].([]map[string]any)[0]
	if patch["name"] != "ApplyPatch" || !strings.Contains(patch["command"].(string), "*** Add File: x.txt") || patch["output"] != "Success\n" {
		t.Fatalf("bad patch block: %+v", patch)
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
