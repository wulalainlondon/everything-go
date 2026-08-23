package goexec

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/executor"
	"everything-go/internal/session"
)

const geminiTurnTimeout = 10 * time.Minute

type geminiState struct {
	mu           sync.Mutex
	proc         *exec.Cmd
	stdin        io.WriteCloser
	rpc          *rpcPlumber
	acpSessionID string
	requestID    string
}

// Gemini implements the Agent Client Protocol over `gemini --acp` stdio.
// Each bridge session owns one ACP process and one Gemini session identity.
type Gemini struct {
	sink   executor.Sink
	bin    string
	mu     sync.Mutex
	states map[string]*geminiState
}

func NewGemini(sink executor.Sink, bin string) *Gemini {
	return &Gemini{sink: sink, bin: bin, states: make(map[string]*geminiState)}
}

func (g *Gemini) state(sessionID string) *geminiState {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.states[sessionID] == nil {
		g.states[sessionID] = &geminiState{rpc: newJSONRPCPlumber("gemini")}
	}
	return g.states[sessionID]
}

func (g *Gemini) Send(ctx context.Context, s *session.Session, reqID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) error {
	st := g.state(s.ID)
	if err := g.ensureStarted(ctx, s, st); err != nil {
		return fmt.Errorf("failed to start gemini: %w", err)
	}
	blocks := make([]map[string]any, 0, 1+len(images))
	for _, file := range files {
		content += fmt.Sprintf("\n\n[File: %s]\n%s", file.Name, file.Content)
	}
	if content != "" {
		blocks = append(blocks, map[string]any{"type": "text", "text": content})
	}
	for _, image := range images {
		blocks = append(blocks, map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": image.MediaType, "data": image.Data}})
	}
	st.mu.Lock()
	st.requestID = reqID
	st.mu.Unlock()
	go g.runTurn(s, st, reqID, blocks)
	return nil
}

func (g *Gemini) ensureStarted(_ context.Context, s *session.Session, st *geminiState) error {
	st.mu.Lock()
	if st.proc != nil && st.proc.Process != nil && st.proc.ProcessState == nil {
		st.mu.Unlock()
		return nil
	}
	st.mu.Unlock()
	bin := strings.TrimSpace(g.bin)
	if bin == "" {
		var err error
		bin, err = exec.LookPath("gemini")
		if err != nil {
			return errors.New("gemini binary not found; install @google/gemini-cli")
		}
	}
	cwd := s.Snapshot().Cwd
	if info, err := os.Stat(cwd); err != nil || !info.IsDir() {
		cwd, _ = os.UserHomeDir()
	}
	cmd := exec.Command(bin, "--acp")
	cmd.Dir = cwd
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	st.mu.Lock()
	st.proc, st.stdin = cmd, stdin
	st.rpc = newJSONRPCPlumber("gemini")
	st.rpc.setWriter(stdin)
	st.mu.Unlock()
	go g.readLoop(s, st, stdout)
	result, err := st.rpc.request("initialize", map[string]any{
		"protocolVersion": 1,
		"clientInfo":      map[string]any{"name": "averything-bridge", "version": "2"},
		"clientCapabilities": map[string]any{
			"fs": map[string]bool{"readTextFile": true, "writeTextFile": true}, "terminal": true,
		},
	}, 15*time.Second)
	if err != nil {
		_ = cmd.Process.Kill()
		return err
	}
	_ = result
	if err := st.rpc.notify("initialized", nil); err != nil {
		return err
	}
	resumeID := s.ResumeID()
	if resumeID != "" {
		result, err = st.rpc.request("session/load", map[string]any{"sessionId": resumeID, "cwd": cwd, "mcpServers": []any{}}, 15*time.Second)
		if err == nil {
			st.acpSessionID = responseSessionID(result, resumeID)
		}
	}
	if st.acpSessionID == "" {
		result, err = st.rpc.request("session/new", map[string]any{"cwd": cwd, "mcpServers": []any{}}, 15*time.Second)
		if err != nil {
			_ = cmd.Process.Kill()
			return err
		}
		st.acpSessionID = responseSessionID(result, "")
		if st.acpSessionID == "" {
			return errors.New("gemini session/new returned no sessionId")
		}
	}
	s.SetResumeID(st.acpSessionID)
	g.sink.Emit(backend.NewSessionUUID(s.ID, st.acpSessionID))
	return nil
}

func responseSessionID(raw json.RawMessage, fallback string) string {
	var response struct {
		SessionID string `json:"sessionId"`
		Session   struct {
			ID string `json:"id"`
		} `json:"session"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return fallback
	}
	if response.SessionID != "" {
		return response.SessionID
	}
	if response.Session.ID != "" {
		return response.Session.ID
	}
	return fallback
}

func (g *Gemini) runTurn(s *session.Session, st *geminiState, reqID string, blocks []map[string]any) {
	result, err := st.rpc.request("session/prompt", map[string]any{"sessionId": st.acpSessionID, "prompt": blocks}, geminiTurnTimeout)
	st.mu.Lock()
	active := st.requestID == reqID
	if active {
		st.requestID = ""
	}
	st.mu.Unlock()
	if !active {
		return
	}
	if err != nil {
		g.sink.Emit(backend.NewError(s.ID, reqID, backend.ErrTurn, "gemini turn failed: "+err.Error()))
		return
	}
	var response struct {
		StopReason string `json:"stopReason"`
	}
	_ = json.Unmarshal(result, &response)
	if response.StopReason == "cancelled" {
		g.sink.Emit(backend.NewStopped(s.ID, reqID))
		return
	}
	g.sink.Emit(backend.NewDone(s.ID, reqID))
}

func (g *Gemini) readLoop(s *session.Session, st *geminiState, stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 128*1024*1024)
	for scanner.Scan() {
		raw := append([]byte(nil), scanner.Bytes()...)
		if st.rpc.dispatchResponse(raw) {
			continue
		}
		var message struct {
			ID     *int            `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if json.Unmarshal(raw, &message) != nil {
			continue
		}
		if message.ID != nil && message.Method != "" {
			g.handleServerRequest(st, *message.ID, message.Method, message.Params)
			continue
		}
		if message.Method == "session/update" {
			g.handleUpdate(s, st, message.Params)
		}
	}
	st.rpc.failAll(errors.New("gemini process exited"))
}

func (g *Gemini) handleServerRequest(st *geminiState, id int, method string, params json.RawMessage) {
	response := map[string]any{"jsonrpc": "2.0", "id": id}
	if method != "session/request_permission" {
		response["error"] = map[string]any{"code": -32601, "message": "unknown method: " + method}
		_ = st.rpc.write(response)
		return
	}
	var body struct {
		Options []struct {
			OptionID string `json:"optionId"`
			Kind     string `json:"kind"`
		} `json:"options"`
	}
	_ = json.Unmarshal(params, &body)
	for _, option := range body.Options {
		if option.Kind == "allow_once" || option.Kind == "allow_always" {
			response["result"] = map[string]any{"outcome": "selected", "optionId": option.OptionID}
			_ = st.rpc.write(response)
			return
		}
	}
	response["result"] = map[string]any{"outcome": "cancelled"}
	_ = st.rpc.write(response)
}

func (g *Gemini) handleUpdate(s *session.Session, st *geminiState, raw json.RawMessage) {
	var body struct {
		Update map[string]any `json:"update"`
	}
	if json.Unmarshal(raw, &body) != nil {
		return
	}
	kind, _ := body.Update["sessionUpdate"].(string)
	st.mu.Lock()
	reqID := st.requestID
	st.mu.Unlock()
	switch kind {
	case "agent_message_chunk":
		if text := contentText(body.Update["content"]); text != "" {
			g.sink.Emit(backend.NewTextChunk(s.ID, reqID, text))
		}
	case "agent_thought_chunk":
		if text := contentText(body.Update["content"]); text != "" {
			g.sink.Emit(backend.NewThinkingChunk(s.ID, reqID, text))
		}
	case "tool_call":
		call, _ := body.Update["toolCall"].(map[string]any)
		id := stringValue(call, "toolCallId", "id")
		name := stringValue(call, "name", "toolName")
		command, _ := json.Marshal(call["input"])
		g.sink.Emit(backend.NewToolStart(s.ID, reqID, id, name, string(command)))
	case "tool_call_update":
		call, _ := body.Update["toolCall"].(map[string]any)
		id := stringValue(call, "toolCallId", "id")
		status := stringValue(body.Update, "status")
		if status == "completed" || status == "done" || status == "success" || status == "error" {
			g.sink.Emit(backend.NewToolResult(s.ID, reqID, id, fmt.Sprint(body.Update["output"])))
			g.sink.Emit(backend.NewToolEnd(s.ID, reqID, id))
		}
	}
}

func contentText(value any) string {
	switch content := value.(type) {
	case string:
		return content
	case map[string]any:
		if content["type"] == "text" {
			return fmt.Sprint(content["text"])
		}
	case []any:
		var b strings.Builder
		for _, item := range content {
			b.WriteString(contentText(item))
		}
		return b.String()
	}
	return ""
}

func stringValue(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key].(string); ok && value != "" {
			return value
		}
	}
	return ""
}

func (g *Gemini) Stop(_ context.Context, s *session.Session) error {
	st := g.state(s.ID)
	st.mu.Lock()
	reqID, sid := st.requestID, st.acpSessionID
	st.requestID = ""
	st.mu.Unlock()
	if sid != "" {
		_ = st.rpc.notify("session/cancel", map[string]any{"sessionId": sid})
	}
	g.sink.Emit(backend.NewStopped(s.ID, reqID))
	return nil
}

func (g *Gemini) Clear(ctx context.Context, s *session.Session) error {
	_ = g.Close(ctx, s)
	s.ClearResumeIDs()
	g.sink.Emit(backend.NewSessionWarning(s.ID, "Session history cleared."))
	return nil
}

func (g *Gemini) Close(_ context.Context, s *session.Session) error {
	g.mu.Lock()
	st := g.states[s.ID]
	delete(g.states, s.ID)
	g.mu.Unlock()
	if st != nil && st.proc != nil && st.proc.Process != nil {
		_ = st.proc.Process.Kill()
	}
	return nil
}
