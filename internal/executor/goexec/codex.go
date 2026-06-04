// Codex executor: drives a single persistent `codex app-server` subprocess via
// newline-delimited JSON-RPC, one thread per session. Fidelity reference:
// bridge/backends/codex_appserver.py.
package goexec

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"everything-go/internal/executor"
	"everything-go/internal/protocol"
	"everything-go/internal/runtime"
	"everything-go/internal/session"
)

const codexDefaultModel = "gpt-5.5"

type codexState struct {
	mu sync.Mutex

	threadID      string
	currentTurnID string
	turnActive    bool
	turnErr       string
	turnDone      chan struct{}
	stopping      bool
	reqID         string
	toolOutputs   map[string]string
}

func newCodexState() *codexState {
	return &codexState{toolOutputs: make(map[string]string)}
}

// finish completes the in-flight turn exactly once. errStr=="" means success;
// "stopped" is the interrupt sentinel.
func (st *codexState) finish(errStr string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.turnActive {
		return
	}
	st.turnActive = false
	if st.turnErr == "" {
		st.turnErr = errStr
	}
	if st.turnDone != nil {
		close(st.turnDone)
	}
}

// Codex implements executor.Executor over the codex app-server.
type Codex struct {
	sink     executor.Sink
	codexBin string
	rpc      *rpcPlumber

	startMu sync.Mutex
	proc    *exec.Cmd
	stdin   io.WriteCloser

	mu              sync.Mutex
	states          map[string]*codexState
	threadToSession map[string]*session.Session
}

func NewCodex(sink executor.Sink, codexBin string) *Codex {
	if codexBin == "" {
		codexBin = "codex"
	}
	return &Codex{
		sink:            sink,
		codexBin:        codexBin,
		rpc:             newRPCPlumber("codex"),
		states:          make(map[string]*codexState),
		threadToSession: make(map[string]*session.Session),
	}
}

func (c *Codex) state(id string) *codexState {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.states[id]
	if st == nil {
		st = newCodexState()
		c.states[id] = st
	}
	return st
}

// ensureServer spawns and initializes the singleton app-server if needed.
func (c *Codex) ensureServer() error {
	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.proc != nil && c.proc.ProcessState == nil {
		return nil // running
	}

	log.Printf("[codex] spawning codex app-server")
	cmd := exec.Command(c.codexBin, "app-server")
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	c.proc = cmd
	c.stdin = stdinPipe
	// Write raw to the pipe (line-delimited JSON, one syscall per line, serialized
	// by the plumber's write mutex) so no flush step can be skipped.
	c.rpc.setWriter(stdinPipe)

	go c.readLoop(stdoutPipe)
	go drainStderr("codex", stderrPipe)
	go func() {
		_ = cmd.Wait()
		c.rpc.failAll(errProcDead)
		c.startMu.Lock()
		c.proc = nil
		c.startMu.Unlock()
	}()

	if _, err := c.rpc.request("initialize", map[string]any{
		"clientInfo": map[string]any{"name": "everything-go", "version": "1.0"},
	}, 30*time.Second); err != nil {
		return err
	}
	return c.rpc.notify("initialized", nil)
}

// rpcCall sends an RPC and waits for the response. Writes go straight to the
// pipe, so there is no flush step.
func (c *Codex) rpcCall(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	return c.rpc.request(method, params, timeout)
}

func (c *Codex) readLoop(stdout interface{ Read([]byte) (int, error) }) {
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		raw := make(json.RawMessage, len(line))
		copy(raw, line)
		if c.rpc.dispatchResponse(raw) {
			continue
		}
		c.dispatch(raw)
	}
	log.Printf("[codex] read loop exited")
}

type codexMsg struct {
	ID     *int            `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func (c *Codex) dispatch(raw json.RawMessage) {
	var m codexMsg
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	// Server→client request (has id + method, no result/error).
	if m.ID != nil && m.Method != "" {
		c.handleServerRequest(*m.ID, m.Method)
		return
	}

	var p struct {
		ThreadID string            `json:"threadId"`
		Delta    string            `json:"delta"`
		Phase    string            `json:"phase"`
		Text     string            `json:"text"`
		Output   json.RawMessage   `json:"output"`
		ItemID   string            `json:"itemId"`
		CallID   string            `json:"callId"`
		Name     string            `json:"name"`
		Command  json.RawMessage   `json:"command"`
		Plan     []json.RawMessage `json:"plan"`
		Turn     struct {
			ID     string `json:"id"`
			Status string `json:"status"`
			Error  struct {
				Message string `json:"message"`
			} `json:"error"`
		} `json:"turn"`
		Item struct {
			ID      string          `json:"id"`
			Name    string          `json:"name"`
			Type    string          `json:"type"`
			Command json.RawMessage `json:"command"`
			Output  json.RawMessage `json:"output"`
		} `json:"item"`
		WillRetry bool `json:"willRetry"`
		Error     struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(m.Params, &p)

	c.mu.Lock()
	s := c.threadToSession[p.ThreadID]
	c.mu.Unlock()
	if s == nil {
		return
	}
	st := c.state(s.ID)
	reqID := st.reqID

	switch m.Method {
	case "turn/started":
		st.mu.Lock()
		st.currentTurnID = p.Turn.ID
		st.mu.Unlock()

	case "item/agentMessage/delta":
		if p.Delta == "" {
			return
		}
		if p.Phase == "commentary" {
			c.sink.Emit(protocol.NewThinkingChunk(s.ID, reqID, p.Delta))
			return
		}
		c.sink.Emit(protocol.NewTextChunk(s.ID, reqID, p.Delta))

	case "item/reasoning/textDelta":
		d := p.Delta
		if d == "" {
			d = p.Text
		}
		if d != "" {
			c.sink.Emit(protocol.NewThinkingChunk(s.ID, reqID, d))
		}

	case "item/commandExecution/outputDelta", "item/fileChange/outputDelta", "item/commandExecution/terminalInteraction":
		itemID := firstNonEmpty(p.ItemID, p.CallID, "codex_item")
		d := p.Delta
		if d == "" {
			d = p.Text
		}
		if d != "" {
			acc := c.accumulate(st, itemID, d)
			c.sink.Emit(protocol.NewToolResult(s.ID, reqID, itemID, acc))
		}

	case "item/started":
		itemID := firstNonEmpty(p.ItemID, p.Item.ID, "codex_item")
		name := firstNonEmpty(p.Name, p.Item.Name, p.Item.Type, "codex")
		command := rawToString(p.Command)
		if command == "" {
			command = rawToString(p.Item.Command)
		}
		st.mu.Lock()
		st.toolOutputs[itemID] = ""
		st.mu.Unlock()
		c.sink.Emit(protocol.NewToolStart(s.ID, reqID, itemID, name, command))

	case "item/completed":
		itemID := firstNonEmpty(p.ItemID, p.Item.ID, "codex_item")
		output := rawToString(p.Output)
		if output == "" {
			output = rawToString(p.Item.Output)
		}
		if output != "" {
			c.sink.Emit(protocol.NewToolResult(s.ID, reqID, itemID, output))
		}
		c.sink.Emit(protocol.NewToolEnd(s.ID, reqID, itemID))
		st.mu.Lock()
		delete(st.toolOutputs, itemID)
		st.mu.Unlock()

	case "turn/plan/updated":
		// Codex update_plan → normalized todo panel. Full replace; step→content.
		if todos := normalizeFullList(p.Plan, "step"); len(todos) > 0 {
			c.sink.Emit(protocol.NewTodoUpdate(s.ID, reqID, todosValue(todos)))
		}

	case "turn/completed":
		if p.Turn.Status == "failed" {
			st.finish(p.Turn.Error.Message)
		} else {
			st.finish("")
		}

	case "error":
		if !p.WillRetry {
			msg := p.Error.Message
			if msg == "" {
				msg = "unknown codex error"
			}
			st.finish(msg)
		}
	}
}

func (c *Codex) accumulate(st *codexState, itemID, delta string) string {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.toolOutputs[itemID] += delta
	return st.toolOutputs[itemID]
}

func (c *Codex) handleServerRequest(id int, method string) {
	approval := map[string]bool{
		"item/commandExecution/requestApproval": true,
		"item/fileChange/requestApproval":       true,
		"item/permissions/requestApproval":      true,
		"applyPatchApproval":                    true,
		"execCommandApproval":                   true,
	}
	switch {
	case approval[method]:
		_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"decision": "accept"}})
	case method == "item/tool/requestUserInput":
		_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"answers": []any{}}})
	default:
		_ = c.rpc.write(map[string]any{"id": id, "error": map[string]any{"code": -32601, "message": "unknown method: " + method}})
	}
}

// --- Executor interface ----------------------------------------------------

func (c *Codex) Send(ctx context.Context, s *session.Session, reqID, content string, _ []protocol.InboundImage, _ []protocol.InboundFile) error {
	// Codex backend is text-only for now; image/file attachments are ignored.
	if err := c.ensureServer(); err != nil {
		c.sink.Emit(protocol.NewError(s.ID, "spawn_failed", "codex app-server failed: "+err.Error()))
		return err
	}
	st := c.state(s.ID)

	if err := c.ensureThread(s, st); err != nil {
		c.sink.Emit(protocol.NewError(s.ID, "spawn_failed", "failed to start codex thread: "+err.Error()))
		return err
	}

	st.mu.Lock()
	st.reqID = reqID
	st.stopping = false
	st.turnErr = ""
	st.turnActive = true
	st.turnDone = make(chan struct{})
	threadID := st.threadID
	done := st.turnDone
	st.mu.Unlock()

	input := []map[string]any{{"type": "text", "text": content, "text_elements": []any{}}}
	go c.runTurn(s, st, threadID, input, done)
	return nil
}

func (c *Codex) runTurn(s *session.Session, st *codexState, threadID string, input []map[string]any, done chan struct{}) {
	_, err := c.rpcCall("turn/start", map[string]any{
		"threadId":       threadID,
		"input":          input,
		"approvalPolicy": "never",
	}, 30*time.Second)
	if err != nil {
		st.finish("turn/start failed: " + err.Error())
	}

	select {
	case <-done:
	case <-time.After(6000 * time.Second):
		st.finish("Codex turn timed out")
		<-done
	}

	st.mu.Lock()
	stopping, turnErr := st.stopping, st.turnErr
	st.mu.Unlock()

	switch {
	case stopping || turnErr == "stopped":
		c.sink.Emit(protocol.NewStopped(s.ID, st.reqID))
	case turnErr != "":
		c.sink.Emit(protocol.NewError(s.ID, "turn_error", turnErr))
	default:
		c.sink.Emit(protocol.NewDone(s.ID, st.reqID))
	}
}

// ensureThread starts or resumes the codex thread for this session.
func (c *Codex) ensureThread(s *session.Session, st *codexState) error {
	st.mu.Lock()
	have := st.threadID
	st.mu.Unlock()
	if have != "" {
		return nil
	}

	snap := s.Snapshot()
	cwd := runtime.ExpandPath(snap.Cwd)
	if fi, err := os.Stat(cwd); err != nil || !fi.IsDir() {
		cwd, _ = os.UserHomeDir()
	}
	sandbox := codexSandbox(snap.Sandbox)

	var threadID string
	if snap.ResumeID != "" {
		raw, err := c.rpcCall("thread/resume", map[string]any{
			"threadId": snap.ResumeID, "cwd": cwd,
			"approvalPolicy": "never", "sandbox": sandbox,
		}, 15*time.Second)
		if err == nil {
			threadID = extractThreadID(raw, snap.ResumeID)
		} else {
			log.Printf("[codex] thread/resume failed, starting new: %v", err)
		}
	}
	if threadID == "" {
		model := snap.Model
		if model == "" {
			model = codexDefaultModel
		}
		raw, err := c.rpcCall("thread/start", map[string]any{
			"model": model, "cwd": cwd, "ephemeral": false,
			"approvalPolicy": "never", "sandbox": sandbox,
		}, 15*time.Second)
		if err != nil {
			return err
		}
		threadID = extractThreadID(raw, "")
		if threadID == "" {
			return errNoThreadID
		}
	}

	st.mu.Lock()
	st.threadID = threadID
	st.mu.Unlock()
	c.mu.Lock()
	c.threadToSession[threadID] = s
	c.mu.Unlock()

	s.SetResumeID(threadID)
	c.sink.Emit(protocol.NewSessionUUID(s.ID, threadID))
	log.Printf("[codex] session=%s thread=%s", s.ID, threadID)
	return nil
}

func (c *Codex) Stop(ctx context.Context, s *session.Session) error {
	st := c.state(s.ID)
	st.mu.Lock()
	st.stopping = true
	threadID, turnID, active := st.threadID, st.currentTurnID, st.turnActive
	st.mu.Unlock()

	if threadID != "" && turnID != "" {
		_, _ = c.rpcCall("turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, 5*time.Second)
	}
	if active {
		st.finish("stopped") // runTurn emits stopped
	} else {
		c.sink.Emit(protocol.NewStopped(s.ID, st.reqID))
	}
	return nil
}

func (c *Codex) Clear(ctx context.Context, s *session.Session) error {
	st := c.state(s.ID)
	_ = c.Stop(ctx, s)
	st.mu.Lock()
	threadID := st.threadID
	st.threadID = ""
	st.toolOutputs = make(map[string]string)
	st.mu.Unlock()
	if threadID != "" {
		_, _ = c.rpcCall("thread/archive", map[string]any{"threadId": threadID}, 5*time.Second)
		c.mu.Lock()
		delete(c.threadToSession, threadID)
		c.mu.Unlock()
	}
	s.SetResumeID("")
	c.sink.Emit(protocol.NewSessionWarning(s.ID, "Session history cleared."))
	return nil
}

func (c *Codex) Close(ctx context.Context, s *session.Session) error {
	st := c.state(s.ID)
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	c.mu.Lock()
	delete(c.states, s.ID)
	if threadID != "" {
		delete(c.threadToSession, threadID)
	}
	c.mu.Unlock()
	return nil
}

// --- helpers ---------------------------------------------------------------

func codexSandbox(s string) string {
	switch s {
	case "read-only", "workspace-write", "danger-full-access":
		return s
	default:
		return "workspace-write"
	}
}

func extractThreadID(raw json.RawMessage, fallback string) string {
	var r struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if json.Unmarshal(raw, &r) == nil && r.Thread.ID != "" {
		return r.Thread.ID
	}
	return fallback
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func rawToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}
