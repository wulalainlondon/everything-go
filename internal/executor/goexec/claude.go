// Package goexec is the pure-Go Executor: it spawns the Claude CLI as a
// persistent stream-json subprocess per session and parses its NDJSON stdout
// into normalized wire events. This is config 2 (pure Go).
//
// Fidelity reference: bridge/backends/claude_stream.py (_stdout_reader) and
// bridge/backends/claude_cli.py (spawn command + stdin message format).
package goexec

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"everything-go/internal/executor"
	"everything-go/internal/protocol"
	"everything-go/internal/runtime"
	"everything-go/internal/session"
)

// maxLine bounds a single NDJSON stdout line. Claude emits large tool results;
// 16 MiB mirrors the Python _STREAM_READER_LIMIT order of magnitude.
const maxLine = 16 * 1024 * 1024

// todoTools are Task/plan tools normalized into todo_update events instead of
// rendered as tool cards. Mirrors claude_stream.py's _TODO_TOOLS.
var todoTools = map[string]bool{
	"TodoWrite": true, "TaskCreate": true, "TaskUpdate": true, "TaskDelete": true,
}

type proc struct {
	cmd    *exec.Cmd
	stdin  *bufWriteCloser
	cancel context.CancelFunc
	reqID  string // request_id of the in-flight turn, stamped onto events

	// Todo/plan panel state, touched only by this proc's readStdout goroutine.
	todo           *todoStore
	todoSuppressed map[string]bool // tool_use_ids whose start/result are swallowed
}

// Claude implements executor.Executor over the local `claude` CLI.
type Claude struct {
	sink        executor.Sink
	claudeBin   string
	projectsDir string

	mu    sync.Mutex
	procs map[string]*proc // sessionID -> running subprocess

	interMu sync.Mutex
	pending map[string]*pendingInteraction // request_id -> paused AskUserQuestion

	mcp *askUserMCP // in-process ask_user MCP server (nil if it failed to start)
}

func NewClaude(sink executor.Sink, claudeBin string) *Claude {
	if claudeBin == "" {
		claudeBin = "claude"
	}
	projectsDir := ""
	if home, err := os.UserHomeDir(); err == nil {
		projectsDir = filepath.Join(home, ".claude", "projects")
	}
	c := &Claude{
		sink: sink, claudeBin: claudeBin, projectsDir: projectsDir,
		procs:   make(map[string]*proc),
		pending: make(map[string]*pendingInteraction),
	}
	// Host the ask_user MCP server so AskUserQuestion-style prompts can actually
	// be answered from the app (the built-in tool can't be answered in headless
	// mode). Best-effort: if it won't start, we fall back to the native path.
	if mcp, err := startAskUserMCP(c); err != nil {
		log.Printf("[mcp] ask_user disabled: %v", err)
	} else {
		c.mcp = mcp
	}
	return c
}

// Send writes a user message to the session's claude process (spawning it on
// first use) and lets the stdout reader stream the response.
func (c *Claude) Send(ctx context.Context, s *session.Session, reqID, content string, images []protocol.InboundImage, files []protocol.InboundFile) error {
	c.mu.Lock()
	p := c.procs[s.ID]
	if p == nil {
		var err error
		p, err = c.spawn(s)
		if err != nil {
			c.mu.Unlock()
			c.sink.Emit(protocol.NewError(s.ID, "session_dead", "Failed to spawn claude: "+err.Error()))
			return err
		}
		c.procs[s.ID] = p
	}
	p.reqID = reqID
	c.mu.Unlock()

	payload := userMessageJSON(content, images, files)
	if _, err := p.stdin.Write(payload); err != nil {
		c.sink.Emit(protocol.NewError(s.ID, "", "stdin write failed: "+err.Error()))
		return err
	}
	return p.stdin.Flush()
}

func (c *Claude) Stop(ctx context.Context, s *session.Session) error {
	c.mu.Lock()
	p := c.procs[s.ID]
	delete(c.procs, s.ID)
	c.mu.Unlock()

	if p != nil {
		p.cancel() // SIGKILL via context; the reader goroutine exits on EOF
	}
	c.cancelInteractionsFor(s.ID)
	c.sink.Emit(protocol.NewStopped(s.ID, ""))
	return nil
}

func (c *Claude) Clear(ctx context.Context, s *session.Session) error {
	c.mu.Lock()
	p := c.procs[s.ID]
	delete(c.procs, s.ID)
	c.mu.Unlock()
	if p != nil {
		p.cancel()
	}
	c.cancelInteractionsFor(s.ID)
	s.SetResumeID("")
	// The next Send spawns a fresh proc (and todoStore), so the panel resets;
	// tell the app to clear it now. Mirrors claude_cli.clear's _evt_todo_update([]).
	c.sink.Emit(protocol.NewTodoUpdate(s.ID, "", nil))
	c.sink.Emit(protocol.NewSessionWarning(s.ID, "Session history cleared."))
	return nil
}

// PID reports the live claude subprocess pid for this session, if running.
// Used by get_tasks.
func (c *Claude) PID(s *session.Session) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[s.ID]
	if p == nil || p.cmd.Process == nil {
		return 0, false
	}
	return p.cmd.Process.Pid, true
}

// KillProc terminates this session's claude subprocess (kill_task). The reader
// goroutine exits on EOF; the next Send re-spawns.
func (c *Claude) KillProc(s *session.Session) bool {
	c.mu.Lock()
	p := c.procs[s.ID]
	delete(c.procs, s.ID)
	c.mu.Unlock()
	if p == nil {
		return false
	}
	p.cancel()
	c.cancelInteractionsFor(s.ID)
	return true
}

func (c *Claude) Close(ctx context.Context, s *session.Session) error {
	c.mu.Lock()
	p := c.procs[s.ID]
	delete(c.procs, s.ID)
	c.mu.Unlock()
	if p != nil {
		p.cancel()
	}
	c.cancelInteractionsFor(s.ID)
	return nil
}

// spawn starts a persistent claude subprocess. Caller holds c.mu.
func (c *Claude) spawn(s *session.Session) (*proc, error) {
	snap := s.Snapshot() // consistent view of model/resume/effort/cwd
	ctx, cancel := context.WithCancel(context.Background())

	args := []string{
		"--print",
		"--input-format", "stream-json",
		"--output-format", "stream-json",
		"--verbose",
		"--dangerously-skip-permissions",
	}
	// Register the in-process ask_user MCP server (per-session URL) and steer
	// Claude to use it instead of the built-in AskUserQuestion, which can't be
	// answered in headless mode. skip-permissions already allows MCP tools.
	if c.mcp != nil {
		cfg := fmt.Sprintf(`{"mcpServers":{"ask_user":{"type":"http","url":%q,"timeout":1800000}}}`, c.mcp.sessionURL(s.ID))
		// NB: keep the append-system-prompt ASCII-only. A non-ASCII char (e.g. an
		// em-dash) in --append-system-prompt makes the claude CLI hang when the
		// turn also contains an image — a real CLI quirk found via A/B testing.
		args = append(args,
			"--mcp-config", cfg,
			"--append-system-prompt", "When you need to ask the user a question or have them choose between options, call the ask_user MCP tool (mcp__ask_user__ask_question) and wait for the result. Do NOT use the built-in AskUserQuestion tool, which cannot be answered in this environment.",
		)
	}
	if snap.Model != "" {
		args = append(args, "--model", snap.Model)
	}
	if snap.ResumeID != "" {
		args = append(args, "--resume", snap.ResumeID)
	}
	if snap.Effort != "" && snap.Effort != "auto" {
		args = append(args, "--effort", snap.Effort)
	}

	cmd := exec.CommandContext(ctx, c.claudeBin, args...)
	if snap.Cwd != "" {
		// Expand "~"/"~/..." like Python's os.path.expanduser before chdir;
		// the app sends "~" as the default cwd and exec won't expand it.
		cmd.Dir = runtime.ExpandPath(snap.Cwd)
	}
	if c.mcp != nil {
		// The per-server timeout in --mcp-config is sometimes ignored; the env var
		// is honored. 30 min lets a human take their time answering ask_user.
		cmd.Env = append(os.Environ(), "MCP_TOOL_TIMEOUT=1800000")
	}
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}

	log.Printf("[%s] spawned claude pid=%d cwd=%s resume=%s", s.ID, cmd.Process.Pid, snap.Cwd, snap.ResumeID)
	p := &proc{
		cmd: cmd, stdin: newBufWriteCloser(stdinPipe), cancel: cancel,
		todo:           newTodoStore(),
		todoSuppressed: map[string]bool{},
	}

	go c.readStdout(s, p, stdoutPipe)
	go drainStderr(s.ID, stderrPipe)
	go func() {
		_ = cmd.Wait()
		c.mu.Lock()
		if c.procs[s.ID] == p {
			delete(c.procs, s.ID)
		}
		c.mu.Unlock()
	}()
	return p, nil
}

// ndLine is the union of stdout line shapes we care about.
type ndLine struct {
	Type    string `json:"type"`
	Subtype string `json:"subtype"`
	Message struct {
		Content []json.RawMessage `json:"content"`
	} `json:"message"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`    // tool_result payload
	SessionID string          `json:"session_id"` // result → new resume uuid
}

type block struct {
	Type     string          `json:"type"`
	Text     string          `json:"text"`
	Thinking string          `json:"thinking"`
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Input    json.RawMessage `json:"input"`
}

func (c *Claude) readStdout(s *session.Session, p *proc, stdout interface{ Read([]byte) (int, error) }) {
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var evt ndLine
		if err := json.Unmarshal(line, &evt); err != nil {
			continue // non-JSON diagnostic line
		}
		reqID := p.reqID
		switch evt.Type {
		case "assistant":
			for _, raw := range evt.Message.Content {
				var b block
				if json.Unmarshal(raw, &b) != nil {
					continue
				}
				switch b.Type {
				case "thinking":
					if b.Thinking != "" {
						c.sink.Emit(protocol.NewThinkingChunk(s.ID, reqID, b.Thinking))
					}
				case "text":
					if b.Text != "" {
						c.sink.Emit(protocol.NewTextChunk(s.ID, reqID, b.Text))
					}
				case "tool_use":
					// Task/plan tools → normalized todo_update panel, not a tool card.
					// Suppress the tool_start (and later its result/end) so they don't
					// clutter the stream. TaskCreate's server id resolves from its result.
					if todoTools[b.Name] {
						changed := false
						switch b.Name {
						case "TodoWrite":
							changed = p.todo.applyTodoWrite(b.Input)
						case "TaskCreate":
							changed = p.todo.noteCreate(b.ID, b.Input)
						case "TaskUpdate":
							changed = p.todo.applyUpdate(b.Input)
						default: // TaskDelete
							changed = p.todo.applyDelete(b.Input)
						}
						if b.ID != "" {
							p.todoSuppressed[b.ID] = true
						}
						if changed {
							c.sink.Emit(protocol.NewTodoUpdate(s.ID, reqID, p.todo.asList()))
						}
						continue
					}
					command := extractCommand(b.Input)
					// AskUserQuestion pauses the turn: the CLI waits on stdin for a
					// tool_result. Surface it as a user_input_request and register it
					// so the eventual answer can be written back. The CLI emits no
					// `result` meanwhile, so the turn correctly stays Streaming.
					if b.Name == "AskUserQuestion" {
						c.registerInteraction(s, b.ID, b.Name, b.Input)
					}
					c.sink.Emit(protocol.NewToolStart(s.ID, reqID, b.ID, b.Name, command))
				}
			}
		case "tool_result":
			output := flattenToolOutput(evt.Content)
			// Swallow results for normalized task/todo tools. For TaskCreate this is
			// where the server-assigned id (#N) arrives — resolve it so later
			// TaskUpdate(taskId) matches, then re-emit the snapshot.
			if p.todoSuppressed[evt.ToolUseID] {
				delete(p.todoSuppressed, evt.ToolUseID)
				if p.todo.resolveCreate(evt.ToolUseID, output) {
					c.sink.Emit(protocol.NewTodoUpdate(s.ID, reqID, p.todo.asList()))
				}
				continue
			}
			c.sink.Emit(protocol.NewToolResult(s.ID, reqID, evt.ToolUseID, output))
			c.sink.Emit(protocol.NewToolEnd(s.ID, reqID, evt.ToolUseID))
		case "result":
			if evt.SessionID != "" && evt.SessionID != s.ResumeID() {
				s.SetResumeID(evt.SessionID)
				c.sink.Emit(protocol.NewSessionUUID(s.ID, evt.SessionID))
			}
			c.sink.Emit(protocol.NewDone(s.ID, reqID))
		}
	}
	// stdout closed → process gone
}

// extractCommand mirrors Python: input.command if present, else the raw input
// JSON string.
func extractCommand(input json.RawMessage) string {
	if len(input) == 0 {
		return ""
	}
	var m map[string]any
	if json.Unmarshal(input, &m) == nil {
		if cmd, ok := m["command"].(string); ok {
			return cmd
		}
	}
	return string(input)
}

// flattenToolOutput mirrors Python: if content is a list of blocks, join their
// text with newlines; otherwise stringify.
func flattenToolOutput(content json.RawMessage) string {
	if len(content) == 0 {
		return ""
	}
	var blocks []block
	if json.Unmarshal(content, &blocks) == nil && len(blocks) > 0 {
		out := ""
		for i, b := range blocks {
			if b.Type == "text" {
				if i > 0 {
					out += "\n"
				}
				out += b.Text
			}
		}
		return out
	}
	var str string
	if json.Unmarshal(content, &str) == nil {
		return str
	}
	return string(content)
}

// userMessageJSON builds the stream-json `user` frame. Attachments become
// content blocks in the same order/shape as claude_cli.py: images first
// (base64 image blocks), then files (PDF → document block, else a fenced text
// block), then the text content last.
func userMessageJSON(content string, images []protocol.InboundImage, files []protocol.InboundFile) []byte {
	blocks := make([]map[string]any, 0, len(images)+len(files)+1)
	for _, img := range images {
		mt := img.MediaType
		if mt == "" {
			mt = "image/jpeg"
		}
		blocks = append(blocks, map[string]any{
			"type":   "image",
			"source": map[string]any{"type": "base64", "media_type": mt, "data": img.Data},
		})
	}
	for _, f := range files {
		mt := f.MediaType
		if mt == "" {
			mt = "text/plain"
		}
		if mt == "application/pdf" {
			blocks = append(blocks, map[string]any{
				"type":   "document",
				"source": map[string]any{"type": "base64", "media_type": "application/pdf", "data": f.Content},
			})
			continue
		}
		name := f.Name
		if name == "" {
			name = "file"
		}
		ext := ""
		if i := strings.LastIndex(name, "."); i >= 0 {
			ext = name[i+1:]
		}
		fenced := f.Content
		if ext != "" {
			fenced = "```" + ext + "\n" + f.Content + "\n```"
		}
		blocks = append(blocks, map[string]any{"type": "text", "text": "[File: " + name + "]\n" + fenced})
	}
	if content != "" {
		blocks = append(blocks, map[string]any{"type": "text", "text": content})
	}
	frame := map[string]any{
		"type":    "user",
		"message": map[string]any{"role": "user", "content": blocks},
	}
	b, _ := json.Marshal(frame)
	return append(b, '\n')
}

func drainStderr(sessionID string, r interface{ Read([]byte) (int, error) }) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	for sc.Scan() {
		log.Printf("[%s] claude stderr: %s", sessionID, sc.Text())
	}
}

// bufWriteCloser wraps the stdin pipe with a buffered writer + flush.
type bufWriteCloser struct {
	w  *bufio.Writer
	c  interface{ Close() error }
	mu sync.Mutex
}

func newBufWriteCloser(wc interface {
	Write([]byte) (int, error)
	Close() error
}) *bufWriteCloser {
	return &bufWriteCloser{w: bufio.NewWriter(wc), c: wc}
}

func (b *bufWriteCloser) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.w.Write(p)
}

func (b *bufWriteCloser) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.w.Flush()
}

func (b *bufWriteCloser) Close() error { return b.c.Close() }
