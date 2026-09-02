// Codex executor: drives a single persistent `codex app-server` subprocess via
// newline-delimited JSON-RPC, one thread per session. Fidelity reference:
// bridge/backends/codex_appserver.py.
package goexec

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"everything-go/internal/backend"
	"everything-go/internal/executor"
	"everything-go/internal/protocol"
	"everything-go/internal/runtime"
	"everything-go/internal/session"
	"everything-go/internal/sourcepolicy"
)

const (
	codexDefaultModel     = "gpt-5.6-sol"
	codexCompactThreshold = 0.80
	codexTurnTimeout      = 100 * time.Minute
	codexStallWarnAfter   = 5 * time.Minute
	codexStallAbortAfter  = 30 * time.Minute
	codexStallCheckEvery  = 30 * time.Second
	// A thread/resume response contains the complete serialized history in one
	// WebSocket message. coder/websocket defaults to 32 KiB, which disconnects
	// healthy daemon sessions as soon as a non-trivial thread is resumed. Keep a
	// finite ceiling for memory safety while matching the hundreds-of-MB replies
	// already supported by the stdio transport below.
	codexRemoteReadLimit = 512 * 1024 * 1024
)

var (
	codexAskUserRE     = regexp.MustCompile("(?s)```(?:json)?\\s*(\\{[^`]*?\"(?:ask_user_question|AskUserQuestion)\"[^`]*?\\})\\s*```|(\\{[^{}]*\"(?:ask_user_question|AskUserQuestion)\"[^{}]*\\})")
	codexInlineImageRE = regexp.MustCompile(`"type"\s*:\s*"input_image"`)
	// Image generation tool results contain an inline input_image data URL plus
	// a compact text block naming the durable file under ~/.codex/generated_images.
	// Extract only that managed path; never forward the multi-megabyte base64
	// payload to the mobile tool card.
	codexGeneratedImagePathRE = regexp.MustCompile(`(/[^\s"'<>]*\.codex/generated_images/[^\s"'<>]+\.(?:png|jpe?g|webp|gif))`)
)

type codexState struct {
	mu       sync.Mutex
	ensureMu sync.Mutex

	threadID        string
	currentTurnID   string
	turnActive      bool
	turnErr         string
	turnDone        chan struct{}
	stopping        bool
	reqID           string
	tempImages      []string
	accumulatedText string
	askExtracted    bool
	contextUsed     int
	contextMax      int
	compactActive   bool
	compactErr      string
	compactDone     chan struct{}
	lastEventAt     time.Time
	stallWarned     bool
	pendingHandoff  string
	pendingRecovery *codexRecovery
	agents          map[string]*codexAgent
}

type codexAgent struct {
	id, agentType, description, parentID, output string
	startMS, endMS                               *int64
	tools                                        []protocol.AgentToolCall
}

func newCodexState() *codexState {
	return &codexState{agents: make(map[string]*codexAgent)}
}

func (st *codexState) finishCompact(errStr string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.compactActive {
		return
	}
	st.compactActive = false
	if st.compactErr == "" {
		st.compactErr = errStr
	}
	if st.compactDone != nil {
		close(st.compactDone)
	}
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
	st.currentTurnID = ""
	if st.turnErr == "" {
		st.turnErr = errStr
	}
	if st.turnDone != nil {
		close(st.turnDone)
	}
}

func (st *codexState) touch(now time.Time) {
	st.mu.Lock()
	st.lastEventAt = now
	st.stallWarned = false
	st.mu.Unlock()
}

// Codex implements executor.Executor over the codex app-server.
type Codex struct {
	sink         executor.Sink
	tools        *toolEmitter
	codexBin     string
	sessionsRoot string
	indexPath    string
	rpc          *rpcPlumber

	startMu               sync.Mutex
	proc                  *exec.Cmd
	stdin                 io.WriteCloser
	procDone              chan struct{}
	remoteConn            *websocket.Conn
	remoteDone            chan struct{}
	remoteCancel          context.CancelFunc
	authPath              string
	serverAuthFingerprint string
	authReloadDeferred    bool
	authWatchOnce         sync.Once

	mu                 sync.Mutex
	states             map[string]*codexState
	threadToSession    map[string]*session.Session
	activeThreadOwner  map[string]*session.Session
	interMu            sync.Mutex
	interactions       map[string]codexInteraction
	catalogMu          sync.RWMutex
	catalog            backend.Definition
	collaborationModes map[string]map[string]any
	rolloutMu          sync.Mutex
	rolloutRoot        string
	rolloutScannedAt   time.Time
	rolloutPaths       []string
	rolloutByID        map[string]string
	turnTimeout        time.Duration
	stallWarnAfter     time.Duration
	stallAbortAfter    time.Duration
	stallCheckEvery    time.Duration
	dataDir            string
	rolloverEnabled    bool
	coldResumeMaxBytes int64
	checkpointMaxBytes int
	appServerMode      string // daemon (default) | stdio (compatibility fallback)
	appServerSocket    string
}

type codexInteraction struct {
	payload      backend.UserInputPayload
	rpcID        any
	responseKind string
	reqID        string
	mcpParams    json.RawMessage
}

func NewCodex(sink executor.Sink, codexBin string) *Codex {
	if codexBin == "" {
		codexBin = "codex"
	}
	codexHome := sourcepolicy.CodexHome()
	home, _ := os.UserHomeDir()
	return &Codex{
		sink:               sink,
		tools:              newToolEmitter(sink),
		codexBin:           codexBin,
		sessionsRoot:       filepath.Join(codexHome, "sessions"),
		indexPath:          filepath.Join(codexHome, "session_index.jsonl"),
		authPath:           filepath.Join(codexHome, "auth.json"),
		rpc:                newRPCPlumber("codex"),
		states:             make(map[string]*codexState),
		threadToSession:    make(map[string]*session.Session),
		activeThreadOwner:  make(map[string]*session.Session),
		interactions:       make(map[string]codexInteraction),
		collaborationModes: make(map[string]map[string]any),
		turnTimeout:        codexTurnTimeout,
		stallWarnAfter:     codexStallWarnAfter,
		stallAbortAfter:    codexStallAbortAfter,
		stallCheckEvery:    codexStallCheckEvery,
		dataDir:            filepath.Join(home, ".everything-go-runtime"),
		rolloverEnabled:    envBool("EVERYTHING_GO_CODEX_ROLLOVER_ENABLED", false),
		coldResumeMaxBytes: envInt64("EVERYTHING_GO_CODEX_COLD_RESUME_MAX_BYTES", 256*1024*1024),
		checkpointMaxBytes: int(envInt64("EVERYTHING_GO_CODEX_CHECKPOINT_MAX_BYTES", 128*1024)),
		appServerMode:      codexAppServerMode(),
		appServerSocket:    strings.TrimSpace(os.Getenv("EVERYTHING_GO_CODEX_APP_SERVER_SOCKET")),
	}
}

func codexAppServerMode() string {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("EVERYTHING_GO_CODEX_APP_SERVER_MODE")))
	if mode == "stdio" || mode == "private" {
		return "stdio"
	}
	return "daemon"
}

// SetDataDir points generation manifests at the bridge's configured state
// directory. It must be called during startup before the executor is shared.
func (c *Codex) SetDataDir(path string) {
	if strings.TrimSpace(path) != "" {
		c.dataDir = runtime.ExpandPath(path)
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

// ensureServer spawns and initializes the singleton app-server if needed. It
// also reloads the process when Codex credentials change on disk. codex
// app-server reads auth.json at startup and otherwise keeps the old account in
// memory, so an external `codex login` cannot take effect without this restart.
func (c *Codex) ensureServer() error {
	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.serverRunningLocked() {
		// A shared daemon owns credential lifecycle. Restarting it from one Bridge
		// would disrupt desktop TUI and other Bridge clients, so only the legacy
		// private stdio mode performs auth-triggered process replacement.
		if c.appServerMode == "daemon" {
			return nil
		}
		fingerprint, err := codexAuthFingerprint(c.authPath)
		if err != nil {
			log.Printf("[codex] auth credential check failed: %v", err)
			return nil
		}
		if fingerprint == c.serverAuthFingerprint {
			c.authReloadDeferred = false
			return nil
		}
		if c.hasActiveWork() || c.rpc.pendingCount() > 0 {
			if !c.authReloadDeferred {
				log.Printf("[codex] auth credentials changed; reload deferred until Codex is idle")
				c.authReloadDeferred = true
			}
			return nil
		}

		log.Printf("[codex] auth credentials changed; restarting codex app-server")
		if err := c.stopServerLocked(); err != nil {
			return err
		}
	}
	return c.startServerLocked()
}

func (c *Codex) startServerLocked() error {
	log.Printf("[codex] connecting app-server mode=%s", c.appServerMode)
	if changed, err := ensureBrowserElicitationRouting(filepath.Dir(c.sessionsRoot)); err != nil {
		log.Printf("[codex] browser elicitation routing config failed: %v", err)
	} else if changed {
		log.Printf("[codex] Browser Use elicitations routed through bridge policy")
	}
	codexHome := filepath.Dir(c.sessionsRoot)
	if c.appServerMode == "daemon" {
		for _, action := range []string{"start", "enable-remote-control"} {
			startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			managed := exec.CommandContext(startCtx, c.codexBin, "app-server", "daemon", action)
			managed.Env = append(os.Environ(), "CODEX_HOME="+codexHome)
			output, err := managed.CombinedOutput()
			cancel()
			if err != nil {
				return fmt.Errorf("codex app-server daemon %s: %w (%s)", action, err, strings.TrimSpace(string(output)))
			}
		}
		return c.startRemoteServerLocked(codexHome)
	}
	args := []string{"app-server"}
	cmd := exec.Command(c.codexBin, args...)
	cmd.Env = append(os.Environ(), "CODEX_HOME="+codexHome)
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
	authFingerprint := "shared-daemon"
	if c.appServerMode != "daemon" {
		var err error
		authFingerprint, err = codexAuthFingerprint(c.authPath)
		if err != nil {
			_ = cmd.Process.Kill()
			return fmt.Errorf("read Codex credentials: %w", err)
		}
	}
	done := make(chan struct{})
	c.proc = cmd
	c.stdin = stdinPipe
	c.procDone = done
	c.serverAuthFingerprint = authFingerprint
	c.authReloadDeferred = false
	// Write raw to the pipe (line-delimited JSON, one syscall per line, serialized
	// by the plumber's write mutex) so no flush step can be skipped.
	c.rpc.setWriter(stdinPipe)

	go c.readLoop(stdoutPipe)
	go drainStderr("codex", stderrPipe)
	go func(started *exec.Cmd, exited chan struct{}) {
		_ = cmd.Wait()
		c.rpc.failAll(errProcDead)
		c.invalidateLiveThreads()
		close(exited)
		c.startMu.Lock()
		if c.proc == started {
			c.proc = nil
			c.stdin = nil
			c.procDone = nil
			c.rpc.setWriter(nil)
		}
		c.startMu.Unlock()
	}(cmd, done)

	if err := c.initializeRPC(); err != nil {
		return err
	}
	c.startAuthWatcher()
	return nil
}

func (c *Codex) initializeRPC() error {
	if _, err := c.rpc.request("initialize", map[string]any{
		"clientInfo": map[string]any{
			"name":    "claude-bridge",
			"title":   "Averything Bridge",
			"version": "1.0",
		},
		"capabilities": map[string]any{
			"experimentalApi":                true,
			"requestAttestation":             false,
			"mcpServerOpenaiFormElicitation": true,
		},
	}, 30*time.Second); err != nil {
		return err
	}
	if err := c.rpc.notify("initialized", nil); err != nil {
		return err
	}
	return nil
}

type websocketRPCWriter struct {
	conn *websocket.Conn
}

func (w websocketRPCWriter) Write(p []byte) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.conn.Write(ctx, websocket.MessageText, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *Codex) daemonSocketPath(codexHome string) string {
	if c.appServerSocket != "" {
		return c.appServerSocket
	}
	return filepath.Join(codexHome, "app-server-control", "app-server-control.sock")
}

func (c *Codex) startRemoteServerLocked(codexHome string) error {
	socketPath := c.daemonSocketPath(codexHome)
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	httpClient := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}}
	ctx, cancelDial := context.WithTimeout(context.Background(), 15*time.Second)
	conn, _, err := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: httpClient})
	cancelDial()
	if err != nil {
		return fmt.Errorf("connect shared codex app-server at %s: %w", socketPath, err)
	}
	conn.SetReadLimit(codexRemoteReadLimit)
	readCtx, cancelRead := context.WithCancel(context.Background())
	done := make(chan struct{})
	c.remoteConn = conn
	c.remoteDone = done
	c.remoteCancel = cancelRead
	c.serverAuthFingerprint = "shared-daemon"
	c.authReloadDeferred = false
	c.rpc.setWriter(websocketRPCWriter{conn: conn})
	go c.readRemoteLoop(readCtx, conn, done)
	if err := c.initializeRPC(); err != nil {
		cancelRead()
		_ = conn.Close(websocket.StatusInternalError, "initialize failed")
		return err
	}
	log.Printf("[codex] connected shared daemon socket=%s", socketPath)
	return nil
}

func (c *Codex) readRemoteLoop(ctx context.Context, conn *websocket.Conn, done chan struct{}) {
	var readErr error
	defer func() {
		failure := error(errProcDead)
		if readErr != nil && ctx.Err() == nil {
			failure = fmt.Errorf("app-server connection lost: %w", readErr)
			log.Printf("[codex] remote read loop error: %v", readErr)
		}
		c.rpc.failAll(failure)
		c.invalidateLiveThreads()
		close(done)
		c.startMu.Lock()
		if c.remoteConn == conn {
			c.remoteConn = nil
			c.remoteDone = nil
			c.remoteCancel = nil
			c.rpc.setWriter(nil)
		}
		c.startMu.Unlock()
	}()
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			readErr = err
			return
		}
		if !c.rpc.dispatchResponse(payload) {
			c.dispatch(payload)
		}
	}
}

func codexAuthFingerprint(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "missing", nil
		}
		return "", err
	}
	sum := sha256.Sum256(b)
	return fmt.Sprintf("sha256:%x", sum), nil
}

func (c *Codex) startAuthWatcher() {
	c.authWatchOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				if c.authCredentialsChanged() {
					if err := c.ensureServer(); err != nil {
						log.Printf("[codex] automatic auth reload failed: %v", err)
					}
				}
			}
		}()
	})
}

func (c *Codex) authCredentialsChanged() bool {
	fingerprint, err := codexAuthFingerprint(c.authPath)
	if err != nil {
		return false
	}
	c.startMu.Lock()
	defer c.startMu.Unlock()
	return c.serverRunningLocked() && fingerprint != c.serverAuthFingerprint
}

func (c *Codex) serverRunningLocked() bool {
	if c.appServerMode == "daemon" {
		if c.remoteConn == nil {
			return false
		}
		if c.remoteDone == nil {
			return true
		}
		select {
		case <-c.remoteDone:
			return false
		default:
			return true
		}
	}
	if c.proc == nil {
		return false
	}
	if c.procDone == nil {
		return true
	}
	select {
	case <-c.procDone:
		return false
	default:
		return true
	}
}

func (c *Codex) hasActiveWork() bool {
	c.mu.Lock()
	states := make([]*codexState, 0, len(c.states))
	for _, st := range c.states {
		states = append(states, st)
	}
	c.mu.Unlock()

	for _, st := range states {
		st.mu.Lock()
		active := st.turnActive || st.compactActive
		st.mu.Unlock()
		if active {
			return true
		}
	}
	return false
}

func (c *Codex) stopServerLocked() error {
	if c.appServerMode == "daemon" {
		conn, done, cancel := c.remoteConn, c.remoteDone, c.remoteCancel
		if cancel != nil {
			cancel()
		}
		if conn != nil {
			_ = conn.Close(websocket.StatusNormalClosure, "bridge client closing")
		}
		if done != nil {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				return fmt.Errorf("timed out closing shared app-server client")
			}
		}
		c.remoteConn = nil
		c.remoteDone = nil
		c.remoteCancel = nil
		c.rpc.setWriter(nil)
		return nil
	}
	cmd := c.proc
	if cmd == nil || !c.serverRunningLocked() {
		c.proc = nil
		c.stdin = nil
		c.procDone = nil
		c.rpc.setWriter(nil)
		return nil
	}

	done := c.procDone
	if c.stdin != nil {
		_ = c.stdin.Close()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				return fmt.Errorf("timed out stopping codex app-server")
			}
		}
	} else {
		_ = cmd.Process.Kill()
	}
	if c.proc == cmd {
		c.proc = nil
		c.stdin = nil
		c.procDone = nil
		c.rpc.setWriter(nil)
	}
	return nil
}

// Catalog reads the app-server catalog instead of duplicating model ids in the
// bridge. The last successful result is retained for transient reconnects.
func (c *Codex) Catalog(ctx context.Context) (backend.Definition, error) {
	if err := c.ensureServer(); err != nil {
		return backend.Definition{}, err
	}
	raw, err := c.rpcCall("model/list", map[string]any{"limit": 100, "includeHidden": false}, 20*time.Second)
	if err != nil {
		return backend.Definition{}, err
	}
	var response struct {
		Data []struct {
			ID          string `json:"id"`
			Model       string `json:"model"`
			DisplayName string `json:"displayName"`
			Description string `json:"description"`
			Hidden      bool   `json:"hidden"`
			Supported   []struct {
				Effort string `json:"reasoningEffort"`
			} `json:"supportedReasoningEfforts"`
			DefaultEffort       string   `json:"defaultReasoningEffort"`
			InputModalities     []string `json:"inputModalities"`
			SupportsPersonality bool     `json:"supportsPersonality"`
			ServiceTiers        []struct {
				ID          string `json:"id"`
				Name        string `json:"name"`
				Description string `json:"description"`
			} `json:"serviceTiers"`
			DefaultServiceTier *string `json:"defaultServiceTier"`
			IsDefault          bool    `json:"isDefault"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return backend.Definition{}, err
	}
	def := backend.Definition{ID: backend.Codex, Label: "Codex", Capabilities: backend.Capabilities{History: true, Usage: true, Interactions: true, Sandbox: true, Images: true, Files: true, Steering: true}}
	for _, m := range response.Data {
		if m.Hidden {
			continue
		}
		id := firstNonEmpty(m.Model, m.ID)
		bm := backend.Model{ID: id, Label: firstNonEmpty(m.DisplayName, id), Description: m.Description, DefaultReasoningEffort: m.DefaultEffort, InputModalities: m.InputModalities, SupportsPersonality: m.SupportsPersonality, IsDefault: m.IsDefault}
		for _, e := range m.Supported {
			bm.SupportedReasoningEfforts = append(bm.SupportedReasoningEfforts, e.Effort)
		}
		for _, t := range m.ServiceTiers {
			bm.ServiceTiers = append(bm.ServiceTiers, backend.ServiceTier{ID: t.ID, Name: t.Name, Description: t.Description})
		}
		if m.DefaultServiceTier != nil {
			bm.DefaultServiceTier = *m.DefaultServiceTier
		}
		def.Models = append(def.Models, bm)
		if m.IsDefault {
			def.DefaultModel = id
		}
	}
	if def.DefaultModel == "" && len(def.Models) > 0 {
		def.DefaultModel = def.Models[0].ID
	}
	if modesRaw, modeErr := c.rpcCall("collaborationMode/list", map[string]any{}, 10*time.Second); modeErr == nil {
		var modes struct {
			Data []struct {
				Name   string  `json:"name"`
				Mode   *string `json:"mode"`
				Model  *string `json:"model"`
				Effort *string `json:"reasoning_effort"`
			} `json:"data"`
		}
		if json.Unmarshal(modesRaw, &modes) == nil {
			modeMap := make(map[string]map[string]any)
			for _, m := range modes.Data {
				mode := "default"
				if m.Mode != nil {
					mode = *m.Mode
				}
				model := def.DefaultModel
				if m.Model != nil {
					model = *m.Model
				}
				effort := any(nil)
				if m.Effort != nil {
					effort = *m.Effort
				}
				def.CollaborationModes = append(def.CollaborationModes, backend.CollaborationMode{Name: m.Name, Mode: mode, Model: model, ReasoningEffort: func() string {
					if m.Effort != nil {
						return *m.Effort
					}
					return ""
				}()})
				modeMap[strings.ToLower(m.Name)] = map[string]any{"mode": mode, "settings": map[string]any{"model": model, "reasoning_effort": effort, "developer_instructions": nil}}
				modeMap[strings.ToLower(mode)] = modeMap[strings.ToLower(m.Name)]
			}
			c.catalogMu.Lock()
			c.collaborationModes = modeMap
			c.catalogMu.Unlock()
		}
	}
	c.catalogMu.Lock()
	c.catalog = def
	c.catalogMu.Unlock()
	return def, nil
}

func (c *Codex) invalidateLiveThreads() {
	c.mu.Lock()
	c.threadToSession = make(map[string]*session.Session)
	c.activeThreadOwner = make(map[string]*session.Session)
	states := make([]*codexState, 0, len(c.states))
	for _, st := range c.states {
		states = append(states, st)
	}
	c.mu.Unlock()

	for _, st := range states {
		st.mu.Lock()
		st.threadID = ""
		st.currentTurnID = ""
		st.mu.Unlock()
	}
}

// rpcCall sends an RPC and waits for the response. Writes go straight to the
// pipe, so there is no flush step.
func (c *Codex) rpcCall(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	return c.rpc.request(method, params, timeout)
}

// readLoop consumes the app-server's newline-delimited stdout. It uses a
// bufio.Reader rather than a bufio.Scanner because a thread/resume reply can
// serialize an entire thread's history into a single line — hundreds of MB,
// far beyond any fixed buffer cap. Scanner would hit ErrTooLong, silently end
// the loop, and (since the app-server is a singleton) wedge every codex
// session. ReadBytes grows unbounded, matching the Python reference's
// readline().
func (c *Codex) readLoop(stdout io.Reader) {
	r := bufio.NewReaderSize(stdout, 64*1024)
	for {
		line, err := r.ReadBytes('\n')
		if n := trimLineEnd(line); n > 0 {
			raw := make(json.RawMessage, n)
			copy(raw, line[:n])
			if !c.rpc.dispatchResponse(raw) {
				c.dispatch(raw)
			}
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("[codex] read loop error: %v", err)
			}
			break
		}
	}
	log.Printf("[codex] read loop exited")
}

// trimLineEnd returns the content length of a line read by ReadBytes('\n'),
// excluding the trailing \n and an optional preceding \r.
func trimLineEnd(line []byte) int {
	n := len(line)
	if n > 0 && line[n-1] == '\n' {
		n--
		if n > 0 && line[n-1] == '\r' {
			n--
		}
	}
	return n
}

type codexMsg struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type codexTokenUsage struct {
	Last struct {
		TotalTokens  int `json:"totalTokens"`
		TotalTokens2 int `json:"total_tokens"`
	} `json:"last"`
	ModelContextWindow  int `json:"modelContextWindow"`
	ModelContextWindow2 int `json:"model_context_window"`
}

func (c *Codex) dispatch(raw json.RawMessage) {
	var m codexMsg
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	// Server→client request (has id + method, no result/error).
	if len(m.ID) > 0 && string(m.ID) != "null" && m.Method != "" {
		// Keep the request ID as raw JSON so app-server string IDs and numeric
		// IDs are echoed with their original type and value.
		c.handleServerRequest(m.ID, m.Method, m.Params)
		return
	}

	var p struct {
		ThreadID string            `json:"threadId"`
		Diff     string            `json:"diff"`
		Message  string            `json:"message"`
		Changes  []map[string]any  `json:"changes"`
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
			ID                string          `json:"id"`
			Name              string          `json:"name"`
			Type              string          `json:"type"`
			Command           json.RawMessage `json:"command"`
			Output            json.RawMessage `json:"output"`
			Status            string          `json:"status"`
			Tool              string          `json:"tool"`
			SenderThreadID    string          `json:"senderThreadId"`
			ReceiverThreadIDs []string        `json:"receiverThreadIds"`
			Prompt            *string         `json:"prompt"`
			Model             *string         `json:"model"`
			AgentsStates      map[string]struct {
				Status  string  `json:"status"`
				Message *string `json:"message"`
			} `json:"agentsStates"`
		} `json:"item"`
		Thread struct {
			ID             string  `json:"id"`
			ParentThreadID *string `json:"parentThreadId"`
			AgentNickname  *string `json:"agentNickname"`
			AgentRole      *string `json:"agentRole"`
		} `json:"thread"`
		WillRetry bool `json:"willRetry"`
		Error     struct {
			Message string `json:"message"`
		} `json:"error"`
		TokenUsage codexTokenUsage `json:"tokenUsage"`
		Usage      codexTokenUsage `json:"usage"`
		Goal       backend.Goal    `json:"goal"`
	}
	_ = json.Unmarshal(m.Params, &p)
	if p.ThreadID == "" {
		p.ThreadID = p.Thread.ID
	}

	c.mu.Lock()
	s := c.activeThreadOwner[p.ThreadID]
	if s == nil {
		s = c.threadToSession[p.ThreadID]
	}
	if s == nil && p.Thread.ParentThreadID != nil {
		s = c.activeThreadOwner[*p.Thread.ParentThreadID]
		if s == nil {
			s = c.threadToSession[*p.Thread.ParentThreadID]
		}
		if s != nil {
			c.threadToSession[p.ThreadID] = s
		}
	}
	c.mu.Unlock()
	if s == nil {
		return
	}
	st := c.state(s.ID)
	st.touch(time.Now())
	st.mu.Lock()
	reqID := st.reqID
	rootThreadID := st.threadID
	st.mu.Unlock()
	isRootThread := p.ThreadID == rootThreadID

	switch m.Method {
	case "thread/started":
		if p.Thread.ParentThreadID != nil {
			c.ensureCodexAgent(s, p.Thread.ID, *p.Thread.ParentThreadID, firstPtr(p.Thread.AgentRole, p.Thread.AgentNickname, "subagent"), "")
			c.emitCodexAgentTree(s)
		}

	case "turn/started":
		if !isRootThread {
			return
		}
		st.mu.Lock()
		st.currentTurnID = p.Turn.ID
		st.mu.Unlock()
		c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "thinking", ""))

	case "item/agentMessage/delta":
		if p.Delta == "" {
			return
		}
		if !isRootThread {
			c.appendCodexAgentOutput(st, p.ThreadID, p.Delta)
			c.emitCodexAgentTree(s)
			return
		}
		if p.Phase == "commentary" {
			// Commentary is user-facing progress (the same channel used for
			// intermediate assistant updates), not private model reasoning.  Sending
			// it as thinking_chunk buries every live update in a collapsed thinking
			// card and makes an active Codex turn look as if it is not streaming.
			c.sink.Emit(backend.NewTextChunk(s.ID, reqID, p.Delta))
			return
		}
		c.appendCodexAgentOutput(st, p.ThreadID, p.Delta)
		st.mu.Lock()
		st.accumulatedText += p.Delta
		st.mu.Unlock()
		c.sink.Emit(backend.NewTextChunk(s.ID, reqID, p.Delta))

	case "item/reasoning/textDelta":
		if !isRootThread {
			return
		}
		d := p.Delta
		if d == "" {
			d = p.Text
		}
		if d != "" {
			c.sink.Emit(backend.NewThinkingChunk(s.ID, reqID, d))
		}

	case "item/commandExecution/outputDelta", "item/fileChange/outputDelta", "item/commandExecution/terminalInteraction":
		if !isRootThread {
			return
		}
		itemID := firstNonEmpty(p.ItemID, p.CallID, "codex_item")
		d := p.Delta
		if d == "" {
			d = p.Text
		}
		if d != "" {
			c.tools.Delta(s.ID, reqID, itemID, d)
		}

	case "item/started":
		if p.Item.Type == "collabAgentToolCall" {
			c.updateCodexCollabItem(s, st, p.Item.Tool, p.Item.SenderThreadID, p.Item.ReceiverThreadIDs, p.Item.Prompt, p.Item.Model, p.Item.AgentsStates, time.Now().UnixMilli())
			return
		}
		c.recordCodexAgentTool(st, p.ThreadID, p.Item.Type, time.Now().UnixMilli())
		if !isRootThread {
			c.emitCodexAgentTree(s)
			return
		}
		tool, ok := normalizeCodexLiveTool(m.Params)
		if !ok {
			return
		}
		c.tools.Start(s.ID, reqID, tool.ID, tool.Name, tool.Command)

	case "item/completed":
		if p.Item.Type == "collabAgentToolCall" {
			c.updateCodexCollabItem(s, st, p.Item.Tool, p.Item.SenderThreadID, p.Item.ReceiverThreadIDs, p.Item.Prompt, p.Item.Model, p.Item.AgentsStates, time.Now().UnixMilli())
			return
		}
		if !isRootThread {
			c.emitCodexAgentTree(s)
			return
		}
		if _, isTool := normalizeCodexLiveTool(m.Params); !isTool {
			return
		}
		itemID := firstNonEmpty(p.ItemID, p.Item.ID, "codex_item")
		rawOutput := p.Output
		if len(rawOutput) == 0 || string(rawOutput) == "null" {
			rawOutput = p.Item.Output
		}
		generatedPaths := codexGeneratedImagePaths(rawOutput)
		output := rawToString(rawOutput)
		if codexHasInlineImage(rawOutput) {
			if len(generatedPaths) > 0 {
				output = "Generated image:\n" + strings.Join(generatedPaths, "\n")
			} else {
				output = "Generated image completed, but no saved file path was returned."
			}
		}
		if output != "" {
			c.tools.Result(s.ID, reqID, itemID, output)
		}
		c.tools.End(s.ID, reqID, itemID)
		for _, path := range generatedPaths {
			c.sink.Emit(protocol.Media{
				Type: "media", SessionID: s.ID, RequestID: reqID,
				MediaType: "image", Path: path,
			})
		}

	case "turn/plan/updated":
		// Codex update_plan → normalized todo panel. Full replace; step→content.
		if todos := normalizeFullList(p.Plan, "step"); len(todos) > 0 {
			c.sink.Emit(backend.NewTodoUpdate(s.ID, reqID, todosValue(todos)))
		}

	case "turn/diff/updated":
		c.sink.Emit(protocol.NewCodexLiveDiff(s.ID, reqID, p.Diff))

	case "item/fileChange/patchUpdated":
		itemID := firstNonEmpty(p.ItemID, "codex_patch")
		c.tools.Delta(s.ID, reqID, itemID, codexJSON(p.Changes))

	case "item/mcpToolCall/progress":
		itemID := firstNonEmpty(p.ItemID, "codex_mcp")
		c.tools.Delta(s.ID, reqID, itemID, p.Message+"\n")

	case "thread/goal/updated":
		if p.Goal.ThreadID != "" {
			c.sink.Emit(backend.NewGoalUpdate(s.ID, p.Goal))
		}

	case "thread/goal/cleared":
		c.sink.Emit(backend.NewGoalCleared(s.ID))

	case "turn/completed":
		if !isRootThread {
			c.finishCodexAgent(st, p.ThreadID, time.Now().UnixMilli())
			c.emitCodexAgentTree(s)
			return
		}
		if p.Turn.Status == "failed" {
			if p.Turn.Error.Message == "" {
				p.Turn.Error.Message = "turn failed"
			}
			if st.compactActive && !st.turnActive {
				st.finishCompact(p.Turn.Error.Message)
			} else {
				st.finish(p.Turn.Error.Message)
			}
		} else {
			if st.compactActive && !st.turnActive {
				st.finishCompact("")
			} else {
				st.finish("")
			}
		}

	case "thread/compacted":
		st.finishCompact("")

	case "thread/tokenUsage/updated":
		if !isRootThread {
			return
		}
		used, maxCtx := codexUsageValues(p.TokenUsage)
		if used == 0 && maxCtx == 0 {
			used, maxCtx = codexUsageValues(p.Usage)
		}
		st.mu.Lock()
		if used > 0 {
			st.contextUsed = used
		}
		if maxCtx > 0 {
			st.contextMax = maxCtx
		}
		st.mu.Unlock()

	case "error":
		if !isRootThread {
			c.finishCodexAgent(st, p.ThreadID, time.Now().UnixMilli())
			c.emitCodexAgentTree(s)
			return
		}
		if !p.WillRetry {
			msg := p.Error.Message
			if msg == "" {
				msg = "unknown codex error"
			}
			st.finish(msg)
		}
	}
}

func codexHasInlineImage(raw json.RawMessage) bool {
	return codexInlineImageRE.Match(bytes.TrimSpace(raw))
}

func codexGeneratedImagePaths(raw json.RawMessage) []string {
	if !codexHasInlineImage(raw) {
		return nil
	}
	seen := make(map[string]bool)
	var paths []string
	for _, match := range codexGeneratedImagePathRE.FindAllSubmatch(raw, -1) {
		if len(match) < 2 {
			continue
		}
		path := strings.ReplaceAll(string(match[1]), `\/`, "/")
		if seen[path] {
			continue
		}
		info, err := os.Stat(path)
		if err != nil || info.IsDir() {
			continue
		}
		seen[path] = true
		paths = append(paths, path)
	}
	return paths
}

func firstPtr(a, b *string, fallback string) string {
	if a != nil && *a != "" {
		return *a
	}
	if b != nil && *b != "" {
		return *b
	}
	return fallback
}

func (c *Codex) ensureCodexAgent(s *session.Session, id, parentID, agentType, description string) {
	if id == "" {
		return
	}
	st := c.state(s.ID)
	now := time.Now().UnixMilli()
	st.mu.Lock()
	a := st.agents[id]
	if a == nil {
		a = &codexAgent{id: id, startMS: &now}
		st.agents[id] = a
	}
	if parentID != st.threadID {
		a.parentID = parentID
	}
	if agentType != "" {
		a.agentType = agentType
	}
	if description != "" {
		a.description = description
	}
	st.mu.Unlock()
	c.mu.Lock()
	c.threadToSession[id] = s
	c.mu.Unlock()
}

func (c *Codex) updateCodexCollabItem(s *session.Session, st *codexState, tool, sender string, receivers []string, prompt, model *string, states map[string]struct {
	Status  string  `json:"status"`
	Message *string `json:"message"`
}, now int64) {
	desc := firstPtr(prompt, nil, "")
	kind := firstPtr(model, nil, "subagent")
	for _, id := range receivers {
		c.ensureCodexAgent(s, id, sender, kind, desc)
	}
	st.mu.Lock()
	for id, state := range states {
		a := st.agents[id]
		if a == nil {
			a = &codexAgent{id: id, agentType: kind, description: desc, startMS: &now}
			st.agents[id] = a
		}
		if state.Message != nil && *state.Message != "" {
			a.output = *state.Message
		}
		switch state.Status {
		case "completed", "errored", "interrupted", "shutdown", "notFound":
			end := now
			a.endMS = &end
		}
	}
	st.mu.Unlock()
	if tool == "closeAgent" {
		for _, id := range receivers {
			st.mu.Lock()
			if a := st.agents[id]; a != nil {
				end := now
				a.endMS = &end
			}
			st.mu.Unlock()
		}
	}
	c.emitCodexAgentTree(s)
}

func (c *Codex) recordCodexAgentTool(st *codexState, threadID, name string, now int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if a := st.agents[threadID]; a != nil && name != "" {
		ts := now
		a.tools = append(a.tools, protocol.AgentToolCall{Name: name, TS: &ts})
	}
}

func (c *Codex) appendCodexAgentOutput(st *codexState, threadID, delta string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if a := st.agents[threadID]; a != nil {
		a.output += delta
		if len(a.output) > 1000 {
			a.output = a.output[len(a.output)-1000:]
		}
	}
}

func (c *Codex) finishCodexAgent(st *codexState, threadID string, now int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if a := st.agents[threadID]; a != nil {
		end := now
		a.endMS = &end
	}
}

func (c *Codex) emitCodexAgentTree(s *session.Session) {
	total, tree := c.BuildAgentTree(s.ResumeID())
	c.sink.Emit(protocol.NewAgentTree(s.ID, s.ResumeID(), total, tree))
}

// BuildAgentTree satisfies core.agentTreeBuilder for Codex and uses live
// app-server lifecycle data instead of parsing Claude transcripts.
func (c *Codex) BuildAgentTree(resumeID string) (int, []*protocol.AgentNode) {
	c.mu.Lock()
	var st *codexState
	for _, candidate := range c.states {
		candidate.mu.Lock()
		match := candidate.threadID == resumeID
		candidate.mu.Unlock()
		if match {
			st = candidate
			break
		}
	}
	c.mu.Unlock()
	if st == nil {
		return 0, []*protocol.AgentNode{}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	children := map[string][]*codexAgent{}
	for _, a := range st.agents {
		children[a.parentID] = append(children[a.parentID], a)
	}
	var build func(*codexAgent) *protocol.AgentNode
	build = func(a *codexAgent) *protocol.AgentNode {
		n := &protocol.AgentNode{AgentID: a.id, AgentType: firstNonEmpty(a.agentType, "subagent"), Description: a.description, StartTS: a.startMS, EndTS: a.endMS, ToolCalls: append([]protocol.AgentToolCall{}, a.tools...), OutputPreview: a.output, Children: []*protocol.AgentNode{}}
		if a.parentID != "" && a.parentID != st.threadID {
			p := a.parentID
			n.ParentAgentID = &p
		}
		if a.startMS != nil && a.endMS != nil {
			d := *a.endMS - *a.startMS
			n.DurationMS = &d
		}
		for _, child := range children[a.id] {
			n.Children = append(n.Children, build(child))
		}
		return n
	}
	roots := []*protocol.AgentNode{}
	for _, a := range st.agents {
		if a.parentID == "" || a.parentID == st.threadID {
			roots = append(roots, build(a))
		}
	}
	return len(st.agents), roots
}

func (c *Codex) handleServerRequest(id any, method string, raw json.RawMessage) {
	approval := map[string]bool{
		"item/commandExecution/requestApproval": true,
		"item/fileChange/requestApproval":       true,
		"item/permissions/requestApproval":      true,
		"applyPatchApproval":                    true,
		"execCommandApproval":                   true,
	}
	switch {
	case approval[method]:
		c.emitCodexApproval(method, raw)
		_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"decision": "accept"}})
	case method == "item/tool/requestUserInput":
		if !c.createUserInputRequest(id, raw) {
			_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"answers": []any{}}})
		}
	case method == "mcpServer/elicitation/request":
		if result, handled := mcpAutomaticResponse(raw); handled {
			_ = c.rpc.write(map[string]any{"id": id, "result": result})
		} else if !c.createMcpElicitationRequest(id, raw) {
			_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"action": "cancel", "content": nil, "_meta": nil}})
		}
	case method == "item/tool/call":
		if !c.createDynamicToolRequest(id, raw) {
			toolName := c.emitUnsupportedCodexTool(raw)
			_ = c.rpc.write(map[string]any{"id": id, "error": map[string]any{"code": -32000, "message": "Codex hosted tool '" + toolName + "' is not supported by this bridge"}})
		}
	case method == "currentTime/read":
		_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"currentTimeAt": time.Now().Unix()}})
	default:
		_ = c.rpc.write(map[string]any{"id": id, "error": map[string]any{"code": -32601, "message": "unknown method: " + method}})
	}
}

func (c *Codex) emitCodexApproval(method string, raw json.RawMessage) {
	params := codexParams(raw)
	s := c.sessionForCodexParams(params)
	if s == nil {
		return
	}
	itemID := codexRequestItemID(params, "codex_approval_"+randHex(8))
	command := codexAnyString(codexFirstAny(params, "command", "changes", "permission"))
	summary := map[string]any{
		"method":        method,
		"environmentId": codexAnyString(codexFirstAny(params, "environmentId", "environment_id")),
		"cwd":           codexAnyString(codexFirstAny(params, "cwd", "workingDirectory")),
	}
	c.tools.Start(s.ID, c.state(s.ID).reqID, itemID, "codex_approval", command)
	c.tools.Result(s.ID, c.state(s.ID).reqID, itemID, codexJSON(summary))
	c.tools.End(s.ID, c.state(s.ID).reqID, itemID)
}

func (c *Codex) emitUnsupportedCodexTool(raw json.RawMessage) string {
	params := codexParams(raw)
	toolName := codexRequestToolName(params)
	s := c.sessionForCodexParams(params)
	if s == nil {
		return toolName
	}
	itemID := codexRequestItemID(params, "codex_tool_"+randHex(8))
	command := codexAnyString(codexFirstAny(params, "input", "arguments", "args"))
	c.tools.Start(s.ID, c.state(s.ID).reqID, itemID, toolName, command)
	c.tools.Result(s.ID, c.state(s.ID).reqID, itemID, "Unsupported Codex hosted tool: "+toolName)
	c.tools.End(s.ID, c.state(s.ID).reqID, itemID)
	return toolName
}

func (c *Codex) createUserInputRequest(rpcID any, raw json.RawMessage) bool {
	var p struct {
		ThreadID  string                     `json:"threadId"`
		ItemID    string                     `json:"itemId"`
		CallID    string                     `json:"callId"`
		ToolID    string                     `json:"toolUseId"`
		Kind      string                     `json:"kind"`
		Header    string                     `json:"header"`
		Title     string                     `json:"title"`
		Agent     string                     `json:"requesting_agent"`
		Questions []map[string]any           `json:"questions"`
		Thread    map[string]json.RawMessage `json:"thread"`
		Item      map[string]json.RawMessage `json:"item"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return false
	}
	threadID := p.ThreadID
	if threadID == "" && p.Thread != nil {
		var id string
		_ = json.Unmarshal(p.Thread["id"], &id)
		threadID = id
	}
	s := c.sessionForThread(threadID)
	if s == nil {
		return false
	}
	toolID := firstNonEmpty(p.ItemID, p.CallID, p.ToolID)
	if toolID == "" && p.Item != nil {
		var id string
		_ = json.Unmarshal(p.Item["id"], &id)
		toolID = id
	}
	reqID := "ui_" + randHex(12)
	header := firstNonEmpty(p.Header, p.Title, "Question")
	kind := firstNonEmpty(p.Kind, "ask_user_question")
	agent := firstNonEmpty(p.Agent, "codex")
	payload := backend.UserInputPayload{
		RequestID: reqID, SessionID: s.ID, Source: "codex", Kind: kind,
		Header: header, ToolUseID: toolID, RequestingAgent: agent,
		Questions: normalizeCodexQuestions(p.Questions), CreatedAt: time.Now().UnixMilli(),
		Status: "pending",
	}
	c.interMu.Lock()
	c.interactions[reqID] = codexInteraction{payload: payload, rpcID: rpcID}
	c.interMu.Unlock()
	c.sink.Emit(backend.NewUserInputRequest(payload))
	return true
}

func (c *Codex) createMcpElicitationRequest(rpcID any, raw json.RawMessage) bool {
	var p struct {
		ThreadID        string         `json:"threadId"`
		TurnID          string         `json:"turnId"`
		ServerName      string         `json:"serverName"`
		Mode            string         `json:"mode"`
		Message         string         `json:"message"`
		URL             string         `json:"url"`
		ElicitationID   string         `json:"elicitationId"`
		RequestedSchema map[string]any `json:"requestedSchema"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return false
	}
	s := c.sessionForThread(p.ThreadID)
	if s == nil {
		return false
	}
	params := decodeObject(raw)
	questions := mcpElicitationQuestions(params)
	reqID := "mcp_" + randHex(12)
	payload := backend.UserInputPayload{RequestID: reqID, SessionID: s.ID, Source: "codex", Kind: "mcp_" + p.Mode, Header: firstNonEmpty(p.ServerName, "MCP request"), ToolUseID: p.ElicitationID, RequestingAgent: p.ServerName, Questions: questions, CreatedAt: time.Now().UnixMilli(), Status: "pending"}
	c.interMu.Lock()
	c.interactions[reqID] = codexInteraction{payload: payload, rpcID: rpcID, responseKind: "mcp", mcpParams: append(json.RawMessage(nil), raw...)}
	c.interMu.Unlock()
	c.sink.Emit(backend.NewUserInputRequest(payload))
	return true
}

func (c *Codex) createDynamicToolRequest(rpcID any, raw json.RawMessage) bool {
	var p struct {
		ThreadID  string  `json:"threadId"`
		TurnID    string  `json:"turnId"`
		CallID    string  `json:"callId"`
		Tool      string  `json:"tool"`
		Namespace *string `json:"namespace"`
		Arguments any     `json:"arguments"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return false
	}
	s := c.sessionForThread(p.ThreadID)
	if s == nil {
		return false
	}
	name := p.Tool
	if p.Namespace != nil && *p.Namespace != "" {
		name = *p.Namespace + "/" + name
	}
	reqID := "tool_" + randHex(12)
	payload := backend.UserInputPayload{RequestID: reqID, SessionID: s.ID, Source: "codex", Kind: "dynamic_tool", Header: "Tool input: " + name, ToolUseID: p.CallID, RequestingAgent: name, Questions: []backend.UserInputQuestion{{QuestionID: "result", Text: "Provide the tool result (arguments: " + codexJSON(p.Arguments) + ")", Header: name, Type: "text", FreeForm: true}}, CreatedAt: time.Now().UnixMilli(), Status: "pending"}
	c.interMu.Lock()
	c.interactions[reqID] = codexInteraction{payload: payload, rpcID: rpcID, responseKind: "dynamic_tool", reqID: c.state(s.ID).reqID}
	c.interMu.Unlock()
	c.tools.Start(s.ID, c.state(s.ID).reqID, p.CallID, name, codexJSON(p.Arguments))
	c.sink.Emit(backend.NewUserInputRequest(payload))
	return true
}

func (c *Codex) emitExtractedAskUserQuestion(s *session.Session, st *codexState) {
	st.mu.Lock()
	if st.askExtracted {
		st.mu.Unlock()
		return
	}
	text := st.accumulatedText
	st.mu.Unlock()

	data := extractCodexAskUserQuestion(text)
	if data == nil {
		return
	}
	questions := normalizeCodexQuestions(codexQuestionMaps(codexFirstAny(data, "questions")))
	reqID := "ui_" + randHex(12)
	toolID := firstNonEmpty(
		codexFirstString(data, "tool_use_id", "toolUseId", "itemId", "id"),
		"ask_user_"+randHex(8),
	)
	header := firstNonEmpty(codexFirstString(data, "header", "title"), "Question")
	payload := backend.UserInputPayload{
		RequestID: reqID, SessionID: s.ID, Source: "codex", Kind: "ask_user_question",
		Header: header, ToolUseID: toolID, RequestingAgent: "AskUserQuestion",
		Questions: questions, CreatedAt: time.Now().UnixMilli(), Status: "pending",
	}
	c.interMu.Lock()
	c.interactions[reqID] = codexInteraction{payload: payload}
	c.interMu.Unlock()

	st.mu.Lock()
	st.askExtracted = true
	st.mu.Unlock()
	c.sink.Emit(backend.NewUserInputRequest(payload))
}

func extractCodexAskUserQuestion(text string) map[string]any {
	for _, match := range codexAskUserRE.FindAllStringSubmatch(text, -1) {
		raw := ""
		if len(match) > 1 {
			raw = match[1]
		}
		if raw == "" && len(match) > 2 {
			raw = match[2]
		}
		if raw == "" {
			continue
		}
		var data map[string]any
		if json.Unmarshal([]byte(raw), &data) != nil {
			continue
		}
		kind := strings.ToLower(strings.TrimSpace(codexFirstString(data, "type")))
		if kind == "ask_user_question" || kind == "askuserquestion" {
			return data
		}
	}
	return nil
}

func codexParams(raw json.RawMessage) map[string]any {
	var params map[string]any
	if json.Unmarshal(raw, &params) != nil || params == nil {
		return map[string]any{}
	}
	return params
}

func (c *Codex) sessionForCodexParams(params map[string]any) *session.Session {
	threadID := codexAnyString(codexFirstAny(params, "threadId", "thread_id"))
	if threadID == "" {
		if thread, ok := params["thread"].(map[string]any); ok {
			threadID = codexAnyString(thread["id"])
		}
	}
	return c.sessionForThread(threadID)
}

func (c *Codex) sessionForThread(threadID string) *session.Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.activeThreadOwner[threadID]; s != nil {
		return s
	}
	return c.threadToSession[threadID]
}

func codexRequestItemID(params map[string]any, fallback string) string {
	if item, ok := params["item"].(map[string]any); ok {
		if id := codexAnyString(item["id"]); id != "" {
			return id
		}
	}
	if id := codexAnyString(codexFirstAny(params, "itemId", "callId", "toolCallId", "toolUseId")); id != "" {
		return id
	}
	return fallback
}

func codexRequestToolName(params map[string]any) string {
	if tool, ok := params["tool"].(map[string]any); ok {
		if name := codexAnyString(codexFirstAny(tool, "name", "type")); name != "" {
			return name
		}
	}
	if item, ok := params["item"].(map[string]any); ok {
		if name := codexAnyString(codexFirstAny(item, "name", "type")); name != "" {
			return name
		}
	}
	if name := codexAnyString(codexFirstAny(params, "name", "toolName")); name != "" {
		return name
	}
	return "codex_tool"
}

func codexAnyString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case nil:
		return ""
	case map[string]any, []any:
		return codexJSON(x)
	default:
		return strings.TrimSpace(fmt.Sprint(x))
	}
}

func codexJSON(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(raw)
}

// --- Executor interface ----------------------------------------------------

func (c *Codex) Send(ctx context.Context, s *session.Session, reqID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) error {
	sandboxOverride, _ := backend.SandboxOverride(ctx)
	if err := c.ensureServer(); err != nil {
		c.sink.Emit(backend.NewError(s.ID, reqID, backend.ErrProcessDied, "codex app-server failed: "+err.Error()))
		return err
	}
	st := c.state(s.ID)
	st.mu.Lock()
	threadAlreadyLoaded := st.threadID != ""
	st.mu.Unlock()
	if !threadAlreadyLoaded {
		if s.ResumeID() != "" {
			c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "resuming_thread", "Loading the existing Codex conversation"))
		} else {
			c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "starting_thread", "Creating a Codex conversation"))
		}
	}

	if err := c.ensureThread(s, st); err != nil {
		if !errors.Is(err, backend.ErrThreadActiveWriter) {
			c.sink.Emit(backend.NewError(s.ID, reqID, backend.ErrProcessDied, "failed to start codex thread: "+err.Error()))
		}
		return err
	}
	c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "submitting_turn", "Sending the turn to Codex"))
	isCompactCommand := strings.TrimSpace(content) == "/compact"
	if isCompactCommand {
		st.mu.Lock()
		st.reqID = reqID
		st.stopping = false
		st.mu.Unlock()
		c.sink.Emit(backend.NewSessionCommandStarted(s.ID, reqID, 0))
		go c.runCompactCommand(s, st, reqID)
		return nil
	}
	st.mu.Lock()
	handoff := st.pendingHandoff
	st.pendingHandoff = ""
	st.mu.Unlock()
	if handoff != "" {
		content = handoff + "\n\n<current_user_request>\n" + content + "\n</current_user_request>"
	}

	st.mu.Lock()
	st.reqID = reqID
	st.stopping = false
	st.turnErr = ""
	st.turnActive = true
	st.turnDone = make(chan struct{})
	st.accumulatedText = ""
	st.askExtracted = false
	st.currentTurnID = ""
	st.lastEventAt = time.Now()
	st.stallWarned = false
	threadID := st.threadID
	done := st.turnDone
	st.mu.Unlock()

	if err := c.claimActiveThread(threadID, s); err != nil {
		st.finish(err.Error())
		return err
	}
	input := c.codexInput(s, reqID, content, images, files, st)
	go c.runTurn(s, st, threadID, input, done, sandboxOverride)
	return nil
}

// Steer injects another user message into the currently active Codex turn.
// The app-server validates expectedTurnId atomically, so a turn that finishes
// between the local snapshot and the RPC is safely rejected instead of being
// attached to the next turn.
func (c *Codex) Steer(ctx context.Context, s *session.Session, clientUserMessageID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) (backend.SteerResult, error) {
	if err := c.ensureServer(); err != nil {
		return backend.SteerResult{}, err
	}
	return c.steerActiveTurn(s, clientUserMessageID, content, images, files)
}

func (c *Codex) steerActiveTurn(s *session.Session, clientUserMessageID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) (backend.SteerResult, error) {
	st := c.state(s.ID)
	st.mu.Lock()
	active := st.turnActive
	threadID := st.threadID
	turnID := st.currentTurnID
	activeRequestID := st.reqID
	st.mu.Unlock()
	if !active || threadID == "" || turnID == "" {
		return backend.SteerResult{}, backend.ErrNoActiveTurn
	}

	input := c.codexInput(s, clientUserMessageID, content, images, files, st)
	params := map[string]any{
		"threadId":            threadID,
		"expectedTurnId":      turnID,
		"input":               input,
		"clientUserMessageId": clientUserMessageID,
	}
	raw, err := c.rpcCall("turn/steer", params, 15*time.Second)
	if err != nil {
		return backend.SteerResult{}, err
	}
	var response struct {
		TurnID string `json:"turnId"`
	}
	if json.Unmarshal(raw, &response) != nil || response.TurnID == "" {
		response.TurnID = turnID
	}
	st.touch(time.Now())
	return backend.SteerResult{TurnID: response.TurnID, RequestID: activeRequestID}, nil
}

func (c *Codex) runTurn(s *session.Session, st *codexState, threadID string, input []map[string]any, done chan struct{}, sandboxOverride string) {
	defer c.cleanupTempImages(st)
	st.mu.Lock()
	requestID := st.reqID
	st.mu.Unlock()
	c.sink.Emit(backend.NewTurnProgress(s.ID, requestID, "waiting_model", "Waiting for Codex to accept the turn"))
	if err := c.startTurnWithStaleRetry(s, st, threadID, input, sandboxOverride); err != nil {
		st.finish("turn/start failed: " + err.Error())
	}

	timeout := time.NewTimer(c.turnTimeout)
	defer timeout.Stop()
	stallTicker := time.NewTicker(c.stallCheckEvery)
	defer stallTicker.Stop()

	waiting := true
	for waiting {
		select {
		case <-done:
			waiting = false
		case <-timeout.C:
			// A request_user_input / MCP elicitation is an explicit hand-off to a
			// human. Human think time must not consume the turn deadline: the app
			// may be backgrounded or disconnected for hours and will recover the
			// pending interaction when it reconnects. Re-arm the full execution
			// window while any interaction for this session remains unresolved.
			// Once the user answers (or explicitly cancels), the next deadline is
			// again an ordinary execution deadline.
			if c.deferTurnDeadlineForInput(s.ID, timeout) {
				continue
			}
			c.interruptCodexTurn(st)
			st.finish("Codex turn timed out")
			<-done
			waiting = false
		case now := <-stallTicker.C:
			st.mu.Lock()
			lastEventAt := st.lastEventAt
			warned := st.stallWarned
			st.mu.Unlock()
			action := codexLivenessActionAt(
				now, lastEventAt, warned, c.hasPendingInteraction(s.ID),
				c.stallWarnAfter, c.stallAbortAfter,
			)
			switch action {
			case codexLivenessWarn:
				st.mu.Lock()
				st.stallWarned = true
				st.mu.Unlock()
				c.sink.Emit(backend.NewSessionWarning(s.ID, fmt.Sprintf(
					"Codex has produced no events for %s; it is still running and will be interrupted after %s of inactivity.",
					c.stallWarnAfter.Round(time.Second), c.stallAbortAfter.Round(time.Second),
				)))
			case codexLivenessAbort:
				c.interruptCodexTurn(st)
				st.finish(fmt.Sprintf("Codex turn stalled for %s and was interrupted", c.stallAbortAfter.Round(time.Second)))
				<-done
				waiting = false
			}
		}
	}

	st.mu.Lock()
	completedThreadID := st.threadID
	st.mu.Unlock()
	c.finalizePendingRecovery(s, st, completedThreadID)

	st.mu.Lock()
	stopping, turnErr := st.stopping, st.turnErr
	st.mu.Unlock()

	// Release before emitting the terminal event. Hub.Emit(done/error/stopped)
	// immediately unlocks the per-session queue, so a deferred release could
	// race the next turn from the same session and delete its newly acquired
	// route.
	c.releaseActiveThreads(s)

	switch {
	case stopping || turnErr == "stopped":
		c.sink.Emit(backend.NewStopped(s.ID, st.reqID))
	case turnErr != "":
		c.sink.Emit(backend.NewError(s.ID, st.reqID, backend.ErrTurn, turnErr))
	default:
		c.emitExtractedAskUserQuestion(s, st)
		// Goal state is durable thread metadata, separate from turn/completed.
		// Re-read it before emitting done so clients cannot miss a terminal goal
		// transition when the app-server notification was dropped or arrived while
		// they were reconnecting.
		c.reconcileGoalAfterTurn(s, st)
		c.sink.Emit(backend.NewDone(s.ID, st.reqID))
		if c.shouldAutoCompact(st) {
			go c.runAutoCompact(s, st)
		}
	}
}

func (c *Codex) deferTurnDeadlineForInput(sessionID string, timer *time.Timer) bool {
	if !c.hasPendingInteraction(sessionID) {
		return false
	}
	timer.Reset(c.turnTimeout)
	return true
}

type codexLivenessAction uint8

const (
	codexLivenessNone codexLivenessAction = iota
	codexLivenessWarn
	codexLivenessAbort
)

func codexLivenessActionAt(now, lastEventAt time.Time, warned, waitingForInput bool, warnAfter, abortAfter time.Duration) codexLivenessAction {
	if waitingForInput || lastEventAt.IsZero() {
		return codexLivenessNone
	}
	idle := now.Sub(lastEventAt)
	if abortAfter > 0 && idle >= abortAfter {
		return codexLivenessAbort
	}
	if !warned && warnAfter > 0 && idle >= warnAfter {
		return codexLivenessWarn
	}
	return codexLivenessNone
}

func (c *Codex) hasPendingInteraction(sessionID string) bool {
	c.interMu.Lock()
	defer c.interMu.Unlock()
	for _, interaction := range c.interactions {
		if interaction.payload.SessionID == sessionID {
			return true
		}
	}
	return false
}

func (c *Codex) interruptCodexTurn(st *codexState) {
	st.mu.Lock()
	threadID, turnID := st.threadID, st.currentTurnID
	st.mu.Unlock()
	if threadID == "" || turnID == "" {
		return
	}
	if _, err := c.rpcCall("turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, 5*time.Second); err != nil {
		log.Printf("[codex] turn interrupt failed thread=%s turn=%s: %v", threadID, turnID, err)
	}
}

func (c *Codex) runCompactCommand(s *session.Session, st *codexState, reqID string) {
	if err := c.runCompact(st, 120*time.Second); err != nil {
		c.sink.Emit(backend.NewSessionCommandFailed(s.ID, reqID, "compact failed: "+err.Error(), 0))
		c.sink.Emit(backend.NewError(s.ID, reqID, backend.ErrTurn, "compact failed: "+err.Error()))
		return
	}
	c.sink.Emit(backend.NewSessionCommandDone(s.ID, reqID, 0))
	c.sink.Emit(backend.NewDone(s.ID, reqID))
}

func (c *Codex) runAutoCompact(s *session.Session, st *codexState) {
	reqID := "compact_" + s.ID
	c.sink.Emit(backend.NewSessionCommandStarted(s.ID, reqID, 0))
	if err := c.runCompact(st, 120*time.Second); err != nil {
		c.sink.Emit(backend.NewSessionCommandFailed(s.ID, reqID, "compact failed: "+err.Error(), 0))
		log.Printf("[codex] auto compact failed session=%s: %v", s.ID, err)
		return
	}
	c.sink.Emit(backend.NewSessionCommandDone(s.ID, reqID, 0))
}

func (c *Codex) runCompact(st *codexState, timeout time.Duration) error {
	st.mu.Lock()
	if st.compactActive {
		done := st.compactDone
		st.mu.Unlock()
		if done != nil {
			select {
			case <-done:
			case <-time.After(timeout):
				return fmt.Errorf("compact timed out")
			}
		}
		return nil
	}
	threadID := st.threadID
	if threadID == "" {
		st.mu.Unlock()
		return fmt.Errorf("no codex thread")
	}
	st.compactActive = true
	st.compactErr = ""
	st.compactDone = make(chan struct{})
	done := st.compactDone
	st.mu.Unlock()

	if _, err := c.rpcCall("thread/compact/start", map[string]any{"threadId": threadID}, 30*time.Second); err != nil {
		st.finishCompact(err.Error())
	}

	select {
	case <-done:
	case <-time.After(timeout):
		st.finishCompact("compact timed out")
		<-done
	}

	st.mu.Lock()
	errStr := st.compactErr
	st.mu.Unlock()
	if errStr != "" {
		return fmt.Errorf("%s", errStr)
	}
	return nil
}

func (c *Codex) SetGoal(ctx context.Context, s *session.Session, objective, status string, tokenBudget *int) error {
	if err := c.ensureServer(); err != nil {
		c.sink.Emit(backend.NewError(s.ID, "", backend.ErrProcessDied, "codex app-server failed: "+err.Error()))
		return err
	}
	st := c.state(s.ID)
	if err := c.ensureThread(s, st); err != nil {
		if !errors.Is(err, backend.ErrThreadActiveWriter) {
			c.sink.Emit(backend.NewError(s.ID, "", backend.ErrProcessDied, "failed to start codex thread: "+err.Error()))
		}
		return err
	}
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	params := map[string]any{"threadId": threadID}
	if objective != "" {
		params["objective"] = objective
	}
	if status != "" {
		params["status"] = status
	}
	if tokenBudget != nil {
		params["tokenBudget"] = *tokenBudget
	}
	raw, err := c.rpcCall("thread/goal/set", params, 30*time.Second)
	if err != nil {
		c.sink.Emit(backend.NewError(s.ID, "", backend.ErrTurn, "goal set failed: "+err.Error()))
		return err
	}
	goal, ok := decodeCodexGoal(raw)
	if !ok {
		err := fmt.Errorf("thread/goal/set returned no goal")
		c.sink.Emit(backend.NewError(s.ID, "", backend.ErrTurn, err.Error()))
		return err
	}
	c.sink.Emit(backend.NewGoalUpdate(s.ID, goal))
	return nil
}

func (c *Codex) GetGoal(ctx context.Context, s *session.Session) error {
	if err := c.ensureServer(); err != nil {
		c.sink.Emit(backend.NewError(s.ID, "", backend.ErrProcessDied, "codex app-server failed: "+err.Error()))
		return err
	}
	st := c.state(s.ID)
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	if threadID == "" {
		// Goal metadata is daemon-global and can be read by thread id while a
		// desktop TUI owns the writer. Do not resume merely to perform this
		// read-only query; Codex rejects that second attachment as an active writer.
		threadID = s.ResumeID()
	}
	if threadID == "" {
		if err := c.ensureThread(s, st); err != nil {
			if !errors.Is(err, backend.ErrThreadActiveWriter) {
				c.sink.Emit(backend.NewError(s.ID, "", backend.ErrProcessDied, "failed to start codex thread: "+err.Error()))
			}
			return err
		}
		st.mu.Lock()
		threadID = st.threadID
		st.mu.Unlock()
	}
	raw, err := c.rpcCall("thread/goal/get", map[string]any{"threadId": threadID}, 30*time.Second)
	if err != nil && isStaleThreadError(err) {
		// An unloaded historical thread still needs one normal resume. A thread
		// already loaded by a desktop client takes the fast read-only path above.
		if resumeErr := c.ensureThread(s, st); resumeErr != nil {
			if !errors.Is(resumeErr, backend.ErrThreadActiveWriter) {
				c.sink.Emit(backend.NewError(s.ID, "", backend.ErrProcessDied, "failed to start codex thread: "+resumeErr.Error()))
			}
			return resumeErr
		}
		st.mu.Lock()
		threadID = st.threadID
		st.mu.Unlock()
		raw, err = c.rpcCall("thread/goal/get", map[string]any{"threadId": threadID}, 30*time.Second)
	}
	if err != nil {
		c.sink.Emit(backend.NewError(s.ID, "", backend.ErrTurn, "goal get failed: "+err.Error()))
		return err
	}
	goal, ok := decodeCodexGoal(raw)
	if !ok {
		c.sink.Emit(backend.NewGoalCleared(s.ID))
		return nil
	}
	c.sink.Emit(backend.NewGoalUpdate(s.ID, goal))
	return nil
}

func (c *Codex) reconcileGoalAfterTurn(s *session.Session, st *codexState) {
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	if threadID == "" {
		return
	}

	raw, err := c.rpcCall("thread/goal/get", map[string]any{"threadId": threadID}, 3*time.Second)
	if err != nil {
		// A successful turn must remain successful even if this best-effort state
		// reconciliation fails. The frontend also refreshes Goal unconditionally
		// on done, and reconnect receives the durable bridge snapshot.
		log.Printf("[%s] post-turn goal reconcile failed: %v", s.ID, err)
		return
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		log.Printf("[%s] post-turn goal response invalid: %v", s.ID, err)
		return
	}
	rawGoal, exists := envelope["goal"]
	if !exists {
		log.Printf("[%s] post-turn goal response missing goal", s.ID)
		return
	}
	if string(rawGoal) == "null" {
		c.sink.Emit(backend.NewGoalCleared(s.ID))
		return
	}
	var goal backend.Goal
	if err := json.Unmarshal(rawGoal, &goal); err != nil || goal.ThreadID == "" {
		log.Printf("[%s] post-turn goal payload invalid", s.ID)
		return
	}
	c.sink.Emit(backend.NewGoalUpdate(s.ID, goal))
}

func (c *Codex) ClearGoal(ctx context.Context, s *session.Session) error {
	if err := c.ensureServer(); err != nil {
		c.sink.Emit(backend.NewError(s.ID, "", backend.ErrProcessDied, "codex app-server failed: "+err.Error()))
		return err
	}
	st := c.state(s.ID)
	if err := c.ensureThread(s, st); err != nil {
		if !errors.Is(err, backend.ErrThreadActiveWriter) {
			c.sink.Emit(backend.NewError(s.ID, "", backend.ErrProcessDied, "failed to start codex thread: "+err.Error()))
		}
		return err
	}
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	if _, err := c.rpcCall("thread/goal/clear", map[string]any{"threadId": threadID}, 30*time.Second); err != nil {
		c.sink.Emit(backend.NewError(s.ID, "", backend.ErrTurn, "goal clear failed: "+err.Error()))
		return err
	}
	c.sink.Emit(backend.NewGoalCleared(s.ID))
	return nil
}

func decodeCodexGoal(raw json.RawMessage) (backend.Goal, bool) {
	var out struct {
		Goal *backend.Goal `json:"goal"`
	}
	if json.Unmarshal(raw, &out) != nil || out.Goal == nil || out.Goal.ThreadID == "" {
		return backend.Goal{}, false
	}
	return *out.Goal, true
}

func (c *Codex) shouldAutoCompact(st *codexState) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.compactActive || st.threadID == "" || st.contextMax <= 0 || st.contextUsed <= 0 {
		return false
	}
	return float64(st.contextUsed)/float64(st.contextMax) >= codexCompactThreshold
}

func (c *Codex) startTurnWithStaleRetry(s *session.Session, st *codexState, threadID string, input []map[string]any, sandboxOverride string) error {
	snap := s.Snapshot()
	err := c.startTurn(threadID, input, snap, sandboxOverride)
	if err == nil {
		c.finalizePendingRecovery(s, st, threadID)
		return nil
	}
	if !isStaleThreadError(err) {
		return err
	}

	log.Printf("[codex] stale thread on turn/start session=%s thread=%s, respawning", s.ID, threadID)
	c.forgetThread(s, st, threadID)
	if err := c.ensureThread(s, st); err != nil {
		return err
	}
	st.mu.Lock()
	newThreadID := st.threadID
	st.mu.Unlock()
	if err := c.moveActiveThreadClaim(threadID, newThreadID, s); err != nil {
		return err
	}
	if err := c.startTurn(newThreadID, input, snap, sandboxOverride); err != nil {
		return err
	}
	c.finalizePendingRecovery(s, st, newThreadID)
	return nil
}

// finalizePendingRecovery advances the durable session mapping only after
// turn/start succeeds. thread/start alone does not necessarily materialize a
// rollout, so committing in ensureThread can leave a registry pointing at an
// in-memory-only thread after the app-server restarts.
func (c *Codex) finalizePendingRecovery(s *session.Session, st *codexState, threadID string) {
	st.mu.Lock()
	recovery := st.pendingRecovery
	st.mu.Unlock()
	if recovery == nil {
		return
	}
	if !c.waitForCodexRollout(threadID, 2*time.Second) {
		// Keep the transaction pending. runTurn retries finalization after the
		// terminal notification, by which point Codex must have flushed the file.
		log.Printf("[codex] recovery rollout not materialized yet session=%s thread=%s", s.ID, threadID)
		return
	}
	if err := c.commitColdRecovery(recovery, threadID); err != nil {
		// The turn was accepted already. Do not retry it and risk duplicate work;
		// retain the usable Codex thread and surface the secondary metadata error.
		log.Printf("[codex] recovery manifest commit failed session=%s thread=%s: %v", s.ID, threadID, err)
		c.sink.Emit(backend.NewSessionWarning(s.ID, "Codex recovery metadata could not be finalized; the active thread remains usable."))
	}
	st.mu.Lock()
	if st.pendingRecovery == recovery {
		st.pendingRecovery = nil
	}
	st.mu.Unlock()
	s.SetResumeID(threadID)
	c.sink.Emit(backend.NewSessionUUID(s.ID, threadID))
	log.Printf("[codex] recovery committed session=%s thread=%s", s.ID, threadID)
}

func (c *Codex) waitForCodexRollout(threadID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if fileSize(c.findCodexSessionFile(threadID)) > 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (c *Codex) claimActiveThread(threadID string, s *session.Session) error {
	if threadID == "" {
		return errNoThreadID
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if owner := c.activeThreadOwner[threadID]; owner != nil && owner.ID != s.ID {
		return fmt.Errorf("Codex thread %s already has an active turn in session %s", threadID, owner.ID)
	}
	c.activeThreadOwner[threadID] = s
	// Keep the idle/fallback route aligned too. Notifications for an active turn
	// always prefer activeThreadOwner, so another duplicate session cannot steal
	// tool, text, interaction, or terminal events by merely resuming the thread.
	c.threadToSession[threadID] = s
	return nil
}

func (c *Codex) moveActiveThreadClaim(oldThreadID, newThreadID string, s *session.Session) error {
	if newThreadID == "" {
		return errNoThreadID
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if owner := c.activeThreadOwner[newThreadID]; owner != nil && owner.ID != s.ID {
		return fmt.Errorf("Codex thread %s already has an active turn in session %s", newThreadID, owner.ID)
	}
	if owner := c.activeThreadOwner[oldThreadID]; owner == s {
		delete(c.activeThreadOwner, oldThreadID)
	}
	c.activeThreadOwner[newThreadID] = s
	c.threadToSession[newThreadID] = s
	return nil
}

func (c *Codex) releaseActiveThreads(s *session.Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for threadID, owner := range c.activeThreadOwner {
		if owner == s {
			delete(c.activeThreadOwner, threadID)
		}
	}
}

func codexTurnParams(threadID string, input []map[string]any, effort string) map[string]any {
	params := map[string]any{
		"threadId":          threadID,
		"input":             input,
		"approvalPolicy":    "never",
		"approvalsReviewer": "user",
	}
	switch effort {
	case "low", "medium", "high", "xhigh", "max", "ultra":
		params["effort"] = effort
	}
	return params
}

func (c *Codex) codexTurnParamsForSession(threadID string, input []map[string]any, snap session.Snapshot, sandboxOverride string) map[string]any {
	params := codexTurnParams(threadID, input, snap.Effort)
	params["sandboxPolicy"] = codexTurnSandboxPolicy(snap, sandboxOverride)
	if snap.ServiceTier != "" {
		params["serviceTier"] = snap.ServiceTier
	}
	if snap.Personality != "" {
		params["personality"] = snap.Personality
	}
	if snap.CollaborationMode != "" {
		params["collaborationMode"] = c.collaborationModeValue(snap)
	}
	return params
}

func (c *Codex) startTurn(threadID string, input []map[string]any, snap session.Snapshot, sandboxOverride string) error {
	_, err := c.rpcCall("turn/start", c.codexTurnParamsForSession(threadID, input, snap, sandboxOverride), 30*time.Second)
	return err
}

func (c *Codex) collaborationModeValue(snap session.Snapshot) map[string]any {
	c.catalogMu.RLock()
	mode := c.collaborationModes[strings.ToLower(snap.CollaborationMode)]
	c.catalogMu.RUnlock()
	if mode != nil {
		return mode
	}
	return map[string]any{"mode": strings.ToLower(snap.CollaborationMode), "settings": map[string]any{"model": snap.Model, "reasoning_effort": nil, "developer_instructions": nil}}
}

func (c *Codex) forgetThread(s *session.Session, st *codexState, threadID string) {
	st.mu.Lock()
	if st.threadID == threadID {
		st.threadID = ""
	}
	st.currentTurnID = ""
	st.mu.Unlock()
	c.mu.Lock()
	if c.threadToSession[threadID] == s {
		delete(c.threadToSession, threadID)
	}
	if c.activeThreadOwner[threadID] == s {
		delete(c.activeThreadOwner, threadID)
	}
	c.mu.Unlock()
}

func isStaleThreadError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown session") || strings.Contains(msg, "thread not found") ||
		strings.Contains(msg, "no rollout found")
}

func isActiveWriterError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "already has an active writer")
}

func codexUsageValues(u codexTokenUsage) (int, int) {
	used := u.Last.TotalTokens
	if used == 0 {
		used = u.Last.TotalTokens2
	}
	maxCtx := u.ModelContextWindow
	if maxCtx == 0 {
		maxCtx = u.ModelContextWindow2
	}
	return used, maxCtx
}

// ensureThread starts or resumes the codex thread for this session.
func (c *Codex) ensureThread(s *session.Session, st *codexState) error {
	st.ensureMu.Lock()
	defer st.ensureMu.Unlock()
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
	sandbox := codexSandboxForSession(snap)

	var threadID string
	var recovery *codexRecovery
	if snap.ResumeID != "" {
		rolloutPath := c.findCodexSessionFile(snap.ResumeID)
		rolloutBytes := fileSize(rolloutPath)
		if c.rolloverEnabled && rolloutBytes > c.coldResumeMaxBytes {
			var err error
			recovery, err = c.prepareColdRecovery(snap, rolloutPath, rolloutBytes, "cold_resume_hard_limit")
			if err != nil {
				return fmt.Errorf("cold recovery required for %s (%d bytes): %w", snap.ResumeID, rolloutBytes, err)
			}
			log.Printf("[codex] cold resume guarded session=%s thread=%s bytes=%d", s.ID, snap.ResumeID, rolloutBytes)
		} else {
			resumeParams := map[string]any{
				"threadId": snap.ResumeID, "cwd": cwd,
				"approvalPolicy": "never", "approvalsReviewer": "user", "sandbox": sandbox,
			}
			if c.appServerMode == "daemon" {
				// A shared daemon already owns the canonical rollout and only needs
				// to subscribe this Bridge connection. Returning every historical
				// turn can produce a hundreds-of-MiB JSON-RPC frame and time out even
				// though the thread itself is healthy. excludeTurns preserves resume
				// and notification subscription semantics while returning metadata.
				resumeParams["excludeTurns"] = true
			}
			raw, err := c.rpcCall("thread/resume", resumeParams, 15*time.Second)
			if err == nil {
				threadID = extractThreadID(raw, snap.ResumeID)
			} else if isActiveWriterError(err) {
				return fmt.Errorf("thread/resume refused: %w: %v", backend.ErrThreadActiveWriter, err)
			} else if isStaleThreadError(err) {
				recoverySnap, recoveryPath, recoveryBytes, ok := c.latestAvailableRecoverySource(snap)
				if !ok {
					return fmt.Errorf("thread not found and no local rollout generation is available: %w", err)
				}
				recovery, err = c.prepareColdRecovery(recoverySnap, recoveryPath, recoveryBytes, "thread_not_found")
				if err != nil {
					return fmt.Errorf("thread not found and recovery checkpoint failed: %w", err)
				}
				log.Printf("[codex] stale thread session=%s; starting recovery generation", s.ID)
			} else {
				// A timeout or transport failure is not proof that the old thread is
				// unusable. Starting a blank fallback here forks the logical session.
				return fmt.Errorf("thread/resume failed without safe fallback: %w", err)
			}
		}
	}
	if threadID == "" {
		model := snap.Model
		if model == "" {
			model = codexDefaultModel
		}
		raw, err := c.rpcCall("thread/start", map[string]any{
			"model": model, "cwd": cwd, "ephemeral": false,
			"approvalPolicy": "never", "approvalsReviewer": "user", "sandbox": sandbox,
			"serviceTier": func() any {
				if snap.ServiceTier == "" {
					return nil
				}
				return snap.ServiceTier
			}(),
			"personality": func() any {
				if snap.Personality == "" {
					return nil
				}
				return snap.Personality
			}(),
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
	if recovery != nil {
		st.pendingHandoff = recovery.Handoff
		st.pendingRecovery = recovery
	}
	st.mu.Unlock()
	c.mu.Lock()
	c.threadToSession[threadID] = s
	c.mu.Unlock()

	if recovery == nil {
		s.SetResumeID(threadID)
		c.sink.Emit(backend.NewSessionUUID(s.ID, threadID))
		log.Printf("[codex] session=%s thread=%s", s.ID, threadID)
	} else {
		log.Printf("[codex] recovery staged session=%s source=%s thread=%s", s.ID, recovery.OldResumeID, threadID)
	}
	return nil
}

func (c *Codex) UpdateSessionSettings(ctx context.Context, s *session.Session) error {
	st := c.state(s.ID)
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	if threadID == "" {
		return nil
	}
	snap := s.Snapshot()
	params := map[string]any{"threadId": threadID, "model": snap.Model, "serviceTier": nil, "personality": nil}
	if snap.ServiceTier != "" {
		params["serviceTier"] = snap.ServiceTier
	}
	if snap.Personality != "" {
		params["personality"] = snap.Personality
	}
	if snap.Effort != "" {
		params["effort"] = snap.Effort
	}
	if snap.CollaborationMode != "" {
		params["collaborationMode"] = c.collaborationModeValue(snap)
	}
	_, err := c.rpcCall("thread/settings/update", params, 15*time.Second)
	return err
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
		c.sink.Emit(backend.NewStopped(s.ID, st.reqID))
	}
	return nil
}

func (c *Codex) Clear(ctx context.Context, s *session.Session) error {
	st := c.state(s.ID)
	_ = c.Stop(ctx, s)
	st.mu.Lock()
	threadID := st.threadID
	st.threadID = ""
	st.mu.Unlock()
	c.tools.ResetSession(s.ID)
	if threadID != "" {
		c.mu.Lock()
		fallbackOwner := c.threadToSession[threadID]
		activeOwner := c.activeThreadOwner[threadID]
		ownedElsewhere := (fallbackOwner != nil && fallbackOwner != s) ||
			(activeOwner != nil && activeOwner != s)
		if fallbackOwner == s {
			delete(c.threadToSession, threadID)
		}
		if activeOwner == s {
			delete(c.activeThreadOwner, threadID)
		}
		c.mu.Unlock()
		// A legacy duplicate session may still share this native thread. Detach
		// only the cleared bridge row; archiving would destroy the canonical
		// session's history and can interrupt its active turn.
		if !ownedElsewhere {
			_, _ = c.rpcCall("thread/archive", map[string]any{"threadId": threadID}, 5*time.Second)
		}
	}
	s.ClearResumeIDs()
	c.sink.Emit(backend.NewSessionWarning(s.ID, "Session history cleared."))
	c.sink.Emit(backend.NewGoalCleared(s.ID))
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
		if c.threadToSession[threadID] == s {
			delete(c.threadToSession, threadID)
		}
		if c.activeThreadOwner[threadID] == s {
			delete(c.activeThreadOwner, threadID)
		}
	}
	c.mu.Unlock()
	return nil
}

// ReleaseSession detaches only this daemon connection from a thread. It does
// not archive, delete, or clear the rollout, so a desktop client can resume the
// same thread id without competing with the Bridge's subscription.
func (c *Codex) ReleaseSession(ctx context.Context, s *session.Session) error {
	if err := c.ensureServer(); err != nil {
		return err
	}
	st := c.state(s.ID)
	st.ensureMu.Lock()
	defer st.ensureMu.Unlock()
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	if threadID == "" {
		threadID = s.ResumeID()
	}
	if threadID == "" {
		return nil
	}
	if _, err := c.rpcCall("thread/unsubscribe", map[string]any{"threadId": threadID}, 15*time.Second); err != nil {
		if isActiveWriterError(err) {
			// Another client already owns the thread; the desired handoff state is
			// therefore already true from the Bridge's perspective.
			c.forgetThread(s, st, threadID)
			return nil
		}
		return fmt.Errorf("thread/unsubscribe: %w", err)
	}
	c.forgetThread(s, st, threadID)
	return nil
}

// ClaimSession proves that the desktop writer is gone by successfully
// resuming the thread before the control lease is returned to mobile.
func (c *Codex) ClaimSession(ctx context.Context, s *session.Session) error {
	if err := c.ensureServer(); err != nil {
		return err
	}
	return c.ensureThread(s, c.state(s.ID))
}

func (c *Codex) RespondUserInput(id string, answers map[string]any, cancelled bool) bool {
	c.interMu.Lock()
	ci, ok := c.interactions[id]
	if !ok {
		for rid, pending := range c.interactions {
			if pending.payload.ToolUseID == id {
				ci, ok = pending, true
				id = rid
				break
			}
		}
	}
	if ok {
		delete(c.interactions, id)
	}
	c.interMu.Unlock()
	if !ok {
		return false
	}
	if answers == nil {
		answers = map[string]any{}
	}
	if ci.rpcID != nil {
		var result any
		switch ci.responseKind {
		case "mcp":
			result = mcpElicitationResponse(ci.mcpParams, answers, cancelled)
		case "dynamic_tool":
			text := codexAnyString(answers["result"])
			result = map[string]any{"contentItems": []any{map[string]any{"type": "inputText", "text": text}}, "success": !cancelled}
			c.tools.ResultEnd(ci.payload.SessionID, ci.reqID, ci.payload.ToolUseID, text)
		default:
			result = map[string]any{"answers": answers, "cancelled": cancelled}
		}
		_ = c.rpc.write(map[string]any{"id": ci.rpcID, "result": result})
	}
	status := "resolved"
	if cancelled {
		status = "cancelled"
	}
	c.sink.Emit(backend.NewInteractionResolved(ci.payload.RequestID, ci.payload.SessionID, status))
	return true
}

func (c *Codex) PendingInteractions(sessionID string) []backend.UserInputPayload {
	c.interMu.Lock()
	defer c.interMu.Unlock()
	out := []backend.UserInputPayload{}
	for _, pending := range c.interactions {
		if sessionID == "" || pending.payload.SessionID == sessionID {
			out = append(out, pending.payload)
		}
	}
	return out
}

// --- helpers ---------------------------------------------------------------

func (c *Codex) codexInput(s *session.Session, reqID, content string, images []backend.ImageAttachment, files []backend.FileAttachment, st *codexState) []map[string]any {
	userText := content
	for _, f := range files {
		name := f.Name
		if name == "" {
			name = "file"
		}
		userText += "\n\n[File: " + name + "]\n" + f.Content
	}
	input := []map[string]any{{"type": "text", "text": userText, "text_elements": []any{}}}
	for _, img := range images {
		if item := c.prepareCodexImageInput(s, reqID, img, st); item != nil {
			input = append(input, item)
		}
	}
	return input
}

func (c *Codex) prepareCodexImageInput(s *session.Session, reqID string, img backend.ImageAttachment, st *codexState) map[string]any {
	raw := strings.TrimSpace(img.Data)
	if raw == "" {
		return nil
	}
	if strings.HasPrefix(strings.ToLower(raw), "data:") {
		if _, rest, ok := strings.Cut(raw, ","); ok {
			raw = rest
		}
	}
	blob, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil
	}
	snap := s.Snapshot()
	base := runtime.ExpandPath(snap.Cwd)
	if fi, err := os.Stat(base); err != nil || !fi.IsDir() {
		base, _ = os.UserHomeDir()
	}
	root := filepath.Join(base, ".bridge_images")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil
	}
	safeReqID := strings.ReplaceAll(reqID, "/", "_")
	path := filepath.Join(root, s.ID+"_"+safeReqID+"_"+randHex(8)+codexImageExt(img.MediaType))
	if err := os.WriteFile(path, blob, 0o600); err != nil {
		return nil
	}
	st.mu.Lock()
	st.tempImages = append(st.tempImages, path)
	st.mu.Unlock()
	return map[string]any{"type": "localImage", "path": path}
}

func (c *Codex) cleanupTempImages(st *codexState) {
	st.mu.Lock()
	paths := append([]string(nil), st.tempImages...)
	st.tempImages = nil
	st.mu.Unlock()
	for _, path := range paths {
		_ = os.Remove(path)
	}
}

func normalizeCodexQuestions(raw []map[string]any) []backend.UserInputQuestion {
	if len(raw) == 0 {
		return []backend.UserInputQuestion{{
			QuestionID: "q1",
			Text:       "Question",
			Type:       "question",
			FreeForm:   true,
		}}
	}
	out := make([]backend.UserInputQuestion, 0, len(raw))
	for i, q := range raw {
		options := normalizeCodexOptions(codexFirstAny(q, "options", "choices"))
		qtype := codexFirstString(q, "type", "kind")
		multi := codexBoolField(q, "multiSelect", "multi_select", "multiple")
		freeForm := codexBoolField(q, "freeForm", "free_form", "allowFreeForm")
		if qtype == "" {
			switch {
			case multi:
				qtype = "multi_choice"
			case len(options) > 0:
				qtype = "choice"
			default:
				qtype = "question"
				freeForm = true
			}
		}
		qid := codexFirstString(q, "question_id", "id")
		if qid == "" {
			qid = "q" + strconv.Itoa(i+1)
		}
		text := codexFirstString(q, "text", "question", "label")
		if text == "" {
			text = "Question"
		}
		out = append(out, backend.UserInputQuestion{
			QuestionID: qid, Text: text, Header: codexFirstString(q, "header", "title"),
			Type: qtype, Options: options, MultiSelect: multi, FreeForm: freeForm,
		})
	}
	return out
}

func normalizeCodexOptions(raw any) []backend.UserInputOption {
	list, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]backend.UserInputOption, 0, len(list))
	for i, item := range list {
		if m, ok := item.(map[string]any); ok {
			label := codexFirstString(m, "label", "text", "value", "id")
			if label == "" {
				label = strconv.Itoa(i)
			}
			id := codexFirstString(m, "id", "value", "label")
			if id == "" {
				id = strconv.Itoa(i)
			}
			out = append(out, backend.UserInputOption{
				ID: id, Label: label, Description: codexFirstString(m, "description", "detail"),
				Recommended: codexBoolField(m, "recommended", "isRecommended"),
			})
			continue
		}
		label := strings.TrimSpace(fmt.Sprint(item))
		id := label
		if id == "" {
			id = strconv.Itoa(i)
		}
		out = append(out, backend.UserInputOption{ID: id, Label: label})
	}
	return out
}

func codexQuestionMaps(raw any) []map[string]any {
	list, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(list))
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func codexFirstAny(m map[string]any, keys ...string) any {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			return v
		}
	}
	return nil
}

func codexFirstString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func codexBoolField(m map[string]any, keys ...string) bool {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if b, ok := v.(bool); ok {
				return b
			}
		}
	}
	return false
}

func codexImageExt(mediaType string) string {
	mt := strings.ToLower(mediaType)
	switch {
	case strings.Contains(mt, "png"):
		return ".png"
	case strings.Contains(mt, "webp"):
		return ".webp"
	case strings.Contains(mt, "gif"):
		return ".gif"
	default:
		return ".jpg"
	}
}

func codexSandbox(s string) string {
	switch s {
	case "read-only", "workspace-write", "danger-full-access":
		return s
	default:
		return "workspace-write"
	}
}

// Browser Use reads its approval configuration through Codex's privileged
// runtime and needs outbound network access. Current Codex builds only expose
// both reliably through danger-full-access. Match the Python bridge for an
// unset sandbox when browser networking is enabled, while preserving every
// explicit sandbox choice and the network opt-out.
func codexSandboxForSession(snap session.Snapshot) string {
	base := codexSandbox(snap.Sandbox)
	if strings.TrimSpace(snap.Sandbox) != "" || strings.EqualFold(strings.TrimSpace(os.Getenv("BRIDGE_BROWSER_ORIGIN_MODE")), "deny") {
		return base
	}
	if disabled := strings.ToLower(strings.TrimSpace(os.Getenv("BRIDGE_BROWSER_ENABLE_NETWORK"))); disabled == "0" || disabled == "false" || disabled == "no" || disabled == "off" {
		return base
	}
	return "danger-full-access"
}

func codexTurnSandboxPolicy(snap session.Snapshot, override string) map[string]any {
	sandbox := strings.TrimSpace(override)
	if sandbox == "" {
		sandbox = codexSandboxForSession(snap)
	}
	switch sandbox {
	case "read-only":
		return map[string]any{"type": "readOnly", "networkAccess": false}
	case "workspace-write":
		roots := []string{}
		if cwd := runtime.ExpandPath(snap.Cwd); filepath.IsAbs(cwd) {
			roots = append(roots, cwd)
		}
		return map[string]any{"type": "workspaceWrite", "networkAccess": false, "writableRoots": roots}
	default:
		return map[string]any{"type": "dangerFullAccess"}
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
