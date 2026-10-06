package goexec

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

type claudeTaskLease struct {
	server    *claudeTaskMCP
	requestID string
	threadID  string
	revoked   bool

	s                  *session.Session
	p                  *proc
	generation, secret string
	loaded             bool
	worker             bool
	mu                 sync.Mutex
}
type claudeTaskMCP struct {
	c      *Claude
	url    string
	leases map[string]*claudeTaskLease
	mu     sync.Mutex
}

func (c *Claude) SetTaskAPIProvider(p backend.TaskAPIProvider) {
	c.taskProvider = p
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return
	}
	server := &claudeTaskMCP{c: c, url: "http://" + ln.Addr().String(), leases: map[string]*claudeTaskLease{}}
	c.taskMCP = server
	srv := &http.Server{Handler: server, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
}
func (m *claudeTaskMCP) newLease(s *session.Session, worker bool) *claudeTaskLease {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return nil
	}
	lease := &claudeTaskLease{server: m, s: s, generation: hex.EncodeToString(bytes[:16]), secret: hex.EncodeToString(bytes), worker: worker}
	m.mu.Lock()
	m.leases[lease.generation] = lease
	m.mu.Unlock()
	return lease
}
func (m *claudeTaskMCP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	if net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() || r.Header.Get("Origin") != "" || r.Method != "POST" {
		http.Error(w, "forbidden", 403)
		return
	}
	generation := strings.TrimPrefix(r.URL.Path, "/task/")
	m.mu.Lock()
	lease := m.leases[generation]
	m.mu.Unlock()
	if lease == nil || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")), []byte(lease.secret)) != 1 {
		http.Error(w, "unauthorized", 401)
		return
	}
	var rpc struct {
		ID     json.RawMessage
		Method string
		Params json.RawMessage
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 256*1024)).Decode(&rpc) != nil {
		http.Error(w, "invalid", 400)
		return
	}
	if len(rpc.ID) == 0 {
		w.WriteHeader(202)
		return
	}
	respond := func(value any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": rpc.ID, "result": value})
	}
	switch rpc.Method {
	case "initialize":
		respond(map[string]any{"protocolVersion": "2025-03-26", "serverInfo": map[string]string{"name": "bridge-tasks", "version": "1.0.0-rc.1"}, "capabilities": map[string]any{"tools": map[string]any{}}})
	case "tools/list":
		tools, err := m.c.taskProvider.TaskTools(lease.s)
		if err != nil {
			http.Error(w, "unsupported", 403)
			return
		}
		lease.mu.Lock()
		lease.loaded = true
		lease.mu.Unlock()
		respond(map[string]any{"tools": tools})
	case "tools/call":
		var call struct {
			Name      string
			Arguments json.RawMessage
		}
		if json.Unmarshal(rpc.Params, &call) != nil {
			http.Error(w, "invalid", 400)
			return
		}
		lease.mu.Lock()
		process := lease.p
		loaded := lease.loaded
		lease.mu.Unlock()
		if process == nil || !loaded {
			http.Error(w, "caller_unbound", 403)
			return
		}
		lease.mu.Lock()
		request := lease.requestID
		threadID := lease.threadID
		revoked := lease.revoked
		lease.mu.Unlock()
		if revoked || process.currentReqID() != request {
			http.Error(w, "caller_unbound", 403)
			return
		}
		if request == "" || threadID == "" {
			http.Error(w, "caller_unbound", 403)
			return
		}
		callID := string(rpc.ID)
		caller := backend.TaskCaller{Session: lease.s, RequestID: request, ThreadID: threadID, CallID: callID, ProcessGeneration: lease.generation, ProviderVersion: m.c.taskVersion(), Validate: func() bool {
			select {
			case <-process.exited:
				return false
			default:
			}
			lease.mu.Lock()
			defer lease.mu.Unlock()
			return !lease.revoked && lease.requestID == request && lease.threadID == threadID && process.currentReqID() == request && process.taskLease == lease
		}}
		var header struct{ Operation string }
		if json.Unmarshal(call.Arguments, &header) != nil || call.Name != "task_"+header.Operation {
			http.Error(w, "invalid", 400)
			return
		}
		definitions, err := m.c.taskProvider.TaskTools(lease.s)
		registered := false
		if err == nil {
			for _, tool := range definitions {
				if tool["name"] == call.Name {
					registered = true
				}
			}
		}
		if !registered {
			http.Error(w, "unsupported_tool_not_registered", 403)
			return
		}
		response := m.c.taskProvider.ExecuteTask(r.Context(), caller, call.Arguments)
		text, _ := json.Marshal(response)
		respond(map[string]any{"content": []map[string]string{{"type": "text", "text": string(text)}}, "isError": !response.OK})
	default:
		http.Error(w, "unsupported", 400)
	}
}
func (c *Claude) taskVersion() string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(ctx, c.claudeBin, "--version").Output()
	if err != nil {
		return "unknown"
	}
	fields := strings.Fields(string(raw))
	if len(fields) == 0 {
		return "unknown"
	}
	return fields[0]
}
func (c *Claude) taskSpawn(s *session.Session, args []string, pm *backend.PMConfiguration) ([]string, *claudeTaskLease, error) {
	var bounded bool
	if c.taskProvider != nil {
		scope, err := c.taskProvider.TaskWorkerScope(s)
		if err != nil {
			return nil, nil, err
		}
		bounded = scope != nil
	}
	if bounded && (c.taskMCP == nil || pm != nil) {
		return nil, nil, fmt.Errorf("unsupported: bounded task MCP enforcement unavailable")
	}
	if c.taskProvider == nil || c.taskMCP == nil || pm != nil {
		return args, nil, nil
	}
	version := c.taskVersion()
	if version != "2.1.280" && version != "2.1.291" {
		if bounded {
			return nil, nil, fmt.Errorf("unsupported: bounded task provider version")
		}
		return args, nil, nil
	}
	scope, err := c.taskProvider.TaskWorkerScope(s)
	if err != nil {
		return nil, nil, err
	}
	lease := c.taskMCP.newLease(s, scope != nil)
	if lease == nil {
		return nil, nil, fmt.Errorf("task lease unavailable")
	}
	servers := map[string]any{"bridge_tasks": map[string]any{"type": "http", "url": c.taskMCP.url + "/task/" + lease.generation, "headers": map[string]string{"Authorization": "Bearer ${BRIDGE_TASK_SESSION_TOKEN}"}}}
	if c.mcp != nil && scope == nil {
		servers["ask_user"] = map[string]any{"type": "http", "url": c.mcp.sessionURL(s.ID)}
	}
	config, _ := json.Marshal(map[string]any{"mcpServers": servers})
	filtered := []string{}
	for i := 0; i < len(args); i++ {
		if args[i] == "--mcp-config" {
			i++
			continue
		}
		filtered = append(filtered, args[i])
	}
	filtered = append(filtered, "--mcp-config", string(config), "--replay-user-messages")
	if scope != nil {
		filtered = append(filtered, "--tools", "", "--strict-mcp-config", "--disable-slash-commands", "--setting-sources", "", "--settings", `{"disableAllHooks":true,"autoMemoryEnabled":false,"enabledPlugins":{}}`)
	}
	return filtered, lease, nil
}
func addTaskEnvironment(current []string, lease *claudeTaskLease) []string {
	if lease == nil {
		return current
	}
	if current == nil {
		current = os.Environ()
	}
	return append(current, "BRIDGE_TASK_SESSION_TOKEN="+lease.secret)
}

func (c *Claude) TaskAPICapabilities() []taskapi.ProviderCapability {
	version := c.taskVersion()
	capability := taskapi.UnloadedCapability("claude", version, "MCP registration or exact provider version is unavailable.")
	if c.taskProvider != nil && c.taskMCP != nil && (version == "2.1.280" || version == "2.1.291") {
		capability.Binding = "mcp_process_binding"
		capability.Lifecycle = "registered"
		capability.NativeEvidence = "exact_provider_token"
		capability.Operations = []string{"capabilities", "create_dispatch", "list", "get", "read_result", "snapshot", "events", "read_input"}
		capability.Models = []any{map[string]any{"model": "sonnet", "efforts": []string{"high"}}, map[string]any{"model": "opus", "efforts": []string{"high"}}}
		capability.Enforcement = taskapi.ScopeEnforcement{Mode: "gateway_only_tools", Roots: true, Tools: true, Network: true, Delegation: true}
		capability.Reason = "Exact CLI tag registered with per-turn MCP lease; tools/list and invocation require the actual process. Init toolset is checked before bounded-worker use. Account/quota are unknown; BG, voice task invocation, active cancel and steer are unsupported."
	}
	return []taskapi.ProviderCapability{capability}
}

func (lease *claudeTaskLease) revoke() {
	lease.mu.Lock()
	lease.revoked = true
	lease.mu.Unlock()
	if lease.server != nil {
		lease.server.mu.Lock()
		if lease.server.leases[lease.generation] == lease {
			delete(lease.server.leases, lease.generation)
		}
		lease.server.mu.Unlock()
	}
}
