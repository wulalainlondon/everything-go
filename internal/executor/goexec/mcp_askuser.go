package goexec

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

// askUserMCP is a minimal Streamable-HTTP MCP server hosted inside the bridge
// process, exposing one tool — `ask_question`. When Claude (steered by an
// appended system prompt) calls it instead of the built-in AskUserQuestion, the
// handler runs IN-PROCESS: it raises a user_input_request to the app, blocks on
// the answer, and returns it as the MCP tool result. Because that result is a
// normal MCP tool result, Claude honors it and continues the turn — the path the
// built-in AskUserQuestion can't take in headless mode (it rejects injected
// answers). This is how Happy / ccpocket achieve interactivity (via the Agent
// SDK + an MCP tool); we implement the same idea natively in Go.
//
// One server serves all sessions; the bridge session id rides in the URL path
// (/mcp/<sessionID>) so the handler knows which chat to surface the question in.
type askUserMCP struct {
	claude  *Claude
	baseURL string
}

// askUserAnswerTimeout caps how long the tool handler waits for a human answer.
// Kept under the MCP per-call timeout (we spawn claude with MCP_TOOL_TIMEOUT
// = 30 min) so we return a clean "no answer" rather than letting the CLI abort.
const askUserAnswerTimeout = 25 * time.Minute

// startAskUserMCP binds a localhost listener and serves the MCP endpoint. Returns
// the base URL (http://127.0.0.1:<port>); per-session URLs append /mcp/<id>.
func startAskUserMCP(c *Claude) (*askUserMCP, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	m := &askUserMCP{claude: c, baseURL: fmt.Sprintf("http://%s", ln.Addr().String())}
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp/", m.handle)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[mcp] ask_user server stopped: %v", err)
		}
	}()
	log.Printf("[mcp] ask_user server on %s", m.baseURL)
	return m, nil
}

// sessionURL is the per-session MCP endpoint passed to claude via --mcp-config.
func (m *askUserMCP) sessionURL(sessionID string) string {
	return m.baseURL + "/mcp/" + sessionID
}

// --- JSON-RPC plumbing ------------------------------------------------------

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"` // absent on notifications
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

func (m *askUserMCP) handle(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// We offer no server-initiated SSE stream at this endpoint.
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	case http.MethodDelete:
		w.WriteHeader(http.StatusOK) // session teardown — nothing to clean up
		return
	case http.MethodPost:
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "bad json-rpc", http.StatusBadRequest)
		return
	}

	// Notifications (no id) get a 202 with no body.
	if len(req.ID) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	sessionID := strings.TrimPrefix(r.URL.Path, "/mcp/")
	resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}

	switch req.Method {
	case "initialize":
		resp.Result = m.initializeResult(req.Params)
		w.Header().Set("Mcp-Session-Id", "askuser-"+randHex(8))
	case "tools/list":
		resp.Result = map[string]any{"tools": []any{askQuestionToolSpec()}}
	case "ping":
		resp.Result = map[string]any{}
	case "tools/call":
		resp.Result = m.callTool(sessionID, req.Params)
	default:
		resp.Error = &rpcError{Code: -32601, Message: "method not found: " + req.Method}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (m *askUserMCP) initializeResult(params json.RawMessage) map[string]any {
	version := "2025-06-18"
	var p struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if json.Unmarshal(params, &p) == nil && p.ProtocolVersion != "" {
		version = p.ProtocolVersion // echo the client's negotiated version
	}
	return map[string]any{
		"protocolVersion": version,
		"capabilities":    map[string]any{"tools": map[string]any{}},
		"serverInfo":      map[string]any{"name": "ask_user", "version": "1.0.0"},
	}
}

func askQuestionToolSpec() map[string]any {
	return map[string]any{
		"name": "ask_question",
		"description": "Ask the user one or more questions and wait for their answer. " +
			"Use this whenever you need the user to choose between options or provide input. " +
			"Each question may carry multiple-choice options; the user's selection is returned.",
		"inputSchema": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"questions": map[string]any{
					"type":        "array",
					"description": "The questions to ask.",
					"items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"question":    map[string]any{"type": "string", "description": "The question text."},
							"header":      map[string]any{"type": "string", "description": "Short label for the question."},
							"multiSelect": map[string]any{"type": "boolean", "description": "Allow selecting multiple options."},
							"options": map[string]any{
								"type": "array",
								"items": map[string]any{
									"type": "object",
									"properties": map[string]any{
										"label":       map[string]any{"type": "string"},
										"description": map[string]any{"type": "string"},
									},
									"required": []string{"label"},
								},
							},
						},
						"required": []string{"question"},
					},
				},
			},
			"required": []string{"questions"},
		},
	}
}

// callTool dispatches a tools/call. Only ask_question is supported; it blocks in
// the bridge process until the app delivers the answer (or the timeout fires).
func (m *askUserMCP) callTool(sessionID string, params json.RawMessage) map[string]any {
	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return toolError("invalid tools/call params")
	}
	if p.Name != "ask_question" {
		return toolError("unknown tool: " + p.Name)
	}

	payload, ch := m.claude.registerMCPInteraction(sessionID, p.Arguments)

	select {
	case ans := <-ch:
		text := buildUserInputResultText(payload, ans.answers, ans.cancelled)
		return map[string]any{
			"content": []any{map[string]any{"type": "text", "text": text}},
			"isError": false,
		}
	case <-time.After(askUserAnswerTimeout):
		m.claude.dropInteraction(payload.RequestID)
		return map[string]any{
			"content": []any{map[string]any{"type": "text", "text": "The user did not answer in time. Continue without their input."}},
			"isError": false,
		}
	}
}

func toolError(msg string) map[string]any {
	return map[string]any{
		"content": []any{map[string]any{"type": "text", "text": msg}},
		"isError": true,
	}
}
