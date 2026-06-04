package goexec

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"everything-go/internal/protocol"
)

func postRPC(t *testing.T, url, body string) map[string]any {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("bad rpc response %q: %v", raw, err)
	}
	return m
}

// TestAskUserMCPRoundTrip exercises the full ask_user MCP path in-process:
// initialize → tools/list → a blocking tools/call that is unblocked by an
// app-style RespondUserInput, returning the answer as the tool result. This is
// the mechanism that lets Claude's questions actually be answered (the built-in
// AskUserQuestion can't be, in headless mode).
func TestAskUserMCPRoundTrip(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	if c.mcp == nil {
		t.Skip("ask_user MCP server failed to start")
	}
	url := c.mcp.sessionURL("s1")

	// initialize echoes the protocol version + advertises tools capability.
	init := postRPC(t, url, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}`)
	res, _ := init["result"].(map[string]any)
	if res == nil || res["protocolVersion"] != "2025-06-18" {
		t.Fatalf("initialize result wrong: %v", init)
	}

	// tools/list advertises ask_question.
	list := postRPC(t, url, `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
	lr, _ := list["result"].(map[string]any)
	tools, _ := lr["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("want 1 tool, got %v", lr["tools"])
	}
	if tool, _ := tools[0].(map[string]any); tool["name"] != "ask_question" {
		t.Fatalf("tool name = %v", tools[0])
	}

	// tools/call blocks until answered; run it in the background.
	callDone := make(chan map[string]any, 1)
	go func() {
		callDone <- postRPC(t, url, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"ask_question","arguments":{"questions":[{"question":"Pick a color","options":[{"label":"Red"},{"label":"Blue"}]}]}}}`)
	}()

	// The handler must have raised a user_input_request; grab its request_id.
	reqID := waitReqID(t, sink)
	if !c.RespondUserInput(reqID, map[string]any{"q1": "Blue"}, false) {
		t.Fatal("RespondUserInput did not match the pending MCP interaction")
	}

	select {
	case call := <-callDone:
		cr, _ := call["result"].(map[string]any)
		content, _ := cr["content"].([]any)
		if len(content) == 0 {
			t.Fatalf("tools/call returned no content: %v", call)
		}
		text, _ := content[0].(map[string]any)["text"].(string)
		if want := `"Pick a color"="Blue"`; !bytes.Contains([]byte(text), []byte(want)) {
			t.Fatalf("tool result should name the answer; got %q", text)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tools/call did not return after the answer")
	}
}

func waitReqID(t *testing.T, sink *capSink) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sink.mu.Lock()
		for _, e := range sink.events {
			if ev, ok := e.(protocol.UserInputRequestEvent); ok {
				sink.mu.Unlock()
				return ev.RequestID
			}
		}
		sink.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no user_input_request emitted")
	return ""
}

// TestAskUserMCPGetRejected confirms GET returns 405 (no server-initiated SSE).
func TestAskUserMCPGetRejected(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	if c.mcp == nil {
		t.Skip("ask_user MCP server failed to start")
	}
	resp, err := http.Get(c.mcp.sessionURL("s1"))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", resp.StatusCode)
	}
}
