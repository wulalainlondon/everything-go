package goexec

import (
	"bytes"
	"encoding/json"
	"testing"

	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func TestGeminiUpdateNormalizesTextThinkingAndTools(t *testing.T) {
	sink := &capSink{}
	g := NewGemini(sink, "")
	s := session.NewRegistry().Create("s1", "Gemini", t.TempDir(), "gemini", "", "", "")
	st := g.state(s.ID)
	st.requestID = "r1"

	g.handleUpdate(s, st, json.RawMessage(`{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello"}}}`))
	g.handleUpdate(s, st, json.RawMessage(`{"update":{"sessionUpdate":"agent_thought_chunk","content":[{"type":"text","text":"think"}]}}`))
	g.handleUpdate(s, st, json.RawMessage(`{"update":{"sessionUpdate":"tool_call","toolCall":{"toolCallId":"tc1","name":"shell","input":{"command":"pwd"}}}}`))
	g.handleUpdate(s, st, json.RawMessage(`{"update":{"sessionUpdate":"tool_call_update","status":"completed","output":"ok","toolCall":{"toolCallId":"tc1"}}}`))

	if len(sink.events) != 5 {
		t.Fatalf("events = %d: %+v", len(sink.events), sink.events)
	}
	if event, ok := sink.events[0].(protocol.TextChunk); !ok || event.Content != "hello" || event.RequestID != "r1" {
		t.Fatalf("text = %#v", sink.events[0])
	}
	if event, ok := sink.events[1].(protocol.ThinkingChunk); !ok || event.Content != "think" {
		t.Fatalf("thinking = %#v", sink.events[1])
	}
	if _, ok := sink.events[2].(protocol.ToolStart); !ok {
		t.Fatalf("tool start = %T", sink.events[2])
	}
	if _, ok := sink.events[3].(protocol.ToolResult); !ok {
		t.Fatalf("tool result = %T", sink.events[3])
	}
	if _, ok := sink.events[4].(protocol.ToolEnd); !ok {
		t.Fatalf("tool end = %T", sink.events[4])
	}
}

func TestJSONRPCPlumberACPFramesIncludeVersion(t *testing.T) {
	var out bytes.Buffer
	p := newJSONRPCPlumber("acp")
	p.setWriter(&out)
	if err := p.notify("initialized", nil); err != nil {
		t.Fatal(err)
	}
	var frame map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &frame); err != nil {
		t.Fatal(err)
	}
	if frame["jsonrpc"] != "2.0" || frame["method"] != "initialized" {
		t.Fatalf("frame = %+v", frame)
	}
}

func TestGeminiSessionIDResponseShapes(t *testing.T) {
	if got := responseSessionID(json.RawMessage(`{"sessionId":"one"}`), ""); got != "one" {
		t.Fatal(got)
	}
	if got := responseSessionID(json.RawMessage(`{"session":{"id":"two"}}`), ""); got != "two" {
		t.Fatal(got)
	}
	if got := responseSessionID(json.RawMessage(`{}`), "fallback"); got != "fallback" {
		t.Fatal(got)
	}
}
