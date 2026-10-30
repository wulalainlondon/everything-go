package goexec

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"strings"
	"testing"
)

func contextEvents(sink *capSink) []backend.ContextTelemetry {
	var out []backend.ContextTelemetry
	for _, e := range sink.events {
		if v, ok := e.(backend.ContextTelemetry); ok {
			out = append(out, v)
		}
	}
	return out
}
func TestContextNativeCodexCallbacksKnownZeroAndRootCompletion(t *testing.T) {
	c, _, sink := observedCodexFixture(t)
	c.sessionsRoot = t.TempDir()
	s := c.threadToSession["root"]
	s.SetResumeID("root")
	c.dispatch(json.RawMessage(`{"method":"turn/started","params":{"threadId":"root","turn":{"id":"turn"}}}`))
	c.dispatch(json.RawMessage(`{"method":"thread/tokenUsage/updated","params":{"threadId":"root","turnId":"turn","tokenUsage":{"last":{"totalTokens":0},"modelContextWindow":1000}}}`))
	c.dispatch(json.RawMessage(`{"method":"item/started","params":{"threadId":"root","turnId":"turn","item":{"id":"item","type":"contextCompaction"}}}`))
	e := contextEvents(sink)
	if len(e) != 1 || e[0].UsedTokens == nil || *e[0].UsedTokens != 0 {
		t.Fatal("unknown/zero/start conflated")
	}
	c.dispatch(json.RawMessage(`{"method":"item/completed","params":{"threadId":"root","turnId":"turn","item":{"id":"item","type":"contextCompaction"}}}`))
	e = contextEvents(sink)
	if len(e) != 2 || e[1].CompletionState != "completed" {
		t.Fatal("native root completion not emitted")
	}
	c.threadToSession["child"] = s
	c.dispatch(json.RawMessage(`{"method":"item/completed","params":{"threadId":"child","turnId":"turn","item":{"id":"childitem","type":"contextCompaction"}}}`))
	if len(contextEvents(sink)) != 2 {
		t.Fatal("child counted")
	}
}
func TestContextManagedCompletionIdentityAndNonTerminals(t *testing.T) {
	c, _, sink := observedCodexFixture(t)
	c.sessionsRoot = t.TempDir()
	s := c.threadToSession["root"]
	s.SetResumeID("root")
	c.maintenance = map[string]backend.Maintenance{s.ID: {SessionID: s.ID, ThreadID: "root", TurnID: "compactturn", OperationID: "operation", State: "running"}}
	c.contextCompletion(s, "root", "compactturn", "item")
	r := c.maintenance[s.ID]
	for _, state := range []string{"running", "unknown", "failed", "completed"} {
		r.State = state
		c.maintenanceContextCompletion(r)
	}
	e := contextEvents(sink)
	if len(e) != 4 || e[0].CompletionState != "observed" || e[1].CompletionState != "unknown" || e[2].CompletionState != "failed" || e[3].CompletionState != "completed" {
		t.Fatalf("wrong terminal evidence: %+v", e)
	}
	if len(e[0].Aliases) != 1 || e[0].Aliases[0] != e[3].EventID {
		t.Fatal("manual/item have no common stable canonical identity")
	}
	r.TurnID = ""
	c.maintenanceContextCompletion(r)
	if len(contextEvents(sink)) != 4 {
		t.Fatal("uncorrelated legacy terminal counted")
	}
}

func TestContextClaudeRootUsageSourceAndUnsupportedCompaction(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	s := session.NewRegistry().Create("claude", "Claude", t.TempDir(), "claude", "claude-sonnet-5-5", "", "")
	s.SetResumeID("native-claude")
	p := &proc{reqID: "r_usage", model: "claude-sonnet-5-5", tools: newToolNormalizer(sink, c)}
	raw := `{"type":"assistant","isSidechain":true,"message":{"usage":{"input_tokens":999},"content":[]}}
{"type":"assistant","message":{"usage":{"input_tokens":0,"cache_read_input_tokens":0},"content":[]}}
{"type":"result","subtype":"success","uuid":"result-identity","session_id":"native-claude","usage":{"input_tokens":999},"modelUsage":{"claude-sonnet-5-5":{"contextWindow":1000000}}}` + "\n"
	c.readStdout(s, p, strings.NewReader(raw))
	events := contextEvents(sink)
	if len(events) != 1 || events[0].UsedTokens == nil || *events[0].UsedTokens != 0 || events[0].Origin != "claude_native_assistant_usage" || events[0].MaxOrigin != "claude_native_model_usage" || events[0].Completed || events[0].CompletionState != "" {
		t.Fatalf("Claude sidechain/usage/unsupported completion: %+v", events)
	}
}
