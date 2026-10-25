package goexec

import (
	"encoding/json"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"os"
	"strings"
	"testing"
)

func TestCodexObservedWebMetadataReachesOriginalToolResult(t *testing.T) {
	raw, err := os.ReadFile("testdata/codex-observed-web-search.json")
	if err != nil {
		t.Fatal(err)
	}
	tool, ok := normalizeCodexLiveTool(raw)
	if !ok || !strings.Contains(tool.Command, "github.com/wulalainlondon/averything-bridge") || !strings.Contains(tool.Output, "Total lines: 263") {
		t.Fatalf("provider fields lost: %+v", tool)
	}
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	s := session.NewRegistry().Create("original", "codex", t.TempDir(), "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "root"
	st.reqID = "r-original"
	c.threadToSession["root"] = s
	// Only completed is observed: synthesize its original start, not a new tool ID.
	frame, _ := json.Marshal(map[string]any{"method": "item/completed", "params": json.RawMessage(raw)})
	c.dispatch(frame)
	starts, results, ends := 0, 0, 0
	for _, event := range sink.events {
		switch e := event.(type) {
		case protocol.ToolStart:
			starts++
			if e.ToolUseID != tool.ID || e.Command != tool.Command {
				t.Fatalf("wrong start %+v", e)
			}
		case protocol.ToolResult:
			results++
			if e.SessionID != s.ID || e.RequestID != "r-original" || e.ToolUseID != tool.ID || e.Output != tool.Output {
				t.Fatalf("wrong observed output %+v", e)
			}
		case protocol.ToolEnd:
			ends++
		}
	}
	if starts != 1 || results != 1 || ends != 1 {
		t.Fatalf("events=%d/%d/%d", starts, results, ends)
	}
}
func TestCodexWebSearchActionsAndUnavailableResultsAreNotInvented(t *testing.T) {
	for _, raw := range []string{`{"item":{"type":"webSearch","id":"search","query":"繁體 search","action":{"type":"search","queries":["one","two"]},"results":[{"url":"https://example.org/","snippet":"observed only"}]}}`, `{"item":{"type":"webSearch","id":"find","query":"","action":{"type":"findInPage","url":"https://example.org/","pattern":"original"},"results":null}}`} {
		tool, ok := normalizeCodexLiveTool(json.RawMessage(raw))
		if !ok || tool.Command == "" || tool.Output == "" {
			t.Fatalf("missing original fields %+v", tool)
		}
		if strings.Contains(raw, `"results":null`) && strings.Contains(tool.Output, "observed only") {
			t.Fatal("invented result")
		}
	}
	tool, _ := normalizeCodexLiveTool(json.RawMessage(`{"item":{"type":"commandExecution","id":"cmd","command":"echo test"}}`))
	if tool.Output != "" {
		t.Fatal("non-web output changed")
	}
}
