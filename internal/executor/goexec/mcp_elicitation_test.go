package goexec

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"everything-go/internal/session"
)

func browserElicitation(origin, tool string) json.RawMessage {
	payload, _ := json.Marshal(map[string]any{
		"threadId":        "root",
		"turnId":          "turn",
		"serverName":      "node_repl",
		"mode":            "openai/form",
		"message":         "Allow Chrome to access " + origin + "?",
		"requestedSchema": map[string]any{},
		"_meta": map[string]any{
			"connector_id": "browser-use",
			"tool_name":    tool,
			"origin":       origin,
		},
	})
	return payload
}

func TestBrowserOriginPolicyAllowlistAndWildcard(t *testing.T) {
	policy := browserOriginPolicy{mode: "allowlist", allowedOrigins: []string{"https://studio.youtube.com", "*.canva.com"}}
	for _, origin := range []string{"https://studio.youtube.com", "https://www.canva.com"} {
		params := decodeObject(browserElicitation(origin, browserOriginTool))
		result, handled := policy.automaticResponse(params)
		if !handled || result["action"] != "accept" || result["_meta"].(map[string]any)["persist"] != "session" {
			t.Fatalf("origin %s was not session-approved: %#v handled=%v", origin, result, handled)
		}
	}
	for _, origin := range []string{"https://youtube.com", "https://canva.com"} {
		if _, handled := policy.automaticResponse(decodeObject(browserElicitation(origin, browserOriginTool))); handled {
			t.Fatalf("origin %s unexpectedly matched allowlist", origin)
		}
	}
}

func TestBrowserOriginAllowAllStillPromptsForRawCDP(t *testing.T) {
	policy := browserOriginPolicy{mode: "allow_all"}
	params := decodeObject(browserElicitation("https://example.com", "access_browser_origin_with_raw_cdp"))
	if _, handled := policy.automaticResponse(params); handled {
		t.Fatal("raw CDP must not be automatically approved")
	}
	questions := mcpElicitationQuestions(params)
	if len(questions) != 1 || questions[0].Options[0].ID != "deny" || !questions[0].Options[0].Recommended {
		t.Fatalf("raw CDP prompt should default to deny: %+v", questions)
	}
}

func TestBrowserOriginRejectsInvalidOrigin(t *testing.T) {
	params := decodeObject(browserElicitation("file:///private/data", browserOriginTool))
	result, handled := (browserOriginPolicy{mode: "allow_all"}).automaticResponse(params)
	if !handled || result["action"] != "decline" {
		t.Fatalf("invalid browser origin should fail closed: %#v handled=%v", result, handled)
	}
}

func TestMcpFormQuestionsAndResponsesPreserveTypes(t *testing.T) {
	raw := json.RawMessage(`{
		"mode":"form","serverName":"exporter","message":"Configure export",
		"requestedSchema":{"type":"object","properties":{
			"count":{"type":"integer","title":"Count"},
			"enabled":{"type":"boolean","title":"Enabled"},
			"formats":{"type":"array","items":{"type":"string","enum":["png","jpg"]}}
		}}
	}`)
	questions := mcpElicitationQuestions(decodeObject(raw))
	if len(questions) != 3 || questions[0].QuestionID != "count" || questions[2].QuestionID != "formats" || !questions[2].MultiSelect {
		t.Fatalf("unexpected form questions: %+v", questions)
	}
	result := mcpElicitationResponse(raw, map[string]any{"count": "3", "enabled": "true", "formats": []any{"png"}}, false)
	content := result["content"].(map[string]any)
	if content["count"] != 3 || content["enabled"] != true || len(content["formats"].([]any)) != 1 {
		t.Fatalf("typed content mismatch: %#v", content)
	}
}

func TestCodexAutoApprovesOrdinaryBrowserOriginWithoutFrontend(t *testing.T) {
	t.Setenv("BRIDGE_BROWSER_ORIGIN_MODE", "allow_all")
	c := NewCodex(&capSink{}, "codex")
	writer := &captureWriter{}
	c.rpc.setWriter(writer)
	c.handleServerRequest(41, "mcpServer/elicitation/request", browserElicitation("https://example.com", browserOriginTool))
	var response struct {
		ID     int `json:"id"`
		Result struct {
			Action string         `json:"action"`
			Meta   map[string]any `json:"_meta"`
		} `json:"result"`
	}
	if json.Unmarshal(bytes.TrimSpace(writer.Bytes()), &response) != nil || response.ID != 41 || response.Result.Action != "accept" || response.Result.Meta["persist"] != "session" {
		t.Fatalf("bad automatic JSON-RPC response: %s", writer.String())
	}
	if len(c.PendingInteractions("")) != 0 {
		t.Fatal("automatic browser approval must not leave a pending interaction")
	}
}

func TestCodexDispatchPreservesStringRequestID(t *testing.T) {
	t.Setenv("BRIDGE_BROWSER_ORIGIN_MODE", "allow_all")
	c := NewCodex(&capSink{}, "codex")
	writer := &captureWriter{}
	c.rpc.setWriter(writer)
	params := browserElicitation("https://example.com", browserOriginTool)
	raw, err := json.Marshal(map[string]any{
		"id":     "browser-request-1",
		"method": "mcpServer/elicitation/request",
		"params": json.RawMessage(params),
	})
	if err != nil {
		t.Fatal(err)
	}
	c.dispatch(raw)
	var response struct {
		ID     string `json:"id"`
		Result struct {
			Action string `json:"action"`
		} `json:"result"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(writer.Bytes()), &response); err != nil || response.ID != "browser-request-1" || response.Result.Action != "accept" {
		t.Fatalf("string request ID was not preserved: %s err=%v", writer.String(), err)
	}
}

func TestBrowserManualResponseMapsPersistence(t *testing.T) {
	raw := browserElicitation("https://example.com", browserOriginTool)
	once := mcpElicitationResponse(raw, map[string]any{elicitationChoice: "approve_once"}, false)
	always := mcpElicitationResponse(raw, map[string]any{elicitationChoice: "approve_always"}, false)
	decline := mcpElicitationResponse(raw, map[string]any{elicitationChoice: "deny"}, false)
	if once["_meta"].(map[string]any)["persist"] != "session" || always["_meta"].(map[string]any)["persist"] != "always" || decline["action"] != "decline" {
		t.Fatalf("manual decision mapping mismatch: once=%#v always=%#v decline=%#v", once, always, decline)
	}
	if !strings.Contains(mcpElicitationQuestions(decodeObject(raw))[0].Text, "example.com") {
		t.Fatal("origin prompt should remain visible to the user")
	}
}

func TestBrowserNetworkingUsesPythonParityForDefaultSandbox(t *testing.T) {
	t.Setenv("BRIDGE_BROWSER_ORIGIN_MODE", "allow_all")
	if got := codexSandboxForSession(session.Snapshot{}); got != "danger-full-access" {
		t.Fatalf("default browser sandbox = %q, want danger-full-access", got)
	}
	if got := codexSandboxForSession(session.Snapshot{Sandbox: "workspace-write"}); got != "workspace-write" {
		t.Fatalf("explicit sandbox choice was not preserved: %q", got)
	}

	t.Setenv("BRIDGE_BROWSER_ORIGIN_MODE", "deny")
	if got := codexSandboxForSession(session.Snapshot{}); got != "workspace-write" {
		t.Fatalf("deny mode must keep the network-isolated default: %q", got)
	}
}

func TestEnsureBrowserElicitationRoutingIsAtomicAndIdempotent(t *testing.T) {
	home := t.TempDir()
	config := filepath.Join(home, "browser", "config.toml")
	if err := os.MkdirAll(filepath.Dir(config), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, []byte("[origins]\nallowed = [\"https://example.com\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	changed, err := ensureBrowserElicitationRouting(home)
	if err != nil || !changed {
		t.Fatalf("first update changed=%v err=%v", changed, err)
	}
	content, err := os.ReadFile(config)
	if err != nil || string(content) != "disable_auto_review = true\n[origins]\nallowed = [\"https://example.com\"]\n" {
		t.Fatalf("unexpected config %q err=%v", content, err)
	}
	changed, err = ensureBrowserElicitationRouting(home)
	if err != nil || changed {
		t.Fatalf("second update should be idempotent changed=%v err=%v", changed, err)
	}

	t.Setenv("BRIDGE_BROWSER_MANAGE_AUTO_REVIEW", "0")
	if err := os.WriteFile(config, []byte("disable_auto_review = false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	changed, err = ensureBrowserElicitationRouting(home)
	if err != nil || changed {
		t.Fatalf("opt-out should preserve config changed=%v err=%v", changed, err)
	}
}
