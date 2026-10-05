package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"os"
	"reflect"
	"testing"
	"time"
)

type controllerTestProvider struct{}

func (controllerTestProvider) ControlSession(context.Context, backend.SessionControlCaller, backend.SessionControlRequest) (any, error) {
	return nil, nil
}
func TestControllerCallerRequiresExactOwnedTurnOrLiveVoice(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	s := session.NewRegistry().Create("parent", "Parent", t.TempDir(), backend.Codex, "", "read-only", "thread")
	st := c.state(s.ID)
	st.threadID = "thread"
	st.observedTurnID = "observed"
	st.observedRequestID = "external"
	if _, e := c.sessionControlCaller(s, "observed", "call"); e == nil {
		t.Fatal("desktop observation authorized")
	}
	c.voiceCalls = map[string]*codexVoiceCall{"thread": {sessionID: s.ID, threadID: "thread", voiceID: "voice"}}
	caller, e := c.sessionControlCaller(s, "observed", "call")
	if e != nil || caller.VoiceID != "voice" || caller.RequestID != "external" {
		t.Fatal(caller, e)
	}
	if _, e := c.sessionControlCaller(s, "wrong-turn", "call"); e == nil {
		t.Fatal("stale voice turn authorized")
	}
	delete(c.voiceCalls, "thread")
	if _, e := c.sessionControlCaller(s, "observed", "call"); e == nil {
		t.Fatal("closed voice authorized")
	}
	st.turnActive = true
	st.currentTurnID = "owned"
	st.reqID = "request"
	if _, e := c.sessionControlCaller(s, "owned", "call"); e != nil {
		t.Fatal(e)
	}
	s.SetResumeID("forked")
	if _, e := c.sessionControlCaller(s, "owned", "call"); e == nil {
		t.Fatal("changed thread authorized")
	}
}
func TestControllerCatalogExcludedFromDelegatedChild(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.SetSessionControlProvider(controllerTestProvider{})
	s := session.NewRegistry().Create("s_dg_child", "Child", t.TempDir(), backend.Codex, "", "read-only", "")
	params := map[string]any{}
	c.applySessionControlThreadTools(s, params)
	if params["dynamicTools"] != nil {
		t.Fatal("child received controller tools")
	}
}

func TestCodexControllerToolCatalogIntegration(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_RUN_CONTROLLER_INTEGRATION") != "1" && os.Getenv("EVERYTHING_GO_RUN_CONTROLLER_COLD_INTEGRATION") != "1" {
		t.Skip("opt-in fresh thread only")
	}
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	c.SetDataDir(t.TempDir())
	c.SetSessionControlProvider(controllerTestProvider{})
	c.SetDelegationProvider(delegationFixtureProvider{calls: make(chan backend.DelegationSpec, 1)})
	if e := c.ensureServer(); e != nil {
		t.Fatal(e)
	}
	defer func() { c.startMu.Lock(); _ = c.stopServerLocked(); c.startMu.Unlock() }()
	s := session.NewRegistry().Create("controller-probe", "Isolated controller protocol probe", t.TempDir(), backend.Codex, "", "read-only", "")
	if e := c.ensureThread(s, c.state(s.ID)); e != nil {
		t.Fatal(e)
	}
	if s.ResumeID() == "" {
		t.Fatal("missing thread")
	}
	t.Logf("fresh isolated thread accepted controller catalog: %s", s.ResumeID())
	defer c.rpcCall("thread/archive", map[string]any{"threadId": s.ResumeID()}, 15*time.Second)
	// The optional cold-resume probe needs a real persisted first turn. Keep it
	// separate from catalog acceptance so account/model availability is visible.
	if os.Getenv("EVERYTHING_GO_RUN_CONTROLLER_COLD_INTEGRATION") != "1" {
		if _, e := c.rpcCall("thread/archive", map[string]any{"threadId": s.ResumeID()}, 15*time.Second); e != nil {
			t.Fatal(e)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if e := c.Send(ctx, s, "controller-probe-turn", "Reply only with controller-probe. Do not use tools or modify files.", nil, nil); e != nil {
		t.Fatal(e)
	}
	st := c.state(s.ID)
	st.mu.Lock()
	done := st.turnDone
	st.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		_ = c.Stop(context.Background(), s)
		t.Fatal("isolated text turn timed out")
	}
	st.mu.Lock()
	turnErr := st.turnErr
	st.mu.Unlock()
	if turnErr != "" {
		t.Fatal("isolated text turn failed", turnErr)
	}
	before, e := c.rpcCall("thread/resume", map[string]any{"threadId": s.ResumeID(), "excludeTurns": true}, 15*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	st.mu.Lock()
	st.threadID = ""
	st.mu.Unlock()
	if id, e := c.voiceThread(s); e != nil || id != s.ResumeID() {
		t.Fatal("cold voice tool resume failed", id, e)
	}
	after, e := c.rpcCall("thread/resume", map[string]any{"threadId": s.ResumeID(), "excludeTurns": true}, 15*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	var b, a map[string]any
	json.Unmarshal(before, &b)
	json.Unmarshal(after, &a)
	if b["sandbox"] == nil || b["approvalPolicy"] == nil {
		t.Fatal("native permission evidence missing")
	}
	for _, key := range []string{"sandbox", "approvalPolicy", "approvalsReviewer"} {
		if !reflect.DeepEqual(b[key], a[key]) {
			t.Fatalf("voice resume changed %s", key)
		}
	}
	t.Log("cold voice resume accepted namespaces and preserved native permissions")

	if _, e := c.rpcCall("thread/archive", map[string]any{"threadId": s.ResumeID()}, 15*time.Second); e != nil {
		t.Fatal(e)
	}
}
