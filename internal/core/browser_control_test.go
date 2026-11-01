package core

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/browsercontrol"
	"everything-go/internal/clientproto"
	"testing"
	"time"
)

type browserResolver struct {
	caller backend.SessionControlCaller
	valid  bool
}

func (f *browserResolver) ResolveBrowserControlCaller(_, _, _ string) (backend.SessionControlCaller, error) {
	if !f.valid {
		return backend.SessionControlCaller{}, errors.New("retired")
	}
	return f.caller, nil
}
func browserFixture(t *testing.T) (*Hub, *Client, *browserResolver) {
	h, c, caller, _ := transferFixture(t)
	h.cfg.DataDir = t.TempDir()
	caller.VoiceID = ""
	h.voiceClients = map[*Client]*clientVoiceCall{}
	started, release := make(chan struct{}), make(chan struct{})
	caller.Parent.SubmitNamed(caller.RequestID, func() { close(started); <-release; caller.Parent.EndTurn() })
	<-started
	t.Cleanup(func() { close(release) })
	h.browserControl = &browserControlState{binding: map[string]browserBinding{}, operations: map[string]*browserOperation{}}
	h.handleBrowserControl(c, clientproto.Command{Kind: "browser_bind", SessionID: caller.Parent.ID, Data: `{"binding":"native-view-binding","enabled":true,"explicitSelect":true}`})
	if _, ok := h.browserControl.binding[caller.Parent.ResumeID()]; !ok {
		t.Fatalf("fixture binding missing: live=%v mobile=%v", c.live(), h.controls.MobileMayWrite(caller.Parent.ID))
	}
	if e := h.controllerCaller(caller); e != nil {
		t.Fatal("fixture caller", e)
	}
	return h, c, &browserResolver{caller: caller, valid: true}
}
func browserReply(h *Hub, c *Client, session, kind, id string, extra map[string]any) {
	v := map[string]any{"binding": "native-view-binding", "operation": id}
	for k, x := range extra {
		v[k] = x
	}
	raw, _ := json.Marshal(v)
	h.handleBrowserControl(c, clientproto.Command{Kind: kind, SessionID: session, Data: string(raw)})
}
func TestBrowserTextFullGatewayPathUsesNoVoiceAndRequiresFinalPermit(t *testing.T) {
	h, c, resolver := browserFixture(t)
	q := browsercontrol.Request{ID: "bop_original_text_open", Thread: resolver.caller.Parent.ResumeID(), Method: "open", Params: []byte(`{"url":"https://example.com"}`)}
	answer := make(chan browsercontrol.Result, 1)
	failure := make(chan error, 1)
	go func() { r, e := h.executeBrowserControl(context.Background(), resolver, q); answer <- r; failure <- e }()
	e := waitForType(t, c, "browser_command")
	if e["actorMode"] != "text" || e["voice_id"] != "" {
		t.Fatal(e)
	}
	op := h.browserControl.operations[q.Thread+"|"+q.ID]
	if op.committed {
		t.Fatal("effect before native readiness")
	}
	browserReply(h, c, resolver.caller.Parent.ID, "browser_ready", q.ID, map[string]any{"permit": "native-final-permit", "stage": "quiet"})
	commit := waitForType(t, c, "browser_commit")
	if commit["permit"] != "native-final-permit" {
		t.Fatal(commit)
	}
	browserReply(h, c, resolver.caller.Parent.ID, "browser_ready", q.ID, map[string]any{"permit": "native-load-permit", "stage": "load"})
	waitForType(t, c, "browser_commit")
	browserReply(h, c, resolver.caller.Parent.ID, "browser_result", q.ID, map[string]any{"result": map[string]any{"state": "confirmed", "operation": q.ID}})
	if !(<-answer).OK {
		t.Fatal("missing original receipt")
	}
	if e := <-failure; e != nil {
		t.Fatal(e)
	}
	q.ID = "bop_query_original_result"
	q.Method = "status"
	q.Params = []byte(`{"operation":"bop_original_text_open"}`)
	if r, e := h.executeBrowserControl(context.Background(), resolver, q); e != nil || !r.OK {
		t.Fatal(r, e)
	}
}
func TestBrowserRetiredSourceCannotCommitAndUnknownNeverReplays(t *testing.T) {
	h, c, resolver := browserFixture(t)
	q := browsercontrol.Request{ID: "bop_delayed_text_action", Thread: resolver.caller.Parent.ResumeID(), Method: "click", Params: []byte(`{"view":"original-view","document":"original-document","snapshot":"original-snapshot","handle":"original-handle"}`)}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); h.executeBrowserControl(ctx, resolver, q) }()
	waitForType(t, c, "browser_command")
	resolver.valid = false
	browserReply(h, c, resolver.caller.Parent.ID, "browser_ready", q.ID, map[string]any{"permit": "native-final-permit", "stage": "effect"})
	if h.browserControl.operations[q.Thread+"|"+q.ID].committed {
		t.Fatal("retired source committed")
	}
	cancel()
	<-finished
	resolver.valid = true
	if _, e := h.executeBrowserControl(context.Background(), resolver, q); e != nil {
		t.Fatal(e)
	}
	select {
	case <-c.send:
		t.Fatal("unknown mutation replayed")
	case <-time.After(10 * time.Millisecond):
	}
	q.Params = []byte(`{"handle":"replacement"}`)
	if _, e := h.executeBrowserControl(context.Background(), resolver, q); e == nil {
		t.Fatal("same id changed payload")
	}
}
func TestBrowserAnotherPairedPhoneCannotSettleOriginalOperation(t *testing.T) {
	h, c, resolver := browserFixture(t)
	other := sharedReadClient(t, h, "other-browser-phone")
	q := browsercontrol.Request{ID: "bop_original_phone_action", Thread: resolver.caller.Parent.ResumeID(), Method: "snapshot", Params: []byte(`{"view":"original-view","document":"original-document"}`)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); h.executeBrowserControl(ctx, resolver, q) }()
	waitForType(t, c, "browser_command")
	browserReply(h, other, resolver.caller.Parent.ID, "browser_result", q.ID, map[string]any{"result": map[string]any{"state": "confirmed"}})
	if !h.browserControl.operations[q.Thread+"|"+q.ID].pending {
		t.Fatal("foreign device settled original")
	}
	cancel()
	<-done
}
func TestBrowserClaimRecoveryDoesNotPersistDOMOrFillValues(t *testing.T) {
	h, c, resolver := browserFixture(t)
	q := browsercontrol.Request{ID: "bop_durable_claim_only", Thread: resolver.caller.Parent.ResumeID(), Method: "fill", Params: []byte(`{"text":"private-draft"}`)}
	binding := h.browserControl.binding[q.Thread]
	if e := h.claimBrowserOperation(q, binding); e != nil {
		t.Fatal(e)
	}
	if e := h.claimBrowserOperation(q, binding); e == nil {
		t.Fatal("reclaimed unknown operation")
	}
	if v, e := h.readBrowserClaim(q.Thread, q.ID, c.deviceID); e != nil || v == nil {
		t.Fatal(v, e)
	}
	if _, e := h.readBrowserClaim(q.Thread, q.ID, "other-device"); e == nil {
		t.Fatal("foreign claim disclosed")
	}
}
