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

func TestIndependentBrowserCurrentStatusNativeStates(t *testing.T) {
	for _, state := range []string{"ready", "loading", "closed", "user_takeover"} {
		t.Run(state, func(t *testing.T) {
			h, c, r := browserFixture(t)
			id := "bop_native_status_" + state
			st := h.browserControl
			st.operations[r.caller.Parent.ResumeID()+"|"+id] = &browserOperation{thread: r.caller.Parent.ResumeID(), device: c.deviceID, binding: "native-view-binding", pending: true, done: make(chan struct{}), method: "status", stages: []string{"effect"}, admitted: time.Now()}
			browserReply(h, c, r.caller.Parent.ID, "browser_result", id, map[string]any{"result": map[string]any{"state": state, "view": "exact-view", "document": "exact-document"}})
			op := st.operations[r.caller.Parent.ResumeID()+"|"+id]
			if op.pending {
				t.Fatalf("real native status %s discarded; current ready/view/document unavailable", state)
			}
			var got map[string]any
			if json.Unmarshal(op.state, &got) != nil || got["state"] != "observed" || got["current"].(map[string]any)["state"] != state {
				t.Fatal(string(op.state))
			}
		})
	}
}
func TestIndependentBrowserCapabilityObservedPositive(t *testing.T) {
	h, c, r := browserFixture(t)
	id := "bop_capability_positive"
	h.browserControl.operations[r.caller.Parent.ResumeID()+"|"+id] = &browserOperation{thread: r.caller.Parent.ResumeID(), device: c.deviceID, binding: "native-view-binding", pending: true, done: make(chan struct{}), method: "capabilities", stages: []string{"effect"}}
	browserReply(h, c, r.caller.Parent.ID, "browser_result", id, map[string]any{"result": map[string]any{"state": "observed", "supported": true}})
	if h.browserControl.operations[r.caller.Parent.ResumeID()+"|"+id].pending {
		t.Fatal("positive receipt rejected")
	}
}

var _ = browsercontrol.Request{}

func TestBrowserDirectorySyncFailureNeverEnqueuesAndOriginalClaimCannotReplay(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(formatUint(uint64(failAt)), func(t *testing.T) {
			h, client, resolver := browserFixture(t)
			for len(client.send) > 0 {
				<-client.send
			}
			count := 0
			h.browserClaimSync = func(string) error {
				count++
				if count == failAt {
					return errors.New("own_fixture_directory_sync_failure")
				}
				return nil
			}
			q := browsercontrol.Request{ID: "bop_sync_failure_original", Thread: resolver.caller.Parent.ResumeID(), Method: "open", Params: []byte(`{"url":"https://example.com"}`)}
			if _, err := h.executeBrowserControl(context.Background(), resolver, q); err == nil {
				t.Fatal("directory-sync refusal ignored")
			}
			if count != failAt {
				t.Fatal("wrong durability branch", count)
			}
			if len(client.send) != 0 || len(h.browserControl.operations) != 0 {
				t.Fatal("effect enqueued before durable claim")
			}
			h.browserClaimSync = nil
			if _, err := h.executeBrowserControl(context.Background(), resolver, q); err == nil {
				t.Fatal("duplicate mutation did not remain refused")
			}
			query := browsercontrol.Request{ID: "bop_sync_original_query", Thread: q.Thread, Method: "status", Params: []byte(`{"operation":"bop_sync_failure_original"}`)}
			if _, err := h.executeBrowserControl(context.Background(), resolver, query); err != nil {
				t.Fatal("original readonly receipt unavailable", err)
			}
			if len(client.send) != 0 {
				t.Fatal("original claim replayed after sync failure")
			}
			q.Params = []byte(`{"url":"https://example.com/changed"}`)
			if _, err := h.executeBrowserControl(context.Background(), resolver, q); err == nil {
				t.Fatal("substitution accepted")
			}
		})
	}
}
