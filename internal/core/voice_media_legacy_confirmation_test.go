package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"os"
	"reflect"
	"testing"
)

type legacyObserveWire struct {
	CommandID string                      `json:"media_command_id"`
	Sequence  uint64                      `json:"media_sequence"`
	State     *protocol.MediaSessionState `json:"media_session_state"`
}

func legacyNativeFixture(t *testing.T, scenario string) (*Hub, *Client, *clientVoiceCall, backend.SessionControlCaller, protocol.VoiceMediaCommand, *callMediaReceipt, legacyObserveWire) {
	t.Helper()
	raw, e := os.ReadFile("testdata/music_legacy_observe_wire.json")
	if e != nil {
		t.Fatal(e)
	}
	var fixture map[string]json.RawMessage
	if e = json.Unmarshal(raw, &fixture); e != nil {
		t.Fatal(e)
	}
	var cases map[string]legacyObserveWire
	if e = json.Unmarshal(fixture[scenario], &cases); e != nil {
		t.Fatal(e)
	}
	native := cases["terminal"]
	h, c, call, caller := sessionMediaFixture(t)
	call.platformNativeMusicProvider = "ytmusic_android"
	cmd := protocol.VoiceMediaCommand{Provider: "call_media_session", Action: "session", CommandID: native.CommandID, Sequence: native.Sequence, MediaSession: &protocol.MediaSessionCommand{OperationID: native.CommandID, Target: "native_music", Action: "play", URI: "https://music.youtube.com/watch?v=yKNxeF4KMsY", MediaID: "yKNxeF4KMsY", Title: "Yellow", Artist: "Coldplay"}}
	r := &callMediaReceipt{command: cmd, settled: make(chan struct{}), result: backend.VoiceMediaResult{State: "queued", Provider: "call_media_session", CommandID: cmd.CommandID, Sequence: cmd.Sequence}}
	call.callMedia = &callMediaSession{receipts: map[string]*callMediaReceipt{"fixture": r}, byCommand: map[string]*callMediaReceipt{cmd.CommandID: r}, current: r, sequence: 1}
	return h, c, call, caller, cmd, r, cases["statusAfterIssuedRead"]
}
func legacyTerminal(t *testing.T, scenario string) *protocol.MediaSessionState {
	t.Helper()
	raw, e := os.ReadFile("testdata/music_legacy_observe_wire.json")
	if e != nil {
		t.Fatal(e)
	}
	var all map[string]json.RawMessage
	json.Unmarshal(raw, &all)
	var cases map[string]legacyObserveWire
	json.Unmarshal(all[scenario], &cases)
	return cases["terminal"].State
}
func TestMusicConfirmationProductionLegacyPreReadTerminalIsCausal(t *testing.T) {
	for _, name := range []string{"unknown", "foreign"} {
		t.Run(name, func(t *testing.T) {
			h, c, call, _, cmd, r, _ := legacyNativeFixture(t, name)
			x := legacyTerminal(t, name)
			sessionFeedback(h, c, call, cmd, x)
			h.voiceMu.Lock()
			defer h.voiceMu.Unlock()
			out := callMediaResult(call.callMedia, r)
			if !r.closed || r.causal == nil || out.ErrorCode != x.Error || out.ActionConfirmed || out.PlaybackConfirmed {
				t.Fatal("lost actual frozen Android terminal", out)
			}
			select {
			case <-r.settled:
			default:
				t.Fatal("first pre-read terminal did not settle")
			}
		})
	}
}
func TestMusicConfirmationProductionLegacyAfterIssuedReadRemainsUnknown(t *testing.T) {
	for _, name := range []string{"unknown", "foreign"} {
		t.Run(name, func(t *testing.T) {
			h, c, call, caller, cmd, r, wire := legacyNativeFixture(t, name)
			done := make(chan any, 1)
			go func() {
				v, _, e := h.executeCallMediaSession(context.Background(), caller.Parent, caller.VoiceID, caller.RequestID, "synthetic-original-read", "fixture", call, "status", backend.VoiceMediaRequest{OriginalCommandID: cmd.CommandID})
				if e != nil {
					done <- e
				} else {
					done <- v
				}
			}()
			read := nextSessionCommand(t, c)
			if read.CommandID != wire.CommandID || read.Sequence != wire.Sequence || read.MediaSession.Action != "observe" {
				t.Fatal("not original read")
			}
			sessionFeedback(h, c, call, cmd, wire.State)
			out, ok := (<-done).(backend.VoiceMediaResult)
			if !ok || out.State != "unknown" || out.ErrorCode != "media_outcome_unknown" || out.ActionConfirmed || out.PlaybackConfirmed {
				t.Fatal("ambiguous response inferred an effect outcome", out)
			}
			h.voiceMu.Lock()
			defer h.voiceMu.Unlock()
			if !r.observeIssued || r.causal != nil || r.closed || r.receivedAt != 0 {
				t.Fatal("read settled pending effect")
			}
			select {
			case <-r.settled:
				t.Fatal("closed effect waiter")
			default:
			}
		})
	}
}
func TestMusicConfirmationLegacyTerminalRequiresCompleteStrictPreReadShape(t *testing.T) {
	for _, change := range []func(*protocol.MediaSessionState){
		func(x *protocol.MediaSessionState) { x.Error = "" }, func(x *protocol.MediaSessionState) { x.ObservationAgeMS = 0 }, func(x *protocol.MediaSessionState) { x.Dispatched = true }, func(x *protocol.MediaSessionState) { x.Phase = "preparing" },
	} {
		h, c, call, _, cmd, r, _ := legacyNativeFixture(t, "unknown")
		x := legacyTerminal(t, "unknown")
		change(x)
		sessionFeedback(h, c, call, cmd, x)
		h.voiceMu.Lock()
		if r.causal != nil || r.closed {
			t.Fatal("incomplete shape guessed as first terminal", x)
		}
		h.voiceMu.Unlock()
	}
}
func TestMusicConfirmationLateCausalRespectsAcceptedObservationFloor(t *testing.T) {
	for _, lowerRevision := range []bool{false, true} {
		t.Run(map[bool]string{false: "older_version", true: "older_revision"}[lowerRevision], func(t *testing.T) {
			h, c, call, _, cmd, r := confirmationStart(t, true)
			before := cloneMediaSession(r.causal)
			at := r.receivedAt
			observed := confirmationObservation(cmd, 4)
			if lowerRevision {
				observed.Revision = 2
			}
			sessionFeedback(h, c, call, cmd, observed)
			late := sessionState(cmd, 2)
			if lowerRevision {
				late.Version = 5
			}
			sessionFeedback(h, c, call, cmd, late)
			h.voiceMu.Lock()
			defer h.voiceMu.Unlock()
			if !reflect.DeepEqual(r.causal, before) || r.receivedAt != at || callMediaResult(call.callMedia, r).PlaybackConfirmed {
				t.Fatal("older causal crossed read ordering floor")
			}
		})
	}
}
func TestMusicConfirmationNewerGenuineCausalStillResolvesPending(t *testing.T) {
	h, c, call, _, cmd, r, _ := legacyNativeFixture(t, "unknown")
	observed := legacyTerminal(t, "unknown")
	observed.Version = 4
	observed.ObservationAgeMS = 0
	r.observeIssued = true
	sessionFeedback(h, c, call, cmd, observed)
	causal := sessionState(cmd, 5)
	causal.NativeProvider = "ytmusic_android"
	sessionFeedback(h, c, call, cmd, causal)
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	out := callMediaResult(call.callMedia, r)
	if !r.closed || r.causal == nil || r.causal.Version != 5 || !out.ActionConfirmed || !out.PlaybackConfirmed {
		t.Fatal("newer real causal receipt lost", out)
	}
}
func TestMusicConfirmationExplicitNewNativeRefusalIsUnambiguousAfterRead(t *testing.T) {
	h, c, call, _, cmd, r, _ := legacyNativeFixture(t, "unknown")
	r.observeIssued = true
	x := legacyTerminal(t, "unknown")
	x.Stage = "refused"
	sessionFeedback(h, c, call, cmd, x)
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if r.causal == nil || !r.closed || r.causal.Error != "initial_media_unknown" || callMediaResult(call.callMedia, r).ActionConfirmed {
		t.Fatal("explicit refusal was lost after read")
	}
}
func TestMusicConfirmationLateInitialTerminalCannotRenewAcceptedObservation(t *testing.T) {
	h, c, call, _, cmd, r, _ := legacyNativeFixture(t, "unknown")
	x := legacyTerminal(t, "unknown")
	x.ObservationAgeMS = 0
	x.Version = 4
	sessionFeedback(h, c, call, cmd, x)
	original := legacyTerminal(t, "unknown")
	sessionFeedback(h, c, call, cmd, original)
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if r.causal != nil || r.closed || r.receivedAt != 0 || call.callMedia.snapshot.Version != 4 {
		t.Fatal("older pre-read terminal bypassed observation floor")
	}
}
