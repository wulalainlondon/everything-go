package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"reflect"
	"strings"
	"testing"
	"time"
)

func confirmationStart(t *testing.T, unknown bool) (*Hub, *Client, *clientVoiceCall, backend.SessionControlCaller, protocol.VoiceMediaCommand, *callMediaReceipt) {
	t.Helper()
	h, c, call, caller := sessionMediaFixture(t)
	done := make(chan any, 1)
	go func() {
		v, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{URL: "https://music.apple.com/us/song/123456789", Title: "Yellow", Artist: "Coldplay"})
		if e != nil {
			done <- e
		} else {
			done <- v
		}
	}()
	cmd := nextSessionCommand(t, c)
	x := sessionState(cmd, 1)
	x.Title = "Yellow"
	x.Artist = "Coldplay"
	if unknown {
		x.Phase = "unknown"
		x.Stage = "start_target"
		x.Error = "playback_unconfirmed"
		x.ActionConfirmed = false
		x.PlaybackConfirmed = false
		x.MayBePlaying = true
	}
	sessionFeedback(h, c, call, cmd, x)
	if _, ok := (<-done).(backend.VoiceMediaResult); !ok {
		t.Fatal("missing effect receipt")
	}
	return h, c, call, caller, cmd, call.callMedia.byCommand[cmd.CommandID]
}
func confirmationObservation(cmd protocol.VoiceMediaCommand, version uint64) *protocol.MediaSessionState {
	x := sessionState(cmd, version)
	x.Stage = "observe"
	x.ActionConfirmed = false
	x.Dispatched = false
	return x
}
func TestMusicConfirmationObservePreservesOriginalOutcomeAndFreshness(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "confirmed", true: "unknown"}[unknown], func(t *testing.T) {
			h, c, call, _, cmd, r := confirmationStart(t, unknown)
			h.voiceMu.Lock()
			r.receivedAt = time.Now().Add(-6 * time.Second).UnixMilli()
			before := cloneMediaSession(r.causal)
			at := r.receivedAt
			h.voiceMu.Unlock()
			x := confirmationObservation(cmd, 2)
			x.Error = "native_bookmark_retired"
			x.Phase = "unknown"
			x.PlaybackConfirmed = false
			if unknown {
				x.Error = ""
				x.Phase = "playing"
				x.PlaybackConfirmed = true
			}
			sessionFeedback(h, c, call, cmd, x)
			h.voiceMu.Lock()
			defer h.voiceMu.Unlock()
			if !reflect.DeepEqual(r.causal, before) || r.receivedAt != at {
				t.Fatal("read-only observation overwrote original effect/freshness", r.causal)
			}
			out := callMediaResult(call.callMedia, r)
			if out.PlaybackConfirmed || out.ActionConfirmed || out.Observed || out.ErrorCode != before.Error {
				t.Fatal("current observation promoted/replaced original outcome", out)
			}
			if out.MediaSession == nil || out.MediaSession.Version != 2 || out.MediaSession.ActionConfirmed {
				t.Fatal("separate current observation missing", out)
			}
		})
	}
}
func TestMusicConfirmationObserveCannotSettlePendingEffect(t *testing.T) {
	h, c, call, _, cmd, r := confirmationStart(t, false)
	h.voiceMu.Lock()
	r.causal = nil
	r.receivedAt = 0
	r.closed = false
	r.settled = make(chan struct{})
	r.observed = make(chan struct{})
	wait := r.observed
	h.voiceMu.Unlock()
	x := confirmationObservation(cmd, 2)
	x.Phase = "unknown"
	x.Error = "native_bookmark_retired"
	x.PlaybackConfirmed = false
	sessionFeedback(h, c, call, cmd, x)
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if r.causal != nil || r.receivedAt != 0 || r.closed {
		t.Fatal("observe settled/assigned pending effect")
	}
	select {
	case <-r.settled:
		t.Fatal("effect waiter closed")
	default:
	}
	select {
	case <-wait:
	default:
		t.Fatal("original status waiter not notified")
	}
}
func TestMusicConfirmationObservedItemChangeIsCurrentOnly(t *testing.T) {
	h, c, call, _, cmd, r := confirmationStart(t, false)
	before := cloneMediaSession(r.causal)
	x := confirmationObservation(cmd, 2)
	x.MediaID = "other-current-item"
	x.Title = "another song"
	sessionFeedback(h, c, call, cmd, x)
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	out := callMediaResult(call.callMedia, r)
	if !reflect.DeepEqual(r.causal, before) || out.MediaSession == nil || out.MediaSession.MediaID != x.MediaID {
		t.Fatal("item change lost or became causal", out)
	}
	if out.ActionConfirmed || out.PlaybackConfirmed {
		t.Fatal("original success treated as current item success", out)
	}
}
func TestMusicConfirmationOriginalStatusQueuesObserveWithoutNewMutation(t *testing.T) {
	h, c, call, caller, cmd, r := confirmationStart(t, true)
	before := cloneMediaSession(r.causal)
	at := r.receivedAt
	done := make(chan any, 1)
	go func() {
		v, _, e := h.executeCallMediaSession(context.Background(), caller.Parent, caller.VoiceID, caller.RequestID, "status-original", "fixture", call, "status", backend.VoiceMediaRequest{OriginalCommandID: cmd.CommandID})
		if e != nil {
			done <- e
		} else {
			done <- v
		}
	}()
	observed := nextSessionCommand(t, c)
	if observed.CommandID != cmd.CommandID || observed.Sequence != cmd.Sequence || observed.MediaSession.Action != "observe" {
		t.Fatal("original operation rerouted", observed)
	}
	sessionFeedback(h, c, call, cmd, confirmationObservation(cmd, 2))
	out, ok := (<-done).(backend.VoiceMediaResult)
	if !ok || out.State != "unknown" || out.ErrorCode != "playback_unconfirmed" || out.PlaybackConfirmed || out.ActionConfirmed {
		t.Fatal(out)
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if !reflect.DeepEqual(r.causal, before) || r.receivedAt != at {
		t.Fatal("status replaced causal receipt")
	}
}

func TestMusicConfirmationDisplayLabelNeverBecomesNativeExpectation(t *testing.T) {
	h, c, call, caller := sessionMediaFixture(t)
	input := backend.VoiceMediaRequest{URL: "https://music.apple.com/us/song/123456789", Title: "Yellow", Artist: "Coldplay", DisplayLabel: "Coldplay - Yellow"}
	cmd, result := runSession(t, h, c, call, caller, "play_music", input, 1)
	if cmd.MediaSession.Title != "Yellow" || cmd.MediaSession.Artist != "Coldplay" || cmd.ExpectedTitle != "" || cmd.Title != "" || result.DisplayLabel != input.DisplayLabel {
		t.Fatal(cmd, result)
	}
	raw, _ := json.Marshal(cmd)
	if strings.Contains(string(raw), input.DisplayLabel) || strings.Contains(string(raw), "display_label") {
		t.Fatal("display label leaked to native wire", string(raw))
	}
	// Neither display text nor an otherwise correct title can waive a different
	// nonempty actual item ID. This is still the original causal matcher.
	wrong := sessionState(cmd, 2)
	wrong.MediaID = "different-exact-id"
	wrong.Title = "Yellow"
	sessionFeedback(h, c, call, cmd, wrong)
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if call.callMedia.byCommand[cmd.CommandID].causal.Version != 1 {
		t.Fatal("label/title bypassed strong ID")
	}
}
func TestMusicConfirmationLegacyLabelCannotChangeExpectedTitleOrID(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var c *Client
	for owner, call := range h.voiceClients {
		c = owner
		call.mediaCapable = true
		call.externalMusicCapable = true
	}
	input := backend.VoiceMediaRequest{URL: "https://music.youtube.com/watch?v=yKNxeF4KMsY", Title: "Yellow", Artist: "Coldplay", DisplayLabel: "Coldplay - Yellow"}
	done := make(chan any, 1)
	go func() {
		r, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", input)
		if e != nil {
			done <- e
		} else {
			done <- r
		}
	}()
	event := nextVoiceState(t, c, "media")
	var cmd protocol.VoiceMediaCommand
	raw, _ := json.Marshal(event["media"])
	if err := json.Unmarshal(raw, &cmd); err != nil {
		t.Fatal(err)
	}
	if cmd.ExpectedTitle != "Yellow" || cmd.ExpectedArtist != "Coldplay" || cmd.ExpectedID != "yKNxeF4KMsY" || strings.Contains(string(raw), input.DisplayLabel) {
		t.Fatal("legacy expectations contaminated", string(raw))
	}
	x := &protocol.ExternalMusicState{Provider: "ytmusic_android", PackageName: "com.google.android.apps.youtube.music", BindingID: "synthetic", OperationID: cmd.CommandID, State: "unknown", Error: "playback_unconfirmed", ObservationAgeMS: 0}
	h.handleVoiceMediaState(c, clientproto.Command{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaCommandID: cmd.CommandID, MediaSequence: cmd.Sequence, ExternalMusic: x})
	if r, ok := (<-done).(backend.VoiceMediaResult); !ok || r.DisplayLabel != input.DisplayLabel || r.PlaybackConfirmed {
		t.Fatal(r)
	}
}
func TestMusicConfirmationLabelsCannotSmuggleControlExpectations(t *testing.T) {
	for _, method := range []string{"control", "music_control", "status", "voice_volume", "media_volume"} {
		t.Run(method, func(t *testing.T) {
			h, c, _, caller := sessionMediaFixture(t)
			if _, err := h.ExecuteVoiceMedia(context.Background(), caller, method, backend.VoiceMediaRequest{Action: "pause", DisplayLabel: "Yellow"}); err == nil {
				t.Fatal("control accepted display-only argument")
			}
			select {
			case <-c.send:
				t.Fatal("invalid label dispatched")
			default:
			}
		})
	}
	for _, label := range []string{strings.Repeat("x", 201), "line\nbreak", string([]byte{0xff})} {
		h, c, _, caller := sessionMediaFixture(t)
		if _, err := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{URL: "https://music.apple.com/us/song/123456789", DisplayLabel: label}); err == nil {
			t.Fatal("unsafe display label")
		}
		select {
		case <-c.send:
			t.Fatal("invalid label dispatched")
		default:
		}
	}
}
func TestMusicConfirmationObservationOrderingForeignAndRetiredReceipt(t *testing.T) {
	h, c, call, caller, cmd, r := confirmationStart(t, true)
	before := cloneMediaSession(r.causal)
	at := r.receivedAt
	x := confirmationObservation(cmd, 4)
	sessionFeedback(h, c, call, cmd, x)
	h.voiceMu.Lock()
	snapshotAt := call.callMedia.snapshotAt
	h.voiceMu.Unlock()
	for _, mutate := range []func(*clientproto.Command){
		func(a *clientproto.Command) { a.ThreadID = "foreign-thread" }, func(a *clientproto.Command) { a.RequestID = "foreign-start" }, func(a *clientproto.Command) { a.VoiceID = "foreign-call" }, func(a *clientproto.Command) { a.MediaSequence++ },
		func(a *clientproto.Command) { a.MediaSessionState.OperationID = "vm_" + strings.Repeat("f", 64) }, func(a *clientproto.Command) { a.MediaSessionState.NativeProvider = "ytmusic_android" },
		func(a *clientproto.Command) { a.MediaSessionState.ActionConfirmed = true }, func(a *clientproto.Command) { a.MediaSessionState.Revision = 0 }, func(a *clientproto.Command) { a.MediaSessionState.Version = 3 },
	} {
		bad := clientproto.Command{SessionID: call.session.ID, ThreadID: call.session.ResumeID(), RequestID: call.requestID, VoiceID: call.voiceID, MediaCommandID: cmd.CommandID, MediaSequence: cmd.Sequence, MediaSessionState: confirmationObservation(cmd, 5)}
		mutate(&bad)
		h.handleVoiceMediaState(c, bad)
	}
	sessionFeedback(h, c, call, cmd, x) // duplicate cannot renew its own observation.
	h.voiceMu.Lock()
	if r.receivedAt != at || !reflect.DeepEqual(r.causal, before) || call.callMedia.snapshot.Version != 4 || call.callMedia.snapshotAt != snapshotAt {
		t.Fatal("foreign/late observation changed scope or renewed receipt")
	}
	h.voiceMu.Unlock()
	caller.ToolCallID = "switch-youtube"
	next, _ := runSession(t, h, c, call, caller, "play_youtube", backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}, 6)
	old := confirmationObservation(cmd, 7)
	old.ActiveSource = "youtube"
	old.MediaID = next.MediaSession.MediaID
	sessionFeedback(h, c, call, cmd, old)
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	out := callMediaResult(call.callMedia, r)
	if !reflect.DeepEqual(r.causal, before) || r.receivedAt != at || out.ActionConfirmed || out.PlaybackConfirmed || out.State != "unknown" || call.callMedia.current.command.CommandID != next.CommandID || call.callMedia.snapshot.Version != 6 {
		t.Fatal("retired observation replaced original/current source", out)
	}
}
func TestMusicConfirmationLegacyObserveFlagCannotOverwriteEffect(t *testing.T) {
	h, c, call, _, cmd, r := confirmationStart(t, true)
	before := cloneMediaSession(r.causal)
	at := r.receivedAt
	x := confirmationObservation(cmd, 2)
	x.Dispatched = true // compatibility: older native retained the original flag.
	sessionFeedback(h, c, call, cmd, x)
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if r.receivedAt != at || !reflect.DeepEqual(r.causal, before) || callMediaResult(call.callMedia, r).PlaybackConfirmed {
		t.Fatal("legacy observe became mutation confirmation")
	}
}
