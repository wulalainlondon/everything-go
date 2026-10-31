package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/mediaservice"
	"everything-go/internal/protocol"
	"fmt"
	"strings"
	"testing"
	"time"
)

func sessionMediaFixture(t *testing.T) (*Hub, *Client, *clientVoiceCall, backend.SessionControlCaller) {
	t.Helper()
	h, _, caller, _ := controllerFixture(t)
	for c, v := range h.voiceClients {
		v.requestID = "native-start-original"
		v.mediaCapable = true
		v.externalMusicPlatform = "ios"
		v.musicProvider = "youtube"
		v.mediaGeneration = "original-generation"
		configureMediaSession(v, true)
		return h, c, v, caller
	}
	panic("fixture")
}
func nextSessionCommand(t *testing.T, c *Client) protocol.VoiceMediaCommand {
	t.Helper()
	event := nextVoiceState(t, c, "media")
	var cmd protocol.VoiceMediaCommand
	b, _ := json.Marshal(event["media"])
	if err := json.Unmarshal(b, &cmd); err != nil {
		t.Fatal(err)
	}
	if event["request_id"] != "native-start-original" || cmd.Provider != "call_media_session" || cmd.Action != "session" || cmd.MediaSession == nil || cmd.MediaSession.OperationID != cmd.CommandID || !mediaSessionCommandID.MatchString(cmd.CommandID) {
		t.Fatalf("wrong single admission: %#v %+v", event, cmd)
	}
	return cmd
}
func sessionState(cmd protocol.VoiceMediaCommand, version uint64) *protocol.MediaSessionState {
	mediaID := cmd.MediaSession.MediaID
	if mediaID == "" {
		mediaID = "M7lc1UVf-VE"
	}
	return &protocol.MediaSessionState{Kind: "receipt", OperationID: cmd.CommandID, Revision: cmd.Sequence, Version: version, ActiveSource: cmd.MediaSession.Target, ReservedSource: cmd.MediaSession.Target, NativeProvider: "apple_music_ios", Phase: "playing", Stage: "confirmed", Dispatched: true, ActionConfirmed: true, PlaybackConfirmed: true, MediaID: mediaID, BindingID: "binding-original", ObservationAgeMS: 0, SupportedActions: []string{"pause", "resume", "seek", "stop"}}
}
func sessionFeedback(h *Hub, c *Client, call *clientVoiceCall, cmd protocol.VoiceMediaCommand, x *protocol.MediaSessionState) {
	h.handleVoiceMediaState(c, clientproto.Command{SessionID: call.session.ID, ThreadID: call.session.ResumeID(), VoiceID: call.voiceID, RequestID: call.requestID, MediaCommandID: cmd.CommandID, MediaSequence: cmd.Sequence, MediaSessionState: x})
}
func runSession(t *testing.T, h *Hub, c *Client, call *clientVoiceCall, caller backend.SessionControlCaller, method string, input backend.VoiceMediaRequest, version uint64) (protocol.VoiceMediaCommand, backend.VoiceMediaResult) {
	t.Helper()
	done := make(chan any, 1)
	go func() {
		r, e := h.ExecuteVoiceMedia(context.Background(), caller, method, input)
		if e != nil {
			done <- e
		} else {
			done <- r
		}
	}()
	cmd := nextSessionCommand(t, c)
	x := sessionState(cmd, version)
	switch cmd.MediaSession.Action {
	case "pause":
		x.Phase = "paused"
		x.PlaybackConfirmed = false
	case "stop":
		x.Phase = "stopped"
		x.PlaybackConfirmed = false
	case "seek":
		x.PositionMS = cmd.MediaSession.PositionMS
	case "volume":
		x.Volume = cmd.MediaSession.Volume
	case "observe":
		x.ActionConfirmed = false
		x.Stage = "observe"
	}
	sessionFeedback(h, c, call, cmd, x)
	v := <-done
	r, ok := v.(backend.VoiceMediaResult)
	if !ok {
		t.Fatal(v)
	}
	return cmd, r
}
func TestMediaSessionNegotiationFixedPlatformAndOldClient(t *testing.T) {
	for _, tc := range []struct {
		platform         string
		cap, media, want bool
		provider         string
	}{{"ios", true, true, true, "apple_music_ios"}, {"android", true, true, true, "ytmusic_android"}, {"ios", false, true, false, ""}, {"ios", true, false, false, ""}, {"desktop", true, true, false, ""}} {
		c := &clientVoiceCall{externalMusicPlatform: tc.platform, musicProvider: "youtube", mediaCapable: tc.media}
		configureMediaSession(c, tc.cap)
		if c.mediaSessionCapable != tc.want || c.platformNativeMusicProvider != tc.provider || c.musicProvider != "youtube" {
			t.Fatal(tc, c)
		}
	}
	h, _, caller, _ := controllerFixture(t)
	for _, v := range h.voiceClients {
		v.mediaCapable = true
	}
	if _, err := h.ExecuteVoiceMedia(context.Background(), caller, "control", backend.VoiceMediaRequest{Source: "youtube", Action: "pause"}); err == nil {
		t.Fatal("new source field silently fell back to legacy")
	}
}
func TestMediaSessionReceiptCannotBeCompletedBySnapshotOrForeignStart(t *testing.T) {
	h, c, call, caller := sessionMediaFixture(t)
	done := make(chan any, 1)
	go func() {
		v, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_youtube", backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"})
		if e != nil {
			done <- e
		} else {
			done <- v
		}
	}()
	cmd := nextSessionCommand(t, c)
	x := sessionState(cmd, 1)
	ack := clientproto.Command{SessionID: call.session.ID, ThreadID: call.session.ResumeID(), VoiceID: call.voiceID, RequestID: call.requestID, MediaCommandID: cmd.CommandID, MediaSequence: cmd.Sequence, MediaSessionState: x}
	for _, mutate := range []func(*clientproto.Command){func(a *clientproto.Command) { a.RequestID = "local-ui-start-id" }, func(a *clientproto.Command) { a.VoiceID = "other" }, func(a *clientproto.Command) { a.ThreadID = "other" }, func(a *clientproto.Command) { a.MediaSequence++ }, func(a *clientproto.Command) { a.MediaCommandID = "vm_" + strings.Repeat("a", 64) }} {
		bad := ack
		mutate(&bad)
		h.handleVoiceMediaState(c, bad)
	}
	bad := *x
	bad.OperationID = "vm_" + strings.Repeat("b", 64)
	sessionFeedback(h, c, call, cmd, &bad)
	bad = *x
	bad.NativeProvider = "ytmusic_android"
	sessionFeedback(h, c, call, cmd, &bad)
	bad = *x
	bad.MediaID = "abcdefghijk"
	sessionFeedback(h, c, call, cmd, &bad)
	// An actual local snapshot can say playing but cannot settle the remote play.
	snapshot := *x
	snapshot.Kind = "snapshot"
	snapshot.OperationID = "local_synthetic"
	snapshot.ActionConfirmed = false
	sessionFeedback(h, c, call, cmd, &snapshot)
	select {
	case v := <-done:
		t.Fatal("noncausal feedback settled", v)
	case <-time.After(20 * time.Millisecond):
	}
	sessionFeedback(h, c, call, cmd, x)
	r, ok := (<-done).(backend.VoiceMediaResult)
	if !ok || !r.ActionConfirmed || !r.PlaybackConfirmed || r.MediaSessionReceipt == nil || r.MediaSession == nil || r.MediaSession.ActionConfirmed {
		t.Fatal(r)
	}
}
func TestMediaSessionSingleSwitchFixedAppleAndOriginalOperationObserve(t *testing.T) {
	h, c, call, caller := sessionMediaFixture(t)
	store, e := mediaservice.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	origin := backend.MediaOrigin{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, Generation: call.mediaGeneration, TurnID: caller.TurnID}
	u := &unifiedVoiceMedia{store: store}
	play := func(id, method, url string, version uint64) (protocol.VoiceMediaCommand, mediaservice.Result) {
		t.Helper()
		done := make(chan mediaservice.Result, 1)
		go func() {
			r, err := h.executeMediaWithOrigin(context.Background(), u, origin, mediaservice.Request{ID: id, Thread: origin.ThreadID, Method: method, Input: backend.VoiceMediaRequest{URL: url}}, "native_tool")
			if err != nil {
				t.Error(err)
			}
			done <- r
		}()
		cmd := nextSessionCommand(t, c)
		sessionFeedback(h, c, call, cmd, sessionState(cmd, version))
		return cmd, <-done
	}
	yt, first := play("vop_original_youtube", "play_youtube", "https://youtu.be/M7lc1UVf-VE", 1)
	apple, second := play("vop_original_apple", "play_music", "https://music.apple.com/us/song/123456789", 2)
	if first.CommandID == second.CommandID || apple.MediaSession.Target != "native_music" || apple.MediaSession.MediaID != "123456789" || call.platformNativeMusicProvider != "apple_music_ios" || call.musicProvider != "youtube" {
		t.Fatal(first, second, apple)
	}
	select {
	case <-c.send:
		t.Fatal("Go emitted a second pause/play step")
	default:
	}
	query := mediaservice.Request{ID: "vop_query_original", Thread: origin.ThreadID, Method: "status", Operation: "vop_original_youtube"}
	done := make(chan mediaservice.Result, 1)
	go func() {
		r, err := h.executeMediaWithOrigin(context.Background(), u, origin, query, "native_tool")
		if err != nil {
			t.Error(err)
		}
		done <- r
	}()
	observe := nextSessionCommand(t, c)
	if observe.CommandID != yt.CommandID || observe.Sequence != yt.Sequence || observe.MediaSession.Target != "youtube" || observe.MediaSession.Action != "observe" {
		t.Fatal("queried new provider", observe)
	}
	x := sessionState(yt, 3)
	x.Phase = "stopped"
	x.ActiveSource = "native_music"
	x.ReservedSource = "native_music"
	x.MediaID = apple.MediaSession.MediaID
	x.ActionConfirmed = false
	x.PlaybackConfirmed = false
	x.Stage = "observe"
	sessionFeedback(h, c, call, yt, x)
	r := <-done
	if r.CommandID != yt.CommandID || r.ActionConfirmed || r.PlaybackConfirmed {
		t.Fatal(r)
	}
	// Cross-transport retry remains a historical receipt, never another start.
	dup, e := h.executeMediaWithOrigin(context.Background(), u, origin, mediaservice.Request{ID: "vop_original_youtube", Thread: origin.ThreadID, Method: "play_youtube", Input: backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}}, "local_user_socket")
	if e != nil || dup.PlaybackConfirmed || dup.MediaSessionReceipt.ActionConfirmed {
		t.Fatal(dup, e)
	}
	select {
	case <-c.send:
		t.Fatal("original intent replayed")
	default:
	}
}
func TestMediaSessionSourceGainAdoptionSeekAndCausalValidation(t *testing.T) {
	h, c, call, caller := sessionMediaFixture(t)
	_, r := runSession(t, h, c, call, caller, "play_youtube", backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}, 1)
	if !r.ActionConfirmed {
		t.Fatal(r)
	}
	volume := 45
	caller.ToolCallID = "gain"
	cmd, r := runSession(t, h, c, call, caller, "control", backend.VoiceMediaRequest{Action: "volume", Source: "youtube", Volume: &volume}, 2)
	if cmd.MediaSession.Volume == nil || cmd.Volume != 0 || !r.ActionConfirmed {
		t.Fatal(cmd, r)
	}
	wrong := sessionState(cmd, 3)
	wrong.Volume = new(int)
	if sessionCausalMatches(&callMediaReceipt{command: cmd}, wrong) {
		t.Fatal("wrong gain confirmed")
	}
	p := int64(4000)
	revision := uint64(2)
	caller.ToolCallID = "seek"
	cmd, r = runSession(t, h, c, call, caller, "control", backend.VoiceMediaRequest{Action: "seek", Source: "youtube", PositionMS: &p, ExpectedRevision: &revision}, 3)
	if cmd.MediaSession.ExpectedRevision == nil || *cmd.MediaSession.PositionMS != p || !r.ActionConfirmed {
		t.Fatal(cmd, r)
	}
	caller.ToolCallID = "adopt"
	cmd, _ = runSession(t, h, c, call, caller, "play_music", backend.VoiceMediaRequest{URL: "https://music.apple.com/us/song/123456789", AdoptCurrent: true, ExpectedMediaID: "foreign-exact"}, 4)
	if !cmd.MediaSession.AdoptCurrent || cmd.MediaSession.ExpectedMediaID != "foreign-exact" {
		t.Fatal(cmd)
	}
	for i, input := range []backend.VoiceMediaRequest{{Action: "volume", Source: "native_music", Volume: &volume}, {Action: "pause", AdoptCurrent: true}, {Action: "pause", ExpectedMediaID: "not-consented"}, {Action: "seek", Source: "youtube"}, {Action: "next", Source: "youtube"}, {Query: "search"}} {
		caller.ToolCallID = fmt.Sprintf("bad-%d", i)
		if _, err := h.ExecuteVoiceMedia(context.Background(), caller, "control", input); err == nil {
			t.Fatal("invalid command admitted", input)
		}
	}
	select {
	case <-c.send:
		t.Fatal("refusal dispatched")
	default:
	}
}
func TestMediaSessionSnapshotOrderingAgingAndReceiptIsolation(t *testing.T) {
	h, c, call, caller := sessionMediaFixture(t)
	cmd, _ := runSession(t, h, c, call, caller, "play_youtube", backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}, 5)
	x := sessionState(cmd, 6)
	x.Kind = "snapshot"
	x.OperationID = "local_exact"
	x.ActionConfirmed = false
	sessionFeedback(h, c, call, cmd, x)
	h.voiceMu.Lock()
	m := call.callMedia
	r := m.byCommand[cmd.CommandID]
	before := m.snapshotAt
	h.voiceMu.Unlock()
	x.Version = 5
	sessionFeedback(h, c, call, cmd, x)
	h.voiceMu.Lock()
	if m.snapshot.Version != 6 || m.snapshotAt != before {
		t.Fatal("old sample renewed")
	}
	r.receivedAt = time.Now().Add(-6 * time.Second).UnixMilli()
	m.snapshotAt = r.receivedAt
	out := callMediaResult(m, r)
	h.voiceMu.Unlock()
	if out.Observed || out.ActionConfirmed || out.PlaybackConfirmed || out.MediaSessionReceipt.ActionConfirmed || out.MediaSessionReceipt.PlaybackConfirmed || out.MediaSession.PlaybackConfirmed {
		t.Fatal("stale nested report promoted", out)
	}
	if !r.causal.ActionConfirmed {
		t.Fatal("projection mutated receipt")
	}
}
func TestMediaSessionObserveDoesNotReplayOriginalGain(t *testing.T) {
	// Causal matching of an observation is never permission to execute the original action.
	h, c, call, caller := sessionMediaFixture(t)
	_, _ = runSession(t, h, c, call, caller, "play_youtube", backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}, 1)
	caller.ToolCallID = "status"
	cmd, r := runSession(t, h, c, call, caller, "status", backend.VoiceMediaRequest{}, 2)
	if cmd.MediaSession.Action != "observe" || r.ActionConfirmed || !r.Observed {
		t.Fatal(cmd, r)
	}
}

func TestMediaSessionConcurrentSameIntentQueuesOnce(t *testing.T) {
	h, c, call, caller := sessionMediaFixture(t)
	done := make(chan any, 12)
	for i := 0; i < 12; i++ {
		go func() {
			v, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_youtube", backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"})
			if e != nil {
				done <- e
			} else {
				done <- v
			}
		}()
	}
	cmd := nextSessionCommand(t, c)
	sessionFeedback(h, c, call, cmd, sessionState(cmd, 1))
	for i := 0; i < 12; i++ {
		if v := <-done; v == nil {
			t.Fatal("missing receipt")
		} else if _, ok := v.(error); ok {
			t.Fatal(v)
		}
	}
	select {
	case <-c.send:
		t.Fatal("concurrent duplicate queued a second effect")
	default:
	}
}
func TestMediaSessionUnknownCausalAndCallEndedDoNotConfirm(t *testing.T) {
	h, c, call, caller := sessionMediaFixture(t)
	done := make(chan any, 1)
	go func() {
		v, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{URL: "https://music.apple.com/us/song/123456789"})
		if e != nil {
			done <- e
		} else {
			done <- v
		}
	}()
	cmd := nextSessionCommand(t, c)
	x := sessionState(cmd, 1)
	x.Phase = "unknown"
	x.Stage = "start_target"
	x.ActionConfirmed = false
	x.PlaybackConfirmed = false
	x.ObservationAgeMS = -1
	x.MayBePlaying = true
	sessionFeedback(h, c, call, cmd, x)
	r := (<-done).(backend.VoiceMediaResult)
	if r.ActionConfirmed || r.PlaybackConfirmed || r.Observed || r.State != "unknown" || !r.MediaSessionReceipt.MayBePlaying {
		t.Fatal(r)
	}
	caller.ToolCallID = "second"
	go func() {
		v, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_youtube", backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"})
		if e != nil {
			done <- e
		} else {
			done <- v
		}
	}()
	next := nextSessionCommand(t, c)
	h.voiceMu.Lock()
	call.stopping = true
	close(call.callMedia.byCommand[next.CommandID].settled)
	call.callMedia.byCommand[next.CommandID].closed = true
	h.voiceMu.Unlock()
	r = (<-done).(backend.VoiceMediaResult)
	if r.State != "voice_ended" || r.PlaybackConfirmed || r.ActionConfirmed {
		t.Fatal(r)
	}
}

func TestMediaSessionConfirmedControlsCannotBorrowOtherSourceObservation(t *testing.T) {
	position := int64(1234)
	for _, action := range []string{"pause", "stop", "seek"} {
		t.Run(action, func(t *testing.T) {
			cmd := protocol.VoiceMediaCommand{MediaSession: &protocol.MediaSessionCommand{Action: action, Target: "youtube", PositionMS: &position}}
			x := &protocol.MediaSessionState{ActiveSource: "native_music", ReservedSource: "native_music", Phase: "paused", Stage: "confirmed", ActionConfirmed: true, PositionMS: &position, ObservationAgeMS: 0}
			if action == "stop" {
				x.Phase = "stopped"
			}
			if sessionCausalMatches(&callMediaReceipt{command: cmd}, x) {
				t.Fatal("other source observation confirmed original control")
			}
			x.ActiveSource = "youtube"
			x.ReservedSource = "youtube"
			if !sessionCausalMatches(&callMediaReceipt{command: cmd}, x) {
				t.Fatal("exact source positive refused")
			}
		})
	}
}
