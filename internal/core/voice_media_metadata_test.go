package core

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"reflect"
	"strings"
	"testing"
)

func metadataReceipt() (*callMediaReceipt, *protocol.MediaSessionState) {
	cmd := protocol.VoiceMediaCommand{CommandID: "vm_original", MediaSession: &protocol.MediaSessionCommand{Target: "native_music", Action: "play", MediaID: "1OK1OqA-En4", Title: "Wish You Well", Artist: "Sigala, Becky Hill"}}
	x := sessionState(cmd, 1)
	x.MediaID = ""
	x.NativeProvider = "ytmusic_android"
	x.Title = "Wish You Well (Official Video)"
	x.Artist = "Sigala 和 Becky Hill"
	x.NativeMatch = &protocol.NativeMusicMatch{Version: 1, Basis: "metadata_v1", Action: "play_uri", ExpectedMediaID: "1OK1OqA-En4"}
	return &callMediaReceipt{command: cmd}, x
}
func TestAndroidMetadataCausalEvidenceIsNotAnEmptyIDWaiver(t *testing.T) {
	cases := []struct {
		name   string
		change func(*callMediaReceipt, *protocol.MediaSessionState)
	}{
		{"missing proof", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.NativeMatch = nil }},
		{"wrong known id", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.MediaID = "wrong123456" }},
		{"wrong expected id", func(r *callMediaReceipt, x *protocol.MediaSessionState) {
			x.NativeMatch.ExpectedMediaID = "wrong123456"
		}},
		{"wrong provider", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.NativeProvider = "apple_music_ios" }},
		{"missing binding", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.BindingID = "" }},
		{"stale", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.ObservationAgeMS = 5001 }},
		{"snapshot", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.Kind = "snapshot" }},
		{"read", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.Stage = "observe" }},
		{"not dispatched", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.Dispatched = false }},
		{"not confirmed", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.ActionConfirmed = false }},
		{"unknown", func(r *callMediaReceipt, x *protocol.MediaSessionState) {
			x.Phase = "unknown"
			x.Error = "playback_unconfirmed"
		}},
		{"not playing", func(r *callMediaReceipt, x *protocol.MediaSessionState) {
			x.Phase = "paused"
			x.PlaybackConfirmed = false
		}},
		{"missing credit", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.Artist = "Sigala" }},
		{"extra credit", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.Artist = "Sigala 和 Becky Hill 和 Guest" }},
		{"wrong version", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.Title = "Wish You Well Live" }},
		{"empty expected artist", func(r *callMediaReceipt, x *protocol.MediaSessionState) { r.command.MediaSession.Artist = "" }},
		{"action borrowed", func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.NativeMatch.Action = "resume" }},
	}
	r, x := metadataReceipt()
	if !validCallMediaState(x) || !sessionCausalMatches(r, x) {
		t.Fatal("exact original metadata proof rejected")
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, x := metadataReceipt()
			c.change(r, x)
			if validCallMediaState(x) && sessionCausalMatches(r, x) {
				t.Fatal("unsafe metadata evidence accepted")
			}
		})
	}
}
func controlMetadataReceipt(action string) (*callMediaReceipt, *protocol.MediaSessionState) {
	r, x := metadataReceipt()
	r.command.MediaSession.Action = action
	r.command.MediaSession.MediaID = ""
	r.command.MediaSession.Title = ""
	r.command.MediaSession.Artist = ""
	r.before = cloneMediaSession(x)
	r.before.Kind = "snapshot"
	r.before.NativeMatch = nil
	r.before.ActionConfirmed = false
	r.before.PlaybackConfirmed = false
	x.NativeMatch.Action = action
	x.NativeMatch.ExpectedMediaID = ""
	x.NativeMatch.BeforeBindingID = r.before.BindingID
	x.NativeMatch.BeforeMediaID = r.before.MediaID
	x.NativeMatch.BeforeTitle = r.before.Title
	x.NativeMatch.BeforeArtist = r.before.Artist
	if action == "next" || action == "previous" {
		x.Title = "Easy Love"
		x.Artist = "Sigala"
	}
	return r, x
}
func TestAndroidMetadataControlsBindImmutableBeforeIdentity(t *testing.T) {
	for _, action := range []string{"next", "previous", "resume", "seek"} {
		t.Run(action, func(t *testing.T) {
			r, x := controlMetadataReceipt(action)
			if action == "seek" {
				v := int64(8000)
				r.command.MediaSession.PositionMS = &v
				x.PositionMS = &v
			}
			if !validCallMediaState(x) || !sessionCausalMatches(r, x) {
				t.Fatal("matching action evidence refused")
			}
			for _, mutate := range []func(*callMediaReceipt, *protocol.MediaSessionState){
				func(r *callMediaReceipt, x *protocol.MediaSessionState) { r.before = nil },
				func(r *callMediaReceipt, x *protocol.MediaSessionState) { r.before.Error = "native_bookmark_retired" },
				func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.NativeMatch.BeforeTitle = "different" },
				func(r *callMediaReceipt, x *protocol.MediaSessionState) { x.BindingID = "replacement" },
			} {
				rr := *r
				rr.before = cloneMediaSession(r.before)
				xx := cloneMediaSession(x)
				mutate(&rr, xx)
				if sessionCausalMatches(&rr, xx) {
					t.Fatal("control borrowed a different before identity")
				}
			}
			if action == "next" || action == "previous" {
				x.Title = r.before.Title
				x.Artist = r.before.Artist
				if sessionCausalMatches(r, x) {
					t.Fatal("unchanged item accepted as next")
				}
			}
		})
	}
}
func TestAndroidMetadataOriginalReceiptSurvivesLaterSnapshot(t *testing.T) {
	h, c, call, caller := sessionMediaFixture(t)
	call.externalMusicPlatform = "android"
	configureMediaSession(call, true)
	done := make(chan any, 1)
	go func() {
		v, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{URL: "https://music.youtube.com/watch?v=1OK1OqA-En4", Title: "Wish You Well", Artist: "Sigala, Becky Hill"})
		if e != nil {
			done <- e
		} else {
			done <- v
		}
	}()
	cmd := nextSessionCommand(t, c)
	_, x := metadataReceipt()
	x.OperationID = cmd.CommandID
	x.Revision = cmd.Sequence
	sessionFeedback(h, c, call, cmd, x)
	result, ok := (<-done).(backend.VoiceMediaResult)
	if !ok || !result.ActionConfirmed || !result.PlaybackConfirmed || result.MediaSessionReceipt == nil || result.MediaSessionReceipt.MediaID != "" {
		t.Fatalf("metadata did not settle original receipt without fabricating id: %+v", result)
	}
	r := call.callMedia.byCommand[cmd.CommandID]
	before := cloneMediaSession(r.causal)
	caller.ToolCallID = "metadata-next-original"
	go func() {
		v, e := h.ExecuteVoiceMedia(context.Background(), caller, "music_control", backend.VoiceMediaRequest{Action: "next"})
		if e != nil {
			done <- e
		} else {
			done <- v
		}
	}()
	next := nextSessionCommand(t, c)
	r2 := call.callMedia.byCommand[next.CommandID]
	if r2.before == nil || r2.before.Title != before.Title {
		t.Fatal("precommand ownership snapshot not captured")
	}
	nextState := cloneMediaSession(before)
	nextState.OperationID = next.CommandID
	nextState.Revision = next.Sequence
	nextState.Version = 21
	nextState.Title = "Easy Love"
	nextState.Artist = "Sigala"
	nextState.NativeMatch = &protocol.NativeMusicMatch{Version: 1, Basis: "metadata_v1", Action: "next", BeforeBindingID: before.BindingID, BeforeMediaID: before.MediaID, BeforeTitle: before.Title, BeforeArtist: before.Artist}
	sessionFeedback(h, c, call, next, nextState)
	result, ok = (<-done).(backend.VoiceMediaResult)
	if !ok || !result.ActionConfirmed || !result.PlaybackConfirmed {
		t.Fatalf("metadata transition not confirmed: %+v", result)
	}
	snap := cloneMediaSession(nextState)
	snap.Kind = "snapshot"
	snap.NativeMatch = nil
	snap.ActionConfirmed = false
	snap.PlaybackConfirmed = false
	snap.Version = 30
	snap.Title = "another track"
	snap.Artist = "another artist"
	sessionFeedback(h, c, call, next, snap)
	if !reflect.DeepEqual(r.causal, before) {
		t.Fatal("snapshot rewrote original metadata proof")
	}
}

func TestAndroidMetadataLateReceiptCannotRewriteSealedOutcome(t *testing.T) {
	for _, outcome := range []string{"unknown", "refused", "success"} {
		t.Run(outcome, func(t *testing.T) {
			h, c, call, _ := sessionMediaFixture(t)
			call.externalMusicPlatform = "android"
			configureMediaSession(call, true)
			r, x := metadataReceipt()
			r.command.CommandID = "vm_" + strings.Repeat("a", 64)
			r.command.Sequence = 1
			x.OperationID = r.command.CommandID
			x.Revision = 1
			first := cloneMediaSession(x)
			switch outcome {
			case "unknown":
				first.NativeMatch = nil
				first.Phase = "unknown"
				first.Error = "playback_unconfirmed"
				first.ActionConfirmed = false
				first.PlaybackConfirmed = false
			case "refused":
				first.NativeMatch = nil
				first.Stage = "refused"
				first.Phase = "error"
				first.Error = "permission_required"
				first.Dispatched = false
				first.ActionConfirmed = false
				first.PlaybackConfirmed = false
			}
			r.causal = cloneMediaSession(first)
			r.closed = true
			r.settled = make(chan struct{})
			close(r.settled)
			r.receivedAt = 12345
			call.callMedia = &callMediaSession{byCommand: map[string]*callMediaReceipt{r.command.CommandID: r}, current: r}
			before := cloneMediaSession(r.causal)
			x.Version = 2
			if outcome == "success" {
				x.NativeMatch = nil
				x.Phase = "unknown"
				x.Error = "playback_unconfirmed"
				x.ActionConfirmed = false
				x.PlaybackConfirmed = false
			}
			sessionFeedback(h, c, call, r.command, x)
			if !reflect.DeepEqual(before, r.causal) || r.receivedAt != 12345 {
				t.Fatal("late causal receipt rewrote sealed outcome")
			}
			snap := cloneMediaSession(x)
			snap.Kind = "snapshot"
			snap.NativeMatch = nil
			snap.ActionConfirmed = false
			snap.PlaybackConfirmed = false
			snap.Version = 3
			sessionFeedback(h, c, call, r.command, snap)
			if call.callMedia.snapshot == nil || !reflect.DeepEqual(before, r.causal) {
				t.Fatal("separate snapshot missing or rewrote sealed receipt")
			}
		})
	}
}
