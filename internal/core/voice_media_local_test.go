package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/voicemediaipc"
	"strings"
	"testing"
)

func TestLocalVoiceMediaUsesRealChannelAndRequiresPlayerAcknowledgement(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var owner *Client
	var call *clientVoiceCall
	for c, v := range h.voiceClients {
		owner = c
		c.clientSurface = "android"
		call = v
		call.mediaCapable = true
		call.externalMusicCapable = true
		call.musicProvider = "ytmusic_android"
	}
	instructions, e := h.openVoiceMediaLocal(owner, call)
	if e != nil || !strings.Contains(instructions, "existing exec_command") || !strings.Contains(instructions, call.voiceID) {
		t.Fatal(instructions, e)
	}
	defer h.closeVoiceMediaLocal(call)
	retired := voicemediaipc.Request{ID: "vop_retired_socket_intent", Thread: caller.Parent.ResumeID(), Voice: caller.VoiceID, Method: "play_youtube", Input: backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}}
	if _, e := voicemediaipc.Send(context.Background(), call.mediaIPC.Path, retired); e == nil || e.Error() != "youtube_playback_retired" {
		t.Fatal("old socket admitted video", e)
	}
	select {
	case e := <-owner.send:
		t.Fatal("retired socket command queued", e)
	default:
	}
	request := voicemediaipc.Request{ID: "vop_actual_local_intent", Thread: caller.Parent.ResumeID(), Voice: caller.VoiceID, Method: "play_music", Input: backend.VoiceMediaRequest{URL: "https://music.youtube.com/watch?v=M7lc1UVf-VE"}}
	done := make(chan json.RawMessage, 1)
	go func() {
		r, e := voicemediaipc.Send(context.Background(), call.mediaIPC.Path, request)
		if e != nil {
			t.Error(e)
		}
		done <- r
	}()
	event := nextVoiceState(t, owner, "media")
	var command protocol.VoiceMediaCommand
	data, _ := json.Marshal(event["media"])
	json.Unmarshal(data, &command)
	h.handleVoiceMediaState(owner, clientproto.Command{SessionID: caller.Parent.ID, ThreadID: request.Thread, VoiceID: request.Voice, MediaCommandID: command.CommandID, MediaSequence: command.Sequence, ExternalMusic: &protocol.ExternalMusicState{Provider: "ytmusic_android", PackageName: "com.google.android.apps.youtube.music", BindingID: "own", State: "playing", TrackMatched: true, ActionConfirmed: true, PlaybackConfirmed: true, MediaID: command.ExpectedID, OperationID: command.CommandID, ObservationAgeMS: 0}})
	var result backend.VoiceMediaResult
	json.Unmarshal(<-done, &result)
	if !result.PlaybackConfirmed || result.Transport != "local_user_socket" {
		t.Fatal(result)
	}
	replay, e := voicemediaipc.Send(context.Background(), call.mediaIPC.Path, request)
	if e != nil || len(replay) == 0 {
		t.Fatal(string(replay), e)
	}
	select {
	case <-owner.send:
		t.Fatal("replayed effect")
	default:
	}
	request.Input.URL = "https://music.youtube.com/watch?v=abcdefghijk"
	if _, e = voicemediaipc.Send(context.Background(), call.mediaIPC.Path, request); e == nil {
		t.Fatal("sameintent changed track")
	}
	h.voiceMu.Lock()
	delete(h.voiceClients, owner)
	h.voiceMu.Unlock()
	request.Method = "status"
	request.Input = backend.VoiceMediaRequest{}
	request.ID = "vop_post_termination"
	if _, e = voicemediaipc.Send(context.Background(), call.mediaIPC.Path, request); e == nil {
		t.Fatal("detachedcall retained authority")
	}
}
