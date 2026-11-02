package core

import (
	"context"
	"everything-go/internal/backend"
	"testing"
)

func TestIOSOfficialMusicRejectsUnavailableControlBeforeDispatch(t *testing.T) {
	for _, input := range []backend.VoiceMediaRequest{{Action: "pause"}, {Action: "resume"}, {Action: "next"}, {Action: "stop"}} {
		h, _, caller, _ := controllerFixture(t)
		var owner *Client
		for c, call := range h.voiceClients {
			owner = c
			c.clientSurface = "ios"
			call.mediaCapable = true
			call.externalMusicCapable = true
		}
		if _, e := h.ExecuteVoiceMedia(context.Background(), caller, "music_control", input); e == nil {
			t.Fatal("iOS must not claim Android remote control", input)
		}
		select {
		case <-owner.send:
			t.Fatal("unsupported action dispatched")
		default:
		}
	}
}
