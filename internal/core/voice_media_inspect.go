package core

import (
	"context"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"time"
)

// Enter with voiceMu locked. Inspection never creates an operation, changes its
// sequence, starts playback or migrates to another socket after reconnect.
// Original-operation status cannot migrate to the current provider. Old clients
// keep their original observation ports only while that exact receipt is current.
func (h *Hub) observeOriginalLegacyMedia(ctx context.Context, call *clientVoiceCall, old backend.VoiceMediaResult) (backend.VoiceMediaResult, error) {
	h.voiceMu.Lock()
	var owner *Client
	for c, v := range h.voiceClients {
		if v == call && c.live() && !call.stopping {
			owner = c
			break
		}
	}
	if owner == nil {
		h.voiceMu.Unlock()
		return backend.VoiceMediaResult{}, errors.New("media_voice_expired")
	}
	if call.externalMusic != nil && call.externalMusic.current != nil {
		m := call.externalMusic
		r := m.current
		if (old.Provider == "ytmusic_android" || old.Provider == "apple_music_ios") && r.command.CommandID == old.CommandID && r.command.Provider == old.Provider && call.musicProvider == old.Provider {
			if m.observation == nil {
				m.observation = make(chan struct{})
			}
			wait := m.observation
			cmd := r.command
			cmd.Action = "observe"
			event := voiceEvent(clientproto.Command{SessionID: call.session.ID, RequestID: call.requestID, VoiceID: call.voiceID}, "media", "")
			event.ThreadID = call.session.ResumeID()
			event.Media = &cmd
			h.voiceMu.Unlock()
			owner.enqueueEvent(event)
			waitMediaSession(ctx, wait, 800*time.Millisecond)
			h.voiceMu.Lock()
			defer h.voiceMu.Unlock()
			if h.voiceClients[owner] != call || call.stopping {
				return backend.VoiceMediaResult{}, errors.New("media_voice_expired")
			}
			return externalResult(r), nil
		}
	}
	h.voiceMu.Unlock()
	return backend.VoiceMediaResult{}, errors.New("media_original_observation_unavailable")
}
