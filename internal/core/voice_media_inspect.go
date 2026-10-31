package core

import (
	"context"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"time"
)

// Enter with voiceMu locked. Inspection never creates an operation, changes its
// sequence, starts playback or migrates to another socket after reconnect.
func (h *Hub) inspectMediaLocked(ctx context.Context, owner *Client, call *clientVoiceCall, m *voiceMediaSession) (any, error) {
	r := m.current
	if !call.mediaObserveCapable || time.Now().UnixMilli()-m.observedAt < 2000 {
		result := mediaResult(m, r)
		h.voiceMu.Unlock()
		return result, nil
	}
	if m.observation == nil {
		m.observation = make(chan struct{})
	}
	observed := m.observation
	event := voiceEvent(clientproto.Command{SessionID: call.session.ID, RequestID: call.requestID, VoiceID: call.voiceID}, "media", "")
	event.ThreadID = call.session.ResumeID()
	event.Media = &protocol.VoiceMediaCommand{Action: "observe", CommandID: r.result.CommandID, Sequence: r.result.Sequence, VideoID: m.video, Title: m.title, Volume: m.volume}
	h.voiceMu.Unlock()
	owner.enqueueEvent(event)
	timer := time.NewTimer(800 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-observed:
	case <-timer.C:
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if h.voiceClients[owner] != call || call.stopping {
		var endpoint *backend.MediaEndpoint
		if r.result.Endpoint != nil {
			copy := *r.result.Endpoint
			copy.Verification = "retired_call"
			endpoint = &copy
		}
		return backend.VoiceMediaResult{State: "voice_ended", AudioCheck: "stale", CommandID: r.result.CommandID, Sequence: r.result.Sequence, Endpoint: endpoint}, nil
	}
	return mediaResult(m, r), nil
}

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
	if call.media != nil && call.media.current != nil && call.media.current.result.CommandID == old.CommandID && (call.musicProvider == "" || call.musicProvider == "youtube") {
		value, err := h.inspectMediaLocked(ctx, owner, call, call.media)
		result, _ := value.(backend.VoiceMediaResult)
		return result, err
	}
	if call.externalMusic != nil && call.externalMusic.current != nil {
		m := call.externalMusic
		r := m.current
		if r.command.CommandID == old.CommandID && r.command.Provider == old.Provider && call.musicProvider == old.Provider {
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
