package core

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

type externalMusicReceipt struct {
	fingerprint string
	command     protocol.VoiceMediaCommand
	result      backend.VoiceMediaResult
	settled     chan struct{}
	closed      bool
}
type externalMusicCall struct {
	sequence    uint64
	current     *externalMusicReceipt
	receipts    map[string]*externalMusicReceipt
	observation chan struct{}
}

func externalResult(r *externalMusicReceipt) backend.VoiceMediaResult {
	out := r.result
	if out.LastObservedMS > 0 {
		age := time.Now().UnixMilli() - out.LastObservedMS
		if age < 0 {
			age = 0
		}
		out.ObservationAgeMS = &age
		if age > 5000 {
			out.Observed = false
			out.PlaybackConfirmed = false
			out.ActionConfirmed = false
			out.AudioCheck = "stale"
		}
	}
	return out
}
func (h *Hub) executeExternalMusic(ctx context.Context, parent *session.Session, voiceID, requestID, operationID, transport string, expected *clientVoiceCall, method string, input backend.VoiceMediaRequest) (any, error) {
	refuse := func(code string) (any, error) {
		return nil, &backend.VoiceMediaNotDispatchedError{Cause: errors.New(code)}
	}
	if ctx.Err() != nil {
		return refuse("media_context_expired")
	}
	s, ok := h.registry.Get(parent.ID)
	if !ok || s != parent || !h.controls.MobileMayWrite(s.ID) {
		return refuse("voice_media_caller_not_owned")
	}
	if input.Volume != nil {
		return refuse("external_music_player_gain_unsupported")
	}
	action := input.Action
	command := protocol.VoiceMediaCommand{Provider: "ytmusic_android"}
	switch method {
	case "play_music":
		if action != "" || input.PositionMS != nil || (input.URL == "") == (input.Query == "") {
			return refuse("external_music_arguments_invalid")
		}
		if input.URL != "" {
			uri, e := url.Parse(input.URL)
			if e != nil || uri.Scheme != "https" || uri.Host != "music.youtube.com" || uri.Path != "/watch" || uri.User != nil || uri.Fragment != "" || len(uri.RawQuery) != 13 || !youtubeVideoID.MatchString(uri.Query().Get("v")) {
				return refuse("external_music_uri_invalid")
			}
			action = "play_uri"
			command.URI = input.URL
			command.ExpectedID = uri.Query().Get("v")
		} else {
			action = "play_query"
			command.Query = strings.TrimSpace(input.Query)
			if command.Query == "" {
				return refuse("external_music_query_required")
			}
		}
		command.ExpectedTitle = input.Title
		command.ExpectedArtist = input.Artist
	case "music_control":
		if input.URL != "" || input.Query != "" || input.Title != "" || input.Artist != "" {
			return refuse("external_music_arguments_invalid")
		}
		switch action {
		case "pause", "resume", "stop", "next", "previous":
			if input.PositionMS != nil {
				return refuse("external_music_arguments_invalid")
			}
		case "seek":
			if input.PositionMS == nil || *input.PositionMS < 0 {
				return refuse("external_music_position_invalid")
			}
			command.PositionMS = input.PositionMS
		default:
			return refuse("external_music_action_invalid")
		}
	case "status":
		if action != "" || input.URL != "" || input.Query != "" || input.Title != "" || input.Artist != "" || input.PositionMS != nil {
			return refuse("external_music_arguments_invalid")
		}
		action = "observe"
	default:
		return refuse("voice_media_tool_unknown")
	}
	for _, v := range []string{input.Title, input.Query, input.Artist} {
		if !utf8.ValidString(v) || len([]rune(v)) > 200 || strings.ContainsAny(v, "\r\n\x00") {
			return refuse("external_music_arguments_invalid")
		}
	}
	h.voiceMu.Lock()
	var owner *Client
	var call *clientVoiceCall
	for c, v := range h.voiceClients {
		if c.live() && v.session == s && v.voiceID == voiceID {
			owner = c
			call = v
			break
		}
	}
	if call == nil || call.stopping || call.kind == "line" || (expected != nil && expected != call) {
		h.voiceMu.Unlock()
		return refuse("voice_media_active_voice_required")
	}
	if !call.externalMusicCapable || !call.mediaCapable {
		h.voiceMu.Unlock()
		return refuse("external_music_not_supported")
	}
	provider := "ytmusic_android"
	if h.mediaEndpoint(owner).Platform == "ios" {
		provider = "ytmusic_ios"
		if method == "music_control" || (method == "play_music" && action != "play_uri") {
			h.voiceMu.Unlock()
			return refuse("external_music_action_unsupported")
		}
	}
	command.Provider = provider
	if method == "play_music" && call.media != nil && (call.media.state == "playing" || call.media.state == "buffering" || call.media.state == "queued") {
		h.voiceMu.Unlock()
		return refuse("media_provider_switch_requires_stop")
	}
	if call.externalMusic == nil {
		call.externalMusic = &externalMusicCall{receipts: map[string]*externalMusicReceipt{}}
	}
	m := call.externalMusic
	if method == "status" && m.current == nil && provider == "ytmusic_ios" {
		out := backend.VoiceMediaResult{Provider: provider, Endpoint: h.mediaEndpoint(owner), State: "unverified", AudioCheck: "external_media_output_unverified", ErrorCode: "external_player_unobservable"}
		h.voiceMu.Unlock()
		return out, nil
	}
	if method == "status" && m.current != nil {
		r := m.current
		if m.observation == nil {
			m.observation = make(chan struct{})
		}
		observed := m.observation
		event := voiceEvent(clientproto.Command{SessionID: s.ID, RequestID: requestID, VoiceID: voiceID}, "media", "")
		event.ThreadID = s.ResumeID()
		copy := r.command
		copy.Action = "observe"
		event.Media = &copy
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
			return backend.VoiceMediaResult{State: "voice_ended", AudioCheck: "stale"}, nil
		}
		return externalResult(r), nil
	}
	encoded, _ := json.Marshal([]any{method, input})
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(encoded))
	key := transport + "|" + operationID
	if old := m.receipts[key]; old != nil {
		if old.fingerprint != fingerprint {
			h.voiceMu.Unlock()
			return refuse("voice_media_intent_conflict")
		}
		out := externalResult(old)
		h.voiceMu.Unlock()
		return out, nil
	}
	if len(m.receipts) >= 64 {
		h.voiceMu.Unlock()
		return refuse("voice_media_command_limit")
	}
	if old := m.current; old != nil && !old.closed && method != "status" {
		h.voiceMu.Unlock()
		return refuse("external_music_operation_pending")
	}
	m.sequence++
	command.Action = action
	command.Sequence = m.sequence
	command.CommandID = fmt.Sprintf("vm_%x", sha256.Sum256([]byte(h.cfg.InstanceID+"\x00"+s.ID+"\x00"+voiceID+"\x00external\x00"+transport+"\x00"+operationID)))
	r := &externalMusicReceipt{fingerprint: fingerprint, command: command, settled: make(chan struct{}), result: backend.VoiceMediaResult{Provider: provider, Endpoint: h.mediaEndpoint(owner), Transport: transport, Action: action, CommandID: command.CommandID, Sequence: m.sequence, State: "queued", AudioCheck: "external_media_output_unverified"}}
	m.current = r
	m.receipts[key] = r
	call.musicProvider = provider
	event := voiceEvent(clientproto.Command{SessionID: s.ID, RequestID: requestID, VoiceID: voiceID}, "media", "")
	event.ThreadID = s.ResumeID()
	event.Media = &r.command
	h.voiceMu.Unlock()
	owner.enqueueEvent(event)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-r.settled:
	case <-timer.C:
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if h.voiceClients[owner] != call || call.stopping {
		return backend.VoiceMediaResult{State: "voice_ended", AudioCheck: "stale"}, nil
	}
	return externalResult(r), nil
}
func validExternalMusic(x *protocol.ExternalMusicState) bool {
	if x == nil || len(x.BindingID) > 100 || len(x.OperationID) > 100 || len(x.SupportedActions) > 16 {
		return false
	}
	switch x.Provider {
	case "ytmusic_android":
		if x.PackageName != "com.google.android.apps.youtube.music" || x.HandoffOpened {
			return false
		}
	case "ytmusic_ios":
		// Public iOS handoff acknowledges opening only. It cannot attest another
		// app's now-playing state, track identity, position or successful playback.
		if x.PackageName != "com.google.ios.youtubemusic" || x.State != "unknown" || x.ActionConfirmed || x.PlaybackConfirmed || x.TrackMatched || x.Title != "" || x.Artist != "" || x.MediaID != "" || x.PositionMS != 0 || x.ObservationAgeMS != -1 {
			return false
		}
		for _, action := range x.SupportedActions {
			if action != "play_uri" && action != "observe" {
				return false
			}
		}
		if x.HandoffOpened && x.Error != "external_playback_unverified" {
			return false
		}
	default:
		return false
	}
	for _, s := range []string{x.Title, x.Artist, x.MediaID} {
		if !utf8.ValidString(s) || len([]rune(s)) > 300 {
			return false
		}
	}
	switch x.State {
	case "playing", "paused", "buffering", "stopped", "idle", "unavailable", "error", "unknown", "refused":
	default:
		return false
	}
	if len(x.Error) > 80 || strings.ContainsAny(x.Error, "\r\n\x00") {
		return false
	}
	return x.ObservationAgeMS >= -1 && x.ObservationAgeMS <= 86400000
}
func (h *Hub) handleExternalMusicState(c *Client, cmd clientproto.Command) {
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	call := h.voiceClients[c]
	x := cmd.ExternalMusic
	if call == nil || call.stopping || call.kind == "line" || !call.mediaCapable || !call.externalMusicCapable || call.voiceID != cmd.VoiceID || call.session.ID != cmd.SessionID || call.session.ResumeID() != cmd.ThreadID || !h.controls.MobileMayWrite(cmd.SessionID) || call.externalMusic == nil || !validExternalMusic(x) {
		return
	}
	m := call.externalMusic
	r := m.current
	if r == nil || r.command.CommandID != cmd.MediaCommandID || r.command.Sequence != cmd.MediaSequence || x.OperationID != cmd.MediaCommandID || x.Provider != r.command.Provider {
		return
	}
	if x.Provider == "ytmusic_ios" {
		// The receipt is a handoff acknowledgement, never a fresh player sample.
		if !r.closed {
			r.result.HandoffOpened = x.HandoffOpened
			r.result.ErrorCode = x.Error
			r.result.NeedsUserAction = x.Error == "foreground_required" || x.Error == "official_app_unavailable"
		}
		r.result.State = "unverified"
		r.result.Observed = false
		r.result.ExternalMusic = nil
		r.result.ActionConfirmed = false
		r.result.PlaybackConfirmed = false
		if m.observation != nil {
			close(m.observation)
			m.observation = nil
		}
		if !r.closed && (x.HandoffOpened || x.Error != "") {
			r.closed = true
			close(r.settled)
		}
		return
	}
	// Observing the old song cannot erase an already settled dispatch failure.
	// Copy the snapshot so the caller-owned payload remains immutable.
	if r.closed && r.result.ErrorCode != "" {
		copy := *x
		copy.Error = r.result.ErrorCode
		copy.ActionConfirmed = false
		copy.PlaybackConfirmed = false
		x = &copy
	}
	r.result.ExternalMusic = x
	r.result.State = x.State
	r.result.LastObservedState = x.State
	r.result.ErrorCode = x.Error
	r.result.Title = x.Title
	r.result.Observed = true
	r.result.LastObservedMS = time.Now().UnixMilli()
	r.result.ActionConfirmed = x.ActionConfirmed
	r.result.PlaybackConfirmed = x.PlaybackConfirmed && x.State == "playing"
	if (r.command.Action == "play_query" || r.command.Action == "play_uri") && !x.TrackMatched {
		r.result.ActionConfirmed = false
		r.result.PlaybackConfirmed = false
	}
	if m.observation != nil {
		close(m.observation)
		m.observation = nil
	}
	if !r.closed && (x.ActionConfirmed || x.Error != "" || r.command.Action == "observe") {
		r.closed = true
		close(r.settled)
	}
}
