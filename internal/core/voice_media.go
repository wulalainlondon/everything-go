package core

import (
	"context"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"regexp"
)

var youtubeVideoID = regexp.MustCompile(`^[A-Za-z0-9_-]{11}$`)

type voiceMediaReceipt struct {
	fingerprint   string
	desiredVolume int
	result        backend.VoiceMediaResult
	settled       chan struct{}
	closed        bool
}
type voiceMediaSession struct {
	observation                    chan struct{}
	audio                          *protocol.MediaAudioInfo
	sequence                       uint64
	current                        *voiceMediaReceipt
	receipts                       map[string]*voiceMediaReceipt
	video, title, state, errorCode string
	volume                         int
	observedAt                     int64
}

func (h *Hub) VoiceMediaEnabled() bool { return true }

// Historical decode projection only. The retired visible player cannot be observed
// or restarted, and its old receipt cannot borrow current native Music state.
func mediaResult(_ *voiceMediaSession, r *voiceMediaReceipt) backend.VoiceMediaResult {
	out := r.result
	out.Observed, out.ActionConfirmed, out.PlaybackConfirmed = false, false, false
	out.Audio, out.MediaSession = nil, nil
	age := int64(-1)
	out.ObservationAgeMS = &age
	out.AudioCheck = "stale"
	return out
}
func (h *Hub) ExecuteVoiceMedia(ctx context.Context, caller backend.SessionControlCaller, method string, input backend.VoiceMediaRequest) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if caller.Parent == nil || caller.ToolCallID == "" || caller.VoiceID == "" {
		return nil, errors.New("voice_media_active_voice_required")
	}
	if e := h.jarvisExternalAdmission(caller, caller.VoiceID != ""); e != nil {
		return nil, e
	}
	return h.executeVoiceMedia(ctx, caller.Parent, caller.VoiceID, caller.RequestID, caller.ToolCallID, "native_tool", nil, method, input)
}

// local_user_socket authority is the private per-call OS-user channel, never a
// fabricated SessionControlCaller. Both transports share the same scope checks.
func (h *Hub) executeVoiceMedia(ctx context.Context, parent *session.Session, voiceID, requestID, operationID, transport string, expectedCall *clientVoiceCall, method string, input backend.VoiceMediaRequest) (value any, err error) {
	// All transports, including stale provider tools and old local clients, cross
	// this boundary before any source pause, mutation receipt or native enqueue.
	if method == "play_youtube" || input.Source == "youtube" {
		return nil, &backend.VoiceMediaNotDispatchedError{Cause: errors.New("youtube_playback_retired")}
	}

	// The optional label belongs to the Go request/receipt, never the native
	// command or exact-title matcher. Validate it for every provider, including
	// legacy clients, before taking any effect route.
	if !safeSessionLabel(input.DisplayLabel, 200) || input.DisplayLabel != "" && method != "play_music" {
		return nil, &backend.VoiceMediaNotDispatchedError{Cause: errors.New("voice_media_display_label_invalid")}
	}
	defer func() {
		if result, ok := value.(backend.VoiceMediaResult); ok && input.DisplayLabel != "" {
			result.DisplayLabel = input.DisplayLabel
			value = result
		}
	}()

	if input.Operation != "" {
		return nil, &backend.VoiceMediaNotDispatchedError{Cause: errors.New("media_original_operation_requires_durable_store")}
	}
	if (method == "voice_volume" || method == "media_volume") && (input.Source != "" || input.ExpectedRevision != nil || input.AdoptCurrent || input.ExpectedMediaID != "" || input.OriginalCommandID != "") {
		return nil, &backend.VoiceMediaNotDispatchedError{Cause: errors.New("voice_media_arguments_invalid")}
	}
	if method == "media_volume" {
		return h.executeSystemMediaVolume(ctx, parent, voiceID, requestID, operationID, transport, expectedCall, input)
	}
	if method == "voice_volume" {
		return h.executeVoiceVolume(ctx, parent, voiceID, requestID, operationID, transport, expectedCall, input)
	}
	if value, handled, err := h.executeCallMediaSession(ctx, parent, voiceID, requestID, operationID, transport, expectedCall, method, input); handled {
		return value, err
	}
	if input.Operation != "" || input.Source != "" || input.ExpectedRevision != nil || input.AdoptCurrent || input.ExpectedMediaID != "" || input.OriginalCommandID != "" {
		return nil, &backend.VoiceMediaNotDispatchedError{Cause: errors.New("media_session_not_supported")}
	}
	if method == "play_music" || method == "music_control" {
		return h.executeExternalMusic(ctx, parent, voiceID, requestID, operationID, transport, expectedCall, method, input)
	}
	if method == "status" || method == "control" {
		h.voiceMu.Lock()
		external := false
		for c, v := range h.voiceClients {
			if c.live() && v.session == parent && v.voiceID == voiceID && (v.musicProvider == "ytmusic_android" || v.musicProvider == "apple_music_ios") {
				external = true
				break
			}
		}
		h.voiceMu.Unlock()
		if external {
			mapped := method
			if method == "control" {
				mapped = "music_control"
			}
			return h.executeExternalMusic(ctx, parent, voiceID, requestID, operationID, transport, expectedCall, mapped, input)
		}
	}
	return nil, &backend.VoiceMediaNotDispatchedError{Cause: errors.New("native_music_not_supported")}
}
func (h *Hub) handleVoiceMediaState(c *Client, cmd clientproto.Command) {
	if cmd.MediaSessionState != nil {
		h.handleCallMediaState(c, cmd)
		return
	}
	if cmd.SystemMediaAudio != nil {
		h.handleSystemMediaState(c, cmd)
		return
	}
	if cmd.VoiceAudio != nil {
		h.handleVoiceAudioState(c, cmd)
		return
	}
	h.voiceMu.Lock()
	sessionCall := h.voiceClients[c]
	newSession := sessionCall != nil && sessionCall.mediaSessionCapable
	h.voiceMu.Unlock()
	if newSession {
		return
	}
	if cmd.ExternalMusic != nil {
		h.handleExternalMusicState(c, cmd)
		return
	}
	// Legacy standalone player feedback is decode-only, never live Music proof.
}
