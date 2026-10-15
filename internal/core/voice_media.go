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
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

var youtubeVideoID = regexp.MustCompile(`^[A-Za-z0-9_-]{11}$`)

func youtubeID(raw string) (string, error) {
	if len(raw) > 2048 {
		return "", errors.New("voice_media_url_invalid")
	}
	u, e := url.Parse(strings.TrimSpace(raw))
	if e != nil || u.Scheme != "https" || u.User != nil || (u.Port() != "" && u.Port() != "443") {
		return "", errors.New("voice_media_youtube_https_required")
	}
	var id string
	switch strings.ToLower(u.Hostname()) {
	case "youtu.be":
		id = strings.TrimPrefix(u.Path, "/")
	case "youtube.com", "www.youtube.com", "m.youtube.com":
		if u.Path == "/watch" {
			if len(u.Query()["v"]) != 1 {
				return "", errors.New("voice_media_video_required")
			}
			id = u.Query().Get("v")
		} else {
			for _, prefix := range []string{"/embed/", "/shorts/", "/live/"} {
				if strings.HasPrefix(u.Path, prefix) {
					id = strings.TrimPrefix(u.Path, prefix)
					break
				}
			}
		}
	default:
		return "", errors.New("voice_media_youtube_url_required")
	}
	if !youtubeVideoID.MatchString(id) {
		return "", errors.New("voice_media_video_required")
	}
	return id, nil
}

type voiceMediaReceipt struct {
	fingerprint   string
	desiredVolume int
	result        backend.VoiceMediaResult
	settled       chan struct{}
	closed        bool
}
type voiceMediaSession struct {
	sequence                       uint64
	current                        *voiceMediaReceipt
	receipts                       map[string]*voiceMediaReceipt
	video, title, state, errorCode string
	volume                         int
	observedAt                     int64
}

func (h *Hub) VoiceMediaEnabled() bool { return true }
func mediaResult(m *voiceMediaSession, r *voiceMediaReceipt) backend.VoiceMediaResult {
	result := r.result
	if r == m.current {
		result.State = m.state
		result.Volume = m.volume
		result.LastObservedMS = m.observedAt
		result.Observed = m.observedAt > 0
		result.ErrorCode = m.errorCode
	}
	fresh := result.LastObservedMS > 0 && time.Now().UnixMilli()-result.LastObservedMS <= 30000
	result.PlaybackConfirmed = result.State == "playing" && fresh && result.ErrorCode == ""
	result.ActionConfirmed = false
	if fresh && result.ErrorCode == "" {
		switch result.Action {
		case "play", "resume":
			result.ActionConfirmed = result.State == "playing"
		case "pause":
			result.ActionConfirmed = result.State == "paused"
		case "stop":
			result.ActionConfirmed = result.State == "stopped"
		case "volume":
			result.ActionConfirmed = result.State != "queued" && result.State != "superseded" && result.Volume == r.desiredVolume
		}
	}

	result.NeedsUserAction = result.ErrorCode == "autoplay_blocked" || result.ErrorCode == "foreground_required"
	if result.Observed && !fresh {
		result.State = "unverified"
		result.Observed = false
	}
	return result
}
func (h *Hub) ExecuteVoiceMedia(ctx context.Context, caller backend.SessionControlCaller, method string, input backend.VoiceMediaRequest) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if caller.Parent == nil || caller.ToolCallID == "" || caller.VoiceID == "" {
		return nil, errors.New("voice_media_active_voice_required")
	}
	return h.executeVoiceMedia(ctx, caller.Parent, caller.VoiceID, caller.RequestID, caller.ToolCallID, "native_tool", nil, method, input)
}

// local_user_socket authority is the private per-call OS-user channel, never a
// fabricated SessionControlCaller. Both transports share the same scope checks.
func (h *Hub) executeVoiceMedia(ctx context.Context, parent *session.Session, voiceID, requestID, operationID, transport string, expectedCall *clientVoiceCall, method string, input backend.VoiceMediaRequest) (any, error) {
	s, ok := h.registry.Get(parent.ID)
	if !ok || s != parent || !h.controls.MobileMayWrite(s.ID) {
		return nil, errors.New("voice_media_caller_not_owned")
	}
	action := input.Action
	video := ""
	switch method {
	case "play_youtube":
		if action != "" {
			return nil, errors.New("voice_media_arguments_invalid")
		}
		action = "play"
		var err error
		video, err = youtubeID(input.URL)
		if err != nil {
			return nil, err
		}
	case "control":
		switch action {
		case "pause", "resume", "stop", "volume":
		default:
			return nil, errors.New("voice_media_action_invalid")
		}
		if input.URL != "" || input.Title != "" {
			return nil, errors.New("voice_media_arguments_invalid")
		}
	case "status":
		if input.Action != "" || input.URL != "" || input.Title != "" || input.Volume != nil {
			return nil, errors.New("voice_media_arguments_invalid")
		}
	default:
		return nil, errors.New("voice_media_tool_unknown")
	}
	if !utf8.ValidString(input.Title) || len([]rune(input.Title)) > 200 {
		return nil, errors.New("voice_media_title_invalid")
	}
	if input.Volume != nil && (*input.Volume < 0 || *input.Volume > 100) {
		return nil, errors.New("voice_media_volume_invalid")
	}
	if action == "volume" && input.Volume == nil {
		return nil, errors.New("voice_media_volume_required")
	}
	h.voiceMu.Lock()
	var owner *Client
	var call *clientVoiceCall
	for c, v := range h.voiceClients {
		if v.session == s && v.voiceID == voiceID && c.live() {
			owner = c
			call = v
			break
		}
	}
	if call == nil || call.stopping || call.kind == "line" || (expectedCall != nil && call != expectedCall) {
		h.voiceMu.Unlock()
		return nil, errors.New("voice_media_active_voice_required")
	}
	if !call.mediaCapable {
		h.voiceMu.Unlock()
		return nil, errors.New("voice_media_player_not_ready")
	}
	if call.media == nil {
		call.media = &voiceMediaSession{receipts: map[string]*voiceMediaReceipt{}, state: "idle", volume: 35}
	}
	m := call.media
	if method == "status" {
		if m.current == nil {
			h.voiceMu.Unlock()
			return backend.VoiceMediaResult{State: "idle", Volume: 35, Observed: true}, nil
		}
		result := mediaResult(m, m.current)
		h.voiceMu.Unlock()
		return result, nil
	}
	encoded, _ := json.Marshal([]any{method, input})
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(encoded))
	if old := m.receipts[transport+"|"+operationID]; old != nil {
		if old.fingerprint != fingerprint {
			h.voiceMu.Unlock()
			return nil, errors.New("voice_media_intent_conflict")
		}
		result := mediaResult(m, old)
		h.voiceMu.Unlock()
		return result, nil
	}
	if len(m.receipts) >= 64 {
		h.voiceMu.Unlock()
		return nil, errors.New("voice_media_command_limit")
	}
	if action != "play" && action != "stop" && m.video == "" {
		h.voiceMu.Unlock()
		return nil, errors.New("voice_media_no_track")
	}
	if old := m.current; old != nil {
		old.result.State = "superseded"
		old.result.Observed = false
		if !old.closed {
			old.closed = true
			close(old.settled)
		}
	}
	if action == "play" {
		m.video = video
		m.title = strings.TrimSpace(input.Title)
	}
	if input.Volume != nil {
		m.volume = *input.Volume
	}
	m.sequence++
	m.state = "queued"
	m.errorCode = ""
	m.observedAt = 0
	id := fmt.Sprintf("vm_%x", sha256.Sum256([]byte(h.cfg.InstanceID+"\x00"+s.ID+"\x00"+voiceID+"\x00"+transport+"\x00"+operationID)))
	receipt := &voiceMediaReceipt{fingerprint: fingerprint, desiredVolume: m.volume, settled: make(chan struct{}), result: backend.VoiceMediaResult{Transport: transport, Action: action, CommandID: id, Sequence: m.sequence, State: "queued", Title: m.title, VideoURL: "https://www.youtube.com/watch?v=" + m.video, Volume: m.volume}}
	if m.video == "" {
		receipt.result.VideoURL = ""
	}
	m.current = receipt
	m.receipts[transport+"|"+operationID] = receipt
	event := voiceEvent(clientproto.Command{SessionID: s.ID, RequestID: requestID, VoiceID: voiceID}, "media", "")
	event.ThreadID = s.ResumeID()
	event.Media = &protocol.VoiceMediaCommand{Action: action, CommandID: id, Sequence: m.sequence, VideoID: m.video, Title: m.title, Volume: m.volume}
	h.voiceMu.Unlock()
	owner.enqueueEvent(event)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-receipt.settled:
	case <-timer.C:
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if h.voiceClients[owner] != call || call.stopping {
		return backend.VoiceMediaResult{Action: action, CommandID: id, Sequence: receipt.result.Sequence, State: "voice_ended"}, nil
	}
	return mediaResult(m, receipt), nil
}
func (h *Hub) handleVoiceMediaState(c *Client, cmd clientproto.Command) {
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	call := h.voiceClients[c]
	if call == nil || call.stopping || call.kind == "line" || !call.mediaCapable || call.voiceID != cmd.VoiceID || call.session.ID != cmd.SessionID || call.session.ResumeID() != cmd.ThreadID || !h.controls.MobileMayWrite(cmd.SessionID) || call.media == nil {
		return
	}
	m := call.media
	r := m.current
	if r == nil || r.result.CommandID != cmd.MediaCommandID || r.result.Sequence != cmd.MediaSequence || cmd.MediaVideoID != m.video {
		return
	}
	switch cmd.MediaState {
	case "playing", "paused", "buffering", "cued", "blocked", "error", "stopped", "ended":
	default:
		return
	}
	switch cmd.MediaErrorCode {
	case "", "autoplay_blocked", "foreground_required", "video_not_found", "embed_blocked", "client_identity", "player_error", "network_error":
	default:
		return
	}
	if cmd.MediaVolume != nil && (*cmd.MediaVolume < 0 || *cmd.MediaVolume > 100) {
		return
	}
	if cmd.MediaVolume != nil {
		m.volume = *cmd.MediaVolume
	}
	m.state = cmd.MediaState
	m.errorCode = cmd.MediaErrorCode
	m.observedAt = time.Now().UnixMilli()
	if m.state == "stopped" {
		m.video = ""
		m.title = ""
	}
	r.result.State = m.state
	r.result.ErrorCode = m.errorCode
	r.result.LastObservedMS = m.observedAt
	r.result.Observed = true
	r.result.Volume = m.volume
	if !r.closed && (mediaResult(m, r).ActionConfirmed || m.state == "blocked" || m.state == "error" || m.state == "ended") {
		r.closed = true
		close(r.settled)
	}
}
