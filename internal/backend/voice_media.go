package backend

import (
	"context"
	"everything-go/internal/protocol"
)

// Controls only the original caller's active voice endpoint. No caller/voice
// identity is accepted in model arguments.
type VoiceMediaRequest struct {
	IntentID string `json:"intent,omitempty"`
	Action   string `json:"action,omitempty"`
	URL      string `json:"url,omitempty"`
	Title    string `json:"title,omitempty"`
	Volume   *int   `json:"volume,omitempty"`
}

// MediaOrigin is populated from provider events, never model arguments.
type MediaOrigin struct {
	SessionID, ThreadID, TurnID, RequestID, VoiceID, Generation string
}
type MediaOriginResolver interface {
	ResolveMediaOrigin(thread, operation string) (MediaOrigin, error)
}
type UnifiedVoiceMediaProvider interface {
	UnifiedVoiceMediaEnabled() bool
	ExecuteScopedVoiceMedia(context.Context, SessionControlCaller, MediaOrigin, string, string, VoiceMediaRequest) (any, error)
}
type VoiceMediaProvider interface {
	VoiceMediaEnabled() bool
	ExecuteVoiceMedia(context.Context, SessionControlCaller, string, VoiceMediaRequest) (any, error)
}
type VoiceMediaResult struct {
	Audio             *protocol.MediaAudioInfo `json:"audio,omitempty"`
	AudioCheck        string                   `json:"audio_check"`
	Transport         string                   `json:"transport,omitempty"`
	Action            string                   `json:"action,omitempty"`
	ActionConfirmed   bool                     `json:"action_confirmed"`
	CommandID         string                   `json:"command_id,omitempty"`
	Sequence          uint64                   `json:"sequence,omitempty"`
	State             string                   `json:"state"`
	Title             string                   `json:"title,omitempty"`
	VideoURL          string                   `json:"video_url,omitempty"`
	Volume            int                      `json:"volume"`
	Observed          bool                     `json:"observed"`
	PlaybackConfirmed bool                     `json:"playback_confirmed"`
	NeedsUserAction   bool                     `json:"needs_user_action"`
	ErrorCode         string                   `json:"error_code,omitempty"`
	LastObservedMS    int64                    `json:"last_observed_ms,omitempty"`
}
