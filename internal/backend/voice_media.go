package backend

import "context"

// Controls only the original caller's active voice endpoint. No caller/voice
// identity is accepted in model arguments.
type VoiceMediaRequest struct {
	Action string `json:"action,omitempty"`
	URL    string `json:"url,omitempty"`
	Title  string `json:"title,omitempty"`
	Volume *int   `json:"volume,omitempty"`
}
type VoiceMediaProvider interface {
	VoiceMediaEnabled() bool
	ExecuteVoiceMedia(context.Context, SessionControlCaller, string, VoiceMediaRequest) (any, error)
}
type VoiceMediaResult struct {
	Transport         string `json:"transport,omitempty"`
	Action            string `json:"action,omitempty"`
	ActionConfirmed   bool   `json:"action_confirmed"`
	CommandID         string `json:"command_id,omitempty"`
	Sequence          uint64 `json:"sequence,omitempty"`
	State             string `json:"state"`
	Title             string `json:"title,omitempty"`
	VideoURL          string `json:"video_url,omitempty"`
	Volume            int    `json:"volume"`
	Observed          bool   `json:"observed"`
	PlaybackConfirmed bool   `json:"playback_confirmed"`
	NeedsUserAction   bool   `json:"needs_user_action"`
	ErrorCode         string `json:"error_code,omitempty"`
	LastObservedMS    int64  `json:"last_observed_ms,omitempty"`
}
