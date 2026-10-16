package protocol

// Ephemeral owner-only command. Never replayed into history or offline queues.
type VoiceMediaCommand struct {
	CommandID string `json:"command_id"`
	Sequence  uint64 `json:"sequence"`
	Action    string `json:"action"`
	VideoID   string `json:"video_id,omitempty"`
	Title     string `json:"title,omitempty"`
	Volume    int    `json:"volume"`
}

// MediaAudioInfo separates player/system gain observations from human audibility.
// VoiceRoute is the confirmed VOICE route, not a claim about WebView media routing.
type MediaAudioInfo struct {
	PlayerMuted       *bool  `json:"player_muted,omitempty"`
	SystemMediaVolume *int   `json:"system_media_volume,omitempty"`
	SystemMediaMuted  *bool  `json:"system_media_muted,omitempty"`
	VoiceRoute        string `json:"voice_route,omitempty"`
}
