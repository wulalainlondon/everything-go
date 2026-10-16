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
	DeviceModel             string   `json:"device_model,omitempty"`
	CommunicationProfile    string   `json:"communication_profile,omitempty"`
	CommunicationDevice     string   `json:"communication_device,omitempty"`
	PredictedMediaRoutes    []string `json:"predicted_media_routes,omitempty"`
	MediaRoutingObservation string   `json:"media_routing_observation,omitempty"`
	SpeakerRouteAvailable   *bool    `json:"speaker_route_available,omitempty"`
	PlayerMuted             *bool    `json:"player_muted,omitempty"`
	SystemMediaVolume       *int     `json:"system_media_volume,omitempty"`
	SystemMediaMuted        *bool    `json:"system_media_muted,omitempty"`
	VoiceRoute              string   `json:"voice_route,omitempty"`
}
