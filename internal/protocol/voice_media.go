package protocol

import (
	"encoding/json"
	"errors"
)

// Ephemeral owner-only command. Never replayed into history or offline queues.
type VoiceMediaCommand struct {
	MediaSession      *MediaSessionCommand `json:"media_session,omitempty"`
	SystemMediaVolume *int                 `json:"system_media_volume,omitempty"`
	SystemMediaDelta  *int                 `json:"system_media_delta,omitempty"`
	AssistantVolume   *int                 `json:"assistant_volume,omitempty"`
	AssistantDelta    *int                 `json:"assistant_delta,omitempty"`
	Provider          string               `json:"provider,omitempty"`
	Query             string               `json:"query,omitempty"`
	URI               string               `json:"uri,omitempty"`
	ExpectedID        string               `json:"expected_id,omitempty"`
	ExpectedTitle     string               `json:"expected_title,omitempty"`
	ExpectedArtist    string               `json:"expected_artist,omitempty"`
	PositionMS        *int64               `json:"position_ms,omitempty"`
	CommandID         string               `json:"command_id"`
	Sequence          uint64               `json:"sequence"`
	Action            string               `json:"action"`
	VideoID           string               `json:"video_id,omitempty"`
	Title             string               `json:"title,omitempty"`
	Volume            int                  `json:"volume"`
}

// MediaAudioInfo separates player/system gain observations from human audibility.
// VoiceRoute is the confirmed VOICE route, not a claim about WebView media routing.
type MediaAudioInfo struct {
	// These observations concern the app-owned WebRTC tracks only. They do not
	// attest to the official YouTube player's actual route or audible output.
	AudioProfile            string   `json:"audio_profile,omitempty"`
	SharedRouteVerified     *bool    `json:"shared_route_verified,omitempty"`
	SpeechOutputObservation string   `json:"speech_output_observation,omitempty"`
	SpeechOutputRoute       string   `json:"speech_output_route,omitempty"`
	SharedOutputDevice      string   `json:"shared_output_device,omitempty"`
	MicrophoneObservation   string   `json:"microphone_observation,omitempty"`
	MicrophoneInput         string   `json:"microphone_input,omitempty"`
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

// ExternalMusicState is a native observation, never a routing address.
type ExternalMusicState struct {
	NeedsUserAction   bool     `json:"needsUserAction,omitempty"`
	HandoffOpened     bool     `json:"handoffOpened,omitempty"`
	Provider          string   `json:"provider"`
	PackageName       string   `json:"packageName"`
	BindingID         string   `json:"bindingId"`
	State             string   `json:"state"`
	Error             string   `json:"error"`
	Title             string   `json:"title"`
	Artist            string   `json:"artist"`
	MediaID           string   `json:"mediaId"`
	SupportedActions  []string `json:"supportedActions"`
	PositionMS        int64    `json:"positionMs"`
	ObservationAgeMS  int64    `json:"observationAgeMs"`
	ActionConfirmed   bool     `json:"actionConfirmed"`
	PlaybackConfirmed bool     `json:"playbackConfirmed"`
	TrackMatched      bool     `json:"trackMatched"`
	OperationID       string   `json:"operationId"`
}

// VoiceAudioState describes app-owned assistant playback gain, never microphone gain.
type VoiceAudioState struct {
	Previous    int    `json:"previous"`
	State       string `json:"state"`
	Error       string `json:"error,omitempty"`
	Configured  int    `json:"configured"`
	Applied     int    `json:"applied"`
	OutputMuted bool   `json:"outputMuted"`
	MediaIndex  int    `json:"mediaIndex"`
	ObservedAt  int64  `json:"observedAtMs"`
}

// SystemMediaAudioState reports Android STREAM_MUSIC, never earbud-private hardware gain.
type SystemMediaAudioState struct {
	MediaMuted    bool   `json:"mediaMuted"`
	MediaFixed    bool   `json:"mediaFixed"`
	Previous      int    `json:"previous"`
	State         string `json:"state"`
	Error         string `json:"error,omitempty"`
	Configured    int    `json:"configured"`
	Applied       int    `json:"applied"`
	MediaIndex    int    `json:"mediaIndex"`
	MediaMax      int    `json:"mediaMax"`
	AssistantGain int    `json:"assistantGain"`
	ObservedAt    int64  `json:"observedAtMs"`
}

// MediaSessionCommand is one coordinator admission, never a Go pause/play chain.
// Its operationId is the exact outer command_id, not the parent's tool/vop intent.
type MediaSessionCommand struct {
	OperationID      string  `json:"operationId"`
	Target           string  `json:"target"`
	Action           string  `json:"action"`
	URI              string  `json:"uri,omitempty"`
	MediaID          string  `json:"mediaId,omitempty"`
	Title            string  `json:"title,omitempty"`
	Artist           string  `json:"artist,omitempty"`
	PositionMS       *int64  `json:"positionMs,omitempty"`
	Volume           *int    `json:"volume,omitempty"`
	ExpectedRevision *uint64 `json:"expectedRevision,omitempty"`
	AdoptCurrent     bool    `json:"adoptCurrent,omitempty"`
	ExpectedMediaID  string  `json:"expectedMediaId,omitempty"`
}

// Receipt causality and local snapshots are independent. Device monotonic age
// is composed with Go receive age; no device wall clock is trusted.
type MediaSessionState struct {
	Volume            *int     `json:"volume,omitempty"`
	Kind              string   `json:"kind"`
	OperationID       string   `json:"operationId"`
	Revision          uint64   `json:"revision"`
	Version           uint64   `json:"version"`
	ActiveSource      string   `json:"activeSource"`
	ReservedSource    string   `json:"reservedSource"`
	NativeProvider    string   `json:"nativeProvider"`
	Phase             string   `json:"phase"`
	Stage             string   `json:"stage"`
	Dispatched        bool     `json:"dispatched"`
	ActionConfirmed   bool     `json:"actionConfirmed"`
	PlaybackConfirmed bool     `json:"playbackConfirmed"`
	MayBePlaying      bool     `json:"mayBePlaying"`
	Error             string   `json:"error"`
	Title             string   `json:"title"`
	Artist            string   `json:"artist"`
	MediaID           string   `json:"mediaId"`
	BindingID         string   `json:"bindingId"`
	SupportedActions  []string `json:"supportedActions"`
	PositionMS        *int64   `json:"positionMs,omitempty"`
	DurationMS        *int64   `json:"durationMs,omitempty"`
	ObservationAgeMS  int64    `json:"observationAgeMs"`
}

// Missing/null source counters and age are unknown, never a synthetic fresh zero.
func (x *MediaSessionState) UnmarshalJSON(raw []byte) error {
	type plain MediaSessionState
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, key := range []string{"revision", "version", "observationAgeMs"} {
		if len(fields[key]) == 0 || string(fields[key]) == "null" {
			return errors.New("media_session_observation_fields_required")
		}
	}
	var v plain
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	*x = MediaSessionState(v)
	return nil
}
