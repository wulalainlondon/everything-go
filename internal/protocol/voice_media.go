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
