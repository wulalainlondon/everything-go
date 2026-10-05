package backend

import (
	"context"
	"everything-go/internal/session"
)

// RealtimeVoice is separate from Send/Stop: ending audio must never interrupt a turn.
type RealtimeVoiceStart struct {
	VoiceID   string
	VoiceName string
	SDP       string
	OnEvent   func(RealtimeVoiceEvent)
}
type RealtimeVoiceAnswer struct{ ThreadID, VoiceID, SDP string }
type RealtimeVoiceEvent struct{ State, Text, Role, Message string }
type RealtimeVoiceExecutor interface {
	StartRealtimeVoice(context.Context, *session.Session, RealtimeVoiceStart) (RealtimeVoiceAnswer, error)
	StopRealtimeVoice(context.Context, *session.Session, string) error
	AppendRealtimeVoiceText(context.Context, *session.Session, string, string) error
}

// Codex rust-v0.160.0 maps realtime v3 to this built-in voice set.
func ValidRealtimeVoiceName(name string) bool {
	switch name {
	case "", "juniper", "maple", "spruce", "ember", "vale", "breeze", "arbor", "sol", "cove":
		return true
	}
	return false
}
