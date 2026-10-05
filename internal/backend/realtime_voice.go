package backend

import (
	"context"
	"everything-go/internal/session"
)

// RealtimeVoice is separate from Send/Stop: ending audio must never interrupt a turn.
type RealtimeVoiceStart struct {
	VoiceID string
	SDP     string
	OnEvent func(RealtimeVoiceEvent)
}
type RealtimeVoiceAnswer struct{ ThreadID, VoiceID, SDP string }
type RealtimeVoiceEvent struct{ State, Text, Role, Message string }
type RealtimeVoiceExecutor interface {
	StartRealtimeVoice(context.Context, *session.Session, RealtimeVoiceStart) (RealtimeVoiceAnswer, error)
	StopRealtimeVoice(context.Context, *session.Session, string) error
	AppendRealtimeVoiceText(context.Context, *session.Session, string, string) error
}
