package backend

import "time"

// NativeTaskAccepted is emitted only after a correlated native input/turn response.
// It never changes queue callback ownership or releases the session actor.
type NativeTaskAccepted struct {
	SessionID, RequestID, ThreadID, TurnID string
	ConsumedResponseAt                     time.Time
}

// Internal executor observation, not model-callable and not a native terminal.
type NativeSubmissionUncertain struct{ SessionID, RequestID, Reason string }
type NativeProviderEvidence struct{ SessionID, RequestID, Backend, ConversationID, Token, TokenKind, MessageID, Text, Status string }

// NativeTaskStarted is a provider-specific live, nonterminal observation. It is
// not a generic runtime transition, historical read, acceptance, or user tool.
type NativeTaskStarted struct {
	SessionID, RequestID, Provider, ThreadID, Token, TokenKind, ProofKind string
	ObservedAt                                                            time.Time
}
