package backend

// NativeTaskAccepted is emitted only after a correlated native input/turn response.
// It never changes queue callback ownership or releases the session actor.
type NativeTaskAccepted struct{ SessionID, RequestID, ThreadID, TurnID string }
