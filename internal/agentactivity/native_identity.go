package agentactivity

// Logical identities survive transport/configuration epochs. A new connection
// is an observation boundary, never a synthetic actor or execution round.
type NativeActorIdentity struct{ Authority, Session, RootThread, ChildThread string }
type NativeExecutionIdentity struct {
	Actor      NativeActorIdentity
	NativeTurn string
}

func (s NativeScope) LogicalActor(child string) (NativeActorIdentity, error) {
	if !s.valid() || !shortID(child) || child == s.RootThread {
		return NativeActorIdentity{}, ErrNativeSource
	}
	return NativeActorIdentity{s.Authority, s.Session, s.RootThread, child}, nil
}
func (s NativeScope) LogicalExecution(child, turn string) (NativeExecutionIdentity, error) {
	a, e := s.LogicalActor(child)
	if e != nil || !shortID(turn) {
		return NativeExecutionIdentity{}, ErrNativeSource
	}
	return NativeExecutionIdentity{a, turn}, nil
}
