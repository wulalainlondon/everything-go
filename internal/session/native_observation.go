package session

import "errors"

// AcquireNativeObservation is a nonblocking admission at the original provider
// writer. It holds identity only through the small RPC write, never response
// wait, and makes no configuration/queue/execution changes. Caller releases it.
func (s *Session) AcquireNativeObservation(root string, revision uint64, backend string) (func(), error) {
	if !s.mu.TryLock() {
		return nil, errors.New("native_observation_busy")
	}
	if root == "" || s.resumeID != root || s.configRevision != revision || s.backend != backend || s.hidden || s.state == Closed || s.configUpdating || s.contextResetID != "" {
		s.mu.Unlock()
		return nil, errors.New("native_observation_changed")
	}
	return s.mu.Unlock, nil
}
