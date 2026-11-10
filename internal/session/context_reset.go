package session

import "errors"

// ReserveContextGeneration shares the actor mutex with Submit. No new in-memory
// turn can be admitted between the safe-idle check and reset's first effect.
func (s *Session) ReserveContextGeneration(intent, thread string, revision uint64) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if intent == "" || s.contextResetID != "" || s.state != Idle || len(s.mailbox) != 0 || s.activeQueuedID != "" || s.configUpdating || s.resumeID != thread || s.sourceRevision != revision {
		return nil, errors.New("context_generation_busy_or_changed")
	}
	s.contextResetID = intent
	s.queueHolds++
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.contextResetID == intent {
			s.contextResetID = ""
			if s.queueHolds > 0 {
				s.queueHolds--
			}
			s.signalQueueLocked()
		}
	}, nil
}
func (s *Session) ContextResetReserved() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.contextResetID != ""
}

// DetachReservedContext preserves the previous physical generation even before
// a new thread exists. SetResumeID("") alone does not append the active ID.
func (s *Session) DetachReservedContext(old string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.contextResetID == "" || old == "" || s.resumeID != old {
		return errors.New("reserved_context_changed")
	}
	s.contextGeneration++
	s.historicalResumeIDs = appendUniqueResumeID(s.historicalResumeIDs, old, "")
	s.resumeID = ""
	s.advanceSourceRevisionLocked()
	s.contextUsed = 0
	s.contextMax = 0
	s.contextUsedKnown = false
	return nil
}
