package session

import "context"

// BindInitializedThread accepts only the exact still-current initialization
// source. valid must be a nonblocking transport check which never calls Session.
// Role authority is checked by the host before this CAS and again at turn/start.
func (s *Session) BindInitializedThread(ctx context.Context, expected Snapshot, thread string, valid func() bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.snapshotLocked()
	if thread == "" || ctx.Err() != nil || now.State == Closed || now.State == Stopping || now.Hidden || now.ResumeID != expected.ResumeID || now.SourceRevision != expected.SourceRevision || now.ContextGeneration != expected.ContextGeneration || ConfigurationFrom(now) != ConfigurationFrom(expected) || !valid() {
		return false
	}
	s.setResumeIDLocked(thread)
	return true
}
