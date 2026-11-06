package session

import "errors"

// This witness is produced only by the serial actor, spanning its automatic
// future-default activation and the exact queued payload activation. No wire
// snapshot can construct one, and any intervening source/ABA mutation retires it.
type queuedConfigurationActivation struct {
	owner         *Session
	request       string
	before, after Snapshot
	config        Configuration
}

func (s *Session) queuedActivationCurrentLocked(t *queuedConfigurationActivation) bool {
	if t == nil || t.owner != s || t.request == "" || s.activeQueuedID != t.request || s.state != Streaming || s.configUpdating || s.contextResetID != "" {
		return false
	}
	now := s.snapshotLocked()
	return !now.Hidden && now.SourceRevision == t.after.SourceRevision && now.ResumeID == t.after.ResumeID && now.Cwd == t.after.Cwd && now.Hidden == t.after.Hidden && ConfigurationFrom(now) == ConfigurationFrom(t.after)
}

// Holds the source through the media journal CAS. This confers neither a new
// admission nor permission to continue another queued request's source.
func (s *Session) WithQueuedConfigurationActivation(request string, fn func(Snapshot, Snapshot, Configuration) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.queuedActivation
	if !s.queuedActivationCurrentLocked(t) || t.request != request {
		return errors.New("queued_configuration_activation_retired")
	}
	return fn(t.before, t.after, t.config)
}
