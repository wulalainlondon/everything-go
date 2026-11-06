package session

import "errors"

// CommitInputSource pins the exact source through the durable queue commit.
// Other runtime/configuration mutators take this same mutex. No transport or
// asynchronous callback may run inside commit.
func (s *Session) CommitInputSource(expected Snapshot, commit func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.snapshotLocked()
	if s.futureConfig != nil {
		current = s.futureConfig.applySnapshot(current)
	}
	if current.SourceRevision != expected.SourceRevision || current.ResumeID != expected.ResumeID || current.ConfigRevision != expected.ConfigRevision || current.Hidden || current.State == Closed || current.Cwd != expected.Cwd || ConfigurationFrom(current) != ConfigurationFrom(expected) {
		return errors.New("input_source_changed")
	}
	return commit()
}
