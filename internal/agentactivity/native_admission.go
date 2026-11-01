package agentactivity

// ValidateProbe is the final normalized-collection admission, after any local
// journal/projection work. It never substitutes a newer registry revision.
func (s *NativeSource) ValidateProbe(p NativeProbe) error {
	if !s.mu.TryLock() {
		return ErrNativeChanged
	}
	defer s.mu.Unlock()
	if s.failed || s.scope != p.Scope || s.revision != p.SourceRevision {
		return ErrNativeChanged
	}
	return nil
}
func (s *NativeSource) Usable(scope NativeScope) bool {
	if !s.mu.TryLock() {
		return false
	}
	defer s.mu.Unlock()
	return !s.failed && s.scope == scope && scope.valid()
}

// Quarantine retires all proof from an original source whose indexed prefix
// changed. Recovery cannot make old pinned pages usable again in this epoch.
func (s *NativeSource) Quarantine() { s.mu.Lock(); s.failed = true; s.mu.Unlock() }
