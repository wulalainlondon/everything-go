package session

import "errors"

// DefaultEffortTransition is a host-only receipt for a provider default, not a
// generic permission to adopt a new source revision. Its fields and constructor
// stay private so a wire event or a caller-supplied snapshot cannot create one.
type DefaultEffortTransition struct {
	owner         *Session
	before, after Snapshot
	reason        string
	epoch, serial uint64
}

type runtimeDefaultEffort struct {
	source uint64
	thread string
	cwd    string
	config Configuration
	effort string
	epoch  uint64
}

func (t *DefaultEffortTransition) Source() string {
	if t == nil {
		return ""
	}
	return t.before.ID
}
func (t *DefaultEffortTransition) RoleEpoch() uint64 {
	if t == nil {
		return 0
	}
	return t.epoch
}

func (s *Session) invalidateDefaultEffortLocked() {
	s.runtimeDefaultEffort = nil
	s.defaultEffortSerial++
}

func knownDefaultEffort(v string) bool {
	switch v {
	case "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra":
		return true
	}
	return false
}

func (s *Session) defaultEffortTransitionLocked(before Snapshot, reason string, epoch uint64) *DefaultEffortTransition {
	s.defaultEffortSerial++
	return &DefaultEffortTransition{s, before, s.snapshotLocked(), reason, epoch, s.defaultEffortSerial}
}

// ApplyProviderRuntimeMetadata preserves the existing metadata/context behavior.
// Only resolving an undeclared effort in an otherwise identical configuration
// can issue a role receipt. A different model, explicit effort or queued config
// remains a real source change and cannot be normalized into authorization.
func (s *Session) ApplyProviderRuntimeMetadata(model, effort *string, roleEpoch uint64) (*string, *string, *DefaultEffortTransition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	before := s.snapshotLocked()
	declared := ConfigurationFrom(before)
	if s.futureConfig != nil {
		declared = *s.futureConfig
	}
	var changedModel, changedEffort *string
	if model != nil && *model != "" && s.model != *model {
		s.advanceSourceRevisionLocked()
		s.model = *model
		v := *model
		changedModel = &v
	}
	if effort != nil && *effort != "" && s.effort != *effort {
		s.advanceSourceRevisionLocked()
		s.effort = *effort
		v := *effort
		changedEffort = &v
	}
	if changedModel == nil && changedEffort != nil && roleEpoch > 0 && before.Backend == "codex" && before.Effort == "" && declared == ConfigurationFrom(before) && knownDefaultEffort(*changedEffort) && !s.configUpdating && s.contextResetID == "" && s.state != Closed {
		s.runtimeDefaultEffort = &runtimeDefaultEffort{s.sourceRevision, s.resumeID, s.cwd, declared, *changedEffort, roleEpoch}
		return changedModel, changedEffort, s.defaultEffortTransitionLocked(before, "provider_default_effort_observed", roleEpoch)
	}
	return changedModel, changedEffort, nil
}

func (s *Session) restorableDefaultEffortLocked() *runtimeDefaultEffort {
	m := s.runtimeDefaultEffort
	if m == nil || s.futureConfig == nil || m.source != s.sourceRevision || m.thread != s.resumeID || m.cwd != s.cwd || *s.futureConfig != m.config || s.configUpdating || s.contextResetID != "" {
		return nil
	}
	active := m.config
	active.Effort = m.effort
	if ConfigurationFrom(s.snapshotLocked()) != active {
		return nil
	}
	return m
}

// WithDefaultEffortTransition fences the actual current Session through the
// durable role CAS. Any intervening mutation (including an ABA) retires it.
func (s *Session) WithDefaultEffortTransition(t *DefaultEffortTransition, fn func(Snapshot, Snapshot, string) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t == nil || t.owner != s || t.epoch == 0 || t.serial != s.defaultEffortSerial || s.state == Closed || s.contextResetID != "" || s.configUpdating {
		return errors.New("default_effort_transition_retired")
	}
	now := s.snapshotLocked()
	if now.SourceRevision != t.after.SourceRevision || now.ResumeID != t.after.ResumeID || now.Cwd != t.after.Cwd || now.Hidden != t.after.Hidden || ConfigurationFrom(now) != ConfigurationFrom(t.after) {
		return errors.New("default_effort_transition_source_changed")
	}
	return fn(t.before, t.after, t.reason)
}
