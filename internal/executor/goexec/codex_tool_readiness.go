package goexec

import (
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"strings"
)

// Current caller-source eligibility is display-only, never an account grant.
func (c *Codex) RuntimeToolReadiness(s *session.Session, requestID string) string {
	if s == nil || strings.HasPrefix(s.ID, "s_dg_") {
		return "verifying"
	}
	source := s.Snapshot()
	if source.Hidden || source.State == session.Closed || source.ResumeID == "" {
		return "verifying"
	}
	if !c.mu.TryLock() {
		return "verifying"
	}
	st := c.states[s.ID]
	c.mu.Unlock()
	if st == nil || !st.mu.TryLock() {
		return "verifying"
	}
	thread := st.threadID
	current := thread == source.ResumeID && st.startAdmission.sourceKnown() && !st.stopping
	owned := current && requestID != "" && st.turnActive && !st.turnStartPending && st.reqID == requestID && st.currentTurnID != "" && st.submissionEpoch == c.rpc.transportEpoch.Load() && st.toolSourceRoleEpoch == st.roleEpoch && st.toolSourceKnown && st.toolSourceRevision == source.SourceRevision && st.toolSourceConfigRevision == source.ConfigRevision
	observed := current && requestID != "" && !st.turnActive && !st.compactActive && st.observedRequestID == requestID && st.observedTurnID != ""
	roleEpoch := st.roleEpoch
	capturedRoleEpoch := st.toolSourceRoleEpoch
	st.mu.Unlock()
	if provider, ok := c.controlProvider.(backend.SessionRoleProvider); ok {
		roleEpoch = provider.SessionRoleEpoch(s)
	}
	if owned && capturedRoleEpoch == roleEpoch {
		return "ready"
	}
	if observed {
		c.voiceMu.Lock()
		call := c.voiceCalls[thread]
		live := call != nil && call.sessionID == s.ID && call.threadID == thread && call.voiceID != "" && call.roleEpoch == roleEpoch && call.sourceRevision == source.SourceRevision && call.configRevision == source.ConfigRevision && call.transportEpoch == c.rpc.transportEpoch.Load()
		c.voiceMu.Unlock()
		if live {
			return "ready"
		}
		return "observing"
	}
	return "verifying"
}
