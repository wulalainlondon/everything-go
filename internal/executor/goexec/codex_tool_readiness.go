package goexec

// RuntimeToolReadiness is a display-only projection. Neither a restored runtime
// journal nor this value can grant a command authority.
func (c *Codex) RuntimeToolReadiness(sessionID, requestID string) string {
	st := c.state(sessionID)
	st.mu.Lock()
	owned := requestID != "" && st.turnActive && !st.turnStartPending && st.reqID == requestID && st.currentTurnID != "" && !st.stopping
	observed := requestID != "" && st.observedRequestID == requestID && st.observedTurnID != ""
	thread := st.threadID
	st.mu.Unlock()
	if owned {
		return "ready"
	}
	if observed {
		c.voiceMu.Lock()
		call := c.voiceCalls[thread]
		liveVoice := call != nil && call.sessionID == sessionID && call.voiceID != ""
		c.voiceMu.Unlock()
		if liveVoice {
			return "ready"
		}
		return "observing"
	}
	return "verifying"
}
