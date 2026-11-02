package goexec

import "testing"

func TestToolReadinessDoesNotPromoteObservedHistoryOrMismatchedRequest(t *testing.T) {
	c, s := lineOriginFixture(t)
	if got := c.RuntimeToolReadiness(s.ID, "original-request"); got != "ready" {
		t.Fatal(got)
	}
	st := c.state(s.ID)
	st.turnActive = false
	st.observedRequestID = "original-request"
	st.observedTurnID = "original-turn"
	if got := c.RuntimeToolReadiness(s.ID, "original-request"); got != "observing" {
		t.Fatal(got)
	}
	if _, err := c.sessionControlCaller(s, "original-turn", "new-native-command"); err == nil {
		t.Fatal("display projection granted authority")
	}
	if got := c.RuntimeToolReadiness(s.ID, "other-request"); got != "verifying" {
		t.Fatal(got)
	}
	c.voiceCalls = map[string]*codexVoiceCall{s.ResumeID(): {sessionID: s.ID, voiceID: "original-live-voice"}}
	if got := c.RuntimeToolReadiness(s.ID, "original-request"); got != "ready" {
		t.Fatal("exact active voice incorrectly retired", got)
	}
	delete(c.voiceCalls, s.ResumeID())
	if got := c.RuntimeToolReadiness(s.ID, "original-request"); got != "observing" {
		t.Fatal("ended voice remained ready", got)
	}
}
