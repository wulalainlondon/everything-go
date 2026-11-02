package goexec

import "testing"

func TestQAReadinessMustAgreeWithCurrentNativeThreadSource(t *testing.T) {
	c, s := toolReadinessFixture(t)
	st := c.state(s.ID)
	st.mu.Lock()
	st.threadID = "different-native-thread"
	st.mu.Unlock()
	if _, err := c.sessionControlCaller(s, "original-turn", "original-call"); err == nil {
		t.Fatal("fixture did not reject changed native source")
	}
	if got := c.RuntimeToolReadiness(s, "original-request"); got == "ready" {
		t.Fatal("REPRO: ready displayed for a thread source rejected by the real caller gate")
	}
}
