package goexec

import (
	"everything-go/internal/session"
	"testing"
)

func toolReadinessFixture(t *testing.T) (*Codex, *session.Session) {
	c, s := lineOriginFixture(t)
	st := c.state(s.ID)
	snapshot := s.Snapshot()
	st.toolSourceKnown = true
	st.toolSourceRevision = snapshot.SourceRevision
	st.toolSourceConfigRevision = snapshot.ConfigRevision
	return c, s
}

func TestToolReadinessDoesNotPromoteObservedHistoryOrMismatchedRequest(t *testing.T) {
	c, s := toolReadinessFixture(t)
	if got := c.RuntimeToolReadiness(s, "original-request"); got != "ready" {
		t.Fatal(got)
	}
	st := c.state(s.ID)
	st.turnActive = false
	st.observedRequestID = "original-request"
	st.observedTurnID = "original-turn"
	if got := c.RuntimeToolReadiness(s, "original-request"); got != "observing" {
		t.Fatal(got)
	}
	if _, err := c.sessionControlCaller(s, "original-turn", "new-native-command"); err == nil {
		t.Fatal("display projection granted authority")
	}
	if got := c.RuntimeToolReadiness(s, "other-request"); got != "verifying" {
		t.Fatal(got)
	}
	c.voiceCalls = map[string]*codexVoiceCall{s.ResumeID(): {sessionID: s.ID, voiceID: "original-live-voice", threadID: s.ResumeID(), sourceRevision: s.Snapshot().SourceRevision, configRevision: s.Snapshot().ConfigRevision, transportEpoch: c.rpc.transportEpoch.Load()}}
	if got := c.RuntimeToolReadiness(s, "original-request"); got != "ready" {
		t.Fatal("exact active voice incorrectly retired", got)
	}
	delete(c.voiceCalls, s.ResumeID())
	if got := c.RuntimeToolReadiness(s, "original-request"); got != "observing" {
		t.Fatal("ended voice remained ready", got)
	}
}

func TestReadinessRejectsRetiredConfigRoleTransportAndContendedState(t *testing.T) {
	for _, kind := range []string{"thread", "hidden", "config", "role", "transport", "compact", "observed-active", "contended"} {
		t.Run(kind, func(t *testing.T) {
			c, s := toolReadinessFixture(t)
			st := c.state(s.ID)
			switch kind {
			case "thread":
				st.threadID = "other"
			case "hidden":
				hidden := true
				s.SetMeta(nil, &hidden)
			case "config":
				snapshot := s.Snapshot()
				st.startAdmission = &codexStartAdmission{Source: s, ThreadID: s.ResumeID(), ConfigRevision: snapshot.ConfigRevision}
				s.SetModel("changed")
			case "role":
				st.roleEpoch = 2
				st.toolSourceRoleEpoch = 1
			case "transport":
				st.submissionEpoch = 1
				c.rpc.transportEpoch.Store(2)
			case "compact":
				st.turnActive = false
				st.compactActive = true
				st.observedRequestID = "original-request"
				st.observedTurnID = "original-turn"
			case "observed-active":
				st.reqID = "other-request"
				st.observedRequestID = "original-request"
				st.observedTurnID = "original-turn"
			case "contended":
				st.mu.Lock()
				defer st.mu.Unlock()
			}
			if got := c.RuntimeToolReadiness(s, "original-request"); got == "ready" {
				t.Fatal("retired source remained ready", kind)
			}
		})
	}
}
