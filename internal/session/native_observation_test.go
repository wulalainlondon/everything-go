package session

import "testing"

func TestNativeObservationAdmissionIsNonblockingAndIdentityOnly(t *testing.T) {
	s := NewRegistry().Create("s1", "fixture", t.TempDir(), "codex", "", "", "root")
	release, e := s.AcquireNativeObservation("root", 0, "codex")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.AcquireNativeObservation("root", 0, "codex"); e == nil {
		t.Fatal("busy guard waited or reentered")
	}
	if s.state != Idle || len(s.mailbox) != 0 || s.configUpdating || s.queueHolds != 0 {
		t.Fatal("read admission changed execution")
	}
	release()
	for name, edit := range map[string]func(*Session){"config": func(s *Session) { s.SetConfigRevision(1) }, "root": func(s *Session) { s.SetResumeID("other") }, "hidden": func(s *Session) { hidden := true; s.SetMeta(nil, &hidden) }, "closed": func(s *Session) { s.Close() }, "backend": func(s *Session) { s.ApplyConfig("claude", "", "") }, "updating": func(s *Session) { s.configUpdating = true }, "resetting": func(s *Session) { s.contextResetID = "reset" }} {
		t.Run(name, func(t *testing.T) {
			fresh := NewRegistry().Create("fresh", "fixture", t.TempDir(), "codex", "", "", "root")
			edit(fresh)
			if _, e := fresh.AcquireNativeObservation("root", 0, "codex"); e == nil {
				t.Fatal("retired identity admitted")
			}
		})
	}
}
