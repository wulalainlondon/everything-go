package core

import "testing"

func TestHistoryReadAnnouncesExistingCodexThreadWithoutStartingWork(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("s_original", "Original", "/work", "codex", "", "", "thread-original")
	before := s.Snapshot()
	c := newTestClient(h)
	route(h, c, `{"type":"request_history","session_id":"s_original","mode":"snapshot"}`)
	event := waitForType(t, c, "session_uuid")
	if event["session_id"] != s.ID || event["claude_uuid"] != "thread-original" {
		t.Fatalf("wrong identity: %#v", event)
	}
	after := s.Snapshot()
	if len(h.registry.List()) != 1 || after.ResumeID != before.ResumeID || after.State != before.State {
		t.Fatal("history read changed development work or created a session")
	}
}
