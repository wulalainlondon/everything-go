package runtimejournal

import "testing"

func TestQueuedWaitCannotInventRuntimeOrRebindAndPersistsWithoutStart(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	if _, changed := s.QueuedWait("absent", "request", "wait"); changed || len(s.records) != 0 {
		t.Fatal("invented runtime")
	}
	before, _ := s.Update("s", "queued", "original", 2, "", "")
	if _, changed := s.QueuedWait("s", "other", "late"); changed {
		t.Fatal("rebound request")
	}
	after, changed := s.QueuedWait("s", "original", "bounded wait")
	if !changed || after.Phase != "queued" || after.Stage != "queued" || after.ActiveStartedAt != before.ActiveStartedAt || after.QueueLength != 2 || after.Unread != before.Unread || after.LastCompletedRevision != before.LastCompletedRevision {
		t.Fatal("invented native start/read/completion", after)
	}
	s.Flush()
	restored := New(dir).Snapshot("fixture", []string{"s"})[0]
	if restored.StageMessage != "bounded wait" || restored.ActiveRequestID != "original" {
		t.Fatal("lost wait evidence", restored)
	}
}
