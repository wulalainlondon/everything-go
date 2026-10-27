package session

import "testing"

func TestContextGenerationReservationSharesAdmissionMutex(t *testing.T) {
	r := NewRegistry()
	s := r.Create("s", "name", "/", "codex", "m", "read-only", "old")
	release, e := s.ReserveContextGeneration("intent", "old", s.Snapshot().SourceRevision)
	if e != nil {
		t.Fatal(e)
	}
	if s.SubmitNamed("late", func() {}) {
		t.Fatal("late input admitted")
	}
	if s.CanDispatchAutomatic() {
		t.Fatal("automatic dispatch admitted")
	}
	if e := s.DetachReservedContext("old"); e != nil {
		t.Fatal(e)
	}
	release()
	if s.ContextResetReserved() {
		t.Fatal("reservation retained")
	}
	if ids := s.ResumeIDs(); len(ids) != 1 || ids[0] != "old" {
		t.Fatal("history lost", ids)
	}
}
