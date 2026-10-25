package messagequeue

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func timingEntry(t *testing.T, s *Store, id string) {
	t.Helper()
	hash := sha256.Sum256([]byte("{}"))
	_, _, err := s.Enqueue(Entry{SessionID: "s", RequestID: id, Content: "work", Payload: []byte("{}"), PayloadHash: hex.EncodeToString(hash[:])})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordNativeAcceptance("s", id, "native-thread", "native-turn-"+id); err != nil {
		t.Fatal(err)
	}
}
func TestElapsedSourceMonotonicSealedAndRestartQualified(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	timingEntry(t, s, "r_live")
	start := time.Now().Add(-3 * time.Second)
	if err = s.RecordNativeClock("s", "r_live", start); err != nil {
		t.Fatal(err)
	}
	first, err := s.Elapsed("s", "r_live")
	if err != nil || first.ElapsedAtSampleMS == nil || *first.ElapsedAtSampleMS < 3000 || first.Quality != "estimate" || first.TerminalElapsedMS != nil {
		t.Fatal(first, err)
	}
	if _, changed, err := s.Transition("s", "r_live", []State{Queued}, Completed, "", "", ""); err != nil || !changed {
		t.Fatal(changed, err)
	}
	final, err := s.Elapsed("s", "r_live")
	if err != nil || final.TerminalElapsedMS == nil {
		t.Fatal(final, err)
	}
	frozen := *final.TerminalElapsedMS
	timingEntry(t, s, "r_running")
	if err = s.RecordNativeClock("s", "r_running", time.Now()); err != nil {
		t.Fatal(err)
	}
	oldDomain := first.ClockDomain
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	reloaded, err := s.Elapsed("s", "r_live")
	if err != nil || reloaded.TerminalElapsedMS == nil || *reloaded.TerminalElapsedMS != frozen || reloaded.ClockDomain != oldDomain || reloaded.BootGeneration == first.BootGeneration {
		t.Fatal(reloaded, err)
	}
	running, err := s.Elapsed("s", "r_running")
	if err != nil || running.ElapsedAtSampleMS != nil || running.Quality != "unavailable" || running.UnavailableReason != "clock_domain_ended" {
		t.Fatal(running, err)
	}
	// A recovered acceptance must never reset an old clock domain.
	if err = s.RecordNativeClock("s", "r_running", time.Now()); err != nil {
		t.Fatal(err)
	}
	running, err = s.Elapsed("s", "r_running")
	if err != nil || running.ElapsedAtSampleMS != nil {
		t.Fatal(running, err)
	}
}
func TestElapsedMissingWallOnlyAndFutureProofNeverInventDuration(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, tc := range []struct {
		id string
		at time.Time
	}{{"r_missing", time.Time{}}, {"r_wall_jump", time.Now().Add(20 * time.Minute).Round(0)}, {"r_wall_only", time.Now().Add(-20 * time.Minute).Round(0)}, {"r_future", time.Now().Add(time.Minute)}} {
		timingEntry(t, s, tc.id)
		if err = s.RecordNativeClock("s", tc.id, tc.at); err != nil {
			t.Fatal(err)
		}
		sample, err := s.Elapsed("s", tc.id)
		if err != nil || sample.ElapsedAtSampleMS != nil || sample.TerminalElapsedMS != nil || sample.Quality != "unavailable" {
			t.Fatal(sample, err)
		}
	}
}
func TestElapsedLateAcceptanceAfterTerminalDoesNotStartAndConcurrentSamples(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	timingEntry(t, s, "r_late")
	if _, _, err = s.Transition("s", "r_late", []State{Queued}, Completed, "", "", ""); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordNativeClock("s", "r_late", time.Now()); err != nil {
		t.Fatal(err)
	}
	sample, err := s.Elapsed("s", "r_late")
	if err != nil || sample.ElapsedAtSampleMS != nil {
		t.Fatal(sample, err)
	}
}

func TestElapsedRequiresCanonicalNativeReceiptAndRejectsLaterConflicts(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, _, err = s.Enqueue(Entry{SessionID: "s", RequestID: "r_no_native", Content: "test", Payload: []byte("{}")})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordNativeClock("s", "r_no_native", time.Now()); err != nil {
		t.Fatal(err)
	}
	sample, err := s.Elapsed("s", "r_no_native")
	if err != nil || sample.Quality != "unavailable" || sample.ElapsedAtSampleMS != nil {
		t.Fatal(sample, err)
	}
	timingEntry(t, s, "r_conflict")
	if err = s.RecordNativeClock("s", "r_conflict", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordNativeAcceptance("s", "r_conflict", "other-thread", "other-turn"); err == nil {
		t.Fatal("expected native conflict")
	}
	sample, err = s.Elapsed("s", "r_conflict")
	if err != nil || sample.Quality != "unavailable" || sample.ElapsedAtSampleMS != nil {
		t.Fatal(sample, err)
	}
}

func TestElapsedPersistenceFailureDoesNotPoisonCommittedTerminal(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	timingEntry(t, s, "r_timing_failure")
	if err = s.RecordNativeClock("s", "r_timing_failure", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`CREATE TRIGGER fail_timing BEFORE UPDATE ON task_elapsed BEGIN SELECT RAISE(FAIL,'optional timing disk failure'); END;`); err != nil {
		t.Fatal(err)
	}
	entry, changed, err := s.Transition("s", "r_timing_failure", []State{Queued}, Completed, "", "", "")
	if err != nil || !changed || entry.State != Completed {
		t.Fatal("optional timing poisoned terminal", entry, changed, err)
	}
	sample, err := s.Elapsed("s", "r_timing_failure")
	if err != nil || sample.Quality != "unavailable" || sample.TerminalElapsedMS != nil || sample.ElapsedAtSampleMS != nil {
		t.Fatal(sample, err)
	}
}
