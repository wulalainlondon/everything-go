package messagequeue

import (
	"errors"
	"sync"
	"testing"

	"everything-go/internal/protocol"
)

func batchFixture(id string, questions ...string) (QuestionBatch, Entry) {
	r := protocol.AsyncBatchRequest{Authority: "authority", SessionID: "s1", RequestID: id, ThreadID: "thread", ConfigRevision: 4}
	covered := []protocol.AsyncCoveredQuestion{}
	for _, q := range questions {
		r.Selected = append(r.Selected, protocol.AsyncBatchSelection{RequestID: q, QuestionRevision: "rev-" + q, Answers: map[string]any{"item": "explicit answer"}})
		covered = append(covered, protocol.AsyncCoveredQuestion{RequestID: q, QuestionRevision: "rev-" + q})
	}
	v := QuestionBatch{Request: r, Hash: "hash-" + id, Phase: "reserved", Owner: "device", OwnerGeneration: 7}
	e := fixture(id)
	e.QuestionBatch = &QuestionBatchAdmission{Hash: v.Hash, ContentHash: "native-envelope", Covered: covered}
	return v, e
}

func TestQuestionBatchAtomicAdmissionConflictAndCanonicalConsumption(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	v, e := batchFixture("batch1", "q1", "q2")
	if _, created, err := s.ClaimQuestionBatch(v); err != nil || !created {
		t.Fatal(created, err)
	}
	if _, created, err := s.Enqueue(e); err != nil || !created {
		t.Fatal(created, err)
	}
	for _, q := range v.Request.Selected {
		got, found, err := s.QuestionCoverage("s1", q.RequestID, q.QuestionRevision)
		if err != nil || !found || got.Phase != "admitted" {
			t.Fatal(got, found, err)
		}
	}
	if err := s.RejectQuestionBatch("s1", "batch1", v.Hash, "late-reject"); !errors.Is(err, ErrConflict) {
		t.Fatal("late rejection overwrote admission", err)
	}
	if _, _, err := s.Enqueue(fixture("q1")); !errors.Is(err, ErrConflict) {
		t.Fatal("single bypassed batch coverage", err)
	}
	v2, e2 := batchFixture("batch2", "q3", "q2")
	s.ClaimQuestionBatch(v2)
	if _, _, err := s.Enqueue(e2); !errors.Is(err, ErrConflict) {
		t.Fatal("overlapping second batch admitted", err)
	}
	if _, found, _ := s.Get("s1", "batch2"); found {
		t.Fatal("partial queue commit")
	}
	if _, found, _ := s.QuestionCoverage("s1", "q3", "rev-q3"); found {
		t.Fatal("partial coverage committed")
	}
	for _, bad := range [][2]string{{"other-thread", "native-envelope"}, {"thread", "other-envelope"}} {
		if err := s.ConsumeQuestionBatch("s1", bad[0], bad[1]); err != nil {
			t.Fatal(err)
		}
		got, _, _ := s.QuestionBatch("s1", "batch1")
		if got.Phase != "admitted" {
			t.Fatal("unrelated native row consumed batch")
		}
	}
	if err := s.ConsumeQuestionBatch("s1", "thread", "native-envelope"); err != nil {
		t.Fatal(err)
	}
	got, _, _ := s.QuestionBatch("s1", "batch1")
	if got.Phase != "consumed" {
		t.Fatal(got)
	}
}

func TestQuestionBatchSingleWinnerAndGuardRollback(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, _, err := s.Enqueue(fixture("q1")); err != nil {
		t.Fatal(err)
	}
	v, e := batchFixture("batch1", "q2", "q1")
	s.ClaimQuestionBatch(v)
	if _, _, err := s.Enqueue(e); !errors.Is(err, ErrConflict) {
		t.Fatal("batch bypassed prior single", err)
	}
	if _, found, _ := s.QuestionCoverage("s1", "q2", "rev-q2"); found {
		t.Fatal("partial coverage on single conflict")
	}
	v, e = batchFixture("guarded", "q3")
	s.ClaimQuestionBatch(v)
	e.QuestionBatch.CanCommit = func() error { return errors.New("source-changed") }
	if _, _, err := s.Enqueue(e); err == nil {
		t.Fatal("commit ignored late source guard")
	}
	if _, found, _ := s.Get("s1", "guarded"); found {
		t.Fatal("guard failure committed queue")
	}
	got, _, _ := s.QuestionBatch("s1", "guarded")
	if got.Phase != "reserved" {
		t.Fatal(got)
	}
	if err := s.RejectQuestionBatch("s1", "guarded", v.Hash, "source-changed"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Enqueue(e); err == nil {
		t.Fatal("rejected original replayed")
	}
}

func TestQuestionBatchCannotCommitPartialOrReorderedCoverage(t *testing.T) {
	for _, kind := range []string{"missing", "reordered"} {
		t.Run(kind, func(t *testing.T) {
			s, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			v, e := batchFixture("batch", "q1", "q2")
			s.ClaimQuestionBatch(v)
			if kind == "missing" {
				e.QuestionBatch.Covered = e.QuestionBatch.Covered[:1]
			} else {
				e.QuestionBatch.Covered[0], e.QuestionBatch.Covered[1] = e.QuestionBatch.Covered[1], e.QuestionBatch.Covered[0]
			}
			if _, _, err := s.Enqueue(e); !errors.Is(err, ErrConflict) {
				t.Fatal("partial frozen mapping admitted", err)
			}
			if _, found, _ := s.Get("s1", "batch"); found {
				t.Fatal("partial queue commit")
			}
		})
	}
}

func TestQuestionBatchImmutableReceiptsAndClockSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	v, _ := batchFixture("unknown-after-crash", "q1")
	var wg sync.WaitGroup
	wins := make(chan bool, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, created, e := s.ClaimQuestionBatch(v)
			if e != nil {
				t.Error(e)
			}
			wins <- created
		}()
	}
	wg.Wait()
	close(wins)
	n := 0
	for created := range wins {
		if created {
			n++
		}
	}
	if n != 1 {
		t.Fatal("multiple immutable claims", n)
	}
	v.Hash = "different-mapping"
	if _, _, err := s.ClaimQuestionBatch(v); !errors.Is(err, ErrConflict) {
		t.Fatal("different answers reused intent", err)
	}
	a, err := s.NextQuestionRevision()
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b, err := s.NextQuestionRevision()
	if err != nil || b <= a {
		t.Fatal("clock rollback", a, b, err)
	}
	got, found, err := s.QuestionBatch("s1", "unknown-after-crash")
	if err != nil || !found || got.Phase != "reserved" || got.OwnerGeneration != 7 {
		t.Fatal(got, found, err)
	}
	if _, created, err := s.ClaimQuestionBatch(got); err != nil || created {
		t.Fatal("unknown original replayed", created, err)
	}
}
