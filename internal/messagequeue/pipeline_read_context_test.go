package messagequeue

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// These original readers are reached by source/decision disclosure inside an
// admission transaction. Empty proof is still empty, never a successful fact.
func TestPipelineOriginalProofReadersStayInSameWriterContext(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e = s.WithNativeAcceptanceRead(ctx, func(bound context.Context) error {
		if _, found, e := s.GetContext(bound, "owned", "r"); e != nil || found {
			t.Fatal(found, e)
		}
		if _, found, e := s.EffectiveInput(bound, "owned", "r"); e != nil || found {
			t.Fatal(found, e)
		}
		if snap, e := s.SnapshotContext(bound, "owned"); e != nil || len(snap.Items) != 0 {
			t.Fatal(snap, e)
		}
		if _, e := s.WorkItem(bound, "scope", "owned", "none"); !errors.Is(e, sql.ErrNoRows) {
			t.Fatal(e)
		}
		if _, found, e := s.ExactNativeAcceptanceContext(bound, "owned", "r"); e != nil || found {
			t.Fatal(found, e)
		}
		if _, found, e := s.NativeStartContext(bound, "owned", "r"); e != nil || found {
			t.Fatal(found, e)
		}
		if conflict, e := s.NativeConflictContext(bound, "owned", "r"); e != nil || conflict {
			t.Fatal(conflict, e)
		}
		if _, _, found := s.NativeExecutionContext(bound, "owned", "r"); found {
			t.Fatal("missing native execution adopted")
		}
		if _, found, e := s.ProviderEvidenceContext(bound, "owned", "r"); e != nil || found {
			t.Fatal(found, e)
		}
		if _, _, found, e := s.ConsumedInput(bound, "owned", "r", "thread"); e != nil || found {
			t.Fatal(found, e)
		}
		if _, e := s.TaskSeal(bound, "owned", "r", ""); e == nil {
			t.Fatal("missing seal accepted")
		}
		if exists, e := s.PipelineArtifactHash(bound, "owned", "r", "seal", "0000000000000000000000000000000000000000000000000000000000000000"); e != nil || exists {
			t.Fatal(exists, e)
		}
		if _, e := s.TaskJournal().SourceRevision(bound, "owned", "r"); e != nil {
			t.Fatal(e)
		}
		return nil
	})
	if e != nil || ctx.Err() != nil {
		t.Fatal("same writer source read reborrowed connection", e, ctx.Err())
	}
}
