package core

import (
	"context"
	"database/sql"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"testing"
)

func TestControllerResultsCohortReadFailureNeverFallsBackToV1(t *testing.T) {
	h, _, parent, target := unifiedFixtureSessions(t)
	ctx := context.Background()
	fe := h.exec.(*scopeFixtureExecutor).fakeExec
	useNativeCorrelationFixture(h, fe, target.ResumeID(), "producer", "native-producer")
	seedControllerResult(t, h, parent, target, "primary", "producer", false)
	h.runtimes.Update(parent.ID, "completed", "", 0, "", "")
	fe.onSend = func(s *session.Session, id, text string) { t.Errorf("v1 writer invoked %s", id) }
	if e := h.dispatches.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error { _, e := tx.Exec(`DROP TABLE controller_result_cohort`); return e }); e != nil {
		t.Fatal(e)
	}
	h.Emit(backend.ObservedTurn{SessionID: parent.ID, RequestID: "previous", Phase: "completed"})
	h.reconcileSessionDispatches(ctx)
	if e, found, err := h.messageQueue.Get(parent.ID, "screturn_primary"); err != nil || found {
		t.Fatalf("old v1 fanout admitted after cohort read failure: %s %v %v", e.State, found, err)
	}
}
