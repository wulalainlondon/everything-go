package controllerresults_test

import (
	"context"
	"database/sql"
	cr "everything-go/internal/controllerresults"
	"testing"
)

func TestControllerResultsFaultDuringObserveRollsBackWholeGroup(t *testing.T) {
	s, scope := fixture(t)
	ctx := context.Background()
	st := s.Results()
	if e := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		_, e := tx.Exec(`CREATE TRIGGER fail_observe BEFORE INSERT ON controller_result_cohort BEGIN SELECT RAISE(ABORT,'synthetic transaction interruption');END`)
		return e
	}); e != nil {
		t.Fatal(e)
	}
	if _, e := st.Observe(ctx, obs(scope, "source", "member")); e == nil {
		t.Fatal("fault not injected")
	}
	for _, table := range []string{"controller_result_groups", "controller_result_members", "controller_result_changes", "controller_result_scopes"} {
		if e := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
			var n int
			if e := tx.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); e != nil {
				return e
			}
			if n != 0 {
				t.Fatalf("partial transaction %s=%d", table, n)
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
	}
	if e := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error { _, e := tx.Exec(`DROP TRIGGER fail_observe`); return e }); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if _, e := st.Observe(ctx, obs(scope, "source", "member")); e != nil {
			t.Fatal(e)
		}
	}
	p, e := st.Read(ctx, scope, cr.ReadProof{Principal: "human"}, "", 20)
	if e != nil || len(p.Groups) != 1 || p.Groups[0].MemberCount != 1 || p.HighWater != 1 {
		t.Fatal(p, e)
	}
}
