package taskapi

import (
	"context"
	"database/sql"
	"errors"
	_ "modernc.org/sqlite"
	"path/filepath"
	"testing"
	"time"
)

func TestOriginalSQLContextExactStoreNestedReadAndOuterRollback(t *testing.T) {
	open := func(name string) *sql.DB {
		db, e := sql.Open("sqlite", filepath.Join(t.TempDir(), name))
		if e != nil {
			t.Fatal(e)
		}
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { db.Close() })
		if e = InstallJournal(db); e != nil {
			t.Fatal(e)
		}
		return db
	}
	a, b := open("a"), open("b")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	outer, e := a.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer outer.Rollback()
	bound := BindSQLTransaction(ctx, a, outer)
	if BoundSQLTransaction(bound, b) != nil || ReadSQL(bound, b) != b {
		t.Fatal("foreign original DB borrowed transaction")
	}
	j := AttachedJournal(a, "original")
	err := j.Transaction(bound, func(tx *sql.Tx) error {
		if tx != outer {
			t.Fatal("nested reader opened another transaction")
		}
		_, e := tx.ExecContext(bound, "INSERT INTO task_api_changes(session_id,request_id,kind,at) VALUES('owned','r','synthetic',1)")
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	if seq, e := j.SourceRevision(bound, "owned", "r"); e != nil || seq != 1 {
		t.Fatal(seq, e)
	}
	if e = outer.Rollback(); e != nil {
		t.Fatal("nested read committed outer writer", e)
	}
	if seq, e := j.SourceRevision(ctx, "owned", "r"); e != nil || seq != 0 {
		t.Fatal("outer rollback did not preserve atomicity", seq, e)
	}
	txB, e := b.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer txB.Rollback()
	both := BindSQLTransaction(bound, b, txB)
	if BoundSQLTransaction(both, a) != outer || BoundSQLTransaction(both, b) != txB {
		t.Fatal("ordered cross-store binding lost original DB")
	}
	if _, e = ReadSQL(both, a).QueryContext(both, "SELECT 1"); !errors.Is(e, sql.ErrTxDone) {
		t.Fatal("stale outer transaction silently escaped to db", e)
	}
}
