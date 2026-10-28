package taskapi

import (
	"context"
	"database/sql"
	"errors"
	_ "modernc.org/sqlite"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestJournalChangesFloorAndPageShareOriginalSnapshot(t *testing.T) {
	db, e := sql.Open("sqlite", filepath.Join(t.TempDir(), "source.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	if _, e = db.Exec(`PRAGMA journal_mode=WAL;PRAGMA busy_timeout=5000`); e != nil {
		t.Fatal(e)
	}
	if e = InstallJournal(db); e != nil {
		t.Fatal(e)
	}
	j := AttachedJournal(db, "original-source")
	ctx := context.Background()
	if e = j.Transaction(ctx, func(tx *sql.Tx) error {
		for k := 0; k < 100; k++ {
			if e := ChangeTx(tx, "synthetic", "original", "native_fact"); e != nil {
				return e
			}
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); <-start; errs <- j.Prune(ctx, time.Now().Add(90*24*time.Hour)) }()
	go func() {
		defer wg.Done()
		<-start
		for k := 0; k < 50; k++ {
			rows, floor, e := j.Changes(ctx, 0, 50)
			if e != nil {
				var api *APIError
				if !errors.As(e, &api) || api.Code != "cursor_expired" || floor < 100 {
					errs <- e
					return
				}
				continue
			}
			if floor != 0 || len(rows) != 50 {
				errs <- errors.New("prune erased unread rows without a cursor gap")
				return
			}
		}
		errs <- nil
	}()
	close(start)
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
}
func TestJournalReadWithinOriginalTxAndIdentitySurvivesReopen(t *testing.T) {
	file := filepath.Join(t.TempDir(), "source.sqlite")
	db, e := sql.Open("sqlite", file)
	if e != nil {
		t.Fatal(e)
	}
	db.SetMaxOpenConns(1)
	if e = InstallJournal(db); e != nil {
		t.Fatal(e)
	}
	j := AttachedJournal(db, "ordinary")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	id, e := j.Identity(ctx)
	if e != nil || len(id) != 32 {
		t.Fatal(id, e)
	}
	if e = j.Transaction(ctx, func(tx *sql.Tx) error {
		if e := ChangeTx(tx, "s", "r", "original"); e != nil {
			return e
		}
		rows, _, e := j.Changes(BindSQLTransaction(ctx, db, tx), 0, 10)
		if e != nil {
			return e
		}
		if len(rows) != 1 {
			return errors.New("bound original read did not observe own transaction")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	db.Close()
	db, e = sql.Open("sqlite", file)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if e = InstallJournal(db); e != nil {
		t.Fatal(e)
	}
	again, e := AttachedJournal(db, "ordinary").Identity(ctx)
	if e != nil || again != id {
		t.Fatal("restart silently changed epoch", id, again, e)
	}
}
