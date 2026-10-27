package taskapi

import (
	"context"
	"database/sql"
	_ "modernc.org/sqlite"
	"path/filepath"
	"testing"
)

func TestUnifiedFacadeTypedAcceptanceAndImmutableOriginalReceipt(t *testing.T) {
	for _, acceptance := range []string{"unknown", "known_receipt", "known_none"} {
		t.Run(acceptance, func(t *testing.T) {
			db, e := sql.Open("sqlite", filepath.Join(t.TempDir(), "isolated.sqlite"))
			if e != nil {
				t.Fatal(e)
			}
			defer db.Close()
			if e = InstallJournal(db); e != nil {
				t.Fatal(e)
			}
			j := AttachedJournal(db, "fixture")
			m := QueueMutation{Scope: "scope", Generation: 1, Intent: "original", Hash: "hash", Operation: "dispatch", Revalidate: func(context.Context) error { return nil }}
			l := Locator{Authority: "fixture", Path: "controller", Operation: "create_dispatch", Key: "original"}
			if _, e = j.UnifiedFacadeClaim(t.Context(), m, []byte(`{}`), l); e != nil {
				t.Fatal(e)
			}
			r, e := j.UnifiedFacadeFinish(t.Context(), m, Response{OK: false, Error: Failure("acceptance_unknown", acceptance, "lookup_original")}, l)
			if e != nil {
				t.Fatal(e)
			}
			want := "unknown"
			if acceptance == "known_none" {
				want = "rejected"
			}
			if r.State != want {
				t.Fatal(r.State, want)
			}
			p, found, e := j.UnifiedFacadeRead(t.Context(), m)
			if e != nil || !found || p.Receipt.State != want {
				t.Fatal(p, e)
			}
			late, e := j.UnifiedFacadeFinish(t.Context(), m, Response{OK: true, Result: map[string]any{"later": "not the original promise"}}, l)
			if e != nil || Digest(late) != Digest(r) {
				t.Fatal("original receipt changed", late, e)
			}
		})
	}
}
