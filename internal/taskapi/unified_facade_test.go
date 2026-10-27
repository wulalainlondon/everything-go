package taskapi

import (
	"context"
	"database/sql"
	"encoding/json"
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
			if e != nil {
				t.Fatal(e)
			}
			if want == "rejected" {
				if Digest(late) != Digest(r) {
					t.Fatal("settled receipt changed", late)
				}
			} else {
				if late.State != "applied" {
					t.Fatal("original positive read did not settle", late)
				}
				raw, _ := json.Marshal(r)
				var saved []byte
				if e := db.QueryRow("SELECT receipt FROM unified_facade_observations WHERE scope=? AND generation=? AND operation=? AND intent=? AND receipt_hash=?", m.Scope, m.Generation, m.Operation, m.Intent, RawDigest(raw)).Scan(&saved); e != nil || string(saved) != string(raw) {
					t.Fatal("unknown observation lost", e)
				}
				again, e := j.UnifiedFacadeFinish(t.Context(), m, Response{OK: true, Result: map[string]any{"different": "must not replace settled"}}, l)
				if e != nil || Digest(again) != Digest(late) {
					t.Fatal("settled receipt changed", again, e)
				}
			}
		})
	}
}

func TestUnifiedFacadeConcurrentExactSettlementHistoryAndNoNegativePromotion(t *testing.T) {
	db, e := sql.Open("sqlite", filepath.Join(t.TempDir(), "isolated.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if e = InstallJournal(db); e != nil {
		t.Fatal(e)
	}
	j := AttachedJournal(db, "fixture")
	m := QueueMutation{Scope: "scope", Generation: 1, Operation: "dispatch", Intent: "once", Hash: "original-input-hash", Revalidate: func(context.Context) error { return nil }}
	l := Locator{Authority: "fixture", Path: "controller", Operation: "create_dispatch", Key: "once"}
	if _, e = j.UnifiedFacadeClaim(t.Context(), m, []byte(`{}`), l); e != nil {
		t.Fatal(e)
	}
	unknown, e := j.UnifiedFacadeFinish(t.Context(), m, Response{OK: false, Error: Failure("unknown", "unknown", "lookup_original")}, l)
	if e != nil {
		t.Fatal(e)
	}
	rejected, e := j.UnifiedFacadeFinish(t.Context(), m, Response{OK: false, Error: Failure("not_found", "known_none", "lookup_original")}, l)
	if e != nil || Digest(rejected) != Digest(unknown) {
		t.Fatal("negative read changed unknown", rejected, e)
	}
	wrong := l
	wrong.Key = "different"
	if _, e = j.UnifiedFacadeFinish(t.Context(), m, Response{OK: true}, wrong); e == nil {
		t.Fatal("foreign locator settled original")
	}
	wrongM := m
	wrongM.Hash = "different"
	if _, e = j.UnifiedFacadeFinish(t.Context(), wrongM, Response{OK: true}, l); e == nil {
		t.Fatal("foreign hash settled original")
	}
	results := make(chan MutationReceipt, 16)
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func() {
			r, e := j.UnifiedFacadeFinish(t.Context(), m, Response{OK: true, Result: map[string]any{"original": l}}, l)
			results <- r
			errs <- e
		}()
	}
	var first MutationReceipt
	for i := 0; i < 16; i++ {
		r := <-results
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
		if r.State != "applied" {
			t.Fatal(r)
		}
		if i == 0 {
			first = r
		} else if Digest(r) != Digest(first) {
			t.Fatal("concurrent settlement differs")
		}
	}
	var count int
	if e = db.QueryRow("SELECT COUNT(*) FROM unified_facade_observations").Scan(&count); e != nil || count != 2 {
		t.Fatal("need exact unknown+positive observations", count, e)
	}
	revoked := m
	revoked.Revalidate = func(context.Context) error { return Failure("permission", "known_none", "refresh_identity") }
	if _, e = j.UnifiedFacadeFinish(t.Context(), revoked, Response{OK: true}, l); e == nil {
		t.Fatal("revoked caller reads receipt")
	}
}
