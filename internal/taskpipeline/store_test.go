package taskpipeline

import (
	"context"
	"database/sql"
	"errors"
	_ "modernc.org/sqlite"
	"path/filepath"
	"testing"
	"time"
)

func fixture(t *testing.T) (*Store, Scope, Issuer) {
	t.Helper()
	db, e := sql.Open("sqlite", filepath.Join(t.TempDir(), "control.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	if e = Install(db); e != nil {
		t.Fatal(e)
	}
	scope := Scope{Principal: "device:synthetic", Generation: 3, Manager: Identity{Authority: "local", Session: "owner", Thread: "thread", Revision: 0}}
	return New(db), scope, Issuer{Scope: scope, Kind: "paired_human", SourceGeneration: 7, Targets: map[string]uint64{}}
}
func identity() Locator {
	return Locator{Kind: "delegation", Authority: "local", TaskID: "task", NativeID: "round1", RoundID: "round1", Session: "worker", Request: "request1", Thread: "native1", Revision: 0}
}
func fact() Fact {
	return Fact{Source: identity(), Store: "delegation", Sequence: 3, Admission: "queued", NativeStart: "observed", InputRevision: 1, InputHash: "actual_input", ConsumedRevision: 1, ConsumedHash: "actual_input", EvidenceStatus: "verified_native_ingress", Terminal: "completed", Final: "sealed", SealID: "seal1", FullHash: digest("sealed"), QA: "not_requested", ObservedAt: time.Now().UnixMilli()}
}
func auth() Authorization {
	return Authorization{Confirmed: true, SourceRef: "original-user-instruction", Budget: Budget{DeadlineMS: time.Now().Add(time.Hour).UnixMilli(), MaxEffects: 3, MaxModelTurns: 3, MaxNoProgress: 2}}
}
func action() *Action {
	a := &Action{Kind: "session_resume", Target: Identity{Authority: "local", Session: "owner", Thread: "thread"}, Instruction: "Continue the original synthetic test only", Disclosure: "references_only"}
	a.InstructionHash = digestText(a.Instruction)
	return a
}
func allow(context.Context) error { return nil }
func create(t *testing.T, s *Store, scope Scope, i Issuer, r Record, initial []Fact) Record {
	t.Helper()
	if e := Validate(r); e != nil {
		t.Fatal(e)
	}
	rec, e := s.Mutate(context.Background(), scope, i, "create", r.Kind+"_create", "hash", "", 0, &r, nil, initial, allow)
	if e != nil {
		t.Fatal(e)
	}
	return *rec.Record
}
func TestWatchNotifyOnlyPersistsNoModelAndBaselineDoesNotCatchUp(t *testing.T) {
	s, scope, i := fixture(t)
	w := Record{Kind: "watch", Watch: &Watch{Source: identity(), Destination: scope.Manager, Predicate: Predicate{Kind: "success_sealed"}, Mode: "notify_only", Authorization: auth()}}
	f := fact()
	r := create(t, s, scope, i, w, []Fact{f})
	if e := s.Observe(context.Background(), r.ID, r.Revision, []Fact{f}, allow); e != nil {
		t.Fatal(e)
	}
	pending, _ := s.Pending(context.Background(), 10)
	if len(pending) != 0 {
		t.Fatal(pending)
	}
	events, _, _, _ := s.Events(context.Background(), scope, r.ID, 0, 10)
	if len(events) != 1 || events[0].State != "baseline" {
		t.Fatal(events)
	}
	r, _, _, _ = s.Get(context.Background(), scope, r.ID)
	if r.State != "active" {
		t.Fatal("historical completion was adopted", r)
	}
}
func TestExactWatchCatchupOneClaimLostACKUnknownNoReplay(t *testing.T) {
	s, scope, i := fixture(t)
	r := create(t, s, scope, i, Record{Kind: "watch", Watch: &Watch{Source: identity(), Destination: scope.Manager, Predicate: Predicate{Kind: "success_sealed"}, Mode: "resume_authorized", CatchUp: true, Action: action(), Authorization: auth()}}, []Fact{fact()})
	if e := s.Observe(context.Background(), r.ID, 1, []Fact{fact()}, allow); e != nil {
		t.Fatal(e)
	}
	r, _, _, _ = s.Get(context.Background(), scope, r.ID)
	if e := s.Observe(context.Background(), r.ID, r.Revision, []Fact{fact()}, allow); e != nil {
		t.Fatal(e)
	}
	pending, e := s.Pending(context.Background(), 10)
	if e != nil || len(pending) != 1 {
		t.Fatal(pending, e)
	}
	c, e := s.MarkIssued(context.Background(), pending[0], allow)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.MarkIssued(context.Background(), c, allow); !errors.Is(e, ErrConflict) {
		t.Fatal("issued intent replayed", e)
	}
	g, e := s.Guard(context.Background(), "owner")
	if e != nil || g.Ready || g.Unknown != 1 {
		t.Fatal(g, e)
	}
}
func TestRevisionAndCancelFenceAdmissionBeforeEffect(t *testing.T) {
	s, scope, i := fixture(t)
	r := create(t, s, scope, i, Record{Kind: "watch", Watch: &Watch{Source: identity(), Destination: scope.Manager, Predicate: Predicate{Kind: "success_sealed"}, Mode: "resume_authorized", CatchUp: true, Action: action(), Authorization: auth()}}, nil)
	if e := s.Observe(context.Background(), r.ID, 1, []Fact{fact()}, allow); e != nil {
		t.Fatal(e)
	}
	pending, _ := s.Pending(context.Background(), 10)
	c, e := s.MarkIssued(context.Background(), pending[0], allow)
	if e != nil {
		t.Fatal(e)
	}
	current, _, _, _ := s.Get(context.Background(), scope, r.ID)
	_, e = s.Mutate(context.Background(), scope, i, "cancel", "watch_cancel", "cancelhash", r.ID, current.Revision, nil, nil, nil, allow)
	if e != nil {
		t.Fatal(e)
	}
	tx, _ := s.db.BeginTx(context.Background(), nil)
	e = AdmitTx(context.Background(), tx, &Admission{ID: c.ID, Fence: c.Fence, Check: allow}, "owner", c.RequestID, "payloadhash")
	tx.Rollback()
	if !errors.Is(e, ErrScope) {
		t.Fatal("cancel lost first effect fence", e)
	}
}
func TestOperationOriginalReceiptScopeHashAndCrashReadback(t *testing.T) {
	s, scope, i := fixture(t)
	r := create(t, s, scope, i, Record{Kind: "mission", Mission: &Mission{Goal: "synthetic", Completion: "exact seal", Steps: []Step{{ID: "source", Title: "source", Required: true, Source: ptr(identity()), Predicate: Predicate{Kind: "success_sealed"}}}, Authorization: auth()}}, nil)
	receipt, found, e := s.Operation(context.Background(), scope, "create", "mission_create", "hash")
	if e != nil || !found || receipt.Record.ID != r.ID {
		t.Fatal(receipt, found, e)
	}
	foreign := scope
	foreign.Manager.Session = "foreign"
	if _, found, e = s.Operation(context.Background(), foreign, "create", "mission_create", "hash"); e != nil || found {
		t.Fatal("cross manager receipt", found, e)
	}
	if _, _, e = s.Operation(context.Background(), scope, "create", "mission_create", "other"); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	other := New(s.db)
	if _, found, e = other.Operation(context.Background(), scope, "create", "mission_create", "hash"); e != nil || !found {
		t.Fatal("restart lost original receipt", e)
	}
}
func ptr(l Locator) *Locator { return &l }
func TestDAGRejectCycleAndQAProofCannotUseModelText(t *testing.T) {
	a := identity()
	r := Record{Kind: "pipeline", Pipeline: &Pipeline{Name: "cycle", Nodes: []Step{{ID: "a", Title: "A", Source: &a}, {ID: "b", Title: "B", Source: &a}}, Edges: []Edge{{ID: "ab", From: []string{"a"}, To: "b"}, {ID: "ba", From: []string{"b"}, To: "a"}}, Authorization: auth()}}
	if e := Validate(r); e == nil {
		t.Fatal("cycle accepted")
	}
	f := fact()
	p := Predicate{Kind: "qa_pass"}
	f.QA = "PASS"
	if PredicateMet(p, f) {
		t.Fatal("unbound PASS accepted")
	}
	f.QARound = "qa-round"
	f.QAOwnerRound = "wrong"
	if PredicateMet(p, f) {
		t.Fatal("wrong owner round accepted")
	}
	f.QAOwnerRound = f.Source.RoundID
	if !PredicateMet(p, f) {
		t.Fatal("exact QA rejected")
	}
	f.NativeStart = "observed"
	f.Terminal = "unknown"
	f.Final = "pending"
	if PredicateMet(Predicate{Kind: "success_sealed"}, f) {
		t.Fatal("native start became completion")
	}
}

func TestBlankOptionalRevisionCannotAdoptEditedOrUnknownNativeFinal(t *testing.T) {
	f := fact()
	p := Predicate{Kind: "success_sealed"}
	f.InputRevision = 2
	f.InputHash = "new"
	if PredicateMet(p, f) {
		t.Fatal("old consumed payload satisfied edited success")
	}
	f.ConsumedRevision = 2
	f.ConsumedHash = "new"
	if !PredicateMet(p, f) {
		t.Fatal("actual matching new ingress failed")
	}
	f.NativeStart = "unknown"
	if PredicateMet(p, f) {
		t.Fatal("unknown native start adopted")
	}
	f.NativeStart = "observed"
	f.ConsumedRevision = 0
	f.ConsumedHash = ""
	if PredicateMet(p, f) {
		t.Fatal("missing ingress treated as success")
	}
}
