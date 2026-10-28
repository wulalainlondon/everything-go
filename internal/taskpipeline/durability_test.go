package taskpipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func notifyWatch(t *testing.T, s *Store, scope Scope, i Issuer) Record {
	return create(t, s, scope, i, Record{Kind: "watch", Manager: scope.Manager, Watch: &Watch{Source: identity(), Destination: scope.Manager, Predicate: Predicate{Kind: "success_sealed"}, Mode: "notify_only", CatchUp: true, Authorization: auth()}}, nil)
}
func TestDurabilityNotifyFailureRetryAndDisplayAreSeparateFromSuccessfulWork(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	r := notifyWatch(t, s, scope, i)
	f := fact()
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{f}, allow); e != nil {
		t.Fatal(e)
	}
	list, _, e := s.Notifications(ctx, scope, r.ID, "", 10)
	if e != nil || len(list) != 1 || list[0].State != "pending" {
		t.Fatal(list, e)
	}
	n := list[0]
	// Failure before the local inbox writer commits keeps successful source proof.
	if e = s.DeliverNotification(ctx, n, func(context.Context) error { return errors.New("temporary delivery failure") }); e == nil {
		t.Fatal("missing negative")
	}
	if e = s.NotificationFailed(ctx, n, "visual_delivery_unavailable"); e != nil {
		t.Fatal(e)
	}
	current, _, _, _ := s.Get(ctx, scope, r.ID)
	if current.State != "done" {
		t.Fatal(current)
	}
	list, _, _ = s.Notifications(ctx, scope, r.ID, "", 10)
	n = list[0]
	if n.State != "failed" || n.Fact.FullHash != f.FullHash {
		t.Fatal(n)
	}
	rec, e := s.MutateDelivery(ctx, scope, "retry-once", "notification_retry", digest("retry"), r.ID, n.ID, n.Revision, false, allow)
	if e != nil {
		t.Fatal(e)
	}
	n = *rec.Notification
	if e = s.DeliverNotification(ctx, n, allow); e != nil {
		t.Fatal(e)
	}
	// A lost inbox ACK is recoverable with the same ID, never a model admission.
	if e = s.DeliverNotification(ctx, n, allow); e != nil {
		t.Fatal(e)
	}
	list, _, _ = s.Notifications(ctx, scope, r.ID, "", 10)
	n = list[0]
	if n.State != "inbox_committed" {
		t.Fatal(n)
	}
	replay, found, e := s.DeliveryOperation(ctx, scope, "retry-once", "notification_retry", digest("retry"))
	if e != nil || !found || replay.Notification.State != "pending" {
		t.Fatal(replay, found, e)
	}
	ack, e := s.MutateDelivery(ctx, scope, "display-once", "notification_ack", digest("display"), r.ID, n.ID, n.Revision, false, allow)
	if e != nil || ack.Notification.State != "displayed" {
		t.Fatal(ack, e)
	}
	if _, e = s.MutateDelivery(ctx, scope, "badretry", "notification_retry", digest("bad"), r.ID, n.ID, ack.Notification.Revision, false, allow); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	var inbox, models int
	s.db.QueryRow(`SELECT count(*) FROM task_pipeline_visual_inbox`).Scan(&inbox)
	s.db.QueryRow(`SELECT count(*) FROM task_pipeline_outbox`).Scan(&models)
	if inbox != 1 || models != 0 {
		t.Fatal(inbox, models)
	}
	// Revocation cannot disclose a foreign principal's delivery receipt.
	foreign := scope
	foreign.Principal = "device:foreign"
	if _, ok, e := s.DeliveryOperation(ctx, foreign, "retry-once", "notification_retry", digest("retry")); e != nil || ok {
		t.Fatal(ok, e)
	}
}
func TestDurabilityNotificationFailureGuardAndConcurrentLocalCommit(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	r := notifyWatch(t, s, scope, i)
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{fact()}, allow); e != nil {
		t.Fatal(e)
	}
	g, e := s.Guard(ctx, scope.Manager.Session)
	if e != nil || g.Ready {
		t.Fatal(g, e)
	}
	tx, _ := s.db.BeginTx(ctx, nil)
	e = GuardTx(ctx, tx, scope.Manager.Session)
	tx.Rollback()
	if e == nil {
		t.Fatal("reset ignored undelivered notice")
	}
	ns, _ := s.PendingNotifications(ctx, 10)
	n := ns[0]
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for k := 0; k < 2; k++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.DeliverNotification(ctx, n, allow) }()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	g, e = s.Guard(ctx, scope.Manager.Session)
	if e != nil || !g.Ready {
		t.Fatal(g, e)
	}
	var count int
	s.db.QueryRow(`SELECT count(*) FROM task_pipeline_visual_inbox`).Scan(&count)
	if count != 1 {
		t.Fatal(count)
	}
}
func TestDurabilityTriggerSwitchClosesFirstEffectAndPreservesOriginalReceipts(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	r := create(t, s, scope, i, Record{Kind: "watch", Manager: scope.Manager, Watch: &Watch{Source: identity(), Destination: scope.Manager, Predicate: Predicate{Kind: "success_sealed"}, Mode: "resume_authorized", CatchUp: true, Action: action(), Authorization: auth()}}, nil)
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{fact()}, allow); e != nil {
		t.Fatal(e)
	}
	pending, _ := s.Pending(ctx, 10)
	c, e := s.MarkIssued(ctx, pending[0], allow)
	if e != nil {
		t.Fatal(e)
	}
	rec, e := s.MutateDelivery(ctx, scope, "disable", "trigger_policy_set", digest("disable"), "", "", 0, false, allow)
	if e != nil || rec.Policy.EffectiveEnabled {
		t.Fatal(rec, e)
	}
	tx, _ := s.db.BeginTx(ctx, nil)
	e = AdmitTx(ctx, tx, &Admission{ID: c.ID, Fence: c.Fence, Check: allow}, c.Action.Target.Session, c.RequestID, "payload")
	tx.Rollback()
	if e == nil {
		t.Fatal("claimed before disable admitted after disable")
	}
	if _, e = s.MarkIssued(ctx, pending[0], allow); e == nil {
		t.Fatal("disabled fresh trigger")
	}
	p, e := New(s.db).TriggerPolicy(ctx, scope)
	if e != nil || p.Enabled || p.Revision != 1 {
		t.Fatal(p, e)
	}
	if _, ok, e := s.Operation(ctx, scope, "create", "watch_create", "hash"); e != nil || !ok {
		t.Fatal(ok, e)
	}
	original, ok, e := s.DeliveryOperation(ctx, scope, "disable", "trigger_policy_set", digest("disable"))
	if e != nil || !ok || original.Policy.Revision != 1 {
		t.Fatal(original, e)
	}
	// A different manager/device policy does not grant control over this one.
	foreign := scope
	foreign.Principal = "device:other"
	p, _ = s.TriggerPolicy(ctx, foreign)
	if !p.EffectiveEnabled || p.Revision != 0 {
		t.Fatal(p)
	}
	t.Setenv("BRIDGE_TASK_PIPELINE_DISABLE_NEW_TRIGGERS", "1")
	p, _ = s.TriggerPolicy(ctx, foreign)
	if !p.OperatorDisabled || p.EffectiveEnabled {
		t.Fatal(p)
	}
}
func TestDurabilityCursorApplicationRollbackPaginationGapAndRetentionProof(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	r := notifyWatch(t, s, scope, i)
	loc := identity()
	c := SourceCursor{RecordID: r.ID, DefinitionRevision: 1, NodeID: "source", Source: loc, Store: "delegation", StoreIdentity: "original-db-epoch", ScopeHash: ScopeKey(scope), After: 100, Watermark: 250, State: "partial"}
	c.Key = SourceCursorKey(scope, r.ID, 1, c.NodeID, loc, c.Store, c.StoreIdentity)
	// Final authority recheck fails after tentative inbox/vector writes: all rollback.
	checks := 0
	e := s.Observe(ctx, r.ID, r.Revision, []Fact{fact()}, func(context.Context) error {
		checks++
		if checks > 1 {
			return ErrScope
		}
		return nil
	}, c)
	if !errors.Is(e, ErrScope) {
		t.Fatal(e)
	}
	cursors, _ := s.SourceCursors(ctx, scope, r.ID)
	if len(cursors) != 0 {
		t.Fatal(cursors)
	}
	notices, _, _ := s.Notifications(ctx, scope, r.ID, "", 10)
	if len(notices) != 0 {
		t.Fatal(notices)
	}
	if e = s.Observe(ctx, r.ID, r.Revision, nil, allow, c); e != nil {
		t.Fatal(e)
	}
	cursors, _ = s.SourceCursors(ctx, scope, r.ID)
	if len(cursors) != 1 || cursors[0].After != 100 {
		t.Fatal(cursors)
	}
	// No-progress observations must still durably commit cursor pages.
	c = cursors[0]
	c.After = 200
	if e = s.Observe(ctx, r.ID, r.Revision, nil, allow, c); e != nil {
		t.Fatal(e)
	}
	stale := c
	stale.After = 250
	if e = s.Observe(ctx, r.ID, r.Revision, nil, allow, stale); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	cursors, _ = s.SourceCursors(ctx, scope, r.ID)
	c = cursors[0]
	c.State = "offline"
	c.Reason = "source_read_unavailable"
	if e = s.Observe(ctx, r.ID, r.Revision, nil, allow, c); e != nil {
		t.Fatal(e)
	}
	cursors, _ = s.SourceCursors(ctx, scope, r.ID)
	if cursors[0].After != 200 {
		t.Fatal(cursors)
	}
	c = cursors[0]
	c.After = 250
	c.State = "snapshot_recovered"
	c.Reason = "retention_gap_exact_snapshot_only"
	if e = s.Observe(ctx, r.ID, r.Revision, []Fact{fact()}, allow, c); e != nil {
		t.Fatal(e)
	}
	events, _, _, _ := s.Events(ctx, scope, r.ID, 0, 100)
	if len(events) != 1 {
		t.Fatal(events)
	}
	floor := events[0].Sequence
	if e = s.RetainEventWindow(ctx, scope, r.ID, floor, allow); e != nil {
		t.Fatal(e)
	}
	if _, _, _, e = s.Events(ctx, scope, r.ID, 0, 10); e == nil {
		t.Fatal("retention gap silently became empty complete")
	}
	var proofs int
	s.db.QueryRow(`SELECT count(*) FROM task_pipeline_events WHERE record_id=?`, r.ID).Scan(&proofs)
	if proofs != 1 {
		t.Fatal("retention deleted referenced proof")
	}
	if _, _, _, e = s.Events(ctx, scope, r.ID, floor, 10); e != nil {
		t.Fatal(e)
	}
}
func TestDurabilityCrossDestinationCannotBeDisplayAcknowledged(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	r := notifyWatch(t, s, scope, i)
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{fact()}, allow); e != nil {
		t.Fatal(e)
	}
	ns, _ := s.PendingNotifications(ctx, 10)
	n := ns[0]
	n.Destination.Session = "foreign"
	n.State = "inbox_committed"
	s.db.Exec(`UPDATE task_pipeline_notifications SET state=?,notification=? WHERE id=?`, n.State, encode(n), n.ID)
	if _, e := s.MutateDelivery(ctx, scope, "wrong-display", "notification_ack", digest("x"), r.ID, n.ID, n.Revision, false, allow); !errors.Is(e, ErrScope) {
		t.Fatal(e)
	}
	var count int
	s.db.QueryRow(`SELECT count(*) FROM task_pipeline_delivery_operations`).Scan(&count)
	if count != 0 {
		t.Fatal(count)
	}
}

var _ *sql.DB

func TestDurabilityCancelRetiresOnlyUncommittedVisualAndCrossJournalKeyIsUnique(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	r := notifyWatch(t, s, scope, i)
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{fact()}, allow); e != nil {
		t.Fatal(e)
	}
	n, _ := s.PendingNotifications(ctx, 10)
	current, _, _, _ := s.Get(ctx, scope, r.ID)
	if _, e := s.Mutate(ctx, scope, i, "cancel", "watch_cancel", "cancel", r.ID, current.Revision, nil, nil, nil, allow); e != nil {
		t.Fatal(e)
	}
	if e := s.DeliverNotification(ctx, n[0], allow); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	ns, _, _ := s.Notifications(ctx, scope, r.ID, "", 10)
	if ns[0].State != "retired" {
		t.Fatal(ns)
	}
	g, e := s.Guard(ctx, scope.Manager.Session)
	if e != nil || !g.Ready {
		t.Fatal(g, e)
	}
	if _, e := s.MutateDelivery(ctx, scope, "create", "trigger_policy_set", digest("a"), "", "", 0, false, allow); !errors.Is(e, ErrConflict) {
		t.Fatal("base intent reused by another journal", e)
	}
	if _, e := s.MutateDelivery(ctx, scope, "policy-original", "trigger_policy_set", digest("b"), "", "", 0, false, allow); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Mutate(ctx, scope, i, "policy-original", "watch_create", "changed", "", 0, &Record{Kind: "watch", Manager: scope.Manager, Watch: r.Watch}, nil, nil, allow); !errors.Is(e, ErrConflict) {
		t.Fatal("delivery intent reused by rule writer", e)
	}
}
func TestDurabilityNoCatchupTerminalBaselineDoesNotNotifyOnUnrelatedQAChange(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	f := fact()
	w := Record{Kind: "watch", Manager: scope.Manager, Watch: &Watch{Source: identity(), Destination: scope.Manager, Predicate: Predicate{Kind: "success_sealed"}, Mode: "notify_only", CatchUp: false, Authorization: auth()}}
	r := create(t, s, scope, i, w, []Fact{f})
	f.QA = "PASS"
	f.QARound = "later-qa"
	f.QAOwnerRound = f.Source.RoundID
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{f}, allow); e != nil {
		t.Fatal(e)
	}
	ns, _, e := s.Notifications(ctx, scope, r.ID, "", 10)
	if e != nil || len(ns) != 0 {
		t.Fatal("no-catch-up adopted an already-terminal source due only to QA metadata", ns, e)
	}
}
func TestDurabilityNoCatchupUnmetQABaselineCanStillNotifyExactNewPASS(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	f := fact()
	w := Record{Kind: "watch", Manager: scope.Manager, Watch: &Watch{Source: identity(), Destination: scope.Manager, Predicate: Predicate{Kind: "qa_pass"}, Mode: "notify_only", CatchUp: false, Authorization: auth()}}
	r := create(t, s, scope, i, w, []Fact{f})
	f.QA = "PASS"
	f.QARound = "actual-new-qa"
	f.QAOwnerRound = f.Source.RoundID
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{f}, allow); e != nil {
		t.Fatal(e)
	}
	ns, _, e := s.Notifications(ctx, scope, r.ID, "", 10)
	if e != nil || len(ns) != 1 || ns[0].Fact.QARound != "actual-new-qa" {
		t.Fatal(ns, e)
	}
}
func TestDurabilityDisabledImportsSourceButCannotCompleteUnissuedAction(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	r := create(t, s, scope, i, Record{Kind: "watch", Manager: scope.Manager, Watch: &Watch{Source: identity(), Destination: scope.Manager, Predicate: Predicate{Kind: "success_sealed"}, Mode: "resume_authorized", CatchUp: true, Action: action(), Authorization: auth()}}, nil)
	if _, e := s.MutateDelivery(ctx, scope, "off", "trigger_policy_set", digest("off"), "", "", 0, false, allow); e != nil {
		t.Fatal(e)
	}
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{fact()}, allow); e != nil {
		t.Fatal(e)
	}
	r, _, _, _ = s.Get(ctx, scope, r.ID)
	if r.State != "active" || len(r.Steps) != 1 || r.Steps[0].Reason != "new_triggers_disabled" {
		t.Fatal(r)
	}
	pending, _ := s.Pending(ctx, 10)
	if len(pending) != 0 {
		t.Fatal(pending)
	}
	if _, e := s.MutateDelivery(ctx, scope, "on", "trigger_policy_set", digest("on"), "", "", 1, true, allow); e != nil {
		t.Fatal(e)
	}
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{fact()}, allow); e != nil {
		t.Fatal(e)
	}
	pending, _ = s.Pending(ctx, 10)
	if len(pending) != 1 {
		t.Fatal(pending)
	}
}
func TestDurabilityUpdatedPredicateHasItsOwnBaselineAndExplicitCatchup(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	f := fact()
	r := create(t, s, scope, i, Record{Kind: "watch", Manager: scope.Manager, Watch: &Watch{Source: identity(), Destination: scope.Manager, Predicate: Predicate{Kind: "success_sealed"}, Mode: "notify_only", CatchUp: false, Authorization: auth()}}, []Fact{f})
	update := func(predicate string, catchup bool) Record {
		t.Helper()
		r, _, _, _ = s.Get(ctx, scope, r.ID)
		w := *r.Watch
		w.Predicate.Kind = predicate
		w.CatchUp = catchup
		rec, e := s.Mutate(ctx, scope, i, "new-version-"+stringUintForTest(r.DefinitionRevision), "watch_update", digest(w), r.ID, r.Revision, &Record{Kind: "watch", Manager: scope.Manager, Watch: &w}, nil, []Fact{f}, allow)
		if e != nil {
			t.Fatal(e)
		}
		return *rec.Record
	}
	r = update("qa_pass", false)
	f.QA = "PASS"
	f.QARound = "actual-new-qa"
	f.QAOwnerRound = f.Source.RoundID
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{f}, allow); e != nil {
		t.Fatal(e)
	}
	ns, _, _ := s.Notifications(ctx, scope, r.ID, "", 10)
	if len(ns) != 1 || ns[0].DefinitionRevision != 2 {
		t.Fatal(ns)
	}
	r = update("success_sealed", false)
	f.QARound = "later-irrelevant-qa"
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{f}, allow); e != nil {
		t.Fatal(e)
	}
	ns, _, _ = s.Notifications(ctx, scope, r.ID, "", 10)
	if len(ns) != 1 {
		t.Fatal("new definition baseline was ignored", ns)
	}
	r = update("success_sealed", true)
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{f}, allow); e != nil {
		t.Fatal(e)
	}
	ns, _, _ = s.Notifications(ctx, scope, r.ID, "", 10)
	if len(ns) != 2 {
		t.Fatal("explicit current-definition catchup suppressed", ns)
	}
}
func stringUintForTest(n uint64) string { return fmt.Sprintf("%d", n) }
func TestDurabilityNoCatchupArtifactArrivesAfterBaseline(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	f := fact()
	hash := digest("actual immutable artifact")
	r := create(t, s, scope, i, Record{Kind: "watch", Manager: scope.Manager, Watch: &Watch{Source: identity(), Destination: scope.Manager, Predicate: Predicate{Kind: "artifact_available", ArtifactHash: hash}, Mode: "notify_only", CatchUp: false, Authorization: auth()}}, []Fact{f})
	f.ArtifactHash = hash
	if e := s.Observe(ctx, r.ID, r.Revision, []Fact{f}, allow); e != nil {
		t.Fatal(e)
	}
	ns, _, _ := s.Notifications(ctx, scope, r.ID, "", 10)
	if len(ns) != 1 {
		t.Fatal(ns)
	}
}
