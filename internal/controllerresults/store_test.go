package controllerresults_test

import (
	"context"
	"database/sql"
	"encoding/json"
	cr "everything-go/internal/controllerresults"
	"everything-go/internal/sessiondispatch"
	"fmt"
	"sync"
	"testing"
	"time"
)

func fixture(t *testing.T) (*sessiondispatch.Store, cr.Scope) {
	t.Helper()
	s, e := sessiondispatch.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	g, e := s.SetGrant(context.Background(), "parent", sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	if e != nil {
		t.Fatal(e)
	}
	return s, cr.Scope{Parent: cr.Identity{"authority", "parent", "parent-thread", 0}, Generation: 1, GrantRevision: g.Revision}
}
func obs(scope cr.Scope, exec, member string) cr.Observation {
	return cr.Observation{Scope: scope, Execution: cr.Execution{Target: cr.Identity{"authority", "worker", "thread", 0}, Request: exec, Turn: "turn-" + exec}, Member: cr.Member{DispatchID: member, Request: member, Mode: "queue", IntentHash: cr.Hash(member), CreatedAt: 1}, State: "completed", Final: &cr.Final{SealID: "seal", Hash: cr.Hash("same text"), Bytes: 9, Request: exec}, Verified: true, CompletedAt: 1}
}
func TestControllerResultsIdentityMigrationAndVersions(t *testing.T) {
	s, scope := fixture(t)
	ctx := context.Background()
	st := s.Results()
	for i := 0; i < 72; i++ {
		o := obs(scope, fmt.Sprint(i%26), fmt.Sprint(i))
		o.Historical = true
		o.Member.LegacyDelivery = "delivered"
		if _, e := st.Observe(ctx, o); e != nil {
			t.Fatal(e)
		}
	}
	p, e := st.Read(ctx, scope, cr.ReadProof{Principal: "human"}, "", 50)
	if e != nil || len(p.Groups) != 26 || p.Unread != 0 {
		t.Fatal(p, e)
	}
	high := p.HighWater
	for i := 0; i < 72; i++ {
		o := obs(scope, fmt.Sprint(i%26), fmt.Sprint(i))
		o.Historical = true
		o.Member.LegacyDelivery = "delivered"
		if _, e := st.Observe(ctx, o); e != nil {
			t.Fatal(e)
		}
	}
	again, _ := st.Read(ctx, scope, cr.ReadProof{Principal: "human"}, "", 50)
	if again.HighWater != high {
		t.Fatal("refresh created facts")
	}
	total := 0
	for _, g := range p.Groups {
		m, e := st.Members(ctx, scope, g.ID, "", 50)
		if e != nil {
			t.Fatal(e)
		}
		total += len(m.Members)
	}
	if total != 72 {
		t.Fatal(total)
	}
	// Same text never merges different execution or parent generation.
	o := obs(scope, "other-round", "new")
	g, e := st.Observe(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	other := scope
	other.Generation++
	foreign, e := st.Observe(ctx, obs(other, "other-round", "foreign"))
	if e != nil || g.ID == foreign.ID {
		t.Fatal(foreign, e)
	}
	o.Final = &cr.Final{SealID: "different", Hash: cr.Hash("changed"), Request: o.Execution.Request}
	conflict, e := st.Observe(ctx, o)
	if e != nil || conflict.State != "conflicted" || len(conflict.ConflictHashes) != 2 {
		t.Fatal(conflict, e)
	}
}
func TestControllerResultsNativeReadSubsetACKAndLateAlias(t *testing.T) {
	s, scope := fixture(t)
	ctx := context.Background()
	st := s.Results()
	for _, id := range []string{"A", "B"} {
		if _, e := st.Observe(ctx, obs(scope, id, id)); e != nil {
			t.Fatal(e)
		}
	}
	proof := cr.ReadProof{Principal: "session:parent", Request: "active", Turn: "native", Native: true}
	p, e := st.Read(ctx, scope, proof, "", 1)
	if e != nil || len(p.Groups) != 1 || !p.HasMore {
		t.Fatal(p, e)
	}
	refs := p.ReadReceipt.Versions
	r, e := st.Mark(ctx, scope, proof, p.ReadReceipt.ID, refs, "ACK", "hash", "ack", func() error { return nil })
	if e != nil {
		t.Fatal(e)
	}
	again, e := st.Mark(ctx, scope, proof, p.ReadReceipt.ID, refs, "ACK", "hash", "ack", func() error { return nil })
	if e != nil || cr.Hash(r) != cr.Hash(again) {
		t.Fatal(again, e)
	}
	if _, e = st.Mark(ctx, scope, proof, p.ReadReceipt.ID, refs, "ACK", "changed", "ack", func() error { return nil }); e == nil {
		t.Fatal("samekey changed intent")
	}
	wrong := append([]cr.Version{}, refs...)
	wrong[0].Version++
	if _, e = st.Mark(ctx, scope, proof, p.ReadReceipt.ID, wrong, "wrong", "wrong", "ack", func() error { return nil }); e == nil {
		t.Fatal("wrong version")
	}
	human := proof
	human.Native = false
	if _, e = st.Mark(ctx, scope, human, p.ReadReceipt.ID, refs, "human-ack", "x", "ack", func() error { return nil }); e == nil {
		t.Fatal("human cannot borrow native receipt")
	}
	late := obs(scope, p.Groups[0].Execution.Request, "late-steer")
	late.Member.Mode = "steer"
	late.Member.AliasProven = true
	if _, e = st.Observe(ctx, late); e != nil {
		t.Fatal(e)
	}
	page, _ := st.Read(ctx, scope, proof, "", 50)
	if page.Unread != 1 || !page.Groups[0].Acknowledged || page.Groups[0].MemberCount != 2 || page.Groups[0].MissionApplied || page.Groups[0].NativeConsumed {
		t.Fatal(page)
	}
	foreign := proof
	foreign.Turn = "different"
	if _, e = st.Read(ctx, scope, foreign, p.NextCursor, 1); e == nil {
		t.Fatal("foreign turn cursor")
	}
	if _, e = st.Read(ctx, scope, proof, "invented", 1); e == nil {
		t.Fatal("invented cursor")
	}
}
func TestControllerResultsOriginalOutboxCrashConcurrencyAndPause(t *testing.T) {
	s, scope := fixture(t)
	st := s.Results()
	ctx := context.Background()
	for i := 0; i < 40; i++ {
		if _, e := st.Observe(ctx, obs(scope, fmt.Sprint(i), fmt.Sprint(i))); e != nil {
			t.Fatal(e)
		}
	}
	if b, _, e := st.Freeze(ctx, scope, func() error { return nil }); e != nil || b.ID != "" {
		t.Fatal("default wake", b, e)
	}
	c := cr.Control{Mode: "resume_authorized", Deadline: time.Now().Add(time.Hour).UnixMilli(), MaxBatches: 2, MaxNoProgress: 1}
	if _, e := st.SetControl(ctx, scope, 0, c, "enable", "hash", func() error { return nil }); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	ids := make(chan string, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, _, e := st.Freeze(ctx, scope, func() error { return nil })
			if e != nil {
				t.Error(e)
			}
			ids <- b.ID
		}()
	}
	wg.Wait()
	close(ids)
	one := ""
	for id := range ids {
		if one != "" && id != one {
			t.Fatal("two wakes")
		}
		one = id
	}
	status, _ := st.Status(ctx, scope)
	if len(status.Batches) != 1 || len(status.Batches[0].Versions) != 32 {
		t.Fatal(status)
	}
	original := status.Batches[0]
	if e := st.SetBatchState(ctx, one, "unknown", ""); e != nil {
		t.Fatal(e)
	}
	b, created, e := st.Freeze(ctx, scope, func() error { return nil })
	if e != nil || created || b.ID != one || b.PayloadHash != original.PayloadHash {
		t.Fatal(b, e)
	}
	// Persisted frozen receipt remains the sole recovery key after unknown.
	if _, e = st.Observe(ctx, obs(scope, "postfreeze", "postfreeze")); e != nil {
		t.Fatal(e)
	}
	if e = st.SetBatchState(ctx, one, "queued", ""); e != nil {
		t.Fatal(e)
	}
	if e = st.SetBatchState(ctx, one, "completed", "parent-native"); e != nil {
		t.Fatal(e)
	}
	if _, _, e = st.Freeze(ctx, scope, func() error { return nil }); e == nil {
		t.Fatal("no-progress budget must block")
	}
	for _, ref := range original.Versions {
		mp, _ := st.Members(ctx, scope, ref.ID, "", 1)
		if !mp.Group.NativeConsumed || mp.Group.MissionApplied {
			t.Fatal(mp)
		}
	}
	for i := 0; i < 32; i++ {
		batch, _, found, e := st.Link(ctx, fmt.Sprint(i))
		if e != nil || !found || batch.ID != one {
			t.Fatal(batch, e)
		}
	}
	c.Mode = "notify_only"
	c.Paused = true
	if _, e = st.SetControl(ctx, scope, 1, c, "pause", "pause", func() error { return nil }); e != nil {
		t.Fatal(e)
	}
	if b, _, e = st.Freeze(ctx, scope, func() error { return nil }); e != nil || b.ID != "" {
		t.Fatal("pause woke", b, e)
	}
}
func TestControllerResultsPendingConflictAndRevokedWriter(t *testing.T) {
	s, scope := fixture(t)
	st := s.Results()
	ctx := context.Background()
	o := obs(scope, "A", "A")
	o.Final = nil
	g, e := st.Observe(ctx, o)
	if e != nil || g.State != "pending_final" {
		t.Fatal(g, e)
	}
	o.Final = &cr.Final{SealID: "seal", Hash: cr.Hash("final"), Request: "A"}
	g, e = st.Observe(ctx, o)
	if e != nil || g.State != "final" || g.Version != 2 {
		t.Fatal(g, e)
	}
	o = obs(scope, "B", "B")
	o.Verified = false
	g, e = st.Observe(ctx, o)
	if e != nil || g.State != "historical_unverified" {
		t.Fatal(g, e)
	}
	proof := cr.ReadProof{Principal: "native", Request: "active", Turn: "turn", Native: true}
	p, _ := st.Read(ctx, scope, proof, "", 50)
	if _, e = s.SetGrant(ctx, "parent", sessiondispatch.Grant{}, scope.GrantRevision); e != nil {
		t.Fatal(e)
	}
	if _, e = st.Mark(ctx, scope, proof, p.ReadReceipt.ID, p.ReadReceipt.Versions, "ACK", "hash", "ack", func() error { return nil }); e == nil {
		t.Fatal("revoked writer")
	}
	if _, e = st.SetControl(ctx, scope, 0, cr.Control{Mode: "notify_only"}, "ctrl", "hash", func() error { return nil }); e == nil {
		t.Fatal("revoked control")
	}
}

// Actual SQLite reopen, not an in-memory cache or a mock copied baseline.
func TestControllerResultsReopenExactOriginalKeys(t *testing.T) {
	dir := t.TempDir()
	s, e := sessiondispatch.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	scope := cr.Scope{Parent: cr.Identity{"a", "p", "t", 0}, Generation: 1, GrantRevision: 1}
	if _, e = s.SetGrant(context.Background(), "p", sessiondispatch.Grant{Enabled: true, Local: true}, 0); e != nil {
		t.Fatal(e)
	}
	g, e := s.Results().Observe(context.Background(), obs(scope, "r", "d"))
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, e = sessiondispatch.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	p, e := s.Results().Read(context.Background(), scope, cr.ReadProof{Principal: "h"}, "", 20)
	if e != nil || len(p.Groups) != 1 || p.Groups[0].ID != g.ID {
		t.Fatal(p, e)
	}
	if _, e = s.Results().Observe(context.Background(), obs(scope, "r", "d")); e != nil {
		t.Fatal(e)
	}
	m, _ := s.Results().Members(context.Background(), scope, g.ID, "", 50)
	if len(m.Members) != 1 {
		t.Fatal(m)
	}
}

var _ *sql.DB
var _ = json.Valid

func TestControllerResultsMembershipSnapshotAndScopeCursor(t *testing.T) {
	s, scope := fixture(t)
	st := s.Results()
	ctx := context.Background()
	for _, id := range []string{"b", "d", "f"} {
		if _, e := st.Observe(ctx, obs(scope, "same", id)); e != nil {
			t.Fatal(e)
		}
	}
	p, _ := st.Read(ctx, scope, cr.ReadProof{Principal: "human"}, "", 20)
	first, e := st.Members(ctx, scope, p.Groups[0].ID, "", 1)
	if e != nil || !first.HasMore {
		t.Fatal(first, e)
	}
	if _, e = st.Observe(ctx, obs(scope, "same", "a-late")); e != nil {
		t.Fatal(e)
	}
	second, e := st.Members(ctx, scope, p.Groups[0].ID, first.NextCursor, 50)
	if e != nil || !second.Complete || len(second.Members) != 2 || second.Group.MemberCount != 3 {
		t.Fatal(second, e)
	}
	fresh, _ := st.Members(ctx, scope, p.Groups[0].ID, "", 50)
	if len(fresh.Members) != 4 {
		t.Fatal(fresh)
	}
	foreign := scope
	foreign.Generation++
	if _, e = st.Members(ctx, foreign, p.Groups[0].ID, first.NextCursor, 1); e == nil {
		t.Fatal("foreign cursor")
	}
	if _, e = st.Members(ctx, scope, p.Groups[0].ID, "invented", 1); e == nil {
		t.Fatal("invented cursor")
	}
	other := scope
	other.Generation++
	if _, e = st.Observe(ctx, obs(other, "other", "other")); e != nil {
		t.Fatal(e)
	}
	events, e := st.Events(ctx, scope, 0, 100)
	if e != nil || events.HighWater != 4 {
		t.Fatal("scope event sequences leak another scope", events, e)
	}
}

func TestControllerResultsGrantABAAndRetentionGapRequireSnapshot(t *testing.T) {
	s, scope := fixture(t)
	ctx := context.Background()
	st := s.Results()
	for _, id := range []string{"one", "two"} {
		if _, e := st.Observe(ctx, obs(scope, id, id)); e != nil {
			t.Fatal(e)
		}
	}
	proof := cr.ReadProof{Principal: "paired"}
	page, e := st.Read(ctx, scope, proof, "", 1)
	if e != nil || !page.HasMore {
		t.Fatal(page, e)
	}
	disabled, e := s.SetGrant(ctx, scope.Parent.Session, sessiondispatch.Grant{}, scope.GrantRevision)
	if e != nil {
		t.Fatal(e)
	}
	reenabled, e := s.SetGrant(ctx, scope.Parent.Session, sessiondispatch.Grant{Enabled: true, Local: true}, disabled.Revision)
	if e != nil {
		t.Fatal(e)
	}
	current := scope
	current.GrantRevision = reenabled.Revision
	if _, e = st.Mark(ctx, current, proof, page.ReadReceipt.ID, page.ReadReceipt.Versions, "old-read", "hash", "seen", func() error { return nil }); e == nil {
		t.Fatal("revoked read receipt adopted after ABA")
	}
	if _, e = st.Read(ctx, current, proof, page.NextCursor, 1); e == nil {
		t.Fatal("revoked snapshot cursor adopted")
	}
	fresh, e := st.Read(ctx, current, proof, "", 1)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		_, e := tx.Exec(`DELETE FROM controller_result_reads WHERE id=?`, fresh.ReadReceipt.ID)
		return e
	}); e != nil {
		t.Fatal(e)
	}
	if _, e = st.Read(ctx, current, proof, fresh.NextCursor, 1); e == nil {
		t.Fatal("expired snapshot reported complete")
	}
	if _, e = st.Read(ctx, current, proof, page.NextCursor+" appended", 1); e == nil {
		t.Fatal("cursor normalized instead of exact")
	}
}
func TestControllerResultsSeenIsExplicitConsistentAndNeverNative(t *testing.T) {
	s, scope := fixture(t)
	st := s.Results()
	ctx := context.Background()
	g, e := st.Observe(ctx, obs(scope, "one", "one"))
	if e != nil {
		t.Fatal(e)
	}
	proof := cr.ReadProof{Principal: "paired"}
	p, e := st.Read(ctx, scope, proof, "", 20)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = st.Mark(ctx, scope, proof, p.ReadReceipt.ID, p.ReadReceipt.Versions, "seen", "hash", "seen", func() error { return nil }); e != nil {
		t.Fatal(e)
	}
	members, e := st.Members(ctx, scope, g.ID, "", 20)
	if e != nil || !members.Group.HumanSeen || members.Group.NativeRead || members.Group.Acknowledged || members.Group.NativeConsumed || members.Group.MissionApplied {
		t.Fatal(members, e)
	}
}
