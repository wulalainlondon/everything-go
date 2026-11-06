package messagequeue

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func humanFixture(t *testing.T) (*Store, HumanBinding) {
	t.Helper()
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s, HumanBinding{Device: "device", Authority: "bridge", Session: "s", Thread: "thread", Config: 1}
}
func humanReserve(t *testing.T, s *Store, b HumanBinding, id, previous string) HumanInput {
	t.Helper()
	r, e := s.HumanApply(b, HumanOperation{ID: "reserve_" + id, Kind: "reserve", Request: id, Content: id, IntentHash: strings.Repeat("a", 64), Assets: json.RawMessage(`[]`), Predecessor: previous}, "boot", time.Now())
	if e != nil || r.Status != "committed" {
		t.Fatalf("reserve %+v %v", r, e)
	}
	return *r.Input
}
func humanSealEntry(v HumanInput) Entry {
	op := HumanOperation{ID: "seal_" + v.ID, Kind: "seal", Request: v.ID, ExpectedRevision: v.Revision, ContentRevision: v.ContentRevision, IntentHash: v.IntentHash, Content: v.Content, ReadyProof: "proof"}
	return Entry{SessionID: v.Binding.Session, RequestID: v.ID, Content: v.Content, Payload: []byte(`{"content":"` + v.Content + `"}`), HumanSeal: &HumanSeal{Binding: v.Binding, Operation: op, Proof: "proof"}}
}
func TestHumanReservationSealOriginalIDAndNoOvertake(t *testing.T) {
	s, b := humanFixture(t)
	a := humanReserve(t, s, b, "a", "")
	c := humanReserve(t, s, b, "c", "a")
	if _, _, e := s.Enqueue(humanSealEntry(c)); e != nil {
		t.Fatal(e)
	}
	if s.HumanReady(b.Session, "c") {
		t.Fatal("ready c overtook reserved a")
	}
	if _, _, e := s.Enqueue(humanSealEntry(a)); e != nil {
		t.Fatal(e)
	}
	if !s.HumanReady(b.Session, "a") {
		t.Fatal("head not ready")
	}
	e, created, err := s.Enqueue(humanSealEntry(a))
	if err != nil || created || e.RequestID != "a" {
		t.Fatal(e, created, err)
	}
	r, err := s.ReadHumanOperation(b, "seal_a", "")
	if err != nil || r.Status != "committed" || r.Input.Sequence != a.Sequence {
		t.Fatal(r, err)
	}
	if _, _, err = s.Transition(b.Session, "a", []State{Queued}, Completed, "", "", ""); err != nil {
		t.Fatal(err)
	}
	if !s.HumanReady(b.Session, "c") {
		t.Fatal("terminal head blocked c")
	}
}
func TestHumanEditAbortExpiryNeverCreatesReadyEntry(t *testing.T) {
	s, b := humanFixture(t)
	a := humanReserve(t, s, b, "a", "")
	humanReserve(t, s, b, "b", "a")
	now := time.Now()
	begin := HumanOperation{ID: "edit", Kind: "begin_edit", Request: a.ID, ExpectedRevision: a.Revision, ContentRevision: a.ContentRevision, ExpectedHash: a.EffectiveHash}
	rev, membership, _ := s.HumanMembership(b)
	begin.QueueRevision = &rev
	begin.MembershipHash = membership
	r, e := s.HumanApply(b, begin, "boot", now)
	if e != nil || r.Status != "committed" {
		t.Fatal(r, e)
	}
	lease := r.Input.Lease
	if lease.BaseState != "reserved_waiting_assets" || lease.BaseProof != "" {
		t.Fatal(lease)
	}
	if err := s.ExpireHumanEdits("boot", now.Add(HumanEditTTL+time.Second)); err != nil {
		t.Fatal(err)
	}
	inputs, _ := s.HumanInputs(b)
	for _, v := range inputs {
		if v.ID == a.ID && (v.State != "reserved_waiting_assets" || v.Lease != nil || v.ReadyProof != "") {
			t.Fatal(v)
		}
	}
	if _, ok, _ := s.Get(b.Session, a.ID); ok {
		t.Fatal("expiry created entry")
	}
	if s.HumanReady(b.Session, "b") {
		t.Fatal("expiry released successor")
	}
	r, e = s.HumanApply(b, HumanOperation{ID: "late", Kind: "renew_edit", Request: a.ID, ExpectedRevision: r.Input.Revision + 1, LeaseID: lease.ID, LeaseGeneration: lease.Generation}, "boot", now.Add(HumanEditTTL+time.Second))
	if e != nil || r.Status != "rejected_known_none" {
		t.Fatal(r, e)
	}
}
func TestHumanCancelProofAndLateSealCannotRevive(t *testing.T) {
	s, b := humanFixture(t)
	a := humanReserve(t, s, b, "a", "")
	r, e := s.HumanApply(b, HumanOperation{ID: "cancel", Kind: "cancel", Request: "a", ExpectedRevision: 1}, "boot", time.Now())
	if e != nil || r.Status != "committed" {
		t.Fatal(r, e)
	}
	if _, _, err := s.Enqueue(humanSealEntry(a)); err == nil {
		t.Fatal("late seal revived cancelled")
	}
	if _, ok, _ := s.Get(b.Session, "a"); ok {
		t.Fatal("cancelled reservation gained Entry")
	}
	got, _ := s.ReadHumanOperation(b, "cancel", "")
	if got.Input.State != "cancelled" {
		t.Fatal(got)
	}
	other := b
	other.Device = "other"
	got, _ = s.ReadHumanOperation(other, "cancel", "")
	if got.Status != "unseen_unknown" {
		t.Fatal("receipt disclosed", got)
	}
}
func TestHumanOperationHashMismatchAndPureLookup(t *testing.T) {
	s, b := humanFixture(t)
	humanReserve(t, s, b, "a", "")
	before, _ := s.Snapshot(b.Session)
	r, e := s.ReadHumanOperation(b, "absent", "")
	if e != nil || r.Status != "unseen_unknown" {
		t.Fatal(r, e)
	}
	after, _ := s.Snapshot(b.Session)
	if after.Revision != before.Revision {
		t.Fatal("lookup wrote")
	}
	r, e = s.HumanApply(b, HumanOperation{ID: "reserve_a", Kind: "reserve", Request: "a", Content: "different", IntentHash: strings.Repeat("a", 64)}, "boot", time.Now())
	if e != ErrConflict {
		t.Fatal(r, e)
	}
}
func TestHumanOwnerBarrierAndActualOrder(t *testing.T) {
	s, b := humanFixture(t)
	humanReserve(t, s, b, "a", "")
	other := b
	other.Device = "other"
	humanReserve(t, s, other, "foreign", "")
	humanReserve(t, s, b, "c", "")
	rev, membership, _ := s.HumanMembership(b)
	r, e := s.HumanApply(b, HumanOperation{QueueRevision: &rev, MembershipHash: membership, ID: "move", Kind: "move", Request: "c", ExpectedRevision: 1, Relative: "a", Placement: "before"}, "boot", time.Now())
	if e != nil || r.Code != "queue_barrier" {
		t.Fatal(r, e)
	}
	r, e = s.HumanApply(b, HumanOperation{ID: "cancel-foreign", Kind: "cancel", Request: "foreign", ExpectedRevision: 1}, "boot", time.Now())
	if e != nil || r.Code != "source_unavailable" {
		t.Fatal(r, e)
	}
}

func TestHumanEditCommitsEffectiveVersionPreservesOriginalIntentAndConsumption(t *testing.T) {
	s, b := humanFixture(t)
	v := humanReserve(t, s, b, "a", "")
	initial, _, err := s.Enqueue(humanSealEntry(v))
	if err != nil {
		t.Fatal(err)
	}
	items, _ := s.HumanInputs(b)
	v = items[0]
	revision, membership, _ := s.HumanMembership(b)
	now := time.Now()
	edit, err := s.HumanApply(b, HumanOperation{ID: "begin", Kind: "begin_edit", Request: v.ID, ExpectedRevision: v.Revision, ContentRevision: v.ContentRevision, ExpectedHash: v.EffectiveHash, QueueRevision: &revision, MembershipHash: membership}, "boot", now)
	if err != nil || edit.Status != "committed" {
		t.Fatal(edit, err)
	}
	lease := edit.Input.Lease
	op := HumanOperation{ID: "commit", Kind: "commit_edit", Request: v.ID, ExpectedRevision: edit.Input.Revision, ContentRevision: v.ContentRevision, IntentHash: v.IntentHash, ExpectedHash: v.EffectiveHash, Content: "edited", Assets: json.RawMessage(`[]`), LeaseID: lease.ID, LeaseGeneration: lease.Generation, ReadyProof: v.IntentHash}
	next := Entry{SessionID: b.Session, RequestID: v.ID, Content: "edited", Payload: []byte(`{"content":"edited"}`), HumanSeal: &HumanSeal{Binding: b, Operation: op, Proof: op.ReadyProof, Boot: "boot", Now: now}}
	effective, created, err := s.Enqueue(next)
	if err != nil || created || effective.Content != "edited" {
		t.Fatal(effective, created, err)
	}
	original, _, _ := s.Get(b.Session, v.ID)
	if string(original.Payload) != string(initial.Payload) || original.PayloadHash != initial.PayloadHash {
		t.Fatal("original intent overwritten")
	}
	receipt, _ := s.ReadHumanOperation(b, op.ID, "")
	if receipt.Input.ContentRevision != 2 || receipt.Input.WireHash == original.PayloadHash || receipt.Input.ReadyProof != v.IntentHash {
		t.Fatal(receipt)
	}
	running, changed, err := s.Transition(b.Session, v.ID, []State{Queued}, Running, "", "", "")
	if err != nil || !changed || string(running.Payload) != `{"content":"edited"}` {
		t.Fatal(running, changed, err)
	}
}

func TestHumanMoveActorRefusalRollsBackOrderAndOperation(t *testing.T) {
	s, b := humanFixture(t)
	humanReserve(t, s, b, "a", "")
	humanReserve(t, s, b, "b", "a")
	revision, membership, _ := s.HumanMembership(b)
	op := HumanOperation{ID: "move-with-refusal", Kind: "move", Request: "b", ExpectedRevision: 1, Relative: "a", Placement: "before", QueueRevision: &revision, MembershipHash: membership}
	_, err := s.HumanApplyOrder(b, op, "boot", time.Now(), func(_ []string, _ func() error) error { return errors.New("actor membership changed") })
	if err == nil {
		t.Fatal("missing actor refusal")
	}
	order, _ := s.HumanOrder(b.Session)
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatal(order)
	}
	original, _ := s.ReadHumanOperation(b, op.ID, "")
	if original.Status != "unseen_unknown" {
		t.Fatal("refused transaction produced committed receipt", original)
	}
}

func TestHumanUnknownReserveCancelFenceCannotBeRevived(t *testing.T) {
	s, b := humanFixture(t)
	original := HumanOperation{ID: "reserve-late", Kind: "reserve", Request: "a", Content: "A", IntentHash: strings.Repeat("a", 64)}
	cancel := HumanOperation{ID: "cancel-before-seen", Kind: "cancel", Request: "a", OriginalReserve: &original}
	receipt, err := s.HumanApply(b, cancel, "boot", time.Now())
	if err != nil || receipt.Status != "committed" || receipt.Input.State != "cancelled" {
		t.Fatal(receipt, err)
	}
	late, err := s.HumanApply(b, original, "boot", time.Now())
	if err != nil || late.Status != "rejected_known_none" {
		t.Fatal(late, err)
	}
	if _, found, _ := s.Get(b.Session, "a"); found {
		t.Fatal("late reserve created runnable entry")
	}
}
