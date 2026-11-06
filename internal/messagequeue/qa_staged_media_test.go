package messagequeue

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestQAStagedEditCommitChecksCurrentMonotonicLease(t *testing.T) {
	s, b := humanFixture(t)
	v := humanReserve(t, s, b, "original", "")
	if _, _, e := s.Enqueue(humanSealEntry(v)); e != nil {
		t.Fatal(e)
	}
	rows, _ := s.HumanInputs(b)
	v = rows[0]
	rev, membership, _ := s.HumanMembership(b)
	boot, start := s.HumanClock()
	// Construct an already-near-expiry lease using the Store's injected time API;
	// no clock mutation, live state or 120-second wall-clock wait is needed.
	begin, e := s.HumanApply(b, HumanOperation{ID: "begin", Kind: "begin_edit", Request: v.ID, ExpectedRevision: v.Revision, ContentRevision: v.ContentRevision, ExpectedHash: v.EffectiveHash, QueueRevision: &rev, MembershipHash: membership}, boot, start.Add(-HumanEditTTL+10*time.Millisecond))
	if e != nil || begin.Status != "committed" {
		t.Fatal(begin, e)
	}
	lease := begin.Input.Lease
	time.Sleep(30 * time.Millisecond)
	_, actual := s.HumanClock()
	if actual.UnixMilli() < lease.Expires {
		t.Fatal("fixture not expired")
	}
	op := HumanOperation{ID: "commit", Kind: "commit_edit", Request: v.ID, ExpectedRevision: begin.Input.Revision, ContentRevision: v.ContentRevision, ExpectedHash: v.EffectiveHash, IntentHash: v.IntentHash, Content: "edited", Assets: json.RawMessage(`[]`), LeaseID: lease.ID, LeaseGeneration: lease.Generation, ReadyProof: v.IntentHash}
	entry := Entry{SessionID: b.Session, RequestID: v.ID, Content: op.Content, Payload: []byte(`{"content":"edited"}`), HumanSeal: &HumanSeal{Binding: b, Operation: op, Proof: op.ReadyProof, Boot: boot, Now: start}}
	got, _, err := s.Enqueue(entry)
	if err == nil {
		t.Fatalf("expired lease accepted using stale captured Now: expiry=%d actual=%d captured=%d content=%q", lease.Expires, actual.UnixMilli(), start.UnixMilli(), got.Content)
	}
}

func TestQAStagedAdjacentFormalQueuePreservesUnsealedBarrier(t *testing.T) {
	s, b := humanFixture(t)
	a := humanReserve(t, s, b, "a", "")
	c := humanReserve(t, s, b, "c", a.ID)
	if _, _, e := s.Enqueue(humanSealEntry(c)); e != nil {
		t.Fatal(e)
	}
	view, e := s.UnifiedQueue(context.Background(), b.Session, map[string]bool{})
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(view)
	t.Log(string(raw))
	if s.HumanReady(b.Session, c.ID) {
		t.Fatal("legacy/formal view skipped incomplete predecessor")
	}
	if len(view.Items) != 2 || view.Items[0].ID != a.ID || view.Items[0].Eligible || view.Items[0].Barrier != "original_input_or_native_boundary" || view.Items[1].ID != c.ID {
		t.Fatal("formal view changed or exposed unsealed ordering", view)
	}
}
