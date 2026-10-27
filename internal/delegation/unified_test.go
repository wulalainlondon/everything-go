package delegation

import (
	"context"
	"everything-go/internal/taskapi"
	"testing"
)

func TestUnifiedDelegationCrossParentDurableRankActualPromotion(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, e := Open(root)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ConfigureCapacity(ctx, CapacityConfig{Default: Limits{Active: 1, Queued: 10, Delivery: 20}}); e != nil {
		t.Fatal(e)
	}
	// Original occupied lanes make both later jobs durably waiting.
	for _, r := range []Record{capacityRecord("pa", "busy-a"), capacityRecord("pb", "busy-b"), capacityRecord("pa", "a"), capacityRecord("pb", "b")} {
		if e = s.CreateBounded(ctx, r); e != nil {
			t.Fatal(e)
		}
	}
	eligible := map[string]bool{"a": true, "b": true}
	v, e := s.UnifiedQueue(ctx, eligible)
	if e != nil {
		t.Fatal(e)
	}
	m := taskapi.QueueMutation{Scope: "dispatcher", Intent: "rank", Hash: "rankhash", Operation: "queue_move", ExpectedRevision: v.Revision, MembershipHash: v.MembershipHash, Item: "b", Relative: "a", Placement: "before", Revalidate: func(context.Context) error { return nil }}
	if _, e = s.UnifiedMutate(ctx, eligible, m); e != nil {
		t.Fatal(e)
	}
	for _, id := range []string{"busy-a", "busy-b"} {
		if _, e = s.db.Exec("UPDATE session_delegations SET state='terminal',delivery_state='delivered' WHERE id=?", id); e != nil {
			t.Fatal(e)
		}
	}
	s.Close()
	s, e = Open(root)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if promoted, e := s.TryPromote(ctx, "a"); e != nil || promoted {
		t.Fatal("audit order bypassed durable rank", promoted, e)
	}
	if promoted, e := s.TryPromote(ctx, "b"); e != nil || !promoted {
		t.Fatal("ranked job not admitted", promoted, e)
	}
}
