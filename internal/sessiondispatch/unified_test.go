package sessiondispatch

import (
	"context"
	"everything-go/internal/taskapi"
	"testing"
)

func TestUnifiedControllerRevisionReservationAndDurableAdmissionOrder(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, _ := Open(root)
	for _, id := range []string{"a", "b"} {
		if _, e := s.Create(ctx, Record{ID: id, ParentID: "owner-" + id, OriginRequestID: "origin-" + id, ToolCallID: "call", IntentHash: taskapi.RawDigest([]byte(id)), SessionID: "target", RequestID: "r-" + id, Content: "original", Mode: "queue"}); e != nil {
			t.Fatal(e)
		}
	}
	eligible := map[string]bool{"a": true, "b": true}
	v, e := s.UnifiedQueue(ctx, "target", eligible)
	if e != nil {
		t.Fatal(e)
	}
	move := taskapi.QueueMutation{Scope: "dispatcher", Generation: 1, Intent: "move", Hash: "h", Operation: "queue_move", ExpectedRevision: v.Revision, MembershipHash: v.MembershipHash, Item: "b", Relative: "a", Placement: "before", Revalidate: func(context.Context) error { return nil }}
	if _, e = s.UnifiedMutate(ctx, "target", eligible, move); e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, _ = Open(root)
	defer s.Close()
	if _, reserved, e := s.ReserveAdmission(ctx, "a"); e != nil || reserved {
		t.Fatal("old audit head admitted", reserved, e)
	}
	v, _ = s.UnifiedQueue(ctx, "target", eligible)
	edit := move
	edit.Operation = "queue_edit"
	edit.Intent = "edit"
	edit.Item = "b"
	edit.ExpectedRevision = v.Revision
	edit.MembershipHash = v.MembershipHash
	edit.ExpectedContentRevision = 1
	edit.ExpectedHash = taskapi.RawDigest([]byte("b"))
	edit.Content = "effective"
	if _, e = s.UnifiedMutate(ctx, "target", eligible, edit); e != nil {
		t.Fatal(e)
	}
	d, reserved, e := s.ReserveAdmission(ctx, "b")
	if e != nil || !reserved || d.Content != "effective" {
		t.Fatal(d, reserved, e)
	}
	original, _, _ := s.Get(ctx, "b")
	if original.Content != "original" || original.IntentHash != taskapi.RawDigest([]byte("b")) {
		t.Fatal("original controller overwritten", original)
	}
	edit.Intent = "late"
	if _, e = s.UnifiedMutate(ctx, "target", eligible, edit); e == nil {
		t.Fatal("edit crossed admission")
	}
}

func TestUnifiedControllerAdmissionBoundaryRechecksStaleEligibility(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, id := range []string{"a", "b"} {
		_, err = s.Create(ctx, Record{ID: id, ParentID: "owner", OriginRequestID: "origin-" + id, ToolCallID: "call", IntentHash: taskapi.RawDigest([]byte(id)), SessionID: "target", RequestID: "r-" + id, Content: "original", Mode: "queue"})
		if err != nil {
			t.Fatal(err)
		}
	}
	eligible := map[string]bool{"a": true, "b": true}
	before, err := s.UnifiedQueue(ctx, "target", eligible)
	if err != nil || len(before.Items) != 2 || !before.Items[0].Eligible || !before.Items[1].Eligible {
		t.Fatal("waiting admission should remain eligible", before, err)
	}
	d, reserved, err := s.ReserveAdmission(ctx, "a")
	if err != nil || !reserved {
		t.Fatal("original admission", d, reserved, err)
	}
	for _, state := range []string{"accepted", "queued", "running", "uncertain"} {
		d.State = state
		if err := s.Put(ctx, d); err != nil {
			t.Fatal(err)
		}
		v, err := s.UnifiedQueue(ctx, "target", eligible) // intentionally stale map
		if err != nil || len(v.Items) != 2 || v.Items[0].Eligible || !v.Items[1].Eligible {
			t.Fatal("admitted row reopened by stale eligibility", state, v, err)
		}
		move := taskapi.QueueMutation{Scope: "dispatcher", Generation: 1, Intent: "move-" + state, Hash: "h", Operation: "queue_move", ExpectedRevision: v.Revision, MembershipHash: v.MembershipHash, Item: "b", Relative: "a", Placement: "before", Revalidate: func(context.Context) error { return nil }}
		if _, err := s.UnifiedMutate(ctx, "target", eligible, move); err == nil {
			t.Fatal("controller transaction crossed original admission", state)
		}
	}
	// An admission racing after a waiting snapshot must fail the original CAS.
	stale := taskapi.QueueMutation{Scope: "dispatcher", Generation: 1, Intent: "stale-admission", Hash: "h", Operation: "queue_move", ExpectedRevision: before.Revision, MembershipHash: before.MembershipHash, Item: "b", Relative: "a", Placement: "before", Revalidate: func(context.Context) error { return nil }}
	if _, err := s.UnifiedMutate(ctx, "target", eligible, stale); err == nil {
		t.Fatal("original pre-admission snapshot survived admission")
	}
}
