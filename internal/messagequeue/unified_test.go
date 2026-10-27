package messagequeue

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
	"sync"
	"testing"
)

func unifiedTestMutation(v taskapi.QueueView, id, op string) taskapi.QueueMutation {
	return taskapi.QueueMutation{Scope: "owner", Generation: 1, Intent: id, Hash: taskapi.RawDigest([]byte(id)), Operation: op, ExpectedRevision: v.Revision, MembershipHash: v.MembershipHash, Revalidate: func(context.Context) error { return nil }}
}
func TestUnifiedEditRetainsAcceptedAttachmentBytesAndPinsNativeVersion(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, _ := Open(root)
	original := []byte(`{"content":"old","images":[{"Data":"original-image","MediaType":"image/png"}],"expected_target":{"thread_id":"t","revision":7}}`)
	entry, _, e := s.Enqueue(Entry{SessionID: "s", RequestID: "r", Payload: original, Content: "old"})
	if e != nil {
		t.Fatal(e)
	}
	v, _ := s.UnifiedQueue(ctx, "s", nil)
	m := unifiedTestMutation(v, "edit-1", "queue_edit")
	m.Item = "r"
	m.ExpectedContentRevision = 1
	m.ExpectedHash = entry.PayloadHash
	m.Content = "new"
	m.Payload = []byte(`{"content":"new","images":[{"Data":"original-image","MediaType":"image/png"}],"expected_target":{"thread_id":"t","revision":7}}`)
	receipt, e := s.UnifiedMutate(ctx, "s", nil, m)
	if e != nil {
		t.Fatal(e)
	}
	again, e := s.UnifiedMutate(ctx, "s", nil, m)
	if e != nil || taskapi.Digest(again) != taskapi.Digest(receipt) {
		t.Fatal(again, e)
	}
	accepted, _, _ := s.Get("s", "r")
	if string(accepted.Payload) != string(original) || accepted.PayloadHash != entry.PayloadHash {
		t.Fatal("accepted intent overwritten")
	}
	s.Close()
	s, _ = Open(root)
	defer s.Close()
	started, changed, e := s.Transition("s", "r", []State{Queued}, Running, "", "", "")
	if e != nil || !changed || started.ContentRevision != 2 || started.EffectiveHash == entry.PayloadHash {
		t.Fatal(started, changed, e)
	}
	var p map[string]any
	_ = json.Unmarshal(started.Payload, &p)
	if p["content"] != "new" {
		t.Fatal(p)
	}
	if _, e = s.UnifiedMutate(ctx, "s", nil, taskapi.QueueMutation{Scope: "owner", Intent: "late", Hash: "h", Operation: "queue_edit", Item: "r", ExpectedRevision: v.Revision, Revalidate: m.Revalidate}); e == nil {
		t.Fatal("late native edit accepted")
	}
}
func TestUnifiedEditRollbackAttachmentSwapAndNativeAcceptance(t *testing.T) {
	ctx := context.Background()
	s, _ := Open(t.TempDir())
	defer s.Close()
	entry, _, _ := s.Enqueue(Entry{SessionID: "s", RequestID: "r", Payload: []byte(`{"content":"old","files":[{"AttachmentID":"original"}]}`)})
	v, _ := s.UnifiedQueue(ctx, "s", nil)
	m := unifiedTestMutation(v, "swap", "queue_edit")
	m.Item = "r"
	m.ExpectedContentRevision = 1
	m.ExpectedHash = entry.PayloadHash
	m.Content = "new"
	m.Payload = []byte(`{"content":"new","files":[{"AttachmentID":"foreign"}]}`)
	if _, e := s.UnifiedMutate(ctx, "s", nil, m); e == nil {
		t.Fatal("attachment swap")
	}
	m.Payload = []byte(`{"content":"new","files":[{"AttachmentID":"original"}]}`)
	m.Intent = "rollback"
	calls := 0
	m.Revalidate = func(context.Context) error {
		calls++
		if calls > 1 {
			return errors.New("source retired")
		}
		return nil
	}
	if _, e := s.UnifiedMutate(ctx, "s", nil, m); e == nil {
		t.Fatal("retired commit")
	}
	effective, _, _ := s.EffectiveInput(ctx, "s", "r")
	if effective.ContentRevision != 1 {
		t.Fatal("rollback wrote version")
	}
	if e := s.RecordNativeAcceptance("s", "r", "thread", "turn"); e != nil {
		t.Fatal(e)
	}
	m.Revalidate = func(context.Context) error { return nil }
	m.Intent = "native"
	if _, e := s.UnifiedMutate(ctx, "s", nil, m); e == nil {
		t.Fatal("queued but already native")
	}
}
func TestUnifiedMoveMixedBarrierCASAuditAndReopen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, _ := Open(root)
	for _, id := range []string{"a", "manual", "b", "c"} {
		if _, _, e := s.Enqueue(Entry{SessionID: "s", RequestID: id, Payload: []byte(`{"content":"original"}`)}); e != nil {
			t.Fatal(e)
		}
	}
	eligible := map[string]bool{"a": true, "b": true, "c": true}
	v, _ := s.UnifiedQueue(ctx, "s", eligible)
	bad := unifiedTestMutation(v, "barrier", "queue_move")
	bad.Item = "c"
	bad.Relative = "a"
	bad.Placement = "before"
	if _, e := s.UnifiedMutate(ctx, "s", eligible, bad); e == nil {
		t.Fatal("crossed manual barrier")
	}
	good := unifiedTestMutation(v, "move", "queue_move")
	good.Item = "c"
	good.Relative = "b"
	good.Placement = "before"
	if _, e := s.UnifiedMutate(ctx, "s", eligible, good); e != nil {
		t.Fatal(e)
	}
	if _, e := s.UnifiedMutate(ctx, "s", eligible, bad); e == nil {
		t.Fatal("stale snapshot")
	}
	s.Close()
	s, _ = Open(root)
	defer s.Close()
	queued, e := s.Queued()
	if e != nil {
		t.Fatal(e)
	}
	want := []string{"a", "manual", "c", "b"}
	for i, r := range queued {
		if r.RequestID != want[i] {
			t.Fatal(queued)
		}
	}
	original, _, _ := s.Get("s", "c")
	if original.Sequence != 4 {
		t.Fatal("audit sequence changed", original.Sequence)
	}
}
func TestUnifiedConcurrentEditStartOnlyOneConsumedVersion(t *testing.T) {
	for trial := 0; trial < 16; trial++ {
		ctx := context.Background()
		s, _ := Open(t.TempDir())
		entry, _, _ := s.Enqueue(Entry{SessionID: "s", RequestID: "r", Payload: []byte(`{"content":"old"}`)})
		v, _ := s.UnifiedQueue(ctx, "s", nil)
		m := unifiedTestMutation(v, "edit", "queue_edit")
		m.Item = "r"
		m.ExpectedHash = entry.PayloadHash
		m.ExpectedContentRevision = 1
		m.Content = "new"
		m.Payload = []byte(`{"content":"new"}`)
		var started Entry
		var editErr, startErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, editErr = s.UnifiedMutate(ctx, "s", nil, m) }()
		go func() {
			defer wg.Done()
			started, _, startErr = s.Transition("s", "r", []State{Queued}, Running, "", "", "")
		}()
		wg.Wait()
		if startErr != nil {
			t.Fatal(startErr)
		}
		if editErr == nil && started.ContentRevision != 2 {
			t.Fatal("new revision not consumed", started)
		}
		if editErr != nil && started.ContentRevision != 1 {
			t.Fatal("failed edit consumed", started)
		}
		s.Close()
	}
}
