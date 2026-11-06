package messagequeue

import (
	"sync"
	"testing"
)

func TestHumanPreparedColdRecoveryIsACommitFenceNotAMissingRowInference(t *testing.T) {
	store, binding := humanFixture(t)
	input := humanReserve(t, store, binding, "original", "")
	entry := humanSealEntry(input)
	if err := store.PrepareHumanSeal(entry.HumanSeal, entry.Payload); err != nil {
		t.Fatal(err)
	}
	if receipt, err := store.ReadHumanOperation(binding, entry.HumanSeal.Operation.ID, ""); err != nil || receipt.Status != "unseen_unknown" {
		t.Fatal("prepare itself claimed a result", receipt, err)
	}
	if err := store.RecoverPreparedHumanSeals(); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.ReadHumanOperation(binding, entry.HumanSeal.Operation.ID, "")
	if err != nil || receipt.Status != "rejected_known_none" {
		t.Fatal(receipt, err)
	}
	entry.HumanSeal.Prepared = true
	if _, _, err = store.Enqueue(entry); err == nil {
		t.Fatal("cold recovery fence permitted a late original writer")
	}
	if _, found, _ := store.Get(binding.Session, input.ID); found {
		t.Fatal("abort created a queue payload")
	}
	rows, _ := store.HumanInputs(binding)
	if rows[0].State != "reserved_waiting_assets" {
		t.Fatal("abort promoted or dropped original reservation", rows)
	}
}
func TestHumanPreparedCommitVersusAbortHasOneDurableWinner(t *testing.T) {
	for i := 0; i < 12; i++ {
		store, binding := humanFixture(t)
		input := humanReserve(t, store, binding, "original", "")
		entry := humanSealEntry(input)
		if err := store.PrepareHumanSeal(entry.HumanSeal, entry.Payload); err != nil {
			t.Fatal(err)
		}
		entry.HumanSeal.Prepared = true
		altered := entry
		altered.Payload = []byte(`{"content":"different"}`)
		if _, _, err := store.Enqueue(altered); err == nil {
			t.Fatal("prepared final body was replaced")
		}
		var group sync.WaitGroup
		group.Add(2)
		go func() { defer group.Done(); _, _, _ = store.Enqueue(entry) }()
		go func() { defer group.Done(); _, _ = store.AbortPreparedHumanSeal(binding, entry.HumanSeal.Operation) }()
		group.Wait()
		receipt, err := store.ReadHumanOperation(binding, entry.HumanSeal.Operation.ID, "")
		if err != nil {
			t.Fatal(err)
		}
		_, found, err := store.Get(binding.Session, input.ID)
		if err != nil {
			t.Fatal(err)
		}
		if found != (receipt.Status == "committed") {
			t.Fatal("receipt and payload disagree", receipt, found)
		}
		if err := store.RecoverPreparedHumanSeals(); err != nil {
			t.Fatal(err)
		}
		after, _ := store.ReadHumanOperation(binding, entry.HumanSeal.Operation.ID, "")
		if after.Status != receipt.Status {
			t.Fatal("recovery rewrote winner", receipt, after)
		}
	}
}

func TestHumanWaitingEditLeaseBlocksOriginalSteeringTransition(t *testing.T) {
	store, binding := humanFixture(t)
	input := humanReserve(t, store, binding, "original", "")
	if _, _, err := store.Enqueue(humanSealEntry(input)); err != nil {
		t.Fatal(err)
	}
	rows, _ := store.HumanInputs(binding)
	input = rows[0]
	revision, membership, _ := store.HumanMembership(binding)
	boot, now := store.HumanClock()
	begun, err := store.HumanApply(binding, HumanOperation{ID: "edit", Kind: "begin_edit", Request: input.ID, ExpectedRevision: input.Revision, ContentRevision: input.ContentRevision, ExpectedHash: input.EffectiveHash, QueueRevision: &revision, MembershipHash: membership}, boot, now)
	if err != nil || begun.Status != "committed" {
		t.Fatal(begun, err)
	}
	if _, changed, err := store.Transition(binding.Session, input.ID, []State{Queued}, Steering, "", "", ""); err == nil || changed {
		t.Fatal("steering crossed original editing lease", changed, err)
	}
}
