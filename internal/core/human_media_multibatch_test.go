package core

import (
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"testing"
	"time"
)

func TestHumanPreparedSuccessorCompletesAfterFutureAndInitialNativeTransitions(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	release := s.HoldQueue()
	defer func() { s.Close(); release() }()
	cfg := session.ConfigurationFrom(s.SettingsSnapshot())
	cfg.Model = "next"
	if _, err := s.SetFutureConfiguration(cfg, nil); err != nil {
		t.Fatal(err)
	}
	a := qaR4ReservePrepare(t, h, s, c, "r_chain_a", "")
	qaR4UploadSeal(t, h, s, c, a)
	b := qaR4ReservePrepare(t, h, s, c, "r_chain_b", a.reservation.Request)
	calls := make(chan string, 3)
	h.SetExecutor(&qaR4Executor{fakeExec: &fakeExec{sink: h}, h: h, calls: calls, firstNative: true})
	release()
	select {
	case got := <-calls:
		if got != a.reservation.Request {
			t.Fatal(got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first batch not consumed")
	}
	qaR4UploadSeal(t, h, s, c, b)
	select {
	case got := <-calls:
		if got != b.reservation.Request {
			t.Fatal(got)
		}
	case <-time.After(3 * time.Second):
		entry, _, _ := h.messageQueue.GetEffective(s.ID, b.reservation.Request)
		t.Fatal("prepared successor not consumed", entry.State, entry.Message)
	}
	done, err := h.media.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	batch := h.media.data.Batches[mediaKey(b.binding.Device, b.binding.Generation, b.prepare.MediaBatchID)]
	if batch.Thread != "" || batch.BodyHash != b.body || batch.SourceRevision != 0 || batch.Config != b.binding.Config || batch.ExecutionThread != "native_first_exact" || batch.ExecutionSource != s.SettingsSnapshot().SourceRevision {
		t.Fatal("original prepare or exact continuation changed", batch)
	}
}
func TestHumanPreparedSuccessorContinuationRejectsChangedOriginal(t *testing.T) {
	for _, change := range []string{"sourceABA", "manifest", "generation", "role", "unknown", "cancelled", "editing"} {
		t.Run(change, func(t *testing.T) {
			h, s, c, _ := mediaFixture(t)
			release := s.HoldQueue()
			defer func() { s.Close(); release() }()
			a := qaR4ReservePrepare(t, h, s, c, "r_guard_a", "")
			qaR4UploadSeal(t, h, s, c, a)
			b := qaR4ReservePrepare(t, h, s, c, "r_guard_b", a.reservation.Request)
			calls := make(chan string, 3)
			h.SetExecutor(&qaR4Executor{fakeExec: &fakeExec{sink: h}, h: h, calls: calls, firstNative: true})
			release()
			select {
			case <-calls:
			case <-time.After(3 * time.Second):
				t.Fatal("first native proof absent")
			}
			if change == "sourceABA" {
				s.SetResumeID("foreign")
				s.SetResumeID("native_first_exact")
			}
			if change == "cancelled" || change == "editing" {
				kind := "cancel"
				op := messagequeue.HumanOperation{ID: "mutation_guard", Kind: kind, Request: b.reservation.Request, ExpectedRevision: 1}
				if change == "editing" {
					op.Kind = "begin_edit"
					rev, membership, err := h.messageQueue.HumanMembership(b.binding)
					if err != nil {
						t.Fatal(err)
					}
					op.QueueRevision = &rev
					op.MembershipHash = membership
					op.ContentRevision = 1
					rows, err := h.messageQueue.HumanInputs(b.binding)
                if err != nil {t.Fatal(err)}
                for _,row := range rows {if row.ID==b.reservation.Request {op.ExpectedHash=row.EffectiveHash}}
				}
				boot, now := h.messageQueue.HumanClock()
				r, err := h.messageQueue.HumanApply(b.binding, op, boot, now)
				if err != nil || r.Status != "committed" {
					t.Fatal("negative fixture mutation", r, err)
				}
			}
			done, err := h.media.begin()
			if err != nil {
				t.Fatal(err)
			}
			defer done()
			batch := h.media.data.Batches[mediaKey(b.binding.Device, b.binding.Generation, b.prepare.MediaBatchID)]
			switch change {
			case "manifest":
				batch.Items[0].Name = "foreign.png"
			case "generation":
				batch.Generation++
			case "role":
				batch.RoleEpoch++
			case "unknown":
				batch.State = "admitting_unknown"
			}
			beforeThread, beforeRevision := batch.ExecutionThread, batch.ExecutionSource
			if err := h.mediaBatchTarget(batch); err == nil {
				t.Fatal("changed original acquired continuation", change)
			}
			if batch.ExecutionThread != beforeThread || batch.ExecutionSource != beforeRevision {
				t.Fatal("refused source mutated original witness")
			}
		})
	}
}
