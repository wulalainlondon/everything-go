package core

import (
	"context"
	cr "everything-go/internal/controllerresults"
	"everything-go/internal/messagequeue"
	"testing"
	"time"
)

func TestControllerResultsUserBarrierAfterFreezeZeroAggregateAdmission(t *testing.T) {
	h, _, parent, target := unifiedFixtureSessions(t)
	ctx := context.Background()
	fe := h.exec.(*scopeFixtureExecutor).fakeExec
	useNativeCorrelationFixture(h, fe, target.ResumeID(), "producer", "native-producer")
	original := seedControllerResult(t, h, parent, target, "primary", "producer", false)
	scope, ok := h.observeControllerResult(ctx, original)
	if !ok {
		t.Fatal("observe")
	}
	h.runtimes.Update(parent.ID, "completed", "", 0, "", "")
	st := h.dispatches.Results()
	if _, e := st.SetControl(ctx, scope, 0, cr.Control{Mode: "resume_authorized", Deadline: time.Now().Add(time.Hour).UnixMilli(), MaxBatches: 1, MaxNoProgress: 1}, "enable", "hash", func() error { return nil }); e != nil {
		t.Fatal(e)
	}
	b, _, e := st.Freeze(ctx, scope, func() error { return nil })
	if e != nil {
		t.Fatal(e)
	}
	user, _, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: parent.ID, RequestID: "actual-original-user", Payload: []byte(`{"content":"original user"}`)})
	if e != nil {
		t.Fatal(e)
	}
	h.reconcileControllerResultBatch(ctx, scope)
	if _, found, e := h.messageQueue.Get(parent.ID, b.Request); e != nil || found {
		t.Fatal("user barrier bypassed", found, e)
	}
	kept, found, e := h.messageQueue.Get(parent.ID, user.RequestID)
	if e != nil || !found || kept.PayloadHash != user.PayloadHash || kept.State != messagequeue.Queued {
		t.Fatal("original user changed", kept, e)
	}
}
