package core

import (
	"testing"
)

func TestTaskUnifiedControllerAdmittedRowsRemainBarriersAndUseOriginalActor(t *testing.T) {
	h, call, a, b := unifiedFixtureSessions(t)
	gate, entered := make(chan struct{}), make(chan struct{})
	if !b.SubmitNamed("isolated-active-gate", func() { close(entered); <-gate; b.EndTurn() }) {
		t.Fatal("isolated actor gate")
	}
	<-entered
	defer close(gate)
	for _, id := range []string{"scd_admitted_a", "scd_admitted_b"} {
		seedUnifiedController(t, h, a, b, id)
		d, found, err := h.dispatches.Get(t.Context(), id)
		if err != nil || !found {
			t.Fatal(d, found, err)
		}
		admitted := h.submitLocalSessionDispatch(t.Context(), d)
		if admitted.State != "queued" {
			t.Fatal("real original admission failed", admitted.State, admitted.Error)
		}
	}
	before := b.WaitingIDs()
	if len(before) != 2 {
		t.Fatalf("actual actor queue=%v", before)
	}
	manager := h.unifiedIdentity(a.ID)
	q := unifiedQueue{"controller_waiting_admission", h.unifiedIdentity(b.ID)}
	r, _ := unifiedHTTP(t, call, manager, "queue_snapshot", "", map[string]any{"queue": q})
	v := unifiedResult(t, r)
	rows := v["items"].([]any)
	if len(rows) != 2 {
		t.Fatal("unexpected isolated membership", rows)
	}
	for _, row := range rows {
		item := row.(map[string]any)
		if item["classification"] != "barrier" || item["can_move"] != false || item["can_edit"] != false {
			t.Fatal("controller audit row advertised a post-admission writer", item)
		}
	}
	first, second := rows[0].(map[string]any), rows[1].(map[string]any)
	move := map[string]any{"queue": q, "expected_queue_revision": v["revision"], "membership_hash": v["membership_hash"], "item_id": second["item_id"], "relative_to": first["item_id"], "placement": "before"}
	r, _ = unifiedHTTP(t, call, manager, "queue_move", "controller-post-admission", move)
	if r["ok"] != false {
		t.Fatal("controller claimed a rank-only mutation after real admission", r)
	}
	if got := b.WaitingIDs(); len(got) != 2 || got[0] != before[0] || got[1] != before[1] {
		t.Fatal("controller boundary altered actor membership", got, before)
	}
	// The original messagequeue writer remains available and moves the actual
	// actor, rather than only the previous admission store's audit rows.
	q.Kind = "session_input"
	r, _ = unifiedHTTP(t, call, manager, "queue_snapshot", "", map[string]any{"queue": q})
	v = unifiedResult(t, r)
	rows = v["items"].([]any)
	first, second = rows[0].(map[string]any), rows[1].(map[string]any)
	if first["can_move"] != true || second["can_move"] != true {
		t.Fatal("original messagequeue writer lost its eligible work", rows)
	}
	move = map[string]any{"queue": q, "expected_queue_revision": v["revision"], "membership_hash": v["membership_hash"], "item_id": second["item_id"], "relative_to": first["item_id"], "placement": "before"}
	r, _ = unifiedHTTP(t, call, manager, "queue_move", "actor-post-admission", move)
	unifiedResult(t, r)
	if got := b.WaitingIDs(); len(got) != 2 || got[0] != before[1] || got[1] != before[0] {
		t.Fatal("original actor order did not follow its own writer", got, before)
	}
}
