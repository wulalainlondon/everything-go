package core

import (
	"context"
	"encoding/json"
	"errors"
	contract "everything-go/contracts/task-api/controller-results-v9"
	"everything-go/internal/backend"
	cr "everything-go/internal/controllerresults"
	"everything-go/internal/messagequeue"
	"everything-go/internal/sessiondispatch"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestControllerResultsConflictPauseAndExactOriginalMQ(t *testing.T) {
	for _, kind := range []string{"native_conflict", "paused_before_admit", "wrong_original_payload", "unknown_not_retired"} {
		t.Run(kind, func(t *testing.T) {
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
			store := h.dispatches.Results()
			_, e := store.SetControl(ctx, scope, 0, cr.Control{Mode: "resume_authorized", Deadline: time.Now().Add(time.Hour).UnixMilli(), MaxBatches: 1, MaxNoProgress: 1}, "enable", "hash", func() error { return nil })
			if e != nil {
				t.Fatal(e)
			}
			batch, created, e := store.Freeze(ctx, scope, func() error { return nil })
			if e != nil || !created {
				t.Fatal(batch, created, e)
			}
			switch kind {
			case "native_conflict":
				if e = h.messageQueue.RecordNativeAcceptance(target.ID, "producer", target.ResumeID(), "foreign-native-turn"); !errors.Is(e, messagequeue.ErrNativeConflict) {
					t.Fatal(e)
				}
				h.observeControllerResult(ctx, original)
			case "paused_before_admit":
				_, e = store.SetControl(ctx, scope, 1, cr.Control{Mode: "notify_only", Paused: true}, "pause", "pausehash", func() error { return nil })
				if e != nil {
					t.Fatal(e)
				}
			case "wrong_original_payload":
				_, _, e = h.messageQueue.Enqueue(messagequeue.Entry{SessionID: parent.ID, RequestID: batch.Request, Content: "foreign body", Payload: []byte(`{"content":"foreign","callback_binding":{"foreign":true}}`)})
				if e != nil {
					t.Fatal(e)
				}
			case "unknown_not_retired":
				if e = store.SetBatchState(ctx, batch.ID, "unknown", ""); e != nil {
					t.Fatal(e)
				}
				_, e = store.SetControl(ctx, scope, 1, cr.Control{Mode: "notify_only", Paused: true}, "pause", "pausehash", func() error { return nil })
				if e != nil {
					t.Fatal(e)
				}
			}
			h.reconcileControllerResultBatch(ctx, scope)
			status, e := store.Status(ctx, scope)
			if e != nil || len(status.Batches) != 1 {
				t.Fatal(status, e)
			}
			b := status.Batches[0]
			if kind == "paused_before_admit" {
				if b.State != "retired" {
					t.Fatal(b)
				}
			} else if kind == "wrong_original_payload" || kind == "unknown_not_retired" {
				if b.State != "unknown" {
					t.Fatal(b)
				}
			}
			if kind != "wrong_original_payload" {
				if _, found, e := h.messageQueue.Get(parent.ID, batch.Request); e != nil || found {
					t.Fatal("unexpected original MQ effect", found, e)
				}
			}
			if kind == "native_conflict" {
				page, e := store.Read(ctx, scope, cr.ReadProof{Principal: "paired"}, "", 20)
				if e != nil || page.Groups[0].State != "conflicted" {
					t.Fatal(page, e)
				}
			}
		})
	}
}
func TestControllerResultsTargetHiddenRefusesWholeBody(t *testing.T) {
	h, call, parent, target := unifiedFixtureSessions(t)
	fe := h.exec.(*scopeFixtureExecutor).fakeExec
	useNativeCorrelationFixture(h, fe, target.ResumeID(), "producer", "native-producer")
	seedControllerResult(t, h, parent, target, "primary", "producer", false)
	h.reconcileSessionDispatches(context.Background())
	manager := cr.Identity{h.cfg.InstanceID, parent.ID, parent.ResumeID(), 0}
	req := resultsRequest("read_dispatch_inbox", "", map[string]any{"manager": manager, "cursor": "", "limit": 20})
	before := resultsDecode(t, call("POST", "task-api", req, "phone").Body.Bytes())
	if !before.OK {
		t.Fatal(before)
	}
	b, _ := json.Marshal(before.Result)
	var page cr.Page
	json.Unmarshal(b, &page)
	g := page.Groups[0]
	hidden := true
	target.SetMeta(nil, &hidden)
	for _, request := range []map[string]any{req, resultsRequest("read_dispatch_group", "", map[string]any{"manager": manager, "group_id": g.ID, "cursor": "", "limit": 20}), resultsRequest("read_dispatch_final", "", map[string]any{"manager": manager, "group_id": g.ID, "seal_id": g.Final.SealID, "expected_hash": g.Final.Hash, "offset": 0, "limit": 100})} {
		r := resultsDecode(t, call("POST", "task-api", request, "phone").Body.Bytes())
		if r.OK || r.Result != nil || r.Error == nil {
			t.Fatal("disclosure", r)
		}
	}
	grant, _ := h.dispatches.Grant(context.Background(), parent.ID)
	if _, e := h.dispatches.SetGrant(context.Background(), parent.ID, sessiondispatch.Grant{}, grant.Revision); e != nil {
		t.Fatal(e)
	}
}

func TestControllerResultsAdvertisedOriginalNativeCatalogAndBoundedPolicy(t *testing.T) {
	h, _, parent, _ := unifiedFixtureSessions(t)
	tools, e := h.TaskTools(parent)
	if e != nil {
		t.Fatal(e)
	}
	count := 0
	for _, tool := range tools {
		if tool["name"] == "task_controller_results" {
			count++
			schema, _ := json.Marshal(tool["inputSchema"])
			var a, b any
			json.Unmarshal(schema, &a)
			json.Unmarshal(contract.Schema, &b)
			if !reflect.DeepEqual(a, b) {
				t.Fatal("catalog differs from closed contract")
			}
		}
	}
	if count != 1 {
		t.Fatal("gateway missing/duplicated", count)
	}
	h2, _, _, first, _ := initializedUnifiedDelegation(t)
	child, found := h2.registry.Get(first.ChildSessionID)
	if !found {
		t.Fatal("original child")
	}
	tools, e = h2.TaskTools(child)
	if e != nil {
		t.Fatal(e)
	}
	for _, tool := range tools {
		if tool["name"] == "task_controller_results" {
			t.Fatal("read-only child unexpectedly gained parent mailbox mutation tool")
		}
	}
}

func TestControllerResultsStatusClosedWaitReasonsFromOriginalGuards(t *testing.T) {
	h, call, parent, _ := unifiedFixtureSessions(t)
	manager := cr.Identity{h.cfg.InstanceID, parent.ID, parent.ResumeID(), 0}
	ctx := context.Background()
	grant, _ := h.dispatches.Grant(ctx, parent.ID)
	scope := cr.Scope{Parent: manager, Generation: parent.Snapshot().SourceRevision, GrantRevision: grant.Revision}
	input := map[string]any{"manager": manager}
	read := func(want string) {
		t.Helper()
		r := resultsDecode(t, call("POST", "task-api", resultsRequest("get_dispatch_return_status", "", input), "phone").Body.Bytes())
		if !r.OK {
			t.Fatal(r.Error)
		}
		b, _ := json.Marshal(r.Result)
		var status cr.Status
		json.Unmarshal(b, &status)
		if status.WakeReason != want || status.ObservedAt <= 0 {
			t.Fatal(status)
		}
	}
	read("paused")
	if _, e := h.dispatches.Results().SetControl(ctx, scope, 0, cr.Control{Mode: "resume_authorized", Deadline: time.Now().Add(time.Hour).UnixMilli(), MaxBatches: 1, MaxNoProgress: 1}, "enable", "hash", func() error { return nil }); e != nil {
		t.Fatal(e)
	}
	h.runtimes.Update(parent.ID, "running", "old-native", 0, "", "")
	read("parent_busy")
	h.runtimes.Update(parent.ID, "interrupted", "", 0, "", "")
	read("native_state_unknown")
	h.runtimes.Update(parent.ID, "completed", "", 0, "", "")
	h.toolRepairRunning.Store(true)
	read("audio_or_maintenance")
	h.toolRepairRunning.Store(false)
	if _, _, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: parent.ID, RequestID: "human-original", Payload: []byte(`{"content":"human"}`)}); e != nil {
		t.Fatal(e)
	}
	read("user_queue")
	if _, _, e := h.messageQueue.Transition(parent.ID, "human-original", []messagequeue.State{messagequeue.Queued}, messagequeue.Cancelled, "", "", ""); e != nil {
		t.Fatal(e)
	}
	fe := h.exec.(*scopeFixtureExecutor).fakeExec
	h.SetExecutor(&queuedReasonExec{fakeExec: fe, readiness: backend.ErrNativeSubmissionBusy})
	read("parent_busy")
	h.SetExecutor(&queuedReasonExec{fakeExec: fe, readiness: errors.New("synthetic private detail must never appear")})
	read("native_state_unknown")
	h.SetExecutor(&queuedReasonExec{fakeExec: fe})
	read("ready")
}

func TestControllerResultsGrantABARequiresFreshExplicitControl(t *testing.T) {
	for _, frozen := range []bool{false, true} {
		t.Run(fmt.Sprint("frozen=", frozen), func(t *testing.T) {
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
			var old cr.Batch
			if frozen {
				var e error
				old, _, e = st.Freeze(ctx, scope, func() error { return nil })
				if e != nil {
					t.Fatal(e)
				}
			}
			disabled, e := h.dispatches.SetGrant(ctx, parent.ID, sessiondispatch.Grant{}, scope.GrantRevision)
			if e != nil {
				t.Fatal(e)
			}
			grant, e := h.dispatches.SetGrant(ctx, parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, disabled.Revision)
			if e != nil {
				t.Fatal(e)
			}
			current := scope
			current.GrantRevision = grant.Revision
			if scope.Key() != current.Key() {
				t.Fatal("inbox key changed")
			}
			h.reconcileControllerResultBatch(ctx, current)
			if frozen {
				if _, found, e := h.messageQueue.Get(parent.ID, old.Request); e != nil || found {
					t.Fatal("old frozen authorization admitted", found, e)
				}
			}
			status, e := st.Status(ctx, current)
			if e != nil {
				t.Fatal(e)
			}
			if reason := h.controllerResultWaitReason(ctx, current, status.Control, false); reason != "scope_changed" {
				t.Fatal("old grant authorization survived", reason)
			}
			snap, e := h.messageQueue.Snapshot(parent.ID)
			if e != nil {
				t.Fatal(e)
			}
			for _, v := range snap.Items {
				if strings.HasPrefix(v.RequestID, "screturn_batch_") {
					t.Fatal("grant ABA silently resumed", v)
				}
			}
			if _, e = st.SetControl(ctx, current, 1, cr.Control{Mode: "resume_authorized", Deadline: time.Now().Add(time.Hour).UnixMilli(), MaxBatches: 2, MaxNoProgress: 2}, "new-human-enable", "newhash", func() error { return nil }); e != nil {
				t.Fatal(e)
			}
			batch, created, e := st.Freeze(ctx, current, func() error { return nil })
			if e != nil || !created || batch.ControlRevision != 2 {
				t.Fatal(batch, created, e)
			}
		})
	}
}
