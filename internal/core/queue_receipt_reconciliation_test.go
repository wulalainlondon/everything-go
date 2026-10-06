package core

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"github.com/coder/websocket"
	"testing"
)

func TestQueueReceiptReadIsExactPureCASAndIndependentOfStateRevision(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("s1", "Task", t.TempDir(), backend.Codex, "", "", "thread")
	owner := sharedReadClient(t, h, "owner")
	e := taskEntry(t, h, s.ID, "r_exact", "owner", messagequeue.Uncertain)
	snap, _ := h.messageQueue.Snapshot(s.ID)
	rev := snap.Revision
	cmd := clientproto.Command{SessionID: s.ID, RequestID: "read-exact", ExpectedRevision: &rev, TaskRequestIDs: []string{e.RequestID}}
	h.reconcileQueueReceipts(owner, cmd)
	out := waitForType(t, owner, "queue_receipts_reconciled")
	fact := out["receipts"].([]any)[0].(map[string]any)
	if out["status"] != "supported" || fact["delivery"] != "unverified" || fact["can_cancel"] != false {
		t.Fatal(out)
	}
	h.Emit(backend.NativeTaskAccepted{SessionID: s.ID, RequestID: e.RequestID, ThreadID: "thread", TurnID: "native-exact"})
	h.reconcileQueueReceipts(owner, cmd)
	out = waitForType(t, owner, "queue_receipts_reconciled")
	fact = out["receipts"].([]any)[0].(map[string]any)
	if fact["delivery"] != "native_consumed" || fact["admission"] != "uncertain" || fact["execution"] != "unverified" || fact["final"] != "unverified" {
		t.Fatal(out)
	}
	after, _, _ := h.messageQueue.Get(s.ID, e.RequestID)
	snapshot, _ := h.messageQueue.Snapshot(s.ID)
	if after.State != e.State || after.PayloadHash != e.PayloadHash || snapshot.Revision != rev {
		t.Fatal("read changed receipt")
	}
	stale := rev + 1
	cmd.ExpectedRevision = &stale
	h.reconcileQueueReceipts(owner, cmd)
	if waitForType(t, owner, "queue_receipts_reconciled")["status"] != "stale_revision" {
		t.Fatal("stale accepted")
	}
	cmd.ExpectedRevision = &rev
	unpaired := newTestClient(h)
	h.reconcileQueueReceipts(unpaired, cmd)
	if waitForType(t, unpaired, "queue_receipts_reconciled")["status"] != "forbidden" {
		t.Fatal("unbound reader")
	}
}
func TestQueueCancelNeverCancelsUncertainNativeOrSystemNotice(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("s1", "Task", t.TempDir(), backend.Codex, "", "", "thread")
	owner := sharedReadClient(t, h, "owner")
	for _, test := range []struct {
		id, purpose, device string
		state               messagequeue.State
	}{{"r_unknown", "instruction", "", messagequeue.Uncertain}, {"r_notice", "result_return", "owner", messagequeue.Queued}, {"r_legacy", "", "", messagequeue.Queued}, {"r_running", "instruction", "owner", messagequeue.Running}, {"r_foreign", "instruction", "other", messagequeue.Queued}, {"r_native", "instruction", "owner", messagequeue.Queued}} {
		payload, _ := json.Marshal(queuedPayload{OwnerDevice: test.device, MessagePurpose: test.purpose, Content: test.id})
		e, _, _ := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: test.id, Payload: payload})
		if test.state != messagequeue.Queued {
			h.messageQueue.Transition(s.ID, e.RequestID, []messagequeue.State{messagequeue.Queued}, test.state, "", "", "")
		}
		if test.id == "r_native" {
			h.Emit(backend.NativeTaskAccepted{SessionID: s.ID, RequestID: test.id, ThreadID: "thread", TurnID: "active"})
		}
		h.cancelQueuedMessage(owner, clientproto.Command{SessionID: s.ID, RequestID: test.id})
		if waitForType(t, owner, "queue_action_result")["status"] != "rejected" {
			t.Fatal(test)
		}
		after, _, _ := h.messageQueue.Get(s.ID, test.id)
		if after.State != test.state {
			t.Fatal("cancel mutated protected receipt", test)
		}
	}
	hold := s.HoldQueue()
	defer hold()
	e := taskEntry(t, h, s.ID, "r_waiting", "owner", messagequeue.Queued)
	s.SubmitNamed(e.RequestID, func() { t.Error("cancelled input executed") })
	h.cancelQueuedMessage(owner, clientproto.Command{SessionID: s.ID, RequestID: e.RequestID})
	if waitForType(t, owner, "queue_action_result")["status"] != "accepted" {
		t.Fatal("owned waiting rejected")
	}
}

func TestQueueReceiptReadRejectsDuplicateAndHiddenScopeWithoutMaintenanceEffects(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("s1", "Task", t.TempDir(), backend.Codex, "", "", "thread")
	owner := sharedReadClient(t, h, "owner")
	e := taskEntry(t, h, s.ID, "r_original", "owner", messagequeue.Uncertain)
	snap, _ := h.messageQueue.Snapshot(s.ID)
	rev := snap.Revision
	cmd := clientproto.Command{SessionID: s.ID, RequestID: "read", ExpectedRevision: &rev, TaskRequestIDs: []string{e.RequestID, e.RequestID}}
	h.reconcileQueueReceipts(owner, cmd)
	out := waitForType(t, owner, "queue_receipts_reconciled")
	if out["status"] != "invalid_request" || len(out["receipts"].([]any)) != 0 {
		t.Fatal(out)
	}
	h.sendReadonlyMessageQueue(owner, clientproto.Command{SessionID: s.ID})
	waitForType(t, owner, "message_queue_snapshot")
	after, _, _ := h.messageQueue.Get(s.ID, e.RequestID)
	if after.State != messagequeue.Uncertain || after.UpdatedAt != e.UpdatedAt {
		t.Fatal("pure snapshot changed original receipt")
	}
	unknown := newTestClient(h)
	h.sendReadonlyMessageQueue(unknown, clientproto.Command{SessionID: s.ID})
	if waitForType(t, unknown, "error")["code"] != "permission_denied" {
		t.Fatal("unbound read")
	}
}

type exactQueueProvider struct {
	*floatingHistoryProvider
	onRead func()
}

func (p *exactQueueProvider) ExactQueueFinal(string, string, string) (bool, error) {
	if p.onRead != nil {
		p.onRead()
	}
	return true, nil
}
func (p *exactQueueProvider) NativeTurnForRequest(*session.Session, string) (string, error) {
	if p.onRead != nil {
		p.onRead()
	}
	return "", nil
}

type exactQueueExec struct {
	*fakeExec
	provider *exactQueueProvider
}

func (e *exactQueueExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.provider, true
}
func (e *exactQueueExec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.provider}
}
func TestQueueReadAndCancelRevalidateRevokedPairingAfterProviderIO(t *testing.T) {
	for _, action := range []string{"read", "cancel"} {
		t.Run(action, func(t *testing.T) {
			h, fe := newTestHub(t)
			s := h.registry.Create("s1", "Task", t.TempDir(), backend.Codex, "", "", "thread")
			owner := sharedReadClient(t, h, "owner")
			hold := s.HoldQueue()
			defer hold()
			e := taskEntry(t, h, s.ID, "r_original", "owner", messagequeue.Queued)
			s.SubmitNamed(e.RequestID, func() {})
			provider := &exactQueueProvider{floatingHistoryProvider: &floatingHistoryProvider{}, onRead: func() { h.pairing.Unclaim("qa-read-owner") }}
			h.SetExecutor(&exactQueueExec{fakeExec: fe, provider: provider})
			if action == "read" {
				h.Emit(backend.NativeTaskAccepted{SessionID: s.ID, RequestID: e.RequestID, ThreadID: "thread", TurnID: "original-turn"})
				snap, _ := h.messageQueue.Snapshot(s.ID)
				rev := snap.Revision
				h.reconcileQueueReceipts(owner, clientproto.Command{SessionID: s.ID, RequestID: "read", TaskRequestIDs: []string{e.RequestID}, ExpectedRevision: &rev})
				out := waitForType(t, owner, "queue_receipts_reconciled")
				if out["status"] != "forbidden" || len(out["receipts"].([]any)) != 0 {
					t.Fatal("revoked reader got native facts", out)
				}
			} else {
				h.cancelQueuedMessage(owner, clientproto.Command{SessionID: s.ID, RequestID: e.RequestID})
				if waitForType(t, owner, "queue_action_result")["status"] != "rejected" {
					t.Fatal("revoked owner cancelled")
				}
				after, _, _ := h.messageQueue.Get(s.ID, e.RequestID)
				if after.State != messagequeue.Queued {
					t.Fatal("revoked cancel modified receipt")
				}
			}
		})
	}
}

func TestQueueReconciliationAuthenticatedWSDecoderAndCapability(t *testing.T) {
	h, _ := newTestHub(t)
	if err := h.pairing.Claim("qa-queue-wire", "queue-device"); err != nil {
		t.Fatal(err)
	}
	s := h.registry.Create("s1", "Task", t.TempDir(), backend.Codex, "", "", "thread")
	e := taskEntry(t, h, s.ID, "r_wire-original", "queue-device", messagequeue.Uncertain)
	h.Emit(backend.NativeTaskAccepted{SessionID: s.ID, RequestID: e.RequestID, ThreadID: "thread", TurnID: "wire-native"})
	conn, ctx, cleanup := dialWS(t, h)
	defer cleanup()
	write := func(raw string) {
		if err := conn.Write(ctx, websocket.MessageText, []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	read := func(kind string) map[string]any {
		for {
			event := readEvent(t, ctx, conn)
			if event["type"] == kind {
				return event
			}
		}
	}
	write(`{"type":"hello","device_id":"queue-device","auth_token":"qa-queue-wire","protocol_version":3,"session_read_sync":true}`)
	hello := read("hello_ack")
	caps := hello["capabilities"].([]any)
	found := false
	for _, v := range caps {
		if v == "queue_receipt_reconciliation_v1" {
			found = true
		}
	}
	if !found {
		t.Fatal("capability not registered")
	}
	write(`{"type":"read_message_queue","session_id":"s1"}`)
	snap := read("message_queue_snapshot")
	item := snap["items"].([]any)[0].(map[string]any)
	if item["payload_hash"] != e.PayloadHash || snap["session_id"] != "sk1:i1:s1" {
		t.Fatal(snap)
	}
	rev := uint64(snap["revision"].(float64))
	raw, _ := json.Marshal(map[string]any{"type": "reconcile_queue_receipts", "session_id": "s1", "request_id": "wire-read", "expected_revision": rev, "task_request_ids": []string{e.RequestID}})
	write(string(raw))
	out := read("queue_receipts_reconciled")
	fact := out["receipts"].([]any)[0].(map[string]any)
	if out["status"] != "supported" || out["request_id"] != "wire-read" || fact["delivery"] != "native_consumed" || out["session_id"] != "sk1:i1:s1" {
		t.Fatal(out)
	}
	after, _, _ := h.messageQueue.Get(s.ID, e.RequestID)
	if after.State != messagequeue.Uncertain || after.PayloadHash != e.PayloadHash {
		t.Fatal("wire read caused state effect")
	}
}
