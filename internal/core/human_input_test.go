package core

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"strings"
	"testing"
	"time"
)

func humanCoreFixture(t *testing.T) (*Hub, *Client, *session.Session, messagequeue.HumanBinding) {
	t.Helper()
	h, _ := newTestHub(t)
	s := h.registry.Create("s1", "Task", t.TempDir(), backend.Codex, "", "", "thread")
	c := sharedReadClient(t, h, "owner")
	actor, e := h.mediaActor(c)
	if e != nil {
		t.Fatal(e)
	}
	b := messagequeue.HumanBinding{Device: "owner", Authority: h.cfg.InstanceID, Session: s.ID, Thread: s.ResumeID(), Config: s.SettingsSnapshot().ConfigRevision, Epoch: h.SessionRoleEpoch(s), Generation: actor.NamespaceGeneration}
	return h, c, s, b
}
func humanCoreCommand(b messagequeue.HumanBinding, op messagequeue.HumanOperation) clientproto.Command {
	raw, _ := json.Marshal(humanInputCommand{Binding: b, Operation: op})
	return clientproto.Command{Kind: "queue_input_request", SessionID: b.Session, RequestID: op.Request, QueueInput: raw, Content: op.Content, MessagePurpose: "instruction"}
}
func TestHumanReservedUploadBlocksLegacyMessageUntilExactSeal(t *testing.T) {
	h, c, s, b := humanCoreFixture(t)
	defer s.Close()
	h.cfg.InstanceID = "authority"
	b.Authority = h.cfg.InstanceID
	r := messagequeue.HumanOperation{ID: "reserve-A", Kind: "reserve", Request: "r_A", Content: "A", IntentHash: strings.Repeat("a", 64)}
	h.handleHumanInput(c, humanCoreCommand(b, r))
	out := waitForType(t, c, "queue_input_result")
	if out["result"].(map[string]any)["status"] != "committed" {
		t.Fatal(out)
	}
	h.enqueueChatMessage(c, clientproto.Command{Kind: "message", SessionID: s.ID, RequestID: "r_B", Content: "B"})
	time.Sleep(20 * time.Millisecond)
	if e, _, _ := h.messageQueue.Get(s.ID, "r_B"); e.State != messagequeue.Queued {
		t.Fatal("legacy B overtook upload", e)
	}
	seal := messagequeue.HumanOperation{ID: "seal-A", Kind: "seal", Request: r.Request, ExpectedRevision: 1, ContentRevision: 1, IntentHash: r.IntentHash, Content: r.Content, ReadyProof: r.IntentHash}
	h.handleHumanInput(c, humanCoreCommand(b, seal))
	out = waitForType(t, c, "queue_input_result")
	if out["result"].(map[string]any)["status"] != "committed" {
		t.Fatal(out)
	}
}
func TestHumanPureSourceAndOperationLookupNeverAdmits(t *testing.T) {
	h, c, s, b := humanCoreFixture(t)
	defer s.Close()
	h.cfg.InstanceID = "authority"
	b.Authority = h.cfg.InstanceID
	raw, _ := json.Marshal(humanInputCommand{Read: true})
	h.handleHumanInput(c, clientproto.Command{SessionID: s.ID, RequestID: "read", QueueInput: raw})
	out := waitForType(t, c, "queue_input_result")
	if out["result"].(map[string]any)["status"] != "source" {
		t.Fatal(out)
	}
	before, _ := h.messageQueue.Snapshot(s.ID)
	raw, _ = json.Marshal(humanInputCommand{Binding: b, LookupOperation: "never-sent"})
	h.handleHumanInput(c, clientproto.Command{SessionID: s.ID, RequestID: "lookup", QueueInput: raw})
	out = waitForType(t, c, "queue_input_result")
	if out["result"].(map[string]any)["status"] != "unseen_unknown" {
		t.Fatal(out)
	}
	after, _ := h.messageQueue.Snapshot(s.ID)
	if before.Revision != after.Revision || s.QueueLen() != 0 {
		t.Fatal("read admitted")
	}
}
func TestHumanProtocolAdapterRetainsExactTypedOperation(t *testing.T) {
	in, e := protocol.ParseInbound([]byte(`{"type":"queue_input_request","session_id":"s","request_id":"r","queue_input":{"read":true}}`))
	if e != nil {
		t.Fatal(e)
	}
	cmd := clientproto.NewAppV1().ParseCommand(in)
	if string(cmd.QueueInput) != `{"read":true}` {
		t.Fatal(string(cmd.QueueInput))
	}
}
