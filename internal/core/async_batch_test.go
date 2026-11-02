package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type batchExecutorFixture struct {
	*fakeExec
	questions    []backend.UserInputPayload
	reservations atomic.Int32
	busy         atomic.Bool
}

func (f *batchExecutorFixture) PendingInteractions(sid string) []backend.UserInputPayload {
	out := []backend.UserInputPayload{}
	for _, q := range f.questions {
		if sid == "" || q.SessionID == sid {
			out = append(out, q)
		}
	}
	return out
}
func (*batchExecutorFixture) RespondUserInput(string, map[string]any, bool) bool { return false }
func (f *batchExecutorFixture) QueuedSubmissionReady(_ context.Context, s *session.Session, _ string) error {
	if f.busy.Load() {
		return backend.ErrNativeSubmissionBusy
	}
	return nil
}
func (f *batchExecutorFixture) PrepareAsyncBatch(s *session.Session, r protocol.AsyncBatchRequest) (string, error) {
	for _, selected := range r.Selected {
		found := false
		for _, q := range f.questions {
			if q.SessionID == s.ID && q.RequestID == selected.RequestID && q.QuestionRevision == selected.QuestionRevision {
				found = true
			}
		}
		if !found {
			return "", fmt.Errorf("question_revision_changed")
		}
	}
	return "<send_user_message_question_reply>\n[{\"questionItemId\":\"fixture\",\"question\":\"fixture\",\"answer\":\"shared\"}]\n</send_user_message_question_reply>", nil
}
func (f *batchExecutorFixture) ReserveAsyncBatch(s *session.Session, r protocol.AsyncBatchRequest) (string, func(), error) {
	text, err := f.PrepareAsyncBatch(s, r)
	if err != nil {
		return "", nil, err
	}
	f.reservations.Add(1)
	return text, func() { f.reservations.Add(-1) }, nil
}

func batchHubFixture(t *testing.T) (*Hub, *Client, *session.Session, *batchExecutorFixture, protocol.AsyncBatchRequest) {
	h, fe := newTestHub(t)
	c := sharedReadClient(t, h, "batch-owner")
	s := h.registry.Create("s1", "native", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "native-thread")
	s.SetConfigRevision(1)
	f := &batchExecutorFixture{fakeExec: fe}
	r := protocol.AsyncBatchRequest{Authority: h.cfg.InstanceID, SessionID: s.ID, RequestID: "ui_async_batch_11111111-1111-4111-8111-111111111111", ThreadID: s.ResumeID(), ConfigRevision: s.SettingsSnapshot().ConfigRevision}
	for _, id := range []string{"ui_async_first", "ui_async_second"} {
		nonblocking := false
		p := protocol.UserInputRequestPayload{SessionID: s.ID, RequestID: id, Kind: "request_user_input_async", NativeThreadID: s.ResumeID(), ConfigRevision: r.ConfigRevision, IsBlocking: &nonblocking, Questions: []protocol.UserInputQuestion{{QuestionID: id + "item", Text: "Question " + id}}}
		p.QuestionRevision = backend.AsyncQuestionRevision(p, p.NativeThreadID, p.ConfigRevision)
		f.questions = append(f.questions, p)
		r.Selected = append(r.Selected, protocol.AsyncBatchSelection{RequestID: id, QuestionRevision: p.QuestionRevision, Answers: map[string]any{id + "item": "shared"}})
	}
	h.SetExecutor(f)
	return h, c, s, f, r
}
func routeBatch(h *Hub, c *Client, r protocol.AsyncBatchRequest) {
	raw, _ := json.Marshal(r)
	var wire map[string]any
	json.Unmarshal(raw, &wire)
	wire["type"] = "user_input_batch_response"
	raw, _ = json.Marshal(wire)
	route(h, c, string(raw))
}

func TestQuestionBatchAuthenticatedTypedOnceAndStatusNeverResends(t *testing.T) {
	h, c, s, f, r := batchHubFixture(t)
	sent := make(chan string, 3)
	f.onSendContext = func(ctx context.Context, s *session.Session, id, text string) {
		if backend.AsyncBatchFrom(ctx) == nil || backend.CheckTaskInputGuard(ctx) != nil || f.reservations.Load() != 0 {
			t.Error("lost immutable binding or descriptor lock not released")
		}
		sent <- id
		h.Emit(protocol.NewDone(s.ID, id))
	}
	routeBatch(h, c, r)
	ack := waitForType(t, c, "user_input_batch_receipt")
	exportQuestionWire(t, "ACTUAL_BATCH_ADMISSION.json", ack)
	if ack["phase"] != "admitted" || ack["known_no_effect"] != false || len(ack["covered"].([]any)) != 2 {
		t.Fatal(ack)
	}
	select {
	case id := <-sent:
		if id != r.RequestID {
			t.Fatal(id)
		}
	case <-time.After(time.Second):
		t.Fatal("no original native handoff")
	}
	routeBatch(h, c, r)
	again := waitForType(t, c, "user_input_batch_receipt")
	if again["phase"] != "admitted" {
		t.Fatal(again)
	}
	route(h, c, `{"type":"user_input_batch_status","session_id":"s1","request_id":"`+r.RequestID+`","authority_instance_id":"i1"}`)
	status := waitForType(t, c, "user_input_batch_receipt")
	exportQuestionWire(t, "ACTUAL_BATCH_STATUS.json", status)
	if status["request_id"] != r.RequestID || status["phase"] != "admitted" {
		t.Fatal(status)
	}
	select {
	case <-sent:
		t.Fatal("duplicate/status replayed native effect")
	default:
	}
	other := sharedReadClient(t, h, "other-reader")
	h.sendPendingInteractions(other, clientproto.Command{SessionID: s.ID})
	snap := waitForType(t, other, "pending_interactions_list")
	exportQuestionWire(t, "ACTUAL_BATCH_PENDING_SNAPSHOT.json", snap)
	if len(snap["admitted"].([]any)) != 2 {
		t.Fatal("valid same-scope reader cannot retire admitted", snap)
	}
	for _, row := range snap["admitted"].([]any) {
		if row.(map[string]any)["session_id"] != s.ID {
			t.Fatal("missing exact admission session", row)
		}
	}
	if snap["snapshot_revision"].(float64) <= ack["revision"].(float64) || snap["snapshot_generation"] != "i1" {
		t.Fatal("snapshot clock not ordered", snap, ack)
	}
	route(h, other, `{"type":"user_input_batch_status","session_id":"s1","request_id":"`+r.RequestID+`","authority_instance_id":"i1"}`)
	if out := waitForType(t, other, "user_input_batch_receipt"); out["phase"] != "unknown" || out["known_no_effect"] != false {
		t.Fatal("other owner obtained status", out)
	}
	text, _ := f.PrepareAsyncBatch(s, r)
	h.Emit(backend.NativeAsyncBatchConsumed{SessionID: s.ID, ThreadID: s.ResumeID(), ContentHash: backend.AsyncContentHash(text)})
	route(h, c, `{"type":"user_input_batch_status","session_id":"s1","request_id":"`+r.RequestID+`","authority_instance_id":"i1"}`)
	if out := waitForType(t, c, "user_input_batch_receipt"); out["phase"] != "consumed" {
		t.Fatal(out)
	}
}

func TestQuestionBatchRejectionsKeepAllProvidedAndUnknownCannotReplay(t *testing.T) {
	h, c, _, f, r := batchHubFixture(t)
	var sends atomic.Int32
	f.onSend = func(*session.Session, string, string) { sends.Add(1) }
	r.Selected[1].QuestionRevision = "changed"
	routeBatch(h, c, r)
	out := waitForType(t, c, "user_input_batch_receipt")
	exportQuestionWire(t, "ACTUAL_BATCH_REJECTION.json", out)
	if out["phase"] != "rejected" || out["known_no_effect"] != true || len(out["covered"].([]any)) != 2 {
		t.Fatal("refusal lost draft locators", out)
	}
	if _, found, _ := h.messageQueue.Get("s1", r.RequestID); found {
		t.Fatal("partial native queue on invalid selection")
	}
	routeBatch(h, c, r)
	if out = waitForType(t, c, "user_input_batch_receipt"); out["phase"] != "rejected" {
		t.Fatal(out)
	}
	r.Selected[1].QuestionRevision = f.questions[1].QuestionRevision
	routeBatch(h, c, r)
	if out = waitForType(t, c, "user_input_batch_receipt"); out["phase"] != "unknown" || out["known_no_effect"] != false {
		t.Fatal("collision advertised safe retry", out)
	}
	route(h, c, `{"type":"user_input_batch_status","session_id":"s1","request_id":"ui_async_batch_missing_original","authority_instance_id":"i1"}`)
	if out = waitForType(t, c, "user_input_batch_receipt"); out["phase"] != "unknown" || out["known_no_effect"] != false {
		t.Fatal("missing receipt advertised no effect", out)
	}
	route(h, c, `{"type":"message","session_id":"s1","request_id":"ui_async_batch_forged_ordinary","content":"forged envelope"}`)
	if out = waitForType(t, c, "error"); out["code"] != "batch_binding_required" {
		t.Fatal(out)
	}
	if sends.Load() != 0 {
		t.Fatal("rejection dispatched native effect")
	}
}

func TestQuestionBatchAdmissionProjectionScopeAndSourceFence(t *testing.T) {
	for _, kind := range []string{"hidden", "closed", "desktop", "delegated", "outside-root", "unpaired"} {
		t.Run(kind, func(t *testing.T) {
			h, c, s, f, r := batchHubFixture(t)
			v := messagequeue.QuestionBatch{Request: r, Hash: "fixture", Phase: "reserved"}
			h.messageQueue.ClaimQuestionBatch(v)
			entry := messagequeue.Entry{SessionID: s.ID, RequestID: r.RequestID, Payload: []byte(`{}`), QuestionBatch: &messagequeue.QuestionBatchAdmission{Hash: v.Hash, ContentHash: "fixture", Covered: batchCovered(r)}}
			if _, _, err := h.messageQueue.Enqueue(entry); err != nil {
				t.Fatal(err)
			}
			if n := len(h.pendingQuestionAdmissions(c, "")); n != 2 {
				t.Fatal("missing control admissions", n)
			}
			switch kind {
			case "hidden":
				hidden := true
				s.SetMeta(nil, &hidden)
			case "closed":
				s.Close()
			case "desktop":
				h.controls.MarkDesktopOwner(s.ID, s.ResumeID())
			case "delegated":
				q := f.questions[0]
				q.SessionID = "s_dg_fixture"
				h.registry.Create(q.SessionID, "delegated", s.Cwd(), backend.Codex, "gpt-6.1-sol", "read-only", s.ResumeID())
				f.questions = []backend.UserInputPayload{q}
			case "outside-root":
				h.cfg.RootDir = t.TempDir()
			case "unpaired":
				h.pairing.Unclaim(c.readIdentity.Load().token)
			}
			if got := h.pendingQuestionAdmissions(c, ""); len(got) != 0 {
				t.Fatal("admission sidechannel leaked excluded source", got)
			}
		})
	}
	h, c, s, _, r := batchHubFixture(t)
	b := &backend.AsyncBatchBinding{Request: r, OwnerDevice: h.pairedTaskDevice(c), OwnerGeneration: jarvisTokenGeneration(c.readIdentity.Load().token), Cwd: s.Cwd(), SourceRevision: s.Snapshot().SourceRevision, Configuration: session.ConfigurationFrom(s.Snapshot())}
	if err := h.asyncBatchSendGuard(s, b); err != nil {
		t.Fatal("valid source rejected", err)
	}
	s.SetModel("changed")
	s.SetModel("gpt-6.1-sol")
	if err := h.asyncBatchSendGuard(s, b); err == nil {
		t.Fatal("ABA source replay admitted")
	}
	if !strings.HasPrefix(r.RequestID, "ui_async_batch_") {
		t.Fatal("fixture invalid")
	}
}

func TestQuestionBatchActiveConfigOneSteerDespiteFutureDefaults(t *testing.T) {
	h, c, s, f, r := batchHubFixture(t)
	defer s.CloseAndWait()
	cfg := session.ConfigurationFrom(s.Snapshot())
	cfg.Model = "" // A future runtime-default selection must not veto the named active model.
	if _, err := s.SetFutureConfiguration(cfg, nil); err != nil {
		t.Fatal(err)
	}
	if s.Snapshot().ConfigRevision != 1 || s.SettingsSnapshot().ConfigRevision != 2 {
		t.Fatal("fixture does not have active1/default2")
	}
	f.busy.Store(true)
	h.Emit(backend.ObservedTurn{SessionID: s.ID, RequestID: "native-external", Phase: "running"})
	var sends, steers atomic.Int32
	f.onSend = func(*session.Session, string, string) { sends.Add(1) }
	f.onSteer = func(_ *session.Session, id, text string) (backend.SteerResult, error) {
		steers.Add(1)
		if id != r.RequestID || text == "" {
			t.Error("batch was split or replaced")
		}
		entry, found, err := h.messageQueue.Get(s.ID, r.RequestID)
		var p queuedPayload
		if err != nil || !found || json.Unmarshal(entry.Payload, &p) != nil || p.Configuration == nil || p.Configuration.Revision != 1 || p.AsyncBatch.Configuration.Revision != 1 {
			t.Error("queue used future defaults", p, err)
		}
		return backend.SteerResult{StrictACK: true, TurnID: "native-turn", RequestID: "native-external"}, nil
	}
	routeBatch(h, c, r)
	if out := waitForType(t, c, "user_input_batch_receipt"); out["phase"] != "admitted" {
		t.Fatal(out)
	}
	if out := waitForType(t, c, "queue_action_result"); out["status"] != "accepted" {
		t.Fatal(out)
	}
	expectState(t, h, r.RequestID, messagequeue.Steered)
	if sends.Load() != 0 || steers.Load() != 1 || s.SettingsSnapshot().ConfigRevision != 2 {
		t.Fatal("active reply changed defaults or started a new turn", sends.Load(), steers.Load())
	}
}
