package core

import (
	"context"
	"encoding/json"
	"errors"
	base "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	ti "everything-go/internal/taskinteraction"
	"testing"
)

func TestNativeInteractionAdvertisesOnlyOriginalHumanReply(t *testing.T) {
	q := ti.Record{ID: "original", NativeQuestion: "ui_original", NativePayload: json.RawMessage(`{"private":"never-relay"}`), QuestionRevision: 1}
	v := interactionView(q, false)
	if v["native_reply_required"] != true || v["answer_via_existing_task_tool"] != nil || v["native_question"] != nil {
		t.Fatal(v)
	}
	q.NativeQuestion = ""
	if interactionView(q, false)["answer_via_existing_task_tool"] == nil {
		t.Fatal("generic clarification route lost")
	}
}
func TestNativeInteractionRejectsGenericAnswerBeforeReservation(t *testing.T) {
	h, parent, child, request := fixtureInteractions(t)
	h.ObserveTaskQuestion(context.Background(), backend.TaskQuestionEvidence{Session: child, RequestID: request, ThreadID: child.ResumeID(), CallID: "native", TurnID: "turn", Payload: protocol.UserInputRequestPayload{RequestID: "ui_original", SessionID: child.ID, Questions: []protocol.UserInputQuestion{{Text: "Human decision"}}}})
	rows, e := h.interactions.List(context.Background(), child.ID, 100)
	if e != nil || len(rows) != 1 {
		t.Fatal(rows, e)
	}
	q := rows[0]
	h.reconcileInteractions(context.Background())
	wake := "tiwake_" + q.ID
	waitActor(t, parent, wake)
	res := interactionCall(t, h, parent, wake, "must-not-reserve", map[string]any{"action": "answer", "interaction_id": q.ID, "expected_revision": 1, "answer": "human said yes"})
	if res.OK || res.Error.Code != "native_human_question_use_original_reply" {
		t.Fatal(res)
	}
	if _, found, e := h.interactions.Command(context.Background(), "session:"+parent.ID, "must-not-reserve"); e != nil || found {
		t.Fatal("native generic command reserved", found, e)
	}
	actual, _ := h.interactions.Get(context.Background(), q.ID)
	if actual.State != "pending_human" || actual.Answer != "" {
		t.Fatal(actual)
	}
	// Exact native receipt retires the pending card without claiming model ACK.
	h.Emit(protocol.NewInteractionResolved(q.NativeQuestion, child.ID, "resolved"))
	actual, _ = h.interactions.Get(context.Background(), q.ID)
	if actual.State != "native_reply_written" || actual.AcknowledgedBy != "" {
		t.Fatal(actual)
	}
}
func TestInteractionRemoteClassificationDoesNotRewriteUnknown(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		fresh  bool
		state  string
		status int
		code   string
	}{
		{"native refusal", controllerRemoteFailure{409, "native_human_question_use_original_reply"}, true, "rejected", 409, "native_human_question_use_original_reply"},
		{"auth refusal", controllerRemoteFailure{403, "interaction_actor_invalid"}, true, "rejected", 403, "interaction_actor_invalid"},
		{"transport", errors.New("secret socket details"), true, "unknown", 0, "controller_remote_unreachable"},
		{"late refusal", controllerRemoteFailure{409, "native_human_question_use_original_reply"}, false, "unknown", 409, "native_human_question_use_original_reply"},
		{"server error", controllerRemoteFailure{503, "native_human_question_use_original_reply"}, true, "unknown", 503, "native_human_question_use_original_reply"},
		{"timeout", controllerRemoteFailure{408, "interaction_actor_invalid"}, true, "unknown", 408, "interaction_actor_invalid"},
		{"rate limit", controllerRemoteFailure{429, "interaction_actor_invalid"}, true, "unknown", 429, "interaction_actor_invalid"},
		{"existing reservation", controllerRemoteFailure{409, "answer_already_reserved"}, true, "unknown", 409, "answer_already_reserved"},
		{"unsafe remote message", controllerRemoteFailure{409, "Bearer private-token"}, true, "unknown", 409, "remote_error_redacted"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := classifyInteractionRemoteFailure(tt.err, tt.fresh)
			if f.State != tt.state || f.Status != tt.status || f.Code != tt.code {
				t.Fatal(f)
			}
		})
	}
}
func TestLegacyInteractionOriginalReceiptLookupUsesExactKey(t *testing.T) {
	h, parent, child, request := fixtureInteractions(t)
	q := resultRecord(t, interactionCall(t, h, child, request, "ask", map[string]any{"action": "ask_parent", "question": "Fixture?", "kind": "task_clarification", "blocking": false}))
	h.reconcileInteractions(context.Background())
	wake := "tiwake_" + q.ID
	waitActor(t, parent, wake)
	interactionCall(t, h, parent, wake, "original-answer", map[string]any{"action": "answer", "interaction_id": q.ID, "expected_revision": 1, "answer": "alpha"})
	for _, tt := range []struct {
		key, authority string
		found, ok      bool
	}{{"original-answer", h.cfg.InstanceID, true, true}, {"unrelated", h.cfg.InstanceID, false, true}, {"original-answer", "foreign", false, false}} {
		view := interactionSubmissionView(q.ID, tt.key, tt.authority, ti.RemoteFailure{State: "unknown"})
		recipe := view["receipt_via_existing_task_tool"].(map[string]any)
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": base.Version, "operation": "get", "correlation_id": "receipt", "input": recipe["input"]})
		caller := backend.TaskCaller{Session: parent, RequestID: wake, ThreadID: parent.ResumeID(), TurnID: "turn", CallID: "receipt", ProviderVersion: "0.162.1", Validate: func() bool { return true }}
		res := h.ExecuteTask(context.Background(), caller, raw)
		if res.OK != tt.ok {
			t.Fatal(res)
		}
		if tt.ok && res.Result.(map[string]any)["command_found"] != tt.found {
			t.Fatal(res)
		}
	}
	// These are receipt reads, never a second original/native submission.
}
