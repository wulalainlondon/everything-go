package core

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// Exports only synthetic fixture wire when this owner explicitly requests it.
func exportQuestionWire(t *testing.T, name string, event any) {
	t.Helper()
	if dir := os.Getenv("BRIDGE_QUESTION_WIRE_FIXTURE_DIR"); dir != "" {
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
func TestQAActualQuestionQueueWire(t *testing.T) {
	h, _ := newTestHub(t)
	for i, state := range []messagequeue.State{messagequeue.Failed, messagequeue.Uncertain, messagequeue.Failed, messagequeue.Uncertain} {
		id := []string{"ui_async_short1", "ui_async_short2", "ui_async_short3", "ui_async_short4"}[i]
		raw := `<send_user_message_question_reply>[{"question":"Original question ` + id + `","answer":"kept"}]</send_user_message_question_reply>`
		p, _ := json.Marshal(queuedPayload{Content: raw, SteerOnly: true, MessagePurpose: "question_reply"})
		if _, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: "s1", RequestID: id, Content: raw, Payload: p}); err != nil {
			t.Fatal(err)
		}
		if _, changed, err := h.messageQueue.Transition("s1", id, []messagequeue.State{messagequeue.Queued}, state, "original receipt unknown or failed", "", ""); err != nil || !changed {
			t.Fatal(err)
		}
	}
	event, err := h.messageQueueSnapshot("s1")
	if err != nil || len(event.Items) != 4 {
		t.Fatal(err, event)
	}
	for _, item := range event.Items {
		if !item.CorrelatedQuestionReply || !strings.HasSuffix(item.Content, "</send_user_message_question_reply>") {
			t.Fatal("original question wire identity lost")
		}
	}
	exportQuestionWire(t, "ACTUAL_SHORT_QUESTION_SNAPSHOT.json", event)
}
func TestQALongQuestionPreviewActualAdmission(t *testing.T) {
	for _, kind := range []string{"original", "utf8", "multiple", "ordinary"} {
		t.Run(kind, func(t *testing.T) {
			h, f := newTestHub(t)
			c := sharedReadClient(t, h, "question-owner")
			c.interactionControl = true
			var starts atomic.Int32
			f.onSend = func(*session.Session, string, string) { starts.Add(1) }
			s := h.registry.Create("long-question", "One", t.TempDir(), backend.Codex, "", "", "original-thread")
			s.Close()
			raw := `<send_user_message_question_reply>[{"question":"Long original question","answer":"` + strings.Repeat("x", 4500) + `"}]</send_user_message_question_reply>`
			if kind == "utf8" {
				raw = `<send_user_message_question_reply>[{"question":"原問題","answer":"` + strings.Repeat("字", 4500) + `"}]</send_user_message_question_reply>`
			}
			if kind == "multiple" {
				rows := []map[string]string{}
				for i := 0; i < 40; i++ {
					rows = append(rows, map[string]string{"question": "Original question", "answer": strings.Repeat("y", 150), "questionItemId": strings.Repeat("routing", 100)})
				}
				b, _ := json.Marshal(rows)
				raw = "<send_user_message_question_reply>" + string(b) + "</send_user_message_question_reply>"
			}
			id := "ui_async_long_original"
			if kind == "ordinary" {
				id = "r_malformed_ordinary"
				raw = "Example <send_user_message_question_reply>" + strings.Repeat("z", 4500)
			}
			h.enqueueChatMessageExpectedInheritedMode(c, clientproto.Command{Kind: "message", SessionID: s.ID, RequestID: id, MessagePurpose: "question_reply", Content: raw}, nil, nil, true)
			before, found, err := h.messageQueue.Get(s.ID, id)
			if err != nil || !found || before.State != messagequeue.Failed {
				t.Fatal("original closed-session receipt not reached", err)
			}
			event, err := h.messageQueueSnapshot(s.ID)
			if err != nil || len(event.Items) != 1 {
				t.Fatal(err, event)
			}
			if event.Items[0].CorrelatedQuestionReply != (kind != "ordinary") {
				t.Fatal("wrong source classification", event)
			}
			after, _, _ := h.messageQueue.Get(s.ID, id)
			if starts.Load() != 0 || before.PayloadHash != after.PayloadHash || before.Content != after.Content || len(after.Payload) != len(before.Payload) {
				t.Fatal("display projection changed original request or sent input")
			}
			if kind == "original" {
				exportQuestionWire(t, "ACTUAL_LONG_QUESTION_SNAPSHOT.json", event)
			}
		})
	}
}
