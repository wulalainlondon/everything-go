package core

import (
	"encoding/json"
	"everything-go/internal/backend"
	"fmt"
	"github.com/coder/websocket"
	"testing"
)

// Real authenticated hello and source read. No instruction/provider execution.
func TestUnifiedHumanInputHelloEligibilityAndReadOnlySource(t *testing.T) {
	for _, tc := range []struct {
		name                                       string
		paired, noQueue, noTask, noMedia, badMedia bool
	}{
		{name: "paired_healthy", paired: true}, {name: "enrollment"},
		{name: "no_queue", paired: true, noQueue: true}, {name: "no_task", paired: true, noTask: true},
		{name: "no_media", paired: true, noMedia: true}, {name: "media_error", paired: true, badMedia: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newControlTestHub(t, t.TempDir())
			if h.messageQueue == nil || h.taskService == nil || h.media == nil || h.media.err != nil {
				t.Fatal("original fixture prerequisites unavailable")
			}
			queue, task, media, mediaErr := h.messageQueue, h.taskService, h.media, h.media.err
			defer func() { h.messageQueue = queue; h.taskService = task; h.media = media; media.err = mediaErr }()
			if tc.noQueue {
				h.messageQueue = nil
			}
			if tc.noTask {
				h.taskService = nil
			}
			if tc.noMedia {
				h.media = nil
			}
			if tc.badMedia {
				h.media.err = fmt.Errorf("original fixture unavailable")
			}
			if tc.paired {
				if e := h.pairing.Claim("fixture-credential", "fixture-phone"); e != nil {
					t.Fatal(e)
				}
			}
			s := h.registry.Create("queue-hello", "Queue", t.TempDir(), backend.Codex, "", "", "thread-original")
			defer s.Close()
			conn, ctx, cleanup := dialWS(t, h)
			defer cleanup()
			if e := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","device_id":"fixture-phone","auth_token":"fixture-credential","protocol_version":3}`)); e != nil {
				t.Fatal(e)
			}
			ack := readEvent(t, ctx, conn)
			if ack["type"] != "hello_ack" {
				t.Fatal(ack)
			}
			caps := map[string]bool{}
			for _, v := range ack["capabilities"].([]any) {
				caps[v.(string)] = true
			}
			want := tc.paired && !tc.noQueue && !tc.noTask && !tc.noMedia && !tc.badMedia
			for _, cap := range []string{"human_input_outbox_v2", "message_queue_reservation_v1", "own_waiting_input_edit_v1", "own_waiting_input_move_v1", "message_projection_v2"} {
				if caps[cap] != want {
					t.Fatalf("%s=%v want%v", cap, caps[cap], want)
				}
			}
			if caps["message_queue_v1"] != (tc.paired && !tc.noQueue) {
				t.Fatal("original legacy queue capability changed")
			}
			if !want {
				return
			}
			before, e := queue.Snapshot(s.ID)
			if e != nil {
				t.Fatal(e)
			}
			raw, _ := json.Marshal(map[string]any{"type": "queue_input_request", "request_id": "hello-source-read", "session_id": s.ID, "queue_input": map[string]any{"read": true}})
			if e = conn.Write(ctx, websocket.MessageText, raw); e != nil {
				t.Fatal(e)
			}
			for {
				event := readEvent(t, ctx, conn)
				if event["type"] != "queue_input_result" || event["request_id"] != "hello-source-read" {
					continue
				}
				result := event["result"].(map[string]any)
				if result["status"] != "source" {
					t.Fatal("advertised source unavailable", event)
				}
				break
			}
			after, e := queue.Snapshot(s.ID)
			if e != nil || after.Revision != before.Revision || s.QueueLen() != 0 {
				t.Fatal("readonly source admitted work", e)
			}
		})
	}
}
