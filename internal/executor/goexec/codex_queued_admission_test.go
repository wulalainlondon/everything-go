package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"everything-go/internal/backend"
	"github.com/coder/websocket"
)

func TestQueuedAdmissionReadsMetadataOnlyAndNeverResumesOrStarts(t *testing.T) {
	for _, status := range []string{"active", "idle", "notLoaded", "systemError", "wrong-thread", "invalid", "epoch", "transport", "local-active", "compact"} {
		t.Run(status, func(t *testing.T) {
			c, s, st, w, sink := livenessFixture(t)
			st.turnActive = false
			st.currentTurnID = ""
			c.appServerMode = "daemon"
			c.remoteConn = &websocket.Conn{} // Readiness uses only the fake RPC writer.
			if status == "local-active" {
				st.turnActive = true
			}
			if status == "compact" {
				st.compactActive = true
			}
			w.reply = func(method string, p json.RawMessage) (any, error) {
				if method != "thread/read" || string(p) != `{"includeTurns":false,"threadId":"root"}` {
					t.Fatal("unexpected mutation/history", method, string(p))
				}
				if status == "transport" {
					return nil, errors.New("unavailable")
				}
				if status == "invalid" {
					return map[string]any{}, nil
				}
				if status == "epoch" {
					c.rpc.transportEpoch.Add(1)
				}
				id := "root"
				if status == "wrong-thread" {
					id = "foreign"
				}
				return map[string]any{"thread": map[string]any{"id": id, "status": map[string]string{"type": status}}}, nil
			}
			err := c.QueuedSubmissionReady(context.Background(), s, "new")
			busy := status == "active" || status == "local-active" || status == "compact"
			if busy && !errors.Is(err, backend.ErrNativeSubmissionBusy) {
				t.Fatal("busy not retained", err)
			}
			if (status == "idle" || status == "notLoaded" || status == "systemError") && err != nil {
				t.Fatal(err)
			}
			if (status == "transport" || status == "invalid" || status == "wrong-thread" || status == "epoch") && err == nil {
				t.Fatal("unverified read admitted")
			}
			if sink.count(func(any) bool { return true }) != 0 || st.reqID != "request-1" {
				t.Fatal("read reassigned original work")
			}
		})
	}
}
