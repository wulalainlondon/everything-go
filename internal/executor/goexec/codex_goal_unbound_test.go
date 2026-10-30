package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"fmt"
	"github.com/coder/websocket"
	"testing"
)

func TestCodexGoalFreshUnboundSessionDoesNotCreateNativeThread(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	c.appServerMode = "daemon"
	c.remoteConn = &websocket.Conn{}
	writer := &rpcCaptureWriter{writes: make(chan []byte, 4)}
	c.rpc.setWriter(writer)
	methods := make(chan string, 4)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case raw := <-writer.writes:
				var frame struct {
					ID     int    `json:"id"`
					Method string `json:"method"`
				}
				if err := json.Unmarshal(raw, &frame); err != nil {
					t.Error(err)
					return
				}
				methods <- frame.Method
				result := `{"goal":null}`
				if frame.Method == "thread/start" {
					result = `{"thread":{"id":"01a18e76-66d8-7e70-afbc-74c93671eb68"}}`
				}
				c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":%s}`, frame.ID, result)))
			}
		}
	}()
	defer func() { close(stop); <-done }()
	s := session.NewRegistry().Create("s_new_no_native", "new", t.TempDir(), backend.Codex, "", "read-only", "")
	if err := c.GetGoal(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	select {
	case method := <-methods:
		t.Fatalf("read-only fresh goal lookup created/attached native state via %s", method)
	default:
	}
	if s.ResumeID() != "" {
		t.Fatal("fresh read-only goal lookup bound native generation")
	}
	if len(sink.events) != 1 {
		t.Fatalf("expected only empty goal projection, got%+v", sink.events)
	}
	if _, ok := sink.events[0].(protocol.GoalCleared); !ok {
		t.Fatalf("want GoalCleared, got%T", sink.events[0])
	}
}
