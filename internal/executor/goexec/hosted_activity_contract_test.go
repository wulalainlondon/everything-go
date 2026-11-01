package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/agentactivity"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"fmt"
	"testing"
)

func TestHostedPublicReturnUsesOneBoundedOriginalItemRead(t *testing.T) {
	c := NewCodex(&capSink{}, "forbidden-model-launch")
	c.appServerMode = "daemon"
	c.RegisterExistingDaemonObservation()
	s := session.NewRegistry().Create("s1", "codex", t.TempDir(), backend.Codex, "", "", "root")
	w := &rpcCaptureWriter{writes: make(chan []byte, 4)}
	c.rpc.setWriter(w)
	rpc := hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: "authority"}}
	scope := rpc.Scope()
	source := agentactivity.NewNativeSource(scope)
	if e := source.Observe(scope, []byte(`{"threadId":"root","turnId":"parent","completedAtMs":1,"item":{"type":"subAgentActivity","id":"event","agentThreadId":"child","agentPath":"/root/worker","kind":"completed"}}`)); e != nil {
		t.Fatal(e)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 4; i++ {
			var f struct {
				ID     int
				Method string
				Params map[string]any
			}
			if e := json.Unmarshal(<-w.writes, &f); e != nil {
				t.Error(e)
				return
			}
			result := ""
			switch i {
			case 0, 1:
				if f.Method != "thread/read" || f.Params["includeTurns"] != false {
					t.Error("nonmetadata read")
				}
				id, _ := f.Params["threadId"].(string)
				expected := "root"
				parent := "null"
				if i == 1 {
					expected = "child"
					parent = `"root"`
				}
				if id != expected {
					t.Error("foreign metadata selection")
				}
				result = fmt.Sprintf(`{"thread":{"id":%q,"parentThreadId":%s,"sessionId":"tree","turns":[]}}`, id, parent)
			case 2:
				if f.Method != "thread/turns/list" || f.Params["itemsView"] != "notLoaded" {
					t.Error("turn item history requested")
				}
				result = `{"data":[{"id":"real-turn","status":"completed","items":[],"itemsView":"notLoaded"}],"nextCursor":null}`
			case 3:
				if f.Method != "thread/items/list" || f.Params["threadId"] != "child" || f.Params["turnId"] != "real-turn" || f.Params["limit"] != float64(1) || f.Params["sortDirection"] != "desc" || len(f.Params) != 4 {
					t.Error("unbounded or foreign report read")
				}
				result = `{"data":[{"turnId":"real-turn","item":{"type":"agentMessage","id":"public-final","phase":"final_answer","text":"UNTRUSTED_PUBLIC_TEXT"}}],"nextCursor":null}`
			}
			c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":%s}`, f.ID, result)))
		}
	}()
	ref, e := source.PublicReturn(context.Background(), rpc, "child", "real-turn")
	<-done
	if e != nil || ref.ItemID != "public-final" || c.proc != nil || c.state(s.ID).turnActive {
		t.Fatal("original public reference source unavailable", e)
	}
}
