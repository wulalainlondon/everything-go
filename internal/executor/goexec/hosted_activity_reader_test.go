package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/agentactivity"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"fmt"
	"strings"
	"testing"
)

func TestHostedOriginalPortCapturesLateParentEventAndReadsOnlyProvenChild(t *testing.T) {
	c := NewCodex(&capSink{}, "forbidden-model-launch")
	c.appServerMode = "daemon"
	c.RegisterExistingDaemonObservation()
	s := session.NewRegistry().Create("s1", "codex", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("root")
	st := c.state(s.ID)
	st.threadID = "root"
	c.threadToSession["root"] = s
	w := &rpcCaptureWriter{writes: make(chan []byte, 8)}
	c.rpc.setWriter(w)
	var methods []string
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
			methods = append(methods, f.Method)
			id, _ := f.Params["threadId"].(string)
			result := ""
			if f.Method == "thread/read" {
				if f.Params["includeTurns"] != false || len(f.Params) != 2 {
					t.Error("history requested")
				}
				parent := "null"
				if id == "child" {
					parent = `"root"`
				}
				result = fmt.Sprintf(`{"thread":{"id":%q,"parentThreadId":%s,"sessionId":"native-tree","turns":[],"preview":"PRIVATE_TEXT","model":"CONFIGURED_NOT_EFFECTIVE"}}`, id, parent)
			} else if f.Method == "thread/turns/list" {
				if id != "child" || f.Params["itemsView"] != "notLoaded" || f.Params["limit"] != float64(16) || f.Params["sortDirection"] != "desc" || len(f.Params) != 4 {
					t.Error("unbounded or foreign child read")
				}
				result = `{"data":[{"id":"real-child-turn","rootTurnId":"old-parent-turn","status":"completed","items":[],"itemsView":"notLoaded"}],"nextCursor":null}`
			} else {
				t.Error("non-read method", f.Method)
				return
			}
			c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":%s}`, f.ID, result)))
		}
	}()
	p, e := c.ObserveHostedActivity(context.Background(), s, "authority")
	if e != nil || len(p.Actors) != 0 || p.InventoryComplete {
		t.Fatal("first metadata capture read not partial", e)
	}
	// Parent is idle; its old native turn must survive the normal retirement gate.
	c.dispatch(json.RawMessage(`{"method":"item/completed","params":{"threadId":"root","turnId":"old-parent-turn","completedAtMs":1000,"item":{"type":"subAgentActivity","id":"native-event","agentThreadId":"child","agentPath":"/root/worker","kind":"completed"}}}`))
	p, e = c.ObserveHostedActivity(context.Background(), s, "authority")
	if e != nil || len(p.Actors) != 1 || len(p.Actors[0].Rounds) != 1 || p.Actors[0].Rounds[0].ID != "real-child-turn" || p.Actors[0].Activities[0].ParentTurn != "old-parent-turn" {
		t.Fatalf("original native source failed %+v %v", p, e)
	}
	<-done
	raw, _ := json.Marshal(p)
	if strings.Contains(string(raw), "PRIVATE_TEXT") || strings.Contains(string(raw), "CONFIGURED_NOT_EFFECTIVE") {
		t.Fatal("private/configured fields escaped whitelist")
	}
	if strings.Join(methods, ",") != "thread/read,thread/read,thread/read,thread/turns/list" || c.proc != nil || st.turnActive {
		t.Fatal("read changed model execution")
	}
}

func TestHostedOriginalPortUnsupportedNeverLaunchesOrWrites(t *testing.T) {
	for _, mode := range []string{"stdio", "daemon"} {
		t.Run(mode, func(t *testing.T) {
			c := NewCodex(&capSink{}, "forbidden-model-launch")
			c.appServerMode = mode
			s := session.NewRegistry().Create("s1", "codex", t.TempDir(), backend.Codex, "", "", "root")
			w := &rpcCaptureWriter{writes: make(chan []byte, 1)}
			c.rpc.setWriter(w)
			if _, e := c.ObserveHostedActivity(context.Background(), s, "authority"); e == nil {
				t.Fatal("unsupported source accepted")
			}
			select {
			case <-w.writes:
				t.Fatal("unsupported port wrote RPC")
			default:
			}
			if c.proc != nil {
				t.Fatal("source launched provider")
			}
		})
	}
}

func TestHostedSourceEpochChangesInvalidateCapture(t *testing.T) {
	c := NewCodex(&capSink{}, "forbidden-model-launch")
	c.appServerMode = "daemon"
	c.RegisterExistingDaemonObservation()
	s := session.NewRegistry().Create("s1", "codex", t.TempDir(), backend.Codex, "", "", "root")
	w := &rpcCaptureWriter{writes: make(chan []byte, 1)}
	c.rpc.setWriter(w)
	rpc := hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: "authority"}}
	scope := rpc.Scope()
	st := c.state(s.ID)
	st.hostedActivity = agentactivity.NewNativeSource(scope)
	st.hostedActivityScope = scope
	c.rpc.setWriter(w) // A new transport epoch is not the old source.
	c.observeHostedNativeItem(s, json.RawMessage(`{"threadId":"root","turnId":"turn","completedAtMs":1,"item":{"type":"subAgentActivity","id":"e","agentThreadId":"child","agentPath":"/root/worker","kind":"started"}}`))
	if _, e := st.hostedActivity.Probe(context.Background(), rpc); e == nil {
		t.Fatal("old source survived transport epoch")
	}
	select {
	case <-w.writes:
		t.Fatal("stale source wrote RPC")
	default:
	}
}

type hostedScopeFixtureRPC struct{ scope agentactivity.NativeScope }

func (r hostedScopeFixtureRPC) Scope() agentactivity.NativeScope { return r.scope }
func (r hostedScopeFixtureRPC) ReadThread(_ context.Context, _ agentactivity.NativeReadProof, id string) (json.RawMessage, error) {
	return json.RawMessage(fmt.Sprintf(`{"thread":{"id":%q,"sessionId":"tree","turns":[]}}`, id)), nil
}
func (r hostedScopeFixtureRPC) ListTurns(context.Context, agentactivity.NativeReadProof, string) (json.RawMessage, error) {
	return nil, agentactivity.ErrNativeSource
}

func TestHostedClosedSessionCannotRegisterOrCapture(t *testing.T) {
	c := NewCodex(&capSink{}, "forbidden-model-launch")
	c.appServerMode = "daemon"
	c.RegisterExistingDaemonObservation()
	s := session.NewRegistry().Create("s1", "codex", t.TempDir(), backend.Codex, "", "", "root")
	w := &rpcCaptureWriter{writes: make(chan []byte, 1)}
	c.rpc.setWriter(w)
	scope := hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: "authority"}}.Scope()
	st := c.state(s.ID)
	st.hostedActivity = agentactivity.NewNativeSource(scope)
	st.hostedActivityScope = scope
	s.Close()
	if _, e := c.ObserveHostedActivity(context.Background(), s, "authority"); e == nil {
		t.Fatal("closed session read accepted")
	}
	c.observeHostedNativeItem(s, json.RawMessage(`{"threadId":"root","turnId":"turn","completedAtMs":1,"item":{"type":"subAgentActivity","id":"late","agentThreadId":"child","agentPath":"/root/worker","kind":"started"}}`))
	p, e := st.hostedActivity.Probe(context.Background(), hostedScopeFixtureRPC{scope})
	if e != nil || len(p.Actors) != 0 || p.SourceRevision != 0 {
		t.Fatal("late activity entered retired source")
	}
	select {
	case <-w.writes:
		t.Fatal("closed session wrote RPC")
	default:
	}
}

func TestHostedClosedDuringReadDiscardsSource(t *testing.T) {
	c := NewCodex(&capSink{}, "forbidden-model-launch")
	c.appServerMode = "daemon"
	c.RegisterExistingDaemonObservation()
	s := session.NewRegistry().Create("s1", "codex", t.TempDir(), backend.Codex, "", "", "root")
	w := &rpcCaptureWriter{writes: make(chan []byte, 1)}
	c.rpc.setWriter(w)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var frame struct{ ID int }
		if e := json.Unmarshal(<-w.writes, &frame); e != nil {
			t.Error(e)
			return
		}
		s.Close()
		c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"thread":{"id":"root","sessionId":"tree","turns":[]}}}`, frame.ID)))
	}()
	p, e := c.ObserveHostedActivity(context.Background(), s, "authority")
	<-done
	if e == nil || len(p.Actors) != 0 || p.SourceRevision != 0 {
		t.Fatal("closed owner returned usable source")
	}
}
