package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/agentactivity"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"fmt"
	"testing"
	"time"
)

type hostedProofCapture struct {
	scope agentactivity.NativeScope
	proof agentactivity.NativeReadProof
}

func (p *hostedProofCapture) Scope() agentactivity.NativeScope { return p.scope }
func (p *hostedProofCapture) ReadThread(_ context.Context, _ agentactivity.NativeReadProof, id string) (json.RawMessage, error) {
	parent := "null"
	if id == "child" {
		parent = `"root"`
	}
	return []byte(fmt.Sprintf(`{"thread":{"id":%q,"parentThreadId":%s,"sessionId":"tree","turns":[]}}`, id, parent)), nil
}
func (p *hostedProofCapture) ListTurns(context.Context, agentactivity.NativeReadProof, string) (json.RawMessage, error) {
	return []byte(`{"data":[{"id":"real-turn","status":"completed","items":[],"itemsView":"notLoaded"}],"nextCursor":null}`), nil
}
func (p *hostedProofCapture) LatestTurnItem(_ context.Context, proof agentactivity.NativeReadProof, _, _ string) (json.RawMessage, error) {
	p.proof = proof
	return []byte(`{"data":[{"turnId":"real-turn","item":{"type":"agentMessage","id":"final","phase":"final_answer"}}],"nextCursor":null}`), nil
}

func hostedFenceFixture(t *testing.T) (*Codex, *session.Session, hostedActivityRPC, *agentactivity.NativeSource, agentactivity.NativeReadProof, *rpcCaptureWriter) {
	t.Helper()
	c := NewCodex(&capSink{}, "forbidden-model-launch")
	c.appServerMode = "daemon"
	c.RegisterExistingDaemonObservation()
	s := session.NewRegistry().Create("s1", "fixture", t.TempDir(), backend.Codex, "", "", "root")
	w := &rpcCaptureWriter{writes: make(chan []byte, 1)}
	c.rpc.setWriter(w)
	r := hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: "authority"}}
	scope := r.Scope()
	source := agentactivity.NewNativeSource(scope)
	if e := source.Observe(scope, []byte(`{"threadId":"root","turnId":"parent","completedAtMs":1,"item":{"type":"subAgentActivity","id":"event","kind":"started","agentThreadId":"child","agentPath":"/root/worker"}}`)); e != nil {
		t.Fatal(e)
	}
	capture := &hostedProofCapture{scope: scope}
	if _, e := source.PublicReturn(context.Background(), capture, "child", "real-turn"); e != nil {
		t.Fatal(e)
	}
	return c, s, r, source, capture.proof, w
}

func TestHostedProofFencedAtWriterAfterQueueWait(t *testing.T) {
	for _, change := range []string{"epoch", "boot", "root", "config", "closed", "hidden", "backend", "mode", "registry"} {
		t.Run(change, func(t *testing.T) {
			c, s, r, source, proof, w := hostedFenceFixture(t)
			c.rpc.wmu.Lock()
			locked := true
			defer func() {
				if locked {
					c.rpc.wmu.Unlock()
				}
			}()
			done := make(chan error, 1)
			go func() { _, e := r.LatestTurnItem(context.Background(), proof, "child", "real-turn"); done <- e }()
			deadline := time.Now().Add(time.Second)
			for {
				c.rpc.mu.Lock()
				waiting := len(c.rpc.pending) > 0
				c.rpc.mu.Unlock()
				if waiting {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("query did not queue at original writer")
				}
				time.Sleep(time.Millisecond)
			}
			switch change {
			case "epoch":
				c.rpc.transportEpoch.Add(1)
			case "boot":
				c.RegisterExistingDaemonObservation()
			case "root":
				s.SetResumeID("new-root")
			case "config":
				s.SetConfigRevision(1)
			case "closed":
				s.Close()
			case "hidden":
				hidden := true
				s.SetMeta(nil, &hidden)
			case "backend":
				s.ApplyConfig("claude", "", "")
			case "mode":
				c.startMu.Lock()
				c.appServerMode = "stdio"
				c.startMu.Unlock()
			case "registry":
				_ = source.Observe(proof.Scope, []byte(`{"threadId":"root","turnId":"parent","completedAtMs":2,"item":{"type":"subAgentActivity","id":"new-event","kind":"completed","agentThreadId":"child","agentPath":"/root/worker"}}`))
			}
			c.rpc.wmu.Unlock()
			locked = false
			select {
			case e := <-done:
				if e == nil {
					t.Fatal("retired proof accepted")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("writer proof guard deadlocked")
			}
			select {
			case <-w.writes:
				t.Fatal("retired proof sent a native read")
			default:
			}
			if !c.startMu.TryLock() {
				t.Fatal("mode guard leaked lock")
			}
			c.startMu.Unlock()
			if !c.mu.TryLock() {
				t.Fatal("owner guard leaked lock")
			}
			c.mu.Unlock()
		})
	}
}

func TestHostedProofCannotBeReconstructedOrRetargeted(t *testing.T) {
	_, _, r, _, proof, w := hostedFenceFixture(t)
	for _, q := range []struct {
		p           agentactivity.NativeReadProof
		child, turn string
	}{{agentactivity.NativeReadProof{Scope: proof.Scope, Revision: proof.Revision}, "child", "real-turn"}, {proof, "foreign-child", "real-turn"}, {proof, "child", "foreign-turn"}} {
		if _, e := r.LatestTurnItem(context.Background(), q.p, q.child, q.turn); e == nil {
			t.Fatal("forged/retargeted read admitted")
		}
	}
	select {
	case <-w.writes:
		t.Fatal("unproven target sent to original writer")
	default:
	}
}

func TestHostedReadAdmissionReleasesBeforeReply(t *testing.T) {
	c, s, r, source, proof, w := hostedFenceFixture(t)
	done := make(chan error, 1)
	go func() { _, e := r.LatestTurnItem(context.Background(), proof, "child", "real-turn"); done <- e }()
	var f struct{ ID int }
	select {
	case raw := <-w.writes:
		if e := json.Unmarshal(raw, &f); e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("valid proof did not write")
	}
	released := make(chan struct{})
	go func() {
		s.Close()
		_ = source.Observe(proof.Scope, []byte(`{"threadId":"root","turnId":"parent","completedAtMs":2,"item":{"type":"subAgentActivity","id":"after-write","kind":"completed","agentThreadId":"child","agentPath":"/root/worker"}}`))
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("read admission held session/registry over response wait")
	}
	c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"data":[],"nextCursor":null}}`, f.ID)))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("admitted read response did not settle")
	}
}
