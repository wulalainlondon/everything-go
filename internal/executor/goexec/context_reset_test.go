package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"github.com/coder/websocket"
	"reflect"
	"testing"
)

func TestUnifiedCodexResetUsesAcknowledgedDetachAndKeepsHistory(t *testing.T) {
	for _, outcome := range []string{"idle", "active", "timeout", "epoch"} {
		t.Run(outcome, func(t *testing.T) {
			c, s, st, w, _ := livenessFixture(t)
			st.turnActive = false
			st.currentTurnID = ""
			c.appServerMode = "daemon"
			c.remoteConn = &websocket.Conn{}
			release, e := s.ReserveContextGeneration("r_reset", "root", s.Snapshot().SourceRevision)
			if e != nil {
				t.Fatal(e)
			}
			defer release()
			w.reply = func(method string, _ json.RawMessage) (any, error) {
				switch method {
				case "thread/read":
					status := outcome
					if outcome == "timeout" || outcome == "epoch" {
						status = "idle"
					}
					if outcome == "epoch" {
						c.rpc.transportEpoch.Add(1)
					}
					return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": status}}}, nil
				case "thread/unsubscribe":
					if outcome == "timeout" {
						return nil, errors.New("timeout")
					}
					return map[string]any{}, nil
				default:
					t.Fatal("unexpected archive/create/clear effect", method)
					return nil, nil
				}
			}
			phase, e := c.ResetContext(backend.WithContextResetGuard(context.Background(), func() bool { return true }), s, "root")
			if outcome == "idle" {
				if e != nil || phase != "awaiting_initialization" || s.ResumeID() != "" || !reflect.DeepEqual(s.ResumeIDs(), []string{"root"}) {
					t.Fatal(phase, e, s.ResumeIDs())
				}
			} else {
				if e == nil || s.ResumeID() != "root" {
					t.Fatal("unknown/busy detach claimed success", phase, e, s.ResumeID())
				}
			}
		})
	}
}
func TestUnifiedCodexTypedStartExcludesTerminalResponse(t *testing.T) {
	for _, status := range []string{"inProgress", "completed", "failed", "interrupted"} {
		t.Run(status, func(t *testing.T) {
			c, s, _, w, sink := livenessFixture(t)
			w.reply = func(method string, _ json.RawMessage) (any, error) {
				if method != "turn/start" {
					t.Fatal(method)
				}
				return map[string]any{"turn": map[string]any{"id": "native-exact", "status": status}}, nil
			}
			_ = c.startTurn("root", nil, s.Snapshot(), "", "request-1")
			count := sink.count(func(v any) bool {
				p, ok := v.(backend.NativeTaskStarted)
				return ok && p.Token == "native-exact" && p.ProofKind == "live_turn_start_in_progress"
			})
			if status == "inProgress" && count != 1 || status != "inProgress" && count != 0 {
				t.Fatal(status, count)
			}
		})
	}
}

func TestUnifiedCodexBufferedTerminalCannotBecomeLiveStart(t *testing.T) {
	c, s, st, w, sink := livenessFixture(t)
	w.reply = func(method string, _ json.RawMessage) (any, error) {
		st.completeOwnedTurn(codexTurnTerminal{ID: "native-exact", Status: "completed"})
		return map[string]any{"turn": map[string]any{"id": "native-exact", "status": "inProgress"}}, nil
	}
	_ = c.startTurn("root", nil, s.Snapshot(), "", "request-1")
	if sink.count(func(v any) bool { _, ok := v.(backend.NativeTaskStarted); return ok }) != 0 {
		t.Fatal("buffered terminal invented start")
	}
}

func TestUnifiedCodexResetRevalidatesOriginalAuthorityAfterIdleRPC(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	st.turnActive = false
	st.currentTurnID = ""
	c.appServerMode = "daemon"
	c.remoteConn = &websocket.Conn{}
	release, e := s.ReserveContextGeneration("r_reset", "root", s.Snapshot().SourceRevision)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	allowed := true
	unsubscribes := 0
	w.reply = func(method string, _ json.RawMessage) (any, error) {
		if method == "thread/read" {
			allowed = false
			return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}, nil
		}
		if method == "thread/unsubscribe" {
			unsubscribes++
		}
		return map[string]any{}, nil
	}
	_, e = c.ResetContext(backend.WithContextResetGuard(context.Background(), func() bool { return allowed }), s, "root")
	if e == nil || unsubscribes != 0 || s.ResumeID() != "root" {
		t.Fatal("idle wait retained stale authority", e, unsubscribes, s.ResumeID())
	}
}
