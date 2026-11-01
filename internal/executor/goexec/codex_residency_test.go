package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/coder/websocket"
	"reflect"
	"testing"
)

func TestResidentExactThreadRecoveryNoInputForkOrFalseToolReadiness(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	c.appServerMode = "daemon"
	c.remoteConn = &websocket.Conn{}
	st.threadID = ""
	st.turnActive = false
	reads := 0
	w.reply = func(method string, raw json.RawMessage) (any, error) {
		var p map[string]any
		json.Unmarshal(raw, &p)
		if p["threadId"] != "root" {
			t.Fatal("wrong thread", p)
		}
		switch method {
		case "thread/read":
			reads++
			if reads == 1 {
				return nil, errors.New("thread not loaded: root")
			}
			if p["includeTurns"] != false {
				t.Fatal("history loaded")
			}
			return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}, nil
		case "thread/resume":
			if p["excludeTurns"] != true || p["sandbox"] != nil || p["approvalPolicy"] != nil {
				t.Fatal("scope or history changed", p)
			}
			return map[string]any{"thread": map[string]string{"id": "root"}}, nil
		default:
			t.Fatal("input/fork/setting effect", method)
			return nil, errors.New("unexpected")
		}
	}
	o, e := c.MaintainResident(context.Background(), s, "root", 9, func() bool { return true })
	if e != nil || o.State != "ready" || !o.Recovered || o.ToolState != "refresh_pending" || o.FreshUntil <= o.CheckedAt || s.ResumeID() != "root" {
		t.Fatal(o, e)
	}
	if !reflect.DeepEqual(w.methods, []string{"thread/read", "thread/resume", "thread/read"}) {
		t.Fatal(w.methods)
	}
	o, e = c.MaintainResident(context.Background(), s, "root", 9, func() bool { return true })
	if e != nil || o.Recovered || o.ToolState != "refresh_pending" {
		t.Fatal("fake loaded tool", o, e)
	}
	st.residentToolsEpoch = 9
	st.residentToolsTransport = c.rpc.transportEpoch.Load()
	o, e = c.MaintainResident(context.Background(), s, "root", 9, func() bool { return true })
	if e != nil || o.ToolState != "loaded" {
		t.Fatal(o, e)
	}
	c.rpc.transportEpoch.Add(1)
	o, e = c.MaintainResident(context.Background(), s, "root", 9, func() bool { return true })
	if e != nil || o.ToolState != "refresh_pending" {
		t.Fatal("old transport tool observation reused", o, e)
	}
}
func TestResidentBusyStaleAndUnknownNeverRecoverOrGenerate(t *testing.T) {
	for _, kind := range []string{"active", "stale", "unknown", "foreign", "wrong-resume", "unsupported", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			c, s, st, w, _ := livenessFixture(t)
			c.appServerMode = "daemon"
			c.remoteConn = &websocket.Conn{}
			st.turnActive = false
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			current := true
			if kind == "stale" {
				current = false
			}
			if kind == "unsupported" {
				c.appServerMode = "stdio"
			}
			if kind == "cancelled" {
				cancel()
			}
			reads := 0
			w.reply = func(method string, raw json.RawMessage) (any, error) {
				if method == "thread/read" {
					reads++
					if kind == "unknown" {
						return nil, errors.New("transport failed")
					}
					if kind == "foreign" {
						return map[string]any{"thread": map[string]any{"id": "other", "status": map[string]string{"type": "idle"}}}, nil
					}
					status := "active"
					if kind == "wrong-resume" {
						status = "notLoaded"
					}
					return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": status}}}, nil
				}
				if method == "thread/resume" && kind == "wrong-resume" {
					return map[string]any{"thread": map[string]string{"id": "replacement"}}, nil
				}
				t.Fatal("unexpected effect", method)
				return nil, nil
			}
			o, e := c.MaintainResident(ctx, s, "root", 5, func() bool { return current })
			switch kind {
			case "active":
				if e != nil || o.State != "active" || reads != 1 {
					t.Fatal(o, e, reads)
				}
			case "unsupported":
				if e != nil || o.State != "limited" || reads != 0 {
					t.Fatal(o, e)
				}
			default:
				if e == nil {
					t.Fatal("invalid accepted", o)
				}
			}
			if s.ResumeID() != "root" {
				t.Fatal("canonical thread changed")
			}
			for _, m := range w.methods {
				if m != "thread/read" && m != "thread/resume" {
					t.Fatal("input or fork", m)
				}
			}
		})
	}
}
