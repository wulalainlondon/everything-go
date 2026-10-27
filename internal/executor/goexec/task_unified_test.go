package goexec

import (
	"bytes"
	"encoding/json"
	"errors"
	unified "everything-go/contracts/task-api/unified-v6"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskUnifiedNativeRegistrationExactEnvelope(t *testing.T) {
	for _, v := range []struct {
		tool, kind, version string
		want                bool
	}{{"task_unified", "task_api_unified_request", unified.Version, true}, {"task_get", "task_api_unified_request", unified.Version, false}, {"task_unified", "task_api_request", "1.0.0-rc.1", false}, {"task_get", "task_api_request", "1.0.0-rc.1", true}} {
		if taskToolMatches(v.tool, v.kind, v.version, "get") != v.want {
			t.Fatal(v)
		}
	}
}
func TestTaskUnifiedCodexGuardAtOriginalWriterRejectsAfterWait(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	var valid atomic.Bool
	valid.Store(true)
	w.reply = func(method string, _ json.RawMessage) (any, error) {
		t.Errorf("retired input reached provider %s", method)
		return nil, errors.New("unexpected")
	}
	c.rpc.wmu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- c.startTurn("root", nil, s.Snapshot(), "", st.reqID, func() error {
			if !valid.Load() {
				return errors.New("original source retired")
			}
			return nil
		})
	}()
	valid.Store(false)
	c.rpc.wmu.Unlock()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("retired accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("writer guard blocked")
	}
}

type unifiedCloseBuffer struct{ bytes.Buffer }

func (b *unifiedCloseBuffer) Close() error { return nil }
func TestTaskUnifiedClaudeGuardAtOriginalWriterRejectsAfterWait(t *testing.T) {
	var out unifiedCloseBuffer
	b := newBufWriteCloser(&out)
	var valid atomic.Bool
	valid.Store(true)
	b.mu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- b.WriteGuarded([]byte("private-input\n"), func() error {
			if !valid.Load() {
				return errors.New("source retired")
			}
			return nil
		})
	}()
	valid.Store(false)
	b.mu.Unlock()
	if e := <-done; e == nil || out.Len() != 0 {
		t.Fatal(e, out.String())
	}
}
