package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/recovery"
)

// The fake clock still uses the real production SQLite transitions/CAS and the
// real Codex turn/start parser, canonical pagination and runTurn terminal path.
type automaticSQLFixture struct {
	store        *messagequeue.Store
	src          recovery.AutomaticSource
	now          int64
	epoch        string
	failFinish   bool
	failAccepted bool
	waitHook     func() error
	writerHook   func()
}

func (a *automaticSQLFixture) Resume() (recovery.Run, error) {
	r, _, e := a.store.Recovery(a.src.Session, a.src.Request)
	return r, e
}
func (a *automaticSQLFixture) BindThread(s string) error {
	return a.store.BindRecoveryThread(a.src.Session, a.src.Request, s, 1, a.epoch)
}
func (a *automaticSQLFixture) BindProvider(s string) error {
	e := a.store.BindRecoveryProvider(a.src.Session, a.src.Request, s, 1, a.epoch)
	if e == nil {
		r, _ := a.Resume()
		a.src = r.Source
	}
	return e
}
func (a *automaticSQLFixture) BeforeDispatch(_ context.Context, hash string, epoch uint64) (recovery.AutomaticNext, error) {
	r, _ := a.Resume()
	_, attempt, e := a.store.ClaimRecovery(a.src.Session, a.src.Request, 1, a.epoch, epoch, hash, a.now)
	return recovery.AutomaticNext{Attempt: attempt, Continuation: r.Kind == "continue", ParentTurn: r.ParentTurn}, e
}
func (a *automaticSQLFixture) Accepted(attempt, thread, turn string) error {
	if a.failAccepted {
		return errors.New("acceptance disk-full fixture")
	}
	return a.store.RecoveryAccepted(a.src.Session, a.src.Request, attempt, thread, turn, 1, a.epoch, a.now)
}
func (a *automaticSQLFixture) Failed(o recovery.AutomaticOutcome) (bool, error) {
	_, retry, e := a.store.ScheduleRecovery(a.src.Session, a.src.Request, 1, a.epoch, o, a.now, 0)
	return retry, e
}
func (a *automaticSQLFixture) ObserveFailure(o recovery.AutomaticOutcome) error {
	return a.store.ObserveRecoveryFailure(a.src.Session, a.src.Request, o, a.now)
}
func (a *automaticSQLFixture) NativeFinished(s string) error {
	if a.failFinish {
		return errors.New("terminal disk-full fixture")
	}
	return a.store.FinishRecovery(a.src.Session, a.src.Request, s)
}
func (a *automaticSQLFixture) Guard() error {
	r, e := a.Resume()
	if e != nil {
		return e
	}
	if r.Generation != 1 || r.State == "cancelled" || r.State == "disabled" {
		return recovery.ErrGuard
	}
	return nil
}
func (a *automaticSQLFixture) VerifyInputs() error { return a.Guard() }
func (a *automaticSQLFixture) WriterGuard() (func(), error) {
	if e := a.Guard(); e != nil {
		return nil, e
	}
	if a.writerHook != nil {
		a.writerHook()
	}
	return func() {}, nil
}
func (a *automaticSQLFixture) Wait(context.Context) error {
	if a.waitHook != nil {
		if e := a.waitHook(); e != nil {
			return e
		}
	}
	r, e := a.Resume()
	if e != nil {
		return e
	}
	a.now = r.NextAt
	return nil
}

type automaticRPCWriter struct {
	mu     sync.Mutex
	c      *Codex
	call   func(string, json.RawMessage) (json.RawMessage, error)
	starts []map[string]any
}

func (w *automaticRPCWriter) Write(raw []byte) (int, error) {
	var q struct {
		ID     int             `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if e := json.Unmarshal(raw, &q); e != nil {
		return 0, e
	}
	if q.Method == "turn/start" {
		var p map[string]any
		json.Unmarshal(q.Params, &p)
		w.mu.Lock()
		w.starts = append(w.starts, p)
		w.mu.Unlock()
	}
	result, e := w.call(q.Method, q.Params)
	if e != nil {
		return 0, e
	}
	w.c.rpc.dispatchResponse(append([]byte(fmt.Sprintf(`{"id":%d,`, q.ID)), append(result, '}')...))
	return len(raw), nil
}
func autoResult(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return append([]byte(`"result":`), b...)
}
func automaticExecutionFixture(t *testing.T) (*Codex, *automaticSQLFixture, *automaticRPCWriter, *capSink, func()) {
	c, s, st, _, sink := livenessFixture(t)
	c.authPath = filepath.Join(t.TempDir(), "auth.json")
	os.WriteFile(c.authPath, []byte(`{"tokens":{"account_id":"original"}}`), 0600)
	s.SetModel("gpt-6.1-sol")
	st.currentTurnID = ""
	st.turnStartPending = true
	st.confirmedTurnID = ""
	st.confirmedRequestID = ""
	st.turnFailure = nil
	req := st.reqID
	store, e := messagequeue.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { store.Close() })
	store.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: req, Payload: []byte(`{"content":"original objective"}`)})
	store.Transition(s.ID, req, []messagequeue.State{messagequeue.Queued}, messagequeue.Running, "", "", "")
	src := recovery.AutomaticSource{Authority: "a", Session: s.ID, Request: req, Thread: "root", Configuration: "fixed", Principal: "caller", Scope: "model"}
	now := time.Now().UnixMilli()
	if _, e = store.BeginRecovery(src, "boot", now); e != nil {
		t.Fatal(e)
	}
	a := &automaticSQLFixture{store: store, src: src, now: now, epoch: "boot"}
	st.automatic = a
	st.automaticInput = []map[string]any{{"type": "text", "text": "original objective"}}
	w := &automaticRPCWriter{c: c}
	c.rpc.setWriter(w)
	return c, a, w, sink, func() { c.runTurn(s, st, "root", st.automaticInput, st.turnDone, "") }
}
func TestAutomaticCodexAcceptedFailedThenOneContinuationUsesSameThreadAndOneLogicalTerminal(t *testing.T) {
	testAutomaticAcceptedFailureContinuation(t, nil)
}

func testAutomaticAcceptedFailureContinuation(t *testing.T, quota map[string]any) {
	t.Helper()
	c, a, w, sink, run := automaticExecutionFixture(t)
	n := 0
	items := []any{map[string]any{"type": "userMessage", "id": "original"}, map[string]any{"type": "reasoning", "id": "reasoning"}}
	w.call = func(method string, raw json.RawMessage) (json.RawMessage, error) {
		var params map[string]any
		json.Unmarshal(raw, &params)
		switch method {
		case "account/read":
			return autoResult(map[string]any{"account": map[string]string{"type": "chatgpt"}}), nil
		case "account/rateLimits/read":
			if quota != nil {
				return autoResult(quota), nil
			}
			return autoResult(map[string]any{"accountId": "original", "ordinaryUsageAllowed": true, "rateLimits": map[string]any{}}), nil
		case "turn/start":
			n++
			status := "failed"
			id := "turn_failed"
			if n == 2 {
				status = "completed"
				id = "turn_final"
			}
			return autoResult(map[string]any{"turn": map[string]any{"id": id, "status": status, "error": map[string]any{"message": "capacity", "codexErrorInfo": "server_overloaded"}}}), nil
		case "thread/read":
			return autoResult(map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}), nil
		case "thread/turns/list":
			return autoResult(map[string]any{"data": []any{map[string]any{"id": "turn_failed", "status": "failed", "itemsView": "full", "items": items}}, "nextCursor": nil}), nil
		case "thread/items/list":
			return autoResult(map[string]any{"data": []any{map[string]any{"turnId": "turn_failed", "item": items[0]}, map[string]any{"turnId": "turn_failed", "item": items[1]}}, "nextCursor": nil}), nil
		default:
			return autoResult(map[string]any{}), nil
		}
	}
	run()
	if n != 2 {
		t.Fatal("not exactly one continuation", n)
	}
	if len(w.starts) != 2 || w.starts[0]["threadId"] != "root" || w.starts[1]["threadId"] != "root" || w.starts[0]["model"] != "gpt-6.1-sol" || w.starts[1]["model"] != "gpt-6.1-sol" {
		t.Fatal(w.starts)
	}
	second, _ := json.Marshal(w.starts[1]["input"])
	if string(second) == "" || string(second) == `[{"text":"original objective","type":"text"}]` {
		t.Fatal("replayed original instead of continuation", string(second))
	}
	if sink.count(func(v any) bool { _, ok := v.(protocol.Done); return ok }) != 1 || sink.count(func(v any) bool { _, ok := v.(protocol.Error); return ok }) != 0 {
		t.Fatal("physical failed leaked logical terminal", sink)
	}
	r, _ := a.Resume()
	if r.State != "native_completed" || r.Attempts != 1 || r.ObjectiveVerified {
		t.Fatal(r)
	}
	native, ok, e := a.store.ExactNativeAcceptance(a.src.Session, a.src.Request)
	if e != nil || !ok || native.TurnID != "turn_final" {
		t.Fatal(native, ok, e)
	}
	mapped, e := c.loadTurnRequests("root")
	if e != nil || len(mapped.Recovery) != 2 {
		t.Fatal(mapped, e)
	}
	current, e := mappedRequestTurn(mapped, a.src.Request)
	if e != nil || current != "turn_final" {
		t.Fatal(current, e)
	}
	history := c.withHistoryRequestIDs([]map[string]any{{"role": "user", "source_turn_id": "turn_failed", "content": "original objective"}, {"role": "user", "source_turn_id": "turn_final", "content": automaticContinuation}, {"role": "assistant", "source_turn_id": "turn_final", "content": "real final"}}, "root")
	if len(history) != 2 || history[0]["content"] != "original objective" {
		t.Fatal("fake recovery user bubble", history)
	}
}
func TestAutomaticCodexIngressRejectionRetriesImmutableInput(t *testing.T) {
	_, a, w, sink, run := automaticExecutionFixture(t)
	n := 0
	w.call = func(method string, _ json.RawMessage) (json.RawMessage, error) {
		switch method {
		case "account/read":
			return autoResult(map[string]any{"account": map[string]string{"type": "chatgpt"}}), nil
		case "account/rateLimits/read":
			return autoResult(map[string]any{"accountId": "original", "ordinaryUsageAllowed": true, "rateLimits": map[string]any{}}), nil
		case "thread/read":
			return autoResult(map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}), nil
		case "turn/start":
			n++
			if n == 1 {
				return json.RawMessage(`"error":{"code":-32001,"message":"Server overloaded; retry later."}`), nil
			}
			return autoResult(map[string]any{"turn": map[string]string{"id": "only-accepted", "status": "completed"}}), nil
		}
		return autoResult(map[string]any{}), nil
	}
	run()
	if n != 2 {
		t.Fatal(n)
	}
	x, _ := json.Marshal(w.starts[0]["input"])
	y, _ := json.Marshal(w.starts[1]["input"])
	if string(x) != string(y) {
		t.Fatal("input mutated across verified ingress rejection", string(x), string(y))
	}
	if sink.count(func(v any) bool { _, ok := v.(protocol.Done); return ok }) != 1 {
		t.Fatal("logical final count")
	}
	r, _ := a.Resume()
	if r.Attempts != 1 {
		t.Fatal(r)
	}
}
func TestAutomaticCodexPartialWriteAndTerminalStorageFailureNeverReplayOrPublishDone(t *testing.T) {
	for _, kind := range []string{"partial-write", "terminal-storage", "acceptance-storage"} {
		t.Run(kind, func(t *testing.T) {
			_, a, w, sink, run := automaticExecutionFixture(t)
			if kind == "terminal-storage" {
				a.failFinish = true
			}
			if kind == "acceptance-storage" {
				a.failAccepted = true
			}
			w.call = func(method string, _ json.RawMessage) (json.RawMessage, error) {
				switch method {
				case "account/read":
					return autoResult(map[string]any{"account": map[string]string{"type": "chatgpt"}}), nil
				case "account/rateLimits/read":
					return autoResult(map[string]any{"accountId": "original", "ordinaryUsageAllowed": true, "rateLimits": map[string]any{}}), nil
				case "turn/start":
					if kind == "partial-write" {
						return nil, errors.New("partial transport write")
					}
					return autoResult(map[string]any{"turn": map[string]string{"id": "complete", "status": "completed"}}), nil
				}
				return autoResult(map[string]any{}), nil
			}
			run()
			if len(w.starts) != 1 || sink.count(func(v any) bool { _, ok := v.(protocol.Done); return ok }) != 0 {
				t.Fatal("unknown/failing persistence published done or replayed")
			}
			if sink.count(func(v any) bool { _, ok := v.(backend.NativeSubmissionUncertain); return ok }) != 1 {
				t.Fatal("uncertain evidence missing")
			}
		})
	}
}

func TestAutomaticCanonicalFullPaginationRejectsUnknownProviderEffects(t *testing.T) {
	for _, kind := range []string{"valid", "missing-full", "summary", "missing-items", "newer-turn", "opaque-exec", "mcp", "background", "unknown-item", "wrong-entry-turn", "duplicate-item", "repeating-cursor", "missing-original-item", "epoch-change", "pending-user"} {
		t.Run(kind, func(t *testing.T) {
			c, s, st, w, _ := livenessFixture(t)
			st.turnActive = false
			st.currentTurnID = ""
			st.turnStartPending = false
			user := map[string]any{"id": "user", "type": "userMessage"}
			reason := map[string]any{"id": "reason", "type": "reasoning"}
			canonical := []any{user, reason}
			pageCount := 0
			w.reply = func(method string, raw json.RawMessage) (any, error) {
				var params map[string]any
				json.Unmarshal(raw, &params)
				switch method {
				case "thread/read":
					return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}, nil
				case "thread/turns/list":
					row := map[string]any{"id": "failed", "status": "failed", "itemsView": "full", "items": canonical}
					switch kind {
					case "missing-full":
						delete(row, "itemsView")
					case "summary":
						row["itemsView"] = "summary"
					case "missing-items":
						delete(row, "items")
					case "newer-turn":
						row["id"] = "foreign"
					}
					return map[string]any{"data": []any{row}, "nextCursor": nil}, nil
				case "thread/items/list":
					pageCount++
					entryTurn := "failed"
					if kind == "wrong-entry-turn" {
						entryTurn = "foreign"
					}
					item := reason
					switch kind {
					case "opaque-exec":
						item = map[string]any{"id": "reason", "type": "dynamicToolCall", "status": "completed", "output": "I promise no effects"}
					case "mcp":
						item = map[string]any{"id": "reason", "type": "mcpToolCall", "status": "completed"}
					case "background":
						item = map[string]any{"id": "reason", "type": "commandExecution", "status": "inProgress"}
					case "unknown-item":
						item = map[string]any{"id": "reason", "type": "futureSafeTool"}
					case "epoch-change":
						c.rpc.transportEpoch.Add(1)
					case "pending-user":
						c.interMu.Lock()
						c.interactions["question"] = codexInteraction{payload: backend.UserInputPayload{SessionID: s.ID}}
						c.interMu.Unlock()
					}
					var next any
					var entries []any
					if pageCount == 1 {
						entries = []any{map[string]any{"turnId": entryTurn, "item": user}}
						next = "page2"
					} else {
						entries = []any{map[string]any{"turnId": entryTurn, "item": item}}
					}
					if kind == "duplicate-item" && pageCount == 2 {
						entries[0] = map[string]any{"turnId": "failed", "item": user}
					}
					if kind == "repeating-cursor" {
						next = "page2"
					}
					if kind == "missing-original-item" {
						entries = []any{}
						next = nil
					}
					return map[string]any{"data": entries, "nextCursor": next}, nil
				default:
					t.Fatal("canonical read made effect", method)
					return nil, nil
				}
			}
			proof, e := c.automaticCanonical(s, st, "failed")
			if kind == "valid" {
				if e != nil || !proof.Complete || !proof.ToolCoverage {
					t.Fatal(proof, e)
				}
			} else if e == nil && proof.Complete && proof.ToolCoverage && !proof.WaitingUser {
				t.Fatal("unverified canonical coverage allowed continuation", kind, proof)
			}
		})
	}
}

func TestAutomaticCanonicalContradictoryToolPayloadCannotBeSafe(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	st.turnActive = false
	st.currentTurnID = ""
	st.turnStartPending = false
	w.reply = func(method string, _ json.RawMessage) (any, error) {
		switch method {
		case "thread/read":
			return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}, nil
		case "thread/turns/list":
			return map[string]any{"data": []any{map[string]any{"id": "failed", "status": "failed", "itemsView": "full", "items": []any{map[string]any{"id": "same", "type": "commandExecution", "status": "completed", "command": "external-effect"}}}}, "nextCursor": nil}, nil
		case "thread/items/list":
			return map[string]any{"data": []any{map[string]any{"turnId": "failed", "item": map[string]any{"id": "same", "type": "reasoning"}}}, "nextCursor": nil}, nil
		}
		t.Fatal(method)
		return nil, nil
	}
	proof, e := c.automaticCanonical(s, st, "failed")
	if e == nil && proof.ToolCoverage && proof.NoUnresolvedEffects {
		t.Fatal("full canonical turn contains opaque command but item-ID-only join certified no unresolved effects", proof)
	}
}

func TestAutomaticCanonicalNewerCompletedTurnDuringPaginationRetiresParent(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	st.turnActive = false
	st.currentTurnID = ""
	st.turnStartPending = false
	latest := "failed"
	user := map[string]any{"id": "user", "type": "userMessage"}
	w.reply = func(method string, _ json.RawMessage) (any, error) {
		switch method {
		case "thread/read":
			return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}, nil
		case "thread/turns/list":
			return map[string]any{"data": []any{map[string]any{"id": latest, "status": "failed", "itemsView": "full", "items": []any{user}}}, "nextCursor": nil}, nil
		case "thread/items/list":
			latest = "foreign-completed"
			return map[string]any{"data": []any{map[string]any{"turnId": "failed", "item": user}}, "nextCursor": nil}, nil
		}
		t.Fatal(method)
		return nil, nil
	}
	proof, e := c.automaticCanonical(s, st, "failed")
	if e == nil && proof.ToolCoverage {
		t.Fatal("proof remained valid after provider's latest turn changed during pagination", proof)
	}
}

func TestAutomaticWriterRechecksAuthorityAfterTransportMutexWait(t *testing.T) {
	p := newRPCPlumber("fixture")
	var valid atomic.Bool
	valid.Store(true)
	var checked atomic.Int32
	var writes atomic.Int32
	p.setWriter(automaticWriterFunc(func(raw []byte) (int, error) { writes.Add(1); return len(raw), nil }))
	p.wmu.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, e := p.requestAtEpochGuarded(context.Background(), "turn/start", nil, time.Millisecond, nil, func() (func(), error) {
			checked.Add(1)
			if !valid.Load() {
				return nil, recovery.ErrGuard
			}
			return func() {}, nil
		})
		done <- e
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	if checked.Load() != 0 {
		p.wmu.Unlock()
		t.Fatal("guard ran before actual transport ownership")
	}
	valid.Store(false)
	p.wmu.Unlock()
	if e := <-done; !errors.Is(e, recovery.ErrGuard) {
		t.Fatal(e)
	}
	if writes.Load() != 0 || checked.Load() != 1 {
		t.Fatal("retired authority wrote after mutex wait", writes.Load(), checked.Load())
	}
}

type automaticWriterFunc func([]byte) (int, error)

func (f automaticWriterFunc) Write(b []byte) (int, error) { return f(b) }

func TestAutomaticScopeContentionWaitsWithoutWriterOrUnknownThenSettlesOnce(t *testing.T) {
	for _, kind := range []string{"release", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			_, a, w, sink, run := automaticExecutionFixture(t)
			subject := recovery.Hash([]byte("chatgpt:original"))
			if e := a.BindProvider(subject); e != nil {
				t.Fatal(e)
			}
			now := a.now
			rejected := recovery.AutomaticOutcome{Acceptance: recovery.Rejected, IngressRejected: true, Failure: recovery.Failure{Category: recovery.ModelCapacity, Source: "codex_ingress"}}
			a.store.ClaimRecovery(a.src.Session, a.src.Request, 1, a.epoch, 1, "input", now)
			waiting, _, e := a.store.ScheduleRecovery(a.src.Session, a.src.Request, 1, a.epoch, rejected, now, 0)
			if e != nil {
				t.Fatal(e)
			}
			other := recovery.AutomaticSource{Authority: "a", Session: "other", Request: "r_other", Thread: "other-thread", Configuration: "fixed", Principal: "other-device", Scope: "model"}
			a.store.Enqueue(messagequeue.Entry{SessionID: other.Session, RequestID: other.Request, Payload: []byte(`{"content":"other"}`)})
			a.store.Transition(other.Session, other.Request, []messagequeue.State{messagequeue.Queued}, messagequeue.Running, "", "", "")
			a.store.BeginRecovery(other, a.epoch, now)
			a.store.BindRecoveryProvider(other.Session, other.Request, subject, 1, a.epoch)
			a.store.ClaimRecovery(other.Session, other.Request, 1, a.epoch, 1, "input", now)
			a.store.ScheduleRecovery(other.Session, other.Request, 1, a.epoch, rejected, now, 0)
			if _, _, e = a.store.ClaimRecovery(other.Session, other.Request, 1, a.epoch, 1, "input", waiting.NextAt); e != nil {
				t.Fatal(e)
			}
			blocked := make(chan struct{})
			release := make(chan struct{})
			waits := 0
			a.waitHook = func() error {
				waits++
				if waits == 2 {
					close(blocked)
					<-release
				}
				return nil
			}
			w.call = func(method string, _ json.RawMessage) (json.RawMessage, error) {
				switch method {
				case "account/read":
					return autoResult(map[string]any{"account": map[string]string{"type": "chatgpt"}}), nil
				case "account/rateLimits/read":
					return autoResult(map[string]any{"accountId": "original", "ordinaryUsageAllowed": true, "rateLimits": map[string]any{}}), nil
				case "thread/read":
					return autoResult(map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}), nil
				case "turn/start":
					return autoResult(map[string]any{"turn": map[string]string{"id": "final", "status": "completed"}}), nil
				}
				return autoResult(map[string]any{}), nil
			}
			done := make(chan struct{})
			go func() { run(); close(done) }()
			select {
			case <-blocked:
			case <-time.After(3 * time.Second):
				t.Fatal("did not reach persistent scheduling wait")
			}
			r, _ := a.Resume()
			w.mu.Lock()
			writes := len(w.starts)
			w.mu.Unlock()
			if r.State != "retry_wait" || r.Attempts != 0 || r.LastAttempt != waiting.LastAttempt || writes != 0 || sink.count(func(v any) bool {
				switch v.(type) {
				case backend.NativeSubmissionUncertain, protocol.Done, protocol.Error:
					return true
				}
				return false
			}) != 0 {
				t.Fatal("scope contention changed native/terminal facts", r, writes)
			}
			if kind == "release" {
				if _, _, e = a.store.ScheduleRecovery(other.Session, other.Request, 1, a.epoch, rejected, waiting.NextAt, 0); e != nil {
					t.Fatal(e)
				}
			} else {
				if e = a.store.InvalidateRecovery(a.src.Session, a.src.Request, "cancel"); e != nil {
					t.Fatal(e)
				}
			}
			close(release)
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("settled contention did not unblock")
			}
			w.mu.Lock()
			writes = len(w.starts)
			w.mu.Unlock()
			if sink.count(func(v any) bool { _, ok := v.(backend.NativeSubmissionUncertain); return ok }) != 0 {
				t.Fatal("known pre-writer wait became native unknown")
			}
			if kind == "release" {
				if writes != 1 || sink.count(func(v any) bool { _, ok := v.(protocol.Done); return ok }) != 1 {
					t.Fatal("not exactly one eventual writer/final", writes)
				}
			} else {
				if writes != 0 || sink.count(func(v any) bool { _, ok := v.(protocol.Stopped); return ok }) != 1 {
					t.Fatal("cancelled wait wrote or did not stop", writes)
				}
			}
		})
	}
}
func TestAutomaticCanonicalStrictJSONAndExactItemEvidence(t *testing.T) {
	for _, raw := range []string{`{"id":"i","type":"reasoning","type":"commandExecution"}`, `{"id":"i","type":"reasoning","summary":{"a":1,"a":2}}`, `{"id":"i","type":"reasoning"} {}`} {
		if _, e := automaticCanonicalItem(json.RawMessage(raw)); e == nil {
			t.Fatal("ambiguous canonical item accepted", raw)
		}
	}
	a, e := automaticCanonicalItem(json.RawMessage(`{"id":"i","type":"reasoning","summary":["one"]}`))
	if e != nil {
		t.Fatal(e)
	}
	b, e := automaticCanonicalItem(json.RawMessage(`{"summary":["one"],"type":"reasoning","id":"i"}`))
	if e != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("key ordering changed evidence", e)
	}
	c, e := automaticCanonicalItem(json.RawMessage(`{"id":"i","type":"reasoning","summary":["two"]}`))
	if e != nil || reflect.DeepEqual(a, c) {
		t.Fatal("same-ID changed content not distinguished", e)
	}
}

func TestAutomaticCanonicalFullAndPagedContentAndLatestEvidenceMustMatch(t *testing.T) {
	for _, kind := range []string{"equal", "item-content", "latest-content", "latest-status", "latest-error"} {
		t.Run(kind, func(t *testing.T) {
			c, s, st, w, _ := livenessFixture(t)
			st.turnActive = false
			st.currentTurnID = ""
			st.turnStartPending = false
			reads := 0
			w.reply = func(method string, _ json.RawMessage) (any, error) {
				full := map[string]any{"id": "reason", "type": "reasoning", "summary": []string{"original"}}
				switch method {
				case "thread/read":
					return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}, nil
				case "thread/turns/list":
					reads++
					row := map[string]any{"id": "failed", "status": "failed", "itemsView": "full", "items": []any{full}, "error": map[string]string{"codexErrorInfo": "serverOverloaded"}}
					if reads > 1 {
						switch kind {
						case "latest-content":
							full["summary"] = []string{"foreign"}
						case "latest-status":
							row["status"] = "completed"
						case "latest-error":
							row["error"] = map[string]string{"codexErrorInfo": "usageLimitExceeded"}
						}
					}
					return map[string]any{"data": []any{row}, "nextCursor": nil}, nil
				case "thread/items/list":
					if kind == "item-content" {
						full["summary"] = []string{"different"}
					}
					return map[string]any{"data": []any{map[string]any{"turnId": "failed", "item": full}}, "nextCursor": nil}, nil
				}
				t.Fatal(method)
				return nil, nil
			}
			proof, e := c.automaticCanonical(s, st, "failed")
			if kind == "equal" {
				if e != nil || !proof.ToolCoverage || reads != 2 {
					t.Fatal(proof, e, reads)
				}
			} else if e == nil && proof.ToolCoverage {
				t.Fatal("contradictory native evidence qualified", kind, proof)
			}
		})
	}
}

func TestAutomaticFinalWriterRejectsEpochOrContextRetiredInsideLocalGuard(t *testing.T) {
	for _, kind := range []string{"epoch", "context"} {
		t.Run(kind, func(t *testing.T) {
			p := newRPCPlumber("fixture")
			writes := 0
			released := 0
			p.setWriter(automaticWriterFunc(func(b []byte) (int, error) { writes++; return len(b), nil }))
			epoch := p.transportEpoch.Load()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, e := p.requestAtEpochGuarded(ctx, "turn/start", nil, time.Millisecond, &epoch, func() (func(), error) {
				if kind == "epoch" {
					p.failAll(errors.New("retired"))
				} else {
					cancel()
				}
				return func() { released++ }, nil
			})
			if e == nil || writes != 0 || released != 1 {
				t.Fatal("retired guard evidence wrote or leaked permit", e, writes, released)
			}
		})
	}
}

func TestAutomaticFinalWriterRechecksLocalAccountAndOwnedTupleAfterCoreReads(t *testing.T) {
	for _, kind := range []string{"account", "native-boundary", "stopped"} {
		t.Run(kind, func(t *testing.T) {
			c, a, w, sink, run := automaticExecutionFixture(t)
			a.writerHook = func() {
				switch kind {
				case "account":
					if e := os.WriteFile(c.authPath, []byte(`{"tokens":{"account_id":"foreign"}}`), 0600); e != nil {
						t.Fatal(e)
					}
				case "native-boundary":
					st := c.state(a.src.Session)
					st.mu.Lock()
					st.nativeBoundary++
					st.mu.Unlock()
				case "stopped":
					st := c.state(a.src.Session)
					st.mu.Lock()
					st.stopping = true
					st.mu.Unlock()
				}
			}
			w.call = func(method string, _ json.RawMessage) (json.RawMessage, error) {
				switch method {
				case "account/read":
					return autoResult(map[string]any{"account": map[string]string{"type": "chatgpt"}}), nil
				case "account/rateLimits/read":
					return autoResult(map[string]any{"accountId": "original", "ordinaryUsageAllowed": true, "rateLimits": map[string]any{}}), nil
				case "thread/read":
					return autoResult(map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}), nil
				case "turn/start":
					t.Fatal("retired account/tuple reached actual writer")
				}
				return autoResult(map[string]any{}), nil
			}
			s := c.sessionForThread("root")
			st := c.state(s.ID)
			admission, e := c.readStartAdmission(s, st, st.reqID)
			if e != nil {
				t.Fatal(e)
			}
			st.startAdmission = admission
			run()
			if len(w.starts) != 0 || sink.count(func(v any) bool { _, ok := v.(protocol.Done); return ok }) != 0 {
				t.Fatal("retired authority sent or completed")
			}
		})
	}
}

func TestAutomaticTerminalFailureDeadlineIncludesCanonicalReconciliationTime(t *testing.T) {
	_, a, w, sink, run := automaticExecutionFixture(t)
	initial := a.now
	starts := 0
	advanced := false
	user := map[string]any{"id": "user", "type": "userMessage"}
	w.call = func(method string, _ json.RawMessage) (json.RawMessage, error) {
		switch method {
		case "account/read":
			return autoResult(map[string]any{"account": map[string]string{"type": "chatgpt"}}), nil
		case "account/rateLimits/read":
			return autoResult(map[string]any{"accountId": "original", "ordinaryUsageAllowed": true, "rateLimits": map[string]any{}}), nil
		case "thread/read":
			return autoResult(map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}), nil
		case "turn/start":
			starts++
			if starts == 1 {
				return autoResult(map[string]any{"turn": map[string]any{"id": "failed", "status": "failed", "error": map[string]any{"message": "capacity", "codexErrorInfo": "server_overloaded"}}}), nil
			}
			return autoResult(map[string]any{"turn": map[string]string{"id": "late-continuation", "status": "completed"}}), nil
		case "thread/turns/list":
			if !advanced {
				advanced = true
				a.now = initial + 1800001
			}
			return autoResult(map[string]any{"data": []any{map[string]any{"id": "failed", "status": "failed", "itemsView": "full", "items": []any{user}}}, "nextCursor": nil}), nil
		case "thread/items/list":
			return autoResult(map[string]any{"data": []any{map[string]any{"turnId": "failed", "item": user}}, "nextCursor": nil}), nil
		}
		return autoResult(map[string]any{}), nil
	}
	run()
	r, _ := a.Resume()
	if starts != 1 || r.FirstFailureAt != initial || sink.count(func(v any) bool { _, ok := v.(protocol.Done); return ok }) != 0 {
		t.Fatalf("canonical reconciliation excluded from original30min budget: starts=%d firstFailure=%d expected=%d state=%s", starts, r.FirstFailureAt, initial, r.State)
	}
}
