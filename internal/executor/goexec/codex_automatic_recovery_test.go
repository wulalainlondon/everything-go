package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
	return func() {}, nil
}
func (a *automaticSQLFixture) Wait(context.Context) error {
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
			return autoResult(map[string]any{"accountId": "original", "ordinaryUsageAllowed": true}), nil
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
			return autoResult(map[string]any{"accountId": "original"}), nil
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
					return autoResult(map[string]any{"accountId": "original"}), nil
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
