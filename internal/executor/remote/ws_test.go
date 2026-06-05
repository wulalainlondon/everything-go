package remote

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type capSink struct {
	mu     sync.Mutex
	events []any
}

func (s *capSink) Emit(e any) {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
}

func (s *capSink) waitFor(t *testing.T, match func(any) bool) any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		for _, e := range s.events {
			if match(e) {
				s.mu.Unlock()
				return e
			}
		}
		s.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Fatalf("timed out waiting for event; got %+v", s.events)
	return nil
}

func wsURL(ts *httptest.Server) string {
	return "ws" + strings.TrimPrefix(ts.URL, "http")
}

func TestWSExecutorTextToolDone(t *testing.T) {
	startSeen := make(chan map[string]any, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		ctx := r.Context()
		_, helloData, err := conn.Read(ctx)
		if err != nil {
			t.Errorf("read remote_hello: %v", err)
			return
		}
		var hello map[string]any
		_ = json.Unmarshal(helloData, &hello)
		if hello["type"] != "remote_hello" {
			t.Errorf("bad hello: %+v", hello)
			return
		}
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Errorf("read turn_start: %v", err)
			return
		}
		var start map[string]any
		_ = json.Unmarshal(data, &start)
		startSeen <- start
		frames := []string{
			`{"type":"text_delta","session_id":"s1","request_id":"r1","delta":"hi"}`,
			`{"type":"tool_start","session_id":"s1","request_id":"r1","tool_id":"t1","name":"Bash","command":"ls"}`,
			`{"type":"tool_delta","session_id":"s1","request_id":"r1","tool_id":"t1","delta":"a"}`,
			`{"type":"tool_delta","session_id":"s1","request_id":"r1","tool_id":"t1","delta":"b"}`,
			`{"type":"tool_end","session_id":"s1","request_id":"r1","tool_id":"t1"}`,
			`{"type":"done","session_id":"s1","request_id":"r1"}`,
		}
		for _, frame := range frames {
			if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
				t.Errorf("write frame: %v", err)
				return
			}
		}
	}))
	defer ts.Close()

	sink := &capSink{}
	ex := NewWS(sink, wsURL(ts), "")
	s := session.NewRegistry().Create("s1", "n", "/tmp", "remote-ws", "", "", "")
	if err := ex.Send(context.Background(), s, "r1", "hello", nil, nil); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case start := <-startSeen:
		if start["type"] != "turn_start" || start["session_id"] != "s1" || start["request_id"] != "r1" {
			t.Fatalf("bad turn_start: %+v", start)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("backend did not receive turn_start")
	}
	sink.waitFor(t, func(e any) bool {
		d, ok := e.(protocol.Done)
		return ok && d.SessionID == "s1" && d.RequestID == "r1"
	})

	var sawAB bool
	sink.mu.Lock()
	for _, e := range sink.events {
		if tr, ok := e.(protocol.ToolResult); ok && tr.ToolUseID == "t1" && tr.Output == "ab" {
			sawAB = true
		}
	}
	sink.mu.Unlock()
	if !sawAB {
		t.Fatalf("expected accumulated tool output 'ab', got %+v", sink.events)
	}
}

func TestWSExecutorDisconnectFailsTurn(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		_, _, _ = conn.Read(r.Context()) // remote_hello
		_, _, _ = conn.Read(r.Context()) // turn_start
		conn.Close(websocket.StatusNormalClosure, "bye")
	}))
	defer ts.Close()

	sink := &capSink{}
	ex := NewWS(sink, wsURL(ts), "")
	s := session.NewRegistry().Create("s1", "n", "/tmp", "remote-ws", "", "", "")
	if err := ex.Send(context.Background(), s, "r1", "hello", nil, nil); err != nil {
		t.Fatalf("Send: %v", err)
	}
	ev := sink.waitFor(t, func(e any) bool {
		er, ok := e.(protocol.Error)
		return ok && er.Code == "remote_disconnected" && er.RequestID == "r1"
	})
	if ev == nil {
		t.Fatal("missing disconnect error")
	}
}

func TestWSExecutorReusesConnectionAcrossTurns(t *testing.T) {
	var accepts int
	turns := make(chan map[string]any, 2)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accepts++
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		ctx := r.Context()
		_, _, _ = conn.Read(ctx) // remote_hello
		for i := 0; i < 2; i++ {
			_, data, err := conn.Read(ctx)
			if err != nil {
				t.Errorf("read turn_start %d: %v", i, err)
				return
			}
			var start map[string]any
			_ = json.Unmarshal(data, &start)
			turns <- start
			done := map[string]any{
				"type":       "done",
				"session_id": start["session_id"],
				"request_id": start["request_id"],
			}
			out, _ := json.Marshal(done)
			if err := conn.Write(ctx, websocket.MessageText, out); err != nil {
				t.Errorf("write done: %v", err)
				return
			}
		}
	}))
	defer ts.Close()

	sink := &capSink{}
	ex := NewWS(sink, wsURL(ts), "")
	reg := session.NewRegistry()
	s1 := reg.Create("s1", "n", "/tmp", "remote-ws", "", "", "")
	s2 := reg.Create("s2", "n", "/tmp", "remote-ws", "", "", "")

	if err := ex.Send(context.Background(), s1, "r1", "one", nil, nil); err != nil {
		t.Fatalf("Send1: %v", err)
	}
	sink.waitFor(t, func(e any) bool {
		d, ok := e.(protocol.Done)
		return ok && d.SessionID == "s1"
	})
	if err := ex.Send(context.Background(), s2, "r2", "two", nil, nil); err != nil {
		t.Fatalf("Send2: %v", err)
	}
	sink.waitFor(t, func(e any) bool {
		d, ok := e.(protocol.Done)
		return ok && d.SessionID == "s2"
	})

	if accepts != 1 {
		t.Fatalf("expected one persistent backend connection, got %d", accepts)
	}
	if len(turns) != 2 {
		t.Fatalf("backend should see 2 turns, got %d", len(turns))
	}
}

func TestWSExecutorCapabilityRPCs(t *testing.T) {
	seen := make(chan string, 4)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		ctx := r.Context()
		_, _, _ = conn.Read(ctx) // remote_hello
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"remote_hello_ack","capabilities":{"history":true,"usage":true}}`))
		for i := 0; i < 3; i++ {
			_, data, err := conn.Read(ctx)
			if err != nil {
				t.Errorf("read rpc: %v", err)
				return
			}
			var req map[string]any
			_ = json.Unmarshal(data, &req)
			typ, _ := req["type"].(string)
			rpcID, _ := req["rpc_id"].(string)
			seen <- typ
			var resp map[string]any
			switch typ {
			case "history_request":
				resp = map[string]any{
					"type": "history_result", "rpc_id": rpcID, "kind": "snapshot",
					"messages":     []any{map[string]any{"role": "user", "content": "hi"}},
					"source_count": 1, "known_id_found": true,
				}
			case "resumable_sessions_request":
				resp = map[string]any{
					"type": "resumable_sessions_result", "rpc_id": rpcID,
					"sessions": []any{map[string]any{"id": "r1", "name": "Remote", "claude_uuid": "u1", "last_used": 123, "cwd": "/tmp", "backend": "remote-ws"}},
				}
			case "usage_request":
				resp = map[string]any{"type": "usage_result", "rpc_id": rpcID, "report": map[string]any{"type": "usage_report"}}
			default:
				t.Errorf("unexpected rpc type: %s", typ)
				return
			}
			out, _ := json.Marshal(resp)
			if err := conn.Write(ctx, websocket.MessageText, out); err != nil {
				t.Errorf("write rpc response: %v", err)
				return
			}
		}
	}))
	defer ts.Close()

	ex := NewWS(&capSink{}, wsURL(ts), "")
	hist, err := ex.LoadHistory("resume1", historyOpts(10))
	if err != nil {
		t.Fatalf("LoadHistory: %v", err)
	}
	if hist.Kind != "snapshot" || hist.SourceCount != 1 || len(hist.Messages) != 1 {
		t.Fatalf("bad history result: %+v", hist)
	}
	resumable, err := ex.ResumableSessions(5)
	if err != nil {
		t.Fatalf("ResumableSessions: %v", err)
	}
	if len(resumable) != 1 || resumable[0].ID != "r1" {
		t.Fatalf("bad resumable result: %+v", resumable)
	}
	usage, err := ex.FetchUsage(context.Background())
	if err != nil {
		t.Fatalf("FetchUsage: %v", err)
	}
	if usage.Type != "usage_report" {
		t.Fatalf("bad usage report: %+v", usage)
	}
	got := []string{<-seen, <-seen, <-seen}
	want := []string{"history_request", "resumable_sessions_request", "usage_request"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rpc order got %v want %v", got, want)
	}
}

func TestWSExecutorCapabilityUnsupported(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		_, _, _ = conn.Read(r.Context()) // remote_hello
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"remote_hello_ack","capabilities":{}}`))
	}))
	defer ts.Close()

	ex := NewWS(&capSink{}, wsURL(ts), "")
	if _, err := ex.ResumableSessions(5); err == nil {
		t.Fatal("history should be unsupported without capability")
	}
	if _, err := ex.FetchUsage(context.Background()); err == nil {
		t.Fatal("usage should be unsupported without capability")
	}
}

func historyOpts(limit int) history.Opts {
	return history.Opts{Limit: limit, Mode: "snapshot"}
}

func TestWSExecutorRemoteInteractions(t *testing.T) {
	responseSeen := make(chan map[string]any, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		ctx := r.Context()
		_, _, _ = conn.Read(ctx) // remote_hello
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"remote_hello_ack","capabilities":{"interactions":true}}`))
		_, _, _ = conn.Read(ctx) // turn_start
		req := map[string]any{
			"type": "user_input_request", "request_id": "ui_1", "session_id": "s1",
			"kind": "ask_user_question", "header": "Confirm", "tool_use_id": "toolu_1",
			"requesting_agent": "remote",
			"questions": []any{map[string]any{
				"question_id": "q1", "text": "Continue?", "type": "choice",
				"options": []any{map[string]any{"id": "yes", "label": "Yes"}},
			}},
		}
		out, _ := json.Marshal(req)
		if err := conn.Write(ctx, websocket.MessageText, out); err != nil {
			t.Errorf("write user_input_request: %v", err)
			return
		}
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Errorf("read user_input_response: %v", err)
			return
		}
		var resp map[string]any
		_ = json.Unmarshal(data, &resp)
		responseSeen <- resp
	}))
	defer ts.Close()

	sink := &capSink{}
	ex := NewWS(sink, wsURL(ts), "")
	s := session.NewRegistry().Create("s1", "n", "/tmp", "remote-ws", "", "", "")
	if err := ex.Send(context.Background(), s, "r1", "hello", nil, nil); err != nil {
		t.Fatalf("Send: %v", err)
	}
	sink.waitFor(t, func(e any) bool {
		ui, ok := e.(protocol.UserInputRequestEvent)
		return ok && ui.RequestID == "ui_1" && ui.ToolUseID == "toolu_1"
	})
	pending := ex.PendingInteractions("s1")
	if len(pending) != 1 || pending[0].RequestID != "ui_1" {
		t.Fatalf("pending interactions wrong: %+v", pending)
	}
	if !ex.RespondUserInput("toolu_1", map[string]any{"q1": "yes"}, false) {
		t.Fatal("RespondUserInput should match by tool_use_id alias")
	}
	resp := <-responseSeen
	if resp["type"] != "user_input_response" || resp["request_id"] != "ui_1" || resp["session_id"] != "s1" {
		t.Fatalf("bad remote response: %+v", resp)
	}
	sink.waitFor(t, func(e any) bool {
		r, ok := e.(protocol.InteractionResolved)
		return ok && r.RequestID == "ui_1" && r.Status == "resolved"
	})
	if got := ex.PendingInteractions("s1"); len(got) != 0 {
		t.Fatalf("pending should be empty after response: %+v", got)
	}
}

func TestWSExecutorRemoteInteractionExpiresOnDisconnect(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		ctx := r.Context()
		_, _, _ = conn.Read(ctx) // remote_hello
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"remote_hello_ack","capabilities":{"interactions":true}}`))
		_, _, _ = conn.Read(ctx) // turn_start
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"user_input_request","request_id":"ui_1","session_id":"s1","questions":[{"question_id":"q1","text":"Q?","type":"question"}]}`))
		conn.Close(websocket.StatusNormalClosure, "bye")
	}))
	defer ts.Close()

	sink := &capSink{}
	ex := NewWS(sink, wsURL(ts), "")
	s := session.NewRegistry().Create("s1", "n", "/tmp", "remote-ws", "", "", "")
	if err := ex.Send(context.Background(), s, "r1", "hello", nil, nil); err != nil {
		t.Fatalf("Send: %v", err)
	}
	sink.waitFor(t, func(e any) bool {
		r, ok := e.(protocol.InteractionResolved)
		return ok && r.RequestID == "ui_1" && r.Status == "expired"
	})
}
