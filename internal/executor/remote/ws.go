package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"everything-go/internal/backend"
	"everything-go/internal/executor"
	"everything-go/internal/history"
	"everything-go/internal/session"
)

const (
	dialTimeout  = 10 * time.Second
	writeTimeout = 5 * time.Second
)

type WS struct {
	sink  executor.Sink
	url   string
	token string

	mu       sync.Mutex
	conn     *websocket.Conn
	dialing  chan struct{}
	active   map[string]*turn
	closed   bool
	writeMu  sync.Mutex
	capMu    sync.Mutex
	capState map[string]bool
	capReady chan struct{}
	nextRPC  atomic.Uint64
	pending  map[string]chan rpcReply
	interMu  sync.Mutex
	inter    map[string]backend.UserInputPayload
}

type rpcReply struct {
	raw json.RawMessage
	err error
}

type turn struct {
	reqID string

	mu      sync.Mutex
	tools   map[string]string
	ended   bool
	session *session.Session
}

func NewWS(sink executor.Sink, url, token string) *WS {
	return &WS{
		sink: sink, url: url, token: token,
		active: make(map[string]*turn), capState: make(map[string]bool),
		pending: make(map[string]chan rpcReply),
		inter:   make(map[string]backend.UserInputPayload),
	}
}

func (w *WS) Send(ctx context.Context, s *session.Session, reqID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) error {
	if w.url == "" {
		return fmt.Errorf("remote websocket url is empty")
	}
	conn, err := w.ensureConn(ctx)
	if err != nil {
		return err
	}
	t := &turn{reqID: reqID, tools: make(map[string]string), session: s}

	w.mu.Lock()
	if old := w.active[s.ID]; old != nil {
		w.failLocked(old, backend.ErrRemoteReplaced, "new turn replaced previous in-flight turn")
	}
	w.active[s.ID] = t
	w.mu.Unlock()

	if err := w.writeConn(ctx, conn, turnStart(s.ID, reqID, content, s.Snapshot().Model, images, files)); err != nil {
		w.removeIfCurrent(s.ID, t)
		w.fail(t, backend.ErrRemoteSendFailed, err.Error())
		w.dropConn(conn, err)
		return err
	}
	return nil
}

func (w *WS) Stop(ctx context.Context, s *session.Session) error {
	t := w.get(s.ID)
	if t == nil {
		w.sink.Emit(backend.NewStopped(s.ID, ""))
		return nil
	}
	if conn := w.currentConn(); conn != nil {
		_ = w.writeConn(ctx, conn, turnStop(s.ID, t.reqID))
	}
	t.markEnded()
	w.removeIfCurrent(s.ID, t)
	w.sink.Emit(backend.NewStopped(s.ID, t.reqID))
	return nil
}

func (w *WS) Clear(ctx context.Context, s *session.Session) error {
	if conn := w.currentConn(); conn != nil {
		_ = w.writeConn(ctx, conn, sessionClear(s.ID))
	}
	if t := w.get(s.ID); t != nil {
		w.removeIfCurrent(s.ID, t)
	}
	s.SetResumeID("")
	w.sink.Emit(backend.NewSessionWarning(s.ID, "Session history cleared."))
	return nil
}

func (w *WS) Close(ctx context.Context, s *session.Session) error {
	if conn := w.currentConn(); conn != nil {
		_ = w.writeConn(ctx, conn, sessionClose(s.ID))
	}
	w.mu.Lock()
	delete(w.active, s.ID)
	w.mu.Unlock()
	return nil
}

func (w *WS) ensureConn(ctx context.Context) (*websocket.Conn, error) {
	for {
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return nil, fmt.Errorf("remote websocket executor is closed")
		}
		if w.conn != nil {
			conn := w.conn
			w.mu.Unlock()
			return conn, nil
		}
		if w.dialing != nil {
			wait := w.dialing
			w.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		wait := make(chan struct{})
		w.dialing = wait
		w.mu.Unlock()

		conn, err := w.dial(ctx)

		w.mu.Lock()
		if err == nil {
			w.conn = conn
			w.capMu.Lock()
			w.capState = make(map[string]bool)
			w.capReady = make(chan struct{})
			w.capMu.Unlock()
			go w.readLoop(conn)
		}
		close(wait)
		w.dialing = nil
		w.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
}

func (w *WS) dial(ctx context.Context) (*websocket.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	opts := &websocket.DialOptions{}
	if w.token != "" {
		opts.HTTPHeader = map[string][]string{"Authorization": {"Bearer " + w.token}}
	}
	conn, _, err := websocket.Dial(dialCtx, w.url, opts)
	if err != nil {
		return nil, err
	}
	if err := w.writeConn(dialCtx, conn, remoteHello()); err != nil {
		conn.Close(websocket.StatusNormalClosure, "hello failed")
		return nil, err
	}
	return conn, nil
}

func (w *WS) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.Read(context.Background())
		if err != nil {
			w.dropConn(conn, err)
			return
		}
		w.handleFrame(data)
	}
}

func (w *WS) handleFrame(data []byte) {
	ev, ok := parseRemoteFrame(data)
	if !ok {
		return
	}
	if ev.Type == frameRemoteHelloAck {
		w.capMu.Lock()
		w.capState = ev.Capabilities
		ready := w.capReady
		w.capReady = nil
		w.capMu.Unlock()
		if ready != nil {
			close(ready)
		}
		return
	}
	if ev.RPCID != "" && w.completeRPC(ev.RPCID, data) {
		return
	}
	if ev.Type == frameUserInputRequest {
		w.handleUserInputRequest(ev.SessionID, ev.RequestID, ev.Kind, ev.Header, ev.ToolUseID, ev.Agent, ev.Questions, ev.CreatedAt, ev.Status)
		return
	}
	if ev.Type == frameInteractionResolved {
		w.resolveInteraction(ev.RequestID, ev.SessionID, first(ev.Status, "resolved"))
		return
	}

	t := w.get(ev.SessionID)
	if t == nil {
		return
	}
	sid := first(ev.SessionID, t.session.ID)
	reqID := first(ev.RequestID, t.reqID)
	toolID := first(ev.ToolID, ev.ToolUseID, "tool")
	switch ev.Type {
	case "text_delta", "text_chunk":
		w.sink.Emit(backend.NewTextChunk(sid, reqID, first(ev.Delta, ev.Content)))
	case "thinking_delta", "thinking_chunk":
		w.sink.Emit(backend.NewThinkingChunk(sid, reqID, first(ev.Delta, ev.Content)))
	case "tool_start":
		t.mu.Lock()
		t.tools[toolID] = ""
		t.mu.Unlock()
		w.sink.Emit(backend.NewToolStart(sid, reqID, toolID, ev.Name, ev.Command))
	case "tool_delta":
		t.mu.Lock()
		if !strings.Contains(t.tools[toolID], backend.ToolResultTruncatedMark) {
			t.tools[toolID] = backend.TruncateToolOutput(t.tools[toolID] + first(ev.Delta, ev.Output))
		}
		out := t.tools[toolID]
		t.mu.Unlock()
		w.sink.Emit(backend.NewToolResult(sid, reqID, toolID, out))
	case "tool_result":
		w.sink.Emit(backend.NewToolResult(sid, reqID, toolID, ev.Output))
	case "tool_end":
		t.mu.Lock()
		delete(t.tools, toolID)
		t.mu.Unlock()
		w.sink.Emit(backend.NewToolEnd(sid, reqID, toolID))
	case "session_uuid":
		if ev.ResumeID != "" {
			t.session.SetResumeID(ev.ResumeID)
			w.sink.Emit(backend.NewSessionUUID(sid, ev.ResumeID))
		}
	case "done":
		t.markEnded()
		w.removeIfCurrent(sid, t)
		w.sink.Emit(backend.NewDone(sid, reqID))
	case "stopped":
		t.markEnded()
		w.removeIfCurrent(sid, t)
		w.sink.Emit(backend.NewStopped(sid, reqID))
	case "error":
		t.markEnded()
		w.removeIfCurrent(sid, t)
		w.sink.Emit(backend.NewError(sid, reqID, first(ev.Code, backend.ErrRemote), ev.Message))
	}
}

func (w *WS) LoadHistory(resumeID string, opts history.Opts) (*history.Result, error) {
	if !w.requireCapability(context.Background(), "history") {
		return nil, fmt.Errorf("remote history unsupported")
	}
	var out historyResultFrame
	if err := w.rpc(context.Background(), frameHistoryRequest, func(rpcID string) any {
		return historyRequest(rpcID, resumeID, opts)
	}, &out); err != nil {
		return nil, err
	}
	if out.Error != "" {
		return nil, errors.New(out.Error)
	}
	return &history.Result{
		Kind: out.Kind, Messages: out.Messages, SourceCount: out.SourceCount,
		KnownIDFound: out.KnownIDFound, SnapshotReason: out.SnapshotReason,
		HasMoreBefore: out.HasMoreBefore,
	}, nil
}

func (w *WS) ResumableSessions(limit int) ([]history.ResumableSession, error) {
	if !w.requireCapability(context.Background(), "history") {
		return nil, fmt.Errorf("remote history unsupported")
	}
	var out resumableSessionsResultFrame
	if err := w.rpc(context.Background(), frameResumableSessionsRequest, func(rpcID string) any {
		return resumableSessionsRequest(rpcID, limit)
	}, &out); err != nil {
		return nil, err
	}
	if out.Error != "" {
		return nil, errors.New(out.Error)
	}
	if out.Sessions == nil {
		out.Sessions = []history.ResumableSession{}
	}
	return out.Sessions, nil
}

func (w *WS) FetchUsage(ctx context.Context) (*backend.UsageReport, error) {
	if !w.requireCapability(ctx, "usage") {
		return nil, fmt.Errorf("remote usage unsupported")
	}
	var out usageResultFrame
	if err := w.rpc(ctx, frameUsageRequest, func(rpcID string) any {
		return usageRequest(rpcID)
	}, &out); err != nil {
		return nil, err
	}
	if out.Error != "" {
		return nil, errors.New(out.Error)
	}
	if out.Report == nil {
		return nil, fmt.Errorf("remote usage response missing report")
	}
	return usageReportFromWire(out.Report), nil
}

func (w *WS) RespondUserInput(id string, answers map[string]any, cancelled bool) bool {
	if !w.requireCapability(context.Background(), "interactions") {
		return false
	}
	w.interMu.Lock()
	payload := w.inter[id]
	if payload.RequestID == "" {
		for rid, p := range w.inter {
			if p.ToolUseID == id {
				payload = p
				id = rid
				break
			}
		}
	}
	w.interMu.Unlock()
	if payload.RequestID == "" {
		return false
	}
	conn, err := w.ensureConn(context.Background())
	if err != nil {
		return false
	}
	if err := w.writeConn(context.Background(), conn, userInputResponse(payload, answers, cancelled)); err != nil {
		return false
	}
	status := "resolved"
	if cancelled {
		status = "cancelled"
	}
	w.resolveInteraction(payload.RequestID, payload.SessionID, status)
	return true
}

func (w *WS) PendingInteractions(sessionID string) []backend.UserInputPayload {
	if !w.requireCapability(context.Background(), "interactions") {
		return []backend.UserInputPayload{}
	}
	w.interMu.Lock()
	defer w.interMu.Unlock()
	out := []backend.UserInputPayload{}
	for _, p := range w.inter {
		if sessionID == "" || p.SessionID == sessionID {
			out = append(out, p)
		}
	}
	return out
}

func (w *WS) handleUserInputRequest(sessionID, requestID, kind, header, toolUseID, agent string, questions []backend.UserInputQuestion, createdAt int64, status string) {
	if requestID == "" || sessionID == "" {
		return
	}
	if kind == "" {
		kind = "ask_user_question"
	}
	if status == "" {
		status = "pending"
	}
	if createdAt == 0 {
		createdAt = time.Now().UnixMilli()
	}
	payload := backend.UserInputPayload{
		RequestID: requestID, SessionID: sessionID, Source: "remote-ws",
		Kind: kind, Header: header, ToolUseID: toolUseID, RequestingAgent: agent,
		Questions: questions, CreatedAt: createdAt, Status: status,
	}
	w.interMu.Lock()
	w.inter[requestID] = payload
	w.interMu.Unlock()
	w.sink.Emit(backend.NewUserInputRequest(payload))
}

func (w *WS) resolveInteraction(requestID, sessionID, status string) {
	if requestID == "" {
		return
	}
	w.interMu.Lock()
	payload := w.inter[requestID]
	delete(w.inter, requestID)
	w.interMu.Unlock()
	if sessionID == "" {
		sessionID = payload.SessionID
	}
	w.sink.Emit(backend.NewInteractionResolved(requestID, sessionID, status))
}

func (w *WS) rpc(ctx context.Context, typ string, build func(rpcID string) any, out any) error {
	conn, err := w.ensureConn(ctx)
	if err != nil {
		return err
	}
	id := "rpc_" + strconv.FormatUint(w.nextRPC.Add(1), 10)
	ch := make(chan rpcReply, 1)
	w.mu.Lock()
	w.pending[id] = ch
	w.mu.Unlock()
	payload := build(id)
	if err := w.writeConn(ctx, conn, payload); err != nil {
		w.mu.Lock()
		delete(w.pending, id)
		w.mu.Unlock()
		return err
	}
	select {
	case reply := <-ch:
		if reply.err != nil {
			return reply.err
		}
		return json.Unmarshal(reply.raw, out)
	case <-time.After(30 * time.Second):
		w.mu.Lock()
		delete(w.pending, id)
		w.mu.Unlock()
		return fmt.Errorf("remote rpc %s timed out", typ)
	case <-ctx.Done():
		w.mu.Lock()
		delete(w.pending, id)
		w.mu.Unlock()
		return ctx.Err()
	}
}

func (w *WS) requireCapability(ctx context.Context, name string) bool {
	if _, err := w.ensureConn(ctx); err != nil {
		return false
	}
	w.capMu.Lock()
	ready := w.capReady
	if ready == nil {
		ok := w.capState[name]
		w.capMu.Unlock()
		return ok
	}
	w.capMu.Unlock()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return false
	}
	w.capMu.Lock()
	defer w.capMu.Unlock()
	return w.capState[name]
}

func (w *WS) completeRPC(id string, raw json.RawMessage) bool {
	w.mu.Lock()
	ch := w.pending[id]
	if ch != nil {
		delete(w.pending, id)
	}
	w.mu.Unlock()
	if ch == nil {
		return false
	}
	ch <- rpcReply{raw: raw}
	return true
}

func (w *WS) hasCapability(name string) bool {
	w.capMu.Lock()
	defer w.capMu.Unlock()
	return w.capState[name]
}

func (w *WS) currentConn() *websocket.Conn {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn
}

func (w *WS) get(sessionID string) *turn {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.active[sessionID]
}

func (w *WS) removeIfCurrent(sessionID string, t *turn) {
	w.mu.Lock()
	if w.active[sessionID] == t {
		delete(w.active, sessionID)
	}
	w.mu.Unlock()
}

func (w *WS) dropConn(conn *websocket.Conn, err error) {
	w.mu.Lock()
	if w.conn != conn {
		w.mu.Unlock()
		return
	}
	w.conn = nil
	active := make([]*turn, 0, len(w.active))
	for _, t := range w.active {
		active = append(active, t)
	}
	w.active = make(map[string]*turn)
	pending := w.pending
	w.pending = make(map[string]chan rpcReply)
	w.mu.Unlock()
	w.interMu.Lock()
	var interactions []backend.UserInputPayload
	for _, p := range w.inter {
		interactions = append(interactions, p)
	}
	w.inter = make(map[string]backend.UserInputPayload)
	w.interMu.Unlock()
	w.capMu.Lock()
	ready := w.capReady
	w.capReady = nil
	w.capState = make(map[string]bool)
	w.capMu.Unlock()
	if ready != nil {
		close(ready)
	}
	_ = conn.Close(websocket.StatusNormalClosure, "")
	for _, t := range active {
		w.fail(t, backend.ErrRemoteDisconnected, err.Error())
	}
	for _, ch := range pending {
		ch <- rpcReply{err: fmt.Errorf("remote disconnected: %w", err)}
	}
	for _, p := range interactions {
		w.sink.Emit(backend.NewInteractionResolved(p.RequestID, p.SessionID, "expired"))
	}
}

func (w *WS) failLocked(t *turn, code, msg string) {
	if t.markEnded() {
		w.sink.Emit(backend.NewError(t.session.ID, t.reqID, code, msg))
	}
}

func (w *WS) fail(t *turn, code, msg string) {
	w.failLocked(t, code, msg)
}

func (w *WS) writeConn(ctx context.Context, conn *websocket.Conn, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	return conn.Write(wctx, websocket.MessageText, data)
}

func (t *turn) markEnded() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ended {
		return false
	}
	t.ended = true
	return true
}

func first(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
