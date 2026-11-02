package goexec

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func nativeBatchFixture(t *testing.T) (*Codex, *session.Session, *capSink, protocol.AsyncBatchRequest) {
	c, s, sink := asyncFixture(t)
	s.SetConfigRevision(1)
	emitAsyncFixture(c, s)
	r := protocol.AsyncBatchRequest{Authority: "authority", SessionID: s.ID, RequestID: "ui_async_batch_11111111-1111-4111-8111-111111111111", ThreadID: s.ResumeID(), ConfigRevision: s.SettingsSnapshot().ConfigRevision}
	for _, p := range c.PendingInteractions(s.ID) {
		r.Selected = append(r.Selected, protocol.AsyncBatchSelection{RequestID: p.RequestID, QuestionRevision: p.QuestionRevision, Answers: map[string]any{p.Questions[0].QuestionID: "One shared explicit answer"}})
	}
	return c, s, sink, r
}

func TestNativeAsyncBatchOneEnvelopeAndCanonicalConsumption(t *testing.T) {
	c, s, sink, r := nativeBatchFixture(t)
	text, err := c.PrepareAsyncBatch(s, r)
	if err != nil {
		t.Fatal(err)
	}
	replies := parseAsyncReply(text)
	if len(replies) != 2 {
		t.Fatal("not one envelope containing all selected questions", text)
	}
	for _, row := range replies {
		if row.Answer != "One shared explicit answer" || row.QuestionItemID == "" {
			t.Fatal(row)
		}
	}
	if len(c.PendingInteractions(s.ID)) != 2 {
		t.Fatal("preparation consumed questions")
	}
	if _, err := c.validateAsyncReply(s, r.RequestID, text); err == nil {
		t.Fatal("batch weakened original single validator")
	}
	if _, err := c.validateAsyncInput(context.Background(), s, r.RequestID, text); err == nil {
		t.Fatal("client envelope bypassed typed binding")
	}
	b := &backend.AsyncBatchBinding{Request: r, Configuration: session.ConfigurationFrom(s.Snapshot()), Cwd: s.Cwd(), SourceRevision: s.Snapshot().SourceRevision, ContentHash: backend.AsyncContentHash(text)}
	if thread, err := c.validateAsyncInput(backend.WithAsyncBatch(context.Background(), b), s, r.RequestID, text); err != nil || thread != r.ThreadID {
		t.Fatal(thread, err)
	}
	st := c.state(s.ID)
	st.turnActive, st.currentTurnID, st.reqID = true, "live-turn", "current-request"
	writes := 0
	w := &toolTestWriter{c: c}
	w.reply = func(method string, raw json.RawMessage) (any, error) {
		writes++
		if method != "turn/steer" {
			t.Fatal("unexpected native transport", method)
		}
		var wire struct {
			Input []struct {
				Text string `json:"text"`
			} `json:"input"`
		}
		if json.Unmarshal(raw, &wire) != nil || len(wire.Input) != 1 || wire.Input[0].Text != text {
			t.Fatal("batch split or changed", string(raw))
		}
		return map[string]any{"turnId": "live-turn"}, nil
	}
	c.rpc.setWriter(w)
	if _, err := c.steerActiveTurn(s, r.RequestID, text, nil, nil); err != nil {
		t.Fatal(err)
	}
	if writes != 1 || len(c.PendingInteractions(s.ID)) != 2 {
		t.Fatal("ACK consumed batch or split transport", writes)
	}
	st.reqID = r.RequestID
	st.asyncBatch = b
	w.reply = func(method string, _ json.RawMessage) (any, error) {
		if method != "turn/start" {
			t.Fatal(method)
		}
		return map[string]any{"turn": map[string]any{"id": "started", "status": "inProgress"}}, nil
	}
	if err := c.startTurnWithStaleRetry(s, st, r.ThreadID, []map[string]any{{"type": "text", "text": text}}, ""); err != nil {
		t.Fatal(err)
	}
	if len(c.PendingInteractions(s.ID)) != 2 {
		t.Fatal("turn/start ACK consumed batch")
	}
	content, _ := json.Marshal([]map[string]string{{"type": "text", "text": text}})
	c.dispatch(json.RawMessage(`{"method":"item/completed","params":{"threadId":"` + r.ThreadID + `","item":{"type":"userMessage","content":` + string(content) + `}}}`))
	if len(c.PendingInteractions(s.ID)) != 0 {
		t.Fatal("canonical batch did not consume each question")
	}
	if sink.count(func(e any) bool { _, ok := e.(backend.NativeAsyncBatchConsumed); return ok }) != 1 {
		t.Fatal("missing exact canonical envelope receipt")
	}
	data, err := os.ReadFile(c.asyncTombstonePath(r.ThreadID))
	if err != nil || strings.Contains(string(data), "shared explicit") {
		t.Fatal("answer leaked into tombstones", err)
	}
	emitAsyncFixture(c, s)
	if len(c.PendingInteractions(s.ID)) != 0 {
		t.Fatal("canonical replay resurrected selected question")
	}
}

func TestNativeAsyncBatchRejectsChangedDescriptorMappingAndScope(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*session.Session, *protocol.AsyncBatchRequest, *Codex)
	}{
		{"duplicate", func(_ *session.Session, r *protocol.AsyncBatchRequest, _ *Codex) {
			r.Selected = append(r.Selected, r.Selected[0])
		}},
		{"revision", func(_ *session.Session, r *protocol.AsyncBatchRequest, _ *Codex) {
			r.Selected[1].QuestionRevision = "stale"
		}},
		{"missing-answer", func(_ *session.Session, r *protocol.AsyncBatchRequest, _ *Codex) {
			r.Selected[1].Answers = map[string]any{}
		}},
		{"wrong-item", func(_ *session.Session, r *protocol.AsyncBatchRequest, _ *Codex) {
			r.Selected[1].Answers = map[string]any{"other": "answer"}
		}},
		{"nontext", func(_ *session.Session, r *protocol.AsyncBatchRequest, _ *Codex) {
			for k := range r.Selected[1].Answers {
				r.Selected[1].Answers[k] = true
			}
		}},
		{"thread", func(s *session.Session, _ *protocol.AsyncBatchRequest, _ *Codex) { s.SetResumeID("other") }},
		{"effective-config-same-revision", func(s *session.Session, _ *protocol.AsyncBatchRequest, _ *Codex) { s.SetModel("different") }},
		{"config", func(s *session.Session, _ *protocol.AsyncBatchRequest, _ *Codex) {
			s.SetConfigRevision(s.SettingsSnapshot().ConfigRevision + 1)
		}},
		{"expired", func(_ *session.Session, r *protocol.AsyncBatchRequest, c *Codex) {
			ci := c.interactions[r.Selected[1].RequestID]
			ci.payload.ExpiresAt = time.Now().Add(-time.Second).UnixMilli()
			c.interactions[r.Selected[1].RequestID] = ci
		}},
		{"too-large", func(_ *session.Session, r *protocol.AsyncBatchRequest, _ *Codex) {
			for k := range r.Selected[1].Answers {
				r.Selected[1].Answers[k] = strings.Repeat("a", 65536)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, s, _, r := nativeBatchFixture(t)
			tc.mutate(s, &r, c)
			if _, err := c.PrepareAsyncBatch(s, r); err == nil {
				t.Fatal("invalid all-or-none batch accepted")
			}
			if len(c.PendingInteractions(s.ID)) != 2 {
				t.Fatal("refusal mutated pending")
			}
		})
	}
}

func TestNativeAsyncBatchReservationAndBindingFence(t *testing.T) {
	c, s, _, r := nativeBatchFixture(t)
	text, release, err := c.ReserveAsyncBatch(s, r)
	if err != nil {
		t.Fatal(err)
	}
	blocked := make(chan struct{})
	go func() { c.resolveAsyncReplies(s, r.ThreadID, text); close(blocked) }()
	select {
	case <-blocked:
		t.Fatal("canonical descriptor changed during queue commit reservation")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("reservation did not release")
	}
	if len(c.PendingInteractions(s.ID)) != 0 {
		t.Fatal("canonical close lost after reservation")
	}
	c, s, _, r = nativeBatchFixture(t)
	text, _ = c.PrepareAsyncBatch(s, r)
	b := &backend.AsyncBatchBinding{Request: r, Configuration: session.ConfigurationFrom(s.Snapshot()), Cwd: s.Cwd(), SourceRevision: s.Snapshot().SourceRevision, ContentHash: backend.AsyncContentHash(text)}
	s.SetModel("changed")
	s.SetModel("")
	if _, err := c.validateAsyncInput(backend.WithAsyncBatch(context.Background(), b), s, r.RequestID, text); err == nil {
		t.Fatal("ABA source accepted")
	}
}

func TestNativeAsyncBatchUsesActiveExecutionNotFutureDefaults(t *testing.T) {
	c, s, _, r := nativeBatchFixture(t)
	cfg := session.ConfigurationFrom(s.Snapshot())
	cfg.Model = "future-only-model"
	if _, err := s.SetFutureConfiguration(cfg, nil); err != nil {
		t.Fatal(err)
	}
	if s.Snapshot().ConfigRevision != 1 || s.SettingsSnapshot().ConfigRevision != 2 {
		t.Fatal("fixture did not separate active/future")
	}
	c.dispatch(json.RawMessage(`{"method":"item/completed","params":{"threadId":"` + s.ResumeID() + `","item":` + strings.ReplaceAll(asyncItemJSON, "call-fixture", "future-call") + `}}`))
	for _, q := range c.PendingInteractions(s.ID) {
		if q.ConfigRevision != 1 {
			t.Fatal("future default mislabeled native active question", q)
		}
	}
	text, err := c.PrepareAsyncBatch(s, r)
	if err != nil {
		t.Fatal("future default blocked original active reply", err)
	}
	b := &backend.AsyncBatchBinding{Request: r, Configuration: session.ConfigurationFrom(s.Snapshot()), Cwd: s.Cwd(), SourceRevision: s.Snapshot().SourceRevision, ContentHash: backend.AsyncContentHash(text)}
	if _, err := c.validateAsyncInput(backend.WithAsyncBatch(context.Background(), b), s, r.RequestID, text); err != nil {
		t.Fatal(err)
	}
	bad := r
	bad.ConfigRevision = 2
	if _, err := c.PrepareAsyncBatch(s, bad); err == nil {
		t.Fatal("future revision crossed active native source")
	}
}

func TestNativeAsyncBatchWriterReservationAndLateRetirement(t *testing.T) {
	for _, retire := range []bool{false, true} {
		t.Run(map[bool]string{false: "holds-through-write", true: "retired-during-writer-wait"}[retire], func(t *testing.T) {
			c, s, _, r := nativeBatchFixture(t)
			text, err := c.PrepareAsyncBatch(s, r)
			if err != nil {
				t.Fatal(err)
			}
			b := &backend.AsyncBatchBinding{Request: r, Configuration: session.ConfigurationFrom(s.Snapshot()), Cwd: s.Cwd(), SourceRevision: s.Snapshot().SourceRevision, ContentHash: backend.AsyncContentHash(text)}
			ctx := backend.WithAsyncBatch(context.Background(), b)
			st := c.state(s.ID)
			st.turnActive = true
			st.reqID = "current"
			st.currentTurnID = "live"
			w := &toolTestWriter{c: c}
			writes := 0
			w.reply = func(string, json.RawMessage) (any, error) {
				writes++
				if c.interMu.TryLock() {
					c.interMu.Unlock()
					t.Error("descriptors not held through original writer bytes")
				}
				return map[string]any{"turnId": "live"}, nil
			}
			c.rpc.setWriter(w)
			c.rpc.wmu.Lock()
			done := make(chan error, 1)
			go func() {
				_, e := c.steerActiveTurnReserved(s, r.RequestID, text, nil, nil, func() (func(), error) { return c.reserveAsyncInput(ctx, s, r.RequestID, text) })
				done <- e
			}()
			if retire {
				s.SetModel("retired-effective-config")
			}
			c.rpc.wmu.Unlock()
			select {
			case err := <-done:
				if retire && (err == nil || writes != 0) {
					t.Fatal("retired source reached native writer", err, writes)
				}
				if !retire && (err != nil || writes != 1) {
					t.Fatal("valid single envelope failed", err, writes)
				}
			case <-time.After(time.Second):
				t.Fatal("writer reservation blocked")
			}
			if !c.interMu.TryLock() {
				t.Fatal("descriptor lock leaked past write/ACK")
			}
			c.interMu.Unlock()
		})
	}
}
