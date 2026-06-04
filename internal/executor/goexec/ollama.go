// Ollama executor: streams from a local Ollama server over HTTP.
// Fidelity reference: bridge/backends/ollama.py.
package goexec

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"everything-go/internal/executor"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

const ollamaHistoryCap = 200

type ollamaMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Ollama implements executor.Executor over POST {host}/api/chat.
type Ollama struct {
	sink         executor.Sink
	host         string
	defaultModel string

	mu        sync.Mutex
	histories map[string][]ollamaMsg
	cancels   map[string]context.CancelFunc
}

func NewOllama(sink executor.Sink, host, model string) *Ollama {
	if host == "" {
		host = "http://localhost:11434"
	}
	if model == "" {
		model = "llama3.2"
	}
	return &Ollama{
		sink: sink, host: host, defaultModel: model,
		histories: make(map[string][]ollamaMsg),
		cancels:   make(map[string]context.CancelFunc),
	}
}

func (o *Ollama) Send(ctx context.Context, s *session.Session, reqID, content string, _ []protocol.InboundImage, _ []protocol.InboundFile) error {
	// Ollama backend is text-only for now; image/file attachments are ignored.
	o.mu.Lock()
	hist := append(o.histories[s.ID], ollamaMsg{Role: "user", Content: content})
	hist = capHistory(hist)
	o.histories[s.ID] = hist
	turnCtx, cancel := context.WithCancel(context.Background())
	o.cancels[s.ID] = cancel
	o.mu.Unlock()

	go o.runTurn(turnCtx, s, reqID, hist)
	return nil
}

func (o *Ollama) runTurn(ctx context.Context, s *session.Session, reqID string, hist []ollamaMsg) {
	model := s.Snapshot().Model
	if model == "" {
		model = o.defaultModel
	}
	body, _ := json.Marshal(map[string]any{"model": model, "messages": hist, "stream": true})
	req, err := http.NewRequestWithContext(ctx, "POST", o.host+"/api/chat", bytes.NewReader(body))
	if err != nil {
		o.fail(s, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 300 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return // stopped
		}
		o.fail(s, err)
		return
	}
	defer resp.Body.Close()

	full := ""
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	for sc.Scan() {
		if ctx.Err() != nil {
			return
		}
		var d struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			Done bool `json:"done"`
		}
		if json.Unmarshal(sc.Bytes(), &d) != nil {
			continue
		}
		if d.Message.Content != "" {
			full += d.Message.Content
			o.sink.Emit(protocol.NewTextChunk(s.ID, reqID, d.Message.Content))
		}
		if d.Done {
			break
		}
	}

	o.mu.Lock()
	o.histories[s.ID] = capHistory(append(o.histories[s.ID], ollamaMsg{Role: "assistant", Content: full}))
	o.mu.Unlock()

	o.sink.Emit(protocol.NewDone(s.ID, reqID))
}

func (o *Ollama) fail(s *session.Session, err error) {
	o.sink.Emit(protocol.NewError(s.ID, "", "Ollama error: "+err.Error()))
}

func (o *Ollama) Stop(ctx context.Context, s *session.Session) error {
	o.mu.Lock()
	if cancel := o.cancels[s.ID]; cancel != nil {
		cancel()
	}
	o.mu.Unlock()
	o.sink.Emit(protocol.NewStopped(s.ID, ""))
	return nil
}

func (o *Ollama) Clear(ctx context.Context, s *session.Session) error {
	o.mu.Lock()
	o.histories[s.ID] = nil
	o.mu.Unlock()
	o.sink.Emit(protocol.NewSessionWarning(s.ID, "History cleared."))
	return nil
}

func (o *Ollama) Close(ctx context.Context, s *session.Session) error {
	o.mu.Lock()
	delete(o.histories, s.ID)
	delete(o.cancels, s.ID)
	o.mu.Unlock()
	return nil
}

func capHistory(h []ollamaMsg) []ollamaMsg {
	if len(h) > ollamaHistoryCap {
		return h[len(h)-ollamaHistoryCap:]
	}
	return h
}
