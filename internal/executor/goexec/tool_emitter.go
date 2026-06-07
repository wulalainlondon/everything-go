package goexec

import (
	"strings"
	"sync"

	"everything-go/internal/backend"
	"everything-go/internal/executor"
)

// toolEmitter is the shared bridge from backend-specific tool events to the
// normalized wire protocol. Backends still parse their own streams, but they no
// longer hand-roll tool_start/tool_result/tool_end emission.
type toolEmitter struct {
	sink executor.Sink

	mu      sync.Mutex
	outputs map[string]string
}

func newToolEmitter(sink executor.Sink) *toolEmitter {
	return &toolEmitter{sink: sink, outputs: make(map[string]string)}
}

func toolKey(sessionID, reqID, toolUseID string) string {
	return sessionID + "\x00" + reqID + "\x00" + toolUseID
}

func (e *toolEmitter) Start(sessionID, reqID, toolUseID, name, command string) {
	if toolUseID == "" {
		toolUseID = "tool"
	}
	e.mu.Lock()
	e.outputs[toolKey(sessionID, reqID, toolUseID)] = ""
	e.mu.Unlock()
	e.sink.Emit(backend.NewToolStart(sessionID, reqID, toolUseID, name, command))
}

func (e *toolEmitter) Result(sessionID, reqID, toolUseID, output string) {
	if toolUseID == "" {
		toolUseID = "tool"
	}
	e.sink.Emit(backend.NewToolResult(sessionID, reqID, toolUseID, output))
}

func (e *toolEmitter) Delta(sessionID, reqID, toolUseID, delta string) string {
	if toolUseID == "" {
		toolUseID = "tool"
	}
	k := toolKey(sessionID, reqID, toolUseID)
	e.mu.Lock()
	if !strings.Contains(e.outputs[k], backend.ToolResultTruncatedMark) {
		e.outputs[k] = backend.TruncateToolOutput(e.outputs[k] + delta)
	}
	out := e.outputs[k]
	e.mu.Unlock()
	e.Result(sessionID, reqID, toolUseID, out)
	return out
}

func (e *toolEmitter) End(sessionID, reqID, toolUseID string) {
	if toolUseID == "" {
		toolUseID = "tool"
	}
	e.mu.Lock()
	delete(e.outputs, toolKey(sessionID, reqID, toolUseID))
	e.mu.Unlock()
	e.sink.Emit(backend.NewToolEnd(sessionID, reqID, toolUseID))
}

func (e *toolEmitter) ResultEnd(sessionID, reqID, toolUseID, output string) {
	e.Result(sessionID, reqID, toolUseID, output)
	e.End(sessionID, reqID, toolUseID)
}

func (e *toolEmitter) ResetSession(sessionID string) {
	e.mu.Lock()
	prefix := sessionID + "\x00"
	for k := range e.outputs {
		if strings.HasPrefix(k, prefix) {
			delete(e.outputs, k)
		}
	}
	e.mu.Unlock()
}
