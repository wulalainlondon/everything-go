package goexec

import (
	"encoding/json"

	"everything-go/internal/executor"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type userInputRegistrar interface {
	RegisterUserInputRequest(s *session.Session, toolUseID, agent string, input json.RawMessage)
}

// toolNormalizer owns backend-agnostic tool presentation rules that require
// state, such as folding Claude Task* tools into the normalized todo panel and
// suppressing their raw tool cards.
type toolNormalizer struct {
	sink      executor.Sink
	questions userInputRegistrar

	todos      *todoStore
	suppressed map[string]bool
}

func newToolNormalizer(sink executor.Sink, questions userInputRegistrar) *toolNormalizer {
	return &toolNormalizer{
		sink:       sink,
		questions:  questions,
		todos:      newTodoStore(),
		suppressed: map[string]bool{},
	}
}

func (n *toolNormalizer) Reset() {
	n.todos.reset()
	n.suppressed = map[string]bool{}
}

// HandleClaudeToolUse returns true when the raw tool_use was consumed and must
// not be emitted as a normal tool_start.
func (n *toolNormalizer) HandleClaudeToolUse(sessionID, reqID, toolUseID, name string, input json.RawMessage) bool {
	if !todoTools[name] {
		return false
	}
	changed := false
	switch name {
	case "TodoWrite":
		changed = n.todos.applyTodoWrite(input)
	case "TaskCreate":
		changed = n.todos.noteCreate(toolUseID, input)
	case "TaskUpdate":
		changed = n.todos.applyUpdate(input)
	default: // TaskDelete
		changed = n.todos.applyDelete(input)
	}
	if toolUseID != "" {
		n.suppressed[toolUseID] = true
	}
	if changed {
		n.sink.Emit(protocol.NewTodoUpdate(sessionID, reqID, n.todos.asList()))
	}
	return true
}

// HandleClaudeVisibleToolUse handles visible, non-suppressed tools that still
// need side effects before their normal tool_start is emitted.
func (n *toolNormalizer) HandleClaudeVisibleToolUse(s *session.Session, toolUseID, name string, input json.RawMessage) {
	if name == "AskUserQuestion" && n.questions != nil {
		n.questions.RegisterUserInputRequest(s, toolUseID, name, input)
	}
}

// HandleClaudeToolResult returns true when the raw tool_result was consumed and
// must not be emitted as a normal tool_result/tool_end.
func (n *toolNormalizer) HandleClaudeToolResult(sessionID, reqID, toolUseID, output string) bool {
	if !n.suppressed[toolUseID] {
		return false
	}
	delete(n.suppressed, toolUseID)
	if n.todos.resolveCreate(toolUseID, output) {
		n.sink.Emit(protocol.NewTodoUpdate(sessionID, reqID, n.todos.asList()))
	}
	return true
}
