package goexec

import (
	"encoding/json"
	"testing"

	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func TestToolNormalizerConsumesClaudeTodoTools(t *testing.T) {
	sink := &capSink{}
	n := newToolNormalizer(sink, nil)

	if !n.HandleClaudeToolUse("s1", "r1", "tu1", "TaskCreate", json.RawMessage(`{"subject":"Build it"}`)) {
		t.Fatal("TaskCreate should be consumed")
	}
	if !n.HandleClaudeToolResult("s1", "r1", "tu1", "Task #9 created successfully") {
		t.Fatal("TaskCreate result should be consumed")
	}

	var updates []protocol.TodoUpdate
	for _, e := range sink.events {
		if tu, ok := e.(protocol.TodoUpdate); ok {
			updates = append(updates, tu)
		}
	}
	if len(updates) != 2 {
		t.Fatalf("want create + resolve todo updates, got %d: %+v", len(updates), updates)
	}
	last := updates[len(updates)-1]
	if len(last.Todos) != 1 || last.Todos[0].ID != "9" || last.Todos[0].Content != "Build it" {
		t.Fatalf("resolved todo wrong: %+v", last)
	}
}

func TestToolNormalizerPassesThroughNormalTools(t *testing.T) {
	n := newToolNormalizer(&capSink{}, nil)
	if n.HandleClaudeToolUse("s1", "r1", "tu1", "Bash", json.RawMessage(`{"command":"ls"}`)) {
		t.Fatal("normal tools should not be consumed")
	}
	if n.HandleClaudeToolResult("s1", "r1", "tu1", "file.txt") {
		t.Fatal("normal tool results should not be consumed")
	}
}

type captureRegistrar struct {
	sessionID string
	toolUseID string
	agent     string
	input     string
}

func (r *captureRegistrar) RegisterUserInputRequest(s *session.Session, toolUseID, agent string, input json.RawMessage) {
	r.sessionID = s.ID
	r.toolUseID = toolUseID
	r.agent = agent
	r.input = string(input)
}

func TestToolNormalizerRegistersAskUserQuestionVisibleTool(t *testing.T) {
	reg := session.NewRegistry()
	s := reg.Create("s1", "n", "/tmp", "claude", "", "", "")
	registrar := &captureRegistrar{}
	n := newToolNormalizer(&capSink{}, registrar)

	n.HandleClaudeVisibleToolUse(s, "ask1", "AskUserQuestion", json.RawMessage(`{"questions":[{"question":"Continue?"}]}`))

	if registrar.sessionID != "s1" || registrar.toolUseID != "ask1" || registrar.agent != "AskUserQuestion" {
		t.Fatalf("AskUserQuestion registration wrong: %+v", registrar)
	}
}
