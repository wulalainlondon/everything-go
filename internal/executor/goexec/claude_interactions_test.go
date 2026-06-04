package goexec

import (
	"encoding/json"
	"sync"
	"testing"

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

func (s *capSink) count(match func(any) bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.events {
		if match(e) {
			n++
		}
	}
	return n
}

// TestNormalizeQuestions ports the parity cases from interactions.py: option
// label/id fallback, choice vs multi_choice vs free-form question inference.
func TestNormalizeQuestions(t *testing.T) {
	input := json.RawMessage(`{"questions":[
		{"question":"Pick one","header":"Choice","multiSelect":false,
		 "options":[{"label":"A","description":"first"},{"label":"B"}]},
		{"question":"Anything?"}
	]}`)
	qs := normalizeQuestions(input)
	if len(qs) != 2 {
		t.Fatalf("want 2 questions, got %d", len(qs))
	}
	q0 := qs[0]
	if q0.QuestionID != "q1" || q0.Text != "Pick one" || q0.Header != "Choice" {
		t.Errorf("q0 fields wrong: %+v", q0)
	}
	if q0.Type != "choice" || q0.MultiSelect || q0.FreeForm {
		t.Errorf("q0 should be a single-choice: %+v", q0)
	}
	if len(q0.Options) != 2 || q0.Options[0].ID != "A" || q0.Options[0].Label != "A" ||
		q0.Options[0].Description != "first" || q0.Options[1].ID != "B" {
		t.Errorf("q0 options wrong: %+v", q0.Options)
	}
	// No options, no type → free-form question.
	if qs[1].Type != "question" || !qs[1].FreeForm {
		t.Errorf("q1 should be free-form question: %+v", qs[1])
	}
}

func TestNormalizeQuestionsMultiSelect(t *testing.T) {
	qs := normalizeQuestions(json.RawMessage(`{"questions":[{"question":"Pick many","multiSelect":true,"options":[{"label":"X"}]}]}`))
	if len(qs) != 1 || qs[0].Type != "multi_choice" || !qs[0].MultiSelect {
		t.Fatalf("expected multi_choice: %+v", qs)
	}
}

// TestBuildUserInputResultText pins Claude Code's native AskUserQuestion answer
// shape: question TEXT + option LABEL (the app sends question_id + option id, so
// we must map back), the exact sentence wrapper, multi-select joining, free-form
// passthrough, and the dismissed fallback.
func TestBuildUserInputResultText(t *testing.T) {
	p := protocol.UserInputRequestPayload{
		Questions: []protocol.UserInputQuestion{{
			QuestionID: "q1", Text: "Pick a color",
			Options: []protocol.UserInputOption{{ID: "opt_red", Label: "Red"}, {ID: "opt_blue", Label: "Blue"}},
		}},
	}
	got := buildUserInputResultText(p, map[string]any{"q1": "opt_blue"}, false)
	want := `User has answered your questions: "Pick a color"="Blue". You can now continue with the user's answers in mind.`
	if got != want {
		t.Errorf("single:\n got=%s\nwant=%s", got, want)
	}

	// Multi-select: ids resolved to labels, joined.
	pm := protocol.UserInputRequestPayload{Questions: []protocol.UserInputQuestion{{
		QuestionID: "q1", Text: "Pick colors", MultiSelect: true,
		Options: []protocol.UserInputOption{{ID: "r", Label: "Red"}, {ID: "b", Label: "Blue"}},
	}}}
	if got := buildUserInputResultText(pm, map[string]any{"q1": []any{"r", "b"}}, false); got != `User has answered your questions: "Pick colors"="Red, Blue". You can now continue with the user's answers in mind.` {
		t.Errorf("multi: %s", got)
	}

	// Free-form: value isn't an option id → passed through.
	pf := protocol.UserInputRequestPayload{Questions: []protocol.UserInputQuestion{{QuestionID: "q1", Text: "Your name?"}}}
	if got := buildUserInputResultText(pf, map[string]any{"q1": "Alice"}, false); got != `User has answered your questions: "Your name?"="Alice". You can now continue with the user's answers in mind.` {
		t.Errorf("freeform: %s", got)
	}

	// Cancelled / empty → dismissed.
	if got := buildUserInputResultText(p, nil, true); got != "The user dismissed the question(s) without answering. Continue without their input." {
		t.Errorf("cancelled: %s", got)
	}
	if got := buildUserInputResultText(p, map[string]any{}, false); got != "The user dismissed the question(s) without answering. Continue without their input." {
		t.Errorf("empty: %s", got)
	}
}

// TestInteractionLifecycle drives register → list → respond → resolved without a
// live subprocess (RespondUserInput tolerates a nil proc, skipping the stdin
// write but still resolving the interaction).
func TestInteractionLifecycle(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	reg := session.NewRegistry()
	s := reg.Create("s1", "n", "/tmp", "claude", "", "", "")

	c.registerInteraction(s, "toolu_1", "AskUserQuestion",
		json.RawMessage(`{"header":"H","questions":[{"question":"Q?","options":[{"label":"Yes"}]}]}`))

	reqs := sink.count(func(e any) bool { _, ok := e.(protocol.UserInputRequestEvent); return ok })
	if reqs != 1 {
		t.Fatalf("want 1 user_input_request emitted, got %d", reqs)
	}
	pending := c.PendingInteractions("s1")
	if len(pending) != 1 || pending[0].Header != "H" || pending[0].ToolUseID != "toolu_1" {
		t.Fatalf("pending wrong: %+v", pending)
	}
	reqID := pending[0].RequestID

	// Unknown id → false, nothing resolved.
	if c.RespondUserInput("nope", nil, false) {
		t.Fatal("unknown id should not match")
	}
	// Correct id → resolves.
	if !c.RespondUserInput(reqID, map[string]any{"Q?": "Yes"}, false) {
		t.Fatal("respond should match the pending interaction")
	}
	if len(c.PendingInteractions("s1")) != 0 {
		t.Fatal("interaction should be cleared after respond")
	}
	resolved := sink.count(func(e any) bool {
		r, ok := e.(protocol.InteractionResolved)
		return ok && r.RequestID == reqID && r.Status == "resolved"
	})
	if resolved != 1 {
		t.Fatalf("want 1 interaction_resolved, got %d", resolved)
	}
}

// TestRespondByToolUseIDAlias confirms the answer can reference the tool_use_id
// instead of the request_id.
func TestRespondByToolUseIDAlias(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	reg := session.NewRegistry()
	s := reg.Create("s2", "n", "/tmp", "claude", "", "", "")
	c.registerInteraction(s, "toolu_xyz", "AskUserQuestion", json.RawMessage(`{"questions":[{"question":"Q?"}]}`))
	if !c.RespondUserInput("toolu_xyz", map[string]any{}, false) {
		t.Fatal("respond by tool_use_id alias should match")
	}
}

// TestCancelInteractionsFor confirms killing a session clears its prompts.
func TestCancelInteractionsFor(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	reg := session.NewRegistry()
	s := reg.Create("s3", "n", "/tmp", "claude", "", "", "")
	c.registerInteraction(s, "t1", "AskUserQuestion", json.RawMessage(`{"questions":[{"question":"Q?"}]}`))
	c.cancelInteractionsFor("s3")
	if len(c.PendingInteractions("s3")) != 0 {
		t.Fatal("cancel should clear pending interactions")
	}
	cancelled := sink.count(func(e any) bool {
		r, ok := e.(protocol.InteractionResolved)
		return ok && r.Status == "cancelled"
	})
	if cancelled != 1 {
		t.Fatalf("want 1 cancelled interaction_resolved, got %d", cancelled)
	}
}
