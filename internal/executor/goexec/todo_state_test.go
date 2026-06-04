package goexec

import (
	"encoding/json"
	"strings"
	"testing"

	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func raw(s string) json.RawMessage { return json.RawMessage(s) }

// TestTodoStoreTaskLifecycle walks the Claude 2.1.142+ protocol: create (id from
// result) → update status → delete by id.
func TestTodoStoreTaskLifecycle(t *testing.T) {
	st := newTodoStore()

	if !st.noteCreate("tu1", raw(`{"subject":"Write tests","activeForm":"Writing tests"}`)) {
		t.Fatal("noteCreate should accept a valid subject")
	}
	list := st.asList()
	if len(list) != 1 || list[0].Content != "Write tests" || list[0].Status != "pending" || list[0].ID != "" {
		t.Fatalf("after create: %+v", list)
	}
	if list[0].ActiveForm == nil || *list[0].ActiveForm != "Writing tests" {
		t.Fatalf("activeForm not captured: %+v", list[0].ActiveForm)
	}

	// Server result assigns id #7.
	if !st.resolveCreate("tu1", "Task #7 created successfully: Write tests") {
		t.Fatal("resolveCreate should match the pending tool id")
	}
	if st.asList()[0].ID != "7" {
		t.Fatalf("id not resolved from #7: %q", st.asList()[0].ID)
	}

	// Update by id → in_progress.
	if !st.applyUpdate(raw(`{"taskId":7,"status":"in_progress"}`)) {
		t.Fatal("applyUpdate should mutate the matching item")
	}
	if st.asList()[0].Status != "in_progress" {
		t.Fatalf("status not updated: %q", st.asList()[0].Status)
	}

	// Update with status "deleted" removes it.
	if !st.applyUpdate(raw(`{"taskId":"7","status":"deleted"}`)) {
		t.Fatal("status=deleted should remove the item")
	}
	if len(st.asList()) != 0 {
		t.Fatalf("item should be gone: %+v", st.asList())
	}
}

func TestTodoStoreDeleteByID(t *testing.T) {
	st := newTodoStore()
	st.noteCreate("a", raw(`{"subject":"one"}`))
	st.resolveCreate("a", "Task #1 created")
	st.noteCreate("b", raw(`{"subject":"two"}`))
	st.resolveCreate("b", "Task #2 created")

	if !st.applyDelete(raw(`{"taskId":1}`)) {
		t.Fatal("applyDelete should remove id 1")
	}
	list := st.asList()
	if len(list) != 1 || list[0].ID != "2" {
		t.Fatalf("after delete: %+v", list)
	}
}

// TestTodoStoreTodoWriteReplace: legacy full-replace overwrites everything.
func TestTodoStoreTodoWriteReplace(t *testing.T) {
	st := newTodoStore()
	st.noteCreate("x", raw(`{"subject":"stale"}`))

	ok := st.applyTodoWrite(raw(`{"todos":[
		{"content":"A","status":"completed","activeForm":"Doing A"},
		{"content":"B","status":"pending"},
		{"content":"","status":"pending"},
		{"content":"C","status":"bogus"}
	]}`))
	if !ok {
		t.Fatal("applyTodoWrite should succeed")
	}
	list := st.asList()
	if len(list) != 2 {
		t.Fatalf("invalid items must be dropped, got %d: %+v", len(list), list)
	}
	if list[0].Content != "A" || list[0].Status != "completed" {
		t.Errorf("item0 wrong: %+v", list[0])
	}
	if list[1].ActiveForm != nil {
		t.Errorf("item1 activeForm should be nil (absent), got %v", *list[1].ActiveForm)
	}
}

// TestNormalizeFullListCodexStep maps Codex update_plan (step→content).
func TestNormalizeFullListCodexStep(t *testing.T) {
	plan := []json.RawMessage{
		raw(`{"step":"Read the code","status":"completed"}`),
		raw(`{"step":"Patch it","status":"in_progress"}`),
		raw(`{"status":"pending"}`), // no step → dropped
	}
	items := normalizeFullList(plan, "step")
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	if items[0].Content != "Read the code" || items[1].Status != "in_progress" {
		t.Fatalf("codex normalization wrong: %+v %+v", items[0], items[1])
	}
}

// TestClaudeStreamTodoSuppression feeds a Task* turn through readStdout and
// asserts: todo_update events are emitted, and the suppressed tool's
// start/result/end never reach the wire.
func TestClaudeStreamTodoSuppression(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	reg := session.NewRegistry()
	s := reg.Create("s1", "n", "/tmp", "claude", "", "", "")
	p := &proc{reqID: "r1", todo: newTodoStore(), todoSuppressed: map[string]bool{}}

	lines := strings.Join([]string{
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"tu1","name":"TaskCreate","input":{"subject":"Build it"}}]}}`,
		`{"type":"tool_result","tool_use_id":"tu1","content":"Task #3 created successfully"}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"tu2","name":"TaskUpdate","input":{"taskId":3,"status":"completed"}}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"tu3","name":"Bash","input":{"command":"ls"}}]}}`,
		`{"type":"tool_result","tool_use_id":"tu3","content":"file.txt"}`,
	}, "\n")

	c.readStdout(s, p, strings.NewReader(lines))

	// No tool_start/result/end for the suppressed task tools (tu1, tu2);
	// the real Bash tool (tu3) is untouched.
	toolStarts := map[string]bool{}
	for _, e := range sink.events {
		if ts, ok := e.(protocol.ToolStart); ok {
			toolStarts[ts.ToolUseID] = true
		}
	}
	if toolStarts["tu1"] || toolStarts["tu2"] {
		t.Fatalf("task tools must be suppressed, got tool_start: %v", toolStarts)
	}
	if !toolStarts["tu3"] {
		t.Fatal("real Bash tool_start should pass through")
	}

	// The suppressed result (tu1) must not surface as tool_result; the Bash one must.
	for _, e := range sink.events {
		if tr, ok := e.(protocol.ToolResult); ok && tr.ToolUseID == "tu1" {
			t.Fatal("suppressed task tool_result leaked to the wire")
		}
	}

	// Final todo snapshot: one item, id 3, completed.
	var last *protocol.TodoUpdate
	n := 0
	for _, e := range sink.events {
		if tu, ok := e.(protocol.TodoUpdate); ok {
			n++
			cp := tu
			last = &cp
		}
	}
	if n < 2 {
		t.Fatalf("expected ≥2 todo_update emissions (create, resolve, update), got %d", n)
	}
	if last == nil || len(last.Todos) != 1 {
		t.Fatalf("final snapshot wrong: %+v", last)
	}
	if last.Todos[0].ID != "3" || last.Todos[0].Status != "completed" || last.Todos[0].Content != "Build it" {
		t.Fatalf("final item wrong: %+v", last.Todos[0])
	}
}
