package goexec

import (
	"encoding/json"
	"regexp"
	"strconv"

	"everything-go/internal/protocol"
)

// Todo/plan normalization — port of bridge/backends/todo_state.py.
//
// Different backends expose the agent's task list under different tool schemas:
//
//	Claude (legacy)   TodoWrite    {todos:[{content,status,activeForm}]}  full replace
//	Claude (2.1.142+) TaskCreate   {subject,description?,activeForm?}      append; id in result
//	                  TaskUpdate   {taskId,status}                         mutate by id
//	                  TaskDelete   {taskId}                                remove by id
//	Codex             update_plan  {plan:[{step,status}]}                  full replace
//	Gemini            write_todos  {todos:[{description,status}]}          full replace
//
// todoStore folds the stateful Claude Task* protocol into one ordered list so the
// bridge emits a single normalized todo_update event. Full-replace backends use
// normalizeFullList directly. Not goroutine-safe: driven from a single stdout reader.
//
// (Go has no Gemini backend, so write_todos has no caller here — parity gap by
// omission, not by design divergence.)

var validTodoStatus = map[string]bool{"pending": true, "in_progress": true, "completed": true}

var taskIDRe = regexp.MustCompile(`#(\d+)`)

// jsonStr unmarshals a JSON string field; "" if absent or not a string.
func jsonStr(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return ""
}

// optStr returns a pointer to the string value, or nil if absent/not a string.
// Mirrors Python's `active_form if isinstance(active_form, str) else None`.
func optStr(raw json.RawMessage) *string {
	if len(raw) == 0 {
		return nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return &s
	}
	return nil
}

// scalarStr stringifies a scalar (string or number) — taskId can arrive either
// way. Mirrors Python's str(inp.get("taskId") or ...).
func scalarStr(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatInt(int64(t), 10)
	}
	return ""
}

// coerceItem returns a canonical item, or nil if content/status are invalid.
func coerceItem(content, status string, activeForm *string, id string) *protocol.TodoItem {
	if content == "" || !validTodoStatus[status] {
		return nil
	}
	return &protocol.TodoItem{ID: id, Content: content, Status: status, ActiveForm: activeForm}
}

// normalizeFullList maps a full-replace payload (TodoWrite/update_plan/write_todos)
// to canonical items. contentKey is the per-item text field ('content'|'step'|'description').
func normalizeFullList(items []json.RawMessage, contentKey string) []*protocol.TodoItem {
	out := []*protocol.TodoItem{}
	for _, raw := range items {
		var m map[string]json.RawMessage
		if json.Unmarshal(raw, &m) != nil {
			continue
		}
		if it := coerceItem(jsonStr(m[contentKey]), jsonStr(m["status"]), optStr(m["activeForm"]), ""); it != nil {
			out = append(out, it)
		}
	}
	return out
}

// todosValue derefs the internal pointer slice into the value slice the wire wants.
func todosValue(items []*protocol.TodoItem) []protocol.TodoItem {
	out := make([]protocol.TodoItem, 0, len(items))
	for _, it := range items {
		out = append(out, *it)
	}
	return out
}

// todoStore accumulates Claude's TaskCreate/TaskUpdate/TaskDelete and legacy
// TodoWrite into one ordered list.
type todoStore struct {
	items   []*protocol.TodoItem
	pending map[string]*protocol.TodoItem // tool_use_id -> item awaiting server id
}

func newTodoStore() *todoStore {
	return &todoStore{pending: map[string]*protocol.TodoItem{}}
}

func (s *todoStore) reset() {
	s.items = nil
	s.pending = map[string]*protocol.TodoItem{}
}

func (s *todoStore) asList() []protocol.TodoItem { return todosValue(s.items) }

// applyTodoWrite: full replace.
func (s *todoStore) applyTodoWrite(input json.RawMessage) bool {
	var in struct {
		Todos []json.RawMessage `json:"todos"`
	}
	if json.Unmarshal(input, &in) != nil {
		return false
	}
	s.items = normalizeFullList(in.Todos, "content")
	s.pending = map[string]*protocol.TodoItem{}
	return true
}

// noteCreate: append now (status pending), resolve server id from the result later.
func (s *todoStore) noteCreate(toolUseID string, input json.RawMessage) bool {
	var in struct {
		Subject    json.RawMessage `json:"subject"`
		Content    json.RawMessage `json:"content"`
		ActiveForm json.RawMessage `json:"activeForm"`
	}
	if json.Unmarshal(input, &in) != nil {
		return false
	}
	content := jsonStr(in.Subject)
	if content == "" {
		content = jsonStr(in.Content)
	}
	item := coerceItem(content, "pending", optStr(in.ActiveForm), "")
	if item == nil {
		return false
	}
	s.items = append(s.items, item)
	if toolUseID != "" {
		s.pending[toolUseID] = item
	}
	return true
}

func (s *todoStore) resolveCreate(toolUseID, resultText string) bool {
	item, ok := s.pending[toolUseID]
	if !ok {
		return false
	}
	delete(s.pending, toolUseID)
	if m := taskIDRe.FindStringSubmatch(resultText); m != nil {
		item.ID = m[1]
	}
	return true
}

// applyUpdate: mutate by id (status "deleted" removes).
func (s *todoStore) applyUpdate(input json.RawMessage) bool {
	var in struct {
		TaskID     json.RawMessage `json:"taskId"`
		TaskIDSnak json.RawMessage `json:"task_id"`
		Status     json.RawMessage `json:"status"`
		Subject    json.RawMessage `json:"subject"`
		ActiveForm json.RawMessage `json:"activeForm"`
	}
	if json.Unmarshal(input, &in) != nil {
		return false
	}
	tid := scalarStr(in.TaskID)
	if tid == "" {
		tid = scalarStr(in.TaskIDSnak)
	}
	if tid == "" {
		return false
	}
	status := jsonStr(in.Status)
	for i, it := range s.items {
		if it.ID != tid {
			continue
		}
		changed := false
		if validTodoStatus[status] {
			it.Status = status
			changed = true
		} else if status == "deleted" {
			s.items = append(s.items[:i], s.items[i+1:]...)
			return true
		}
		if sub := optStr(in.Subject); sub != nil {
			it.Content = *sub
			changed = true
		}
		if af := optStr(in.ActiveForm); af != nil {
			it.ActiveForm = af
			changed = true
		}
		return changed
	}
	return false
}

// applyDelete: remove by id.
func (s *todoStore) applyDelete(input json.RawMessage) bool {
	var in struct {
		TaskID     json.RawMessage `json:"taskId"`
		TaskIDSnak json.RawMessage `json:"task_id"`
	}
	if json.Unmarshal(input, &in) != nil {
		return false
	}
	tid := scalarStr(in.TaskID)
	if tid == "" {
		tid = scalarStr(in.TaskIDSnak)
	}
	for i, it := range s.items {
		if it.ID == tid {
			s.items = append(s.items[:i], s.items[i+1:]...)
			return true
		}
	}
	return false
}
