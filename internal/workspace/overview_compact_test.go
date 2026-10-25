package workspace

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func compactFor(t *testing.T, f *fixture, view string) *Overview {
	return overviewFor(t, f, f.owner, Command{Data: raw(overviewInput{View: view, PresentationVersion: 2})})
}
func TestCompactOverviewStableSummaryFullFilterAndCompatibility(t *testing.T) {
	f := setup(t)
	for i := 0; i < 101; i++ {
		r := f.call(f.owner, Command{Action: "create_room", WorkspaceID: f.workspace.ID, Data: raw(map[string]any{"name": fmt.Sprintf("room-%03d", i)})}).Room
		f.call(f.owner, Command{Action: "post_message", RoomID: r.ID, Data: raw(Message{Body: "original"})})
	}
	f.call(f.owner, Command{Action: "create_human_todo", RoomID: f.room.ID, Data: raw(HumanTodoInput{Title: "Choose", Assignee: f.owner.ID, Kind: "question"})})
	for _, view := range []string{"channels", "needs_reply", "needs_acceptance", "archived", "active"} {
		o := compactFor(t, f, view)
		if o.SchemaVersion != 2 || o.SummaryTotals == nil || o.FilteredTotals == nil || o.SummaryTotals.Unread != 101 || o.SummaryTotals.NeedsReply != 1 {
			t.Fatalf("view %s summary drift: %+v", view, o)
		}
		if view == "needs_reply" && (len(o.Rooms) != 1 || o.FilteredTotals.NeedsReply != 1 || o.Rooms[0].PrimaryItem.Intent != "view_question") {
			t.Fatal("reply full filter or intent")
		}
		if view == "needs_acceptance" && (len(o.Rooms) != 0 || o.FilteredTotals.Unread != 0) {
			t.Fatal("empty view mistaken as base")
		}
	}
	old := overviewFor(t, f, f.owner, Command{})
	b, _ := json.Marshal(old)
	for _, key := range []string{"summary_totals", "filtered_totals", "primary_item", "row_state", "preview", "summary_scope"} {
		if strings.Contains(string(b), `"`+key+`"`) {
			t.Fatal("v1 shape changed", key)
		}
	}
	f.reject(f.owner, Command{Action: "overview", Data: raw(overviewInput{View: "needs_reply"})}, "overview_filter_invalid")
	first := overviewFor(t, f, f.owner, Command{Limit: 1})
	f.reject(f.owner, Command{Action: "overview", Cursor: first.NextCursor, Data: raw(overviewInput{PresentationVersion: 2})}, "overview_cursor_scope_changed")
}
func TestCompactOverviewHumanTodoIntentPriorityAndQA(t *testing.T) {
	f := setup(t)
	todo := f.call(f.owner, Command{Action: "create_human_todo", RoomID: f.room.ID, Data: raw(HumanTodoInput{Title: "Question", Assignee: f.owner.ID, Kind: "question"})}).Entity
	if _, e := f.s.db.Exec(`UPDATE entities SET body=json_set(body,'$.state','answered') WHERE id=?`, todo.ID); e != nil {
		t.Fatal(e)
	}
	o := compactFor(t, f, "needs_acceptance")
	if len(o.Rooms) != 1 || o.Rooms[0].PrimaryItem.Intent != "view_result" || o.Rooms[0].PrimaryItem.Kind != "human_todo" {
		t.Fatal("answered todo mislabeled")
	}
	f.call(f.owner, Command{Action: "create_human_todo", RoomID: f.room.ID, Data: raw(HumanTodoInput{Title: "New question", Assignee: f.owner.ID, Kind: "question"})})
	if compactFor(t, f, "needs_me").Rooms[0].PrimaryItem.Intent != "view_question" {
		t.Fatal("priority drift")
	}
	a := f.agent("worker", "original", true)
	task := f.task(a, nil)
	f.final(task, "final")
	if _, e := f.s.db.Exec(`UPDATE entities SET body=json_set(body,'$.axes.qa','FAIL') WHERE id=?`, task.ID); e != nil {
		t.Fatal(e)
	}
	if compactFor(t, f, "channels").SummaryTotals.NeedsAcceptance != 1 {
		t.Fatal("QA FAIL counted as acceptable")
	}
}
func TestCompactOverviewUnicodeAndPrivateProjection(t *testing.T) {
	f := setup(t)
	guest, _ := f.invite("viewer")
	private := f.call(f.owner, Command{Action: "create_room", WorkspaceID: f.workspace.ID, Data: raw(map[string]any{"name": "PRIVATE secret"})}).Room
	f.call(f.owner, Command{Action: "create_human_todo", RoomID: private.ID, Data: raw(HumanTodoInput{Title: "PRIVATE action", Assignee: f.owner.ID, Kind: "question"})})
	o := overviewFor(t, f, guest, Command{Data: raw(overviewInput{PresentationVersion: 2})})
	b, _ := json.Marshal(o)
	if strings.Contains(string(b), "PRIVATE") || o.SummaryTotals.NeedsMe != 0 {
		t.Fatal("private compact leak")
	}
	overviewFor(t, f, f.owner, Command{Query: strings.Repeat("中", 66), Data: raw(overviewInput{PresentationVersion: 2})})
	f.reject(f.owner, Command{Action: "overview", Query: strings.Repeat("中", 67), Data: raw(overviewInput{PresentationVersion: 2})}, "overview_query_limit")
}
