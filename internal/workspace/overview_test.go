package workspace

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func overviewFor(t *testing.T, f *fixture, a Actor, c Command) *Overview {
	t.Helper()
	c.Action = "overview"
	c.Authority = f.s.authority
	r, e := f.s.Apply(f.ctx, a, c)
	if e != nil {
		t.Fatal(e)
	}
	if r.Overview == nil {
		t.Fatal("missing overview")
	}
	return r.Overview
}
func TestOverviewReadOnlyAndIndependentUnreadActions(t *testing.T) {
	f := setup(t)
	f.post(f.owner, "original")
	f.call(f.owner, Command{Action: "create_human_todo", RoomID: f.room.ID, Data: raw(HumanTodoInput{Title: "Question", Body: "Decide", Assignee: f.owner.ID, Kind: "question"})})
	var before int
	if e := f.s.db.QueryRow("SELECT COUNT(*) FROM mutations").Scan(&before); e != nil {
		t.Fatal(e)
	}
	o := overviewFor(t, f, f.owner, Command{})
	if o.Coverage != "complete" || o.Totals.Unread != 1 || o.Totals.NeedsReply != 1 || o.Totals.NeedsMe != 1 {
		t.Fatalf("incorrect counts: %+v", o)
	}
	var after, seen int
	_ = f.s.db.QueryRow("SELECT COUNT(*) FROM mutations").Scan(&after)
	_ = f.s.db.QueryRow("SELECT COUNT(*) FROM read_messages").Scan(&seen)
	if before != after || seen != 0 {
		t.Fatal("overview caused a write")
	}
	if !ReadAction("overview") {
		t.Fatal("overview classified as mutation")
	}
}
func TestOverviewPrivateRoomsAndScopedTotals(t *testing.T) {
	f := setup(t)
	guest, _ := f.invite("viewer")
	secret := f.call(f.owner, Command{Action: "create_room", WorkspaceID: f.workspace.ID, Data: raw(map[string]any{"name": "secret-room"})}).Room
	f.call(f.owner, Command{Action: "post_message", RoomID: secret.ID, Data: raw(Message{Body: "PRIVATE"})})
	o := overviewFor(t, f, guest, Command{})
	b, _ := json.Marshal(o)
	if len(o.Rooms) != 1 || o.Rooms[0].Room.ID != f.room.ID || o.Totals.Unread != 0 || strings.Contains(string(b), secret.ID) || strings.Contains(string(b), "PRIVATE") {
		t.Fatalf("private data leaked: %s", b)
	}
	filtered := overviewFor(t, f, guest, Command{Query: "secret-room"})
	if len(filtered.Rooms) != 0 {
		t.Fatal("name search leaked private room")
	}
}
func TestOverviewTotalsCoverAllPagesAndCatalog(t *testing.T) {
	f := setup(t)
	for i := 0; i < 101; i++ {
		r := f.call(f.owner, Command{Action: "create_room", WorkspaceID: f.workspace.ID, Data: raw(map[string]any{"name": fmt.Sprintf("room-%03d", i)})}).Room
		f.call(f.owner, Command{Action: "post_message", RoomID: r.ID, Data: raw(Message{Body: "unread"})})
	}
	first := overviewFor(t, f, f.owner, Command{Limit: 30})
	if len(first.Rooms) != 30 || !first.HasMore || first.Totals.Unread != 101 {
		t.Fatalf("page count mistaken as totals: %+v", first.Totals)
	}
	seen := map[string]bool{}
	page := first
	for {
		for _, r := range page.Rooms {
			if seen[r.Room.ID] {
				t.Fatal("duplicate room across stable pages")
			}
			seen[r.Room.ID] = true
		}
		if !page.HasMore {
			break
		}
		page = overviewFor(t, f, f.owner, Command{Limit: 30, Cursor: page.NextCursor})
	}
	if len(seen) != 102 {
		t.Fatalf("missing rooms: %d", len(seen))
	}
	for i := 0; i < 31; i++ {
		f.call(f.owner, Command{Action: "create_workspace", Data: raw(map[string]any{"name": fmt.Sprintf("empty-%02d", i)})})
	}
	catalog := overviewFor(t, f, f.owner, Command{Data: raw(overviewInput{CatalogLimit: 10})})
	if len(catalog.Catalog.Items) != 10 || !catalog.Catalog.HasMore {
		t.Fatal("catalog unbounded or falsely complete")
	}
	count := len(catalog.Catalog.Items)
	for catalog.Catalog.HasMore {
		catalog = overviewFor(t, f, f.owner, Command{Data: raw(overviewInput{CatalogLimit: 10, CatalogCursor: catalog.Catalog.NextCursor})})
		count += len(catalog.Catalog.Items)
	}
	if count != 32 {
		t.Fatalf("empty owned workspace omitted: %d", count)
	}
}
func TestOverviewCursorRejectsWrongQueryActorRevocationAndExpiry(t *testing.T) {
	f := setup(t)
	guest, _ := f.invite("editor")
	f.call(f.owner, Command{Action: "create_room", WorkspaceID: f.workspace.ID, Data: raw(map[string]any{"name": "other"})})
	first := overviewFor(t, f, f.owner, Command{Limit: 1})
	for _, c := range []Command{{Cursor: first.NextCursor, Query: "changed"}, {Cursor: first.NextCursor, Data: raw(overviewInput{View: "unread"})}} {
		c.Action = "overview"
		f.reject(f.owner, c, "overview_cursor_scope_changed")
	}
	f.reject(guest, Command{Action: "overview", Cursor: first.NextCursor}, "overview_cursor_scope_changed")
	now := f.s.now()
	f.s.now = func() time.Time { return now.Add(16 * time.Minute) }
	f.reject(f.owner, Command{Action: "overview", Cursor: first.NextCursor}, "overview_cursor_scope_changed")
	f.s.now = func() time.Time { return now }
	f.call(f.owner, Command{Action: "update_member", RoomID: f.room.ID, ExpectedRevision: ptr(1), Data: raw(map[string]any{"principal_id": guest.ID, "role": "viewer", "active": true})})
	f.reject(f.owner, Command{Action: "overview", Cursor: first.NextCursor}, "overview_cursor_scope_changed")
}
func TestOverviewSealedQAAndExecutionAxes(t *testing.T) {
	f := setup(t)
	agent := f.agent("worker", "session", true)
	task := f.task(agent, nil)
	waiting := overviewFor(t, f, f.owner, Command{})
	if waiting.Totals.Waiting != 1 || waiting.Totals.Running != 0 || waiting.Totals.NeedsAcceptance != 0 {
		t.Fatalf("ACK confused with execution/final: %+v", waiting.Totals)
	}
	f.final(task, "original final")
	sealed := overviewFor(t, f, f.owner, Command{})
	if sealed.Totals.Active != 0 || sealed.Totals.NeedsAcceptance != 1 {
		t.Fatalf("sealed acceptance wrong: %+v", sealed.Totals)
	}
	// Existing mutator and overview must use the same latest-workflow/QA rule.
	if _, e := f.s.db.Exec(`UPDATE entities SET body=json_set(body,'$.axes.qa','FAIL') WHERE id=?`, task.ID); e != nil {
		t.Fatal(e)
	}
	failed := overviewFor(t, f, f.owner, Command{})
	if failed.Totals.NeedsAcceptance != 0 {
		t.Fatal("failed QA offered as acceptable")
	}
	yes, e := taskAcceptanceAvailable(f.ctx, f.s.db, f.room.ID, task.ID)
	if e != nil || yes {
		t.Fatal("acceptance predicate drift")
	}
}
func TestOverviewArchivedRoleAndUnknownCounts(t *testing.T) {
	f := setup(t)
	viewer, _ := f.invite("viewer")
	f.call(f.owner, Command{Action: "create_human_todo", RoomID: f.room.ID, Data: raw(HumanTodoInput{Title: "mine", Assignee: f.owner.ID, Kind: "todo"})})
	if o := overviewFor(t, f, viewer, Command{}); o.Totals.NeedsMe != 0 {
		t.Fatal("viewer got owner actions")
	}
	f.call(f.owner, Command{Action: "archive_room", RoomID: f.room.ID, ExpectedRevision: ptr(f.call(f.owner, Command{Action: "snapshot", RoomID: f.room.ID}).Snapshot.Room.Revision), Data: raw(map[string]any{"archived": true})})
	if o := overviewFor(t, f, f.owner, Command{}); len(o.Rooms) != 0 {
		t.Fatal("archived in active view")
	}
	if o := overviewFor(t, f, f.owner, Command{Data: raw(overviewInput{View: "archived"})}); len(o.Rooms) != 1 || o.Totals.NeedsMe != 0 {
		t.Fatal("archived has writable action badge")
	}
}

func TestOverviewTargetsBoundedAndOriginalWorkflowVersion(t *testing.T) {
	f := setup(t)
	for i := 0; i < 5; i++ {
		f.call(f.owner, Command{Action: "create_human_todo", RoomID: f.room.ID, Data: raw(HumanTodoInput{Title: fmt.Sprintf("choice-%d", i), Body: "PRIVATE BODY NOT A SUMMARY", Assignee: f.owner.ID, Kind: "question"})})
	}
	o := overviewFor(t, f, f.owner, Command{})
	if o.Totals.NeedsReply != 5 || len(o.Rooms[0].Targets) != 3 {
		t.Fatalf("unbounded targets or partial totals: %+v", o)
	}
	b, _ := json.Marshal(o)
	if strings.Contains(string(b), "PRIVATE BODY") {
		t.Fatal("summary exposed full private body")
	}
	a := f.agent("original", "session-original", true)
	task := f.task(a, nil)
	f.final(task, "sealed")
	// A stale draft must not be offered as human-acceptable after its workflow advances.
	if _, err := f.s.db.Exec(`UPDATE entities SET body=json_set(body,'$.workflow_id','workflow-original') WHERE id=?`, task.ID); err != nil {
		t.Fatal(err)
	}
	if overviewFor(t, f, f.owner, Command{}).Totals.NeedsAcceptance != 0 {
		t.Fatal("missing workflow offered for acceptance")
	}
	ok, err := taskAcceptanceAvailable(f.ctx, f.s.db, f.room.ID, task.ID)
	if err != nil || ok {
		t.Fatal("actual acceptance guard diverged", err)
	}
}

func TestOverviewUnknownNotActiveAndRevokedActor(t *testing.T) {
	f := setup(t)
	guest, _ := f.invite("editor")
	agent := f.agent("worker", "native-worker", true)
	task := f.task(agent, nil)
	if _, err := f.s.db.Exec(`UPDATE entities SET body=json_set(body,'$.axes.execution','unknown') WHERE id=?`, task.ID); err != nil {
		t.Fatal(err)
	}
	o := overviewFor(t, f, f.owner, Command{})
	if o.Totals.Unknown != 1 || o.Totals.Active != 0 || o.Totals.Running != 0 {
		t.Fatalf("unknown presented as active: %+v", o.Totals)
	}
	f.call(f.owner, Command{Action: "update_member", RoomID: f.room.ID, ExpectedRevision: ptr(1), Data: raw(map[string]any{"principal_id": guest.ID, "role": "editor", "active": false})})
	hidden := overviewFor(t, f, guest, Command{})
	if len(hidden.Rooms) != 0 || hidden.Totals.Unknown != 0 {
		t.Fatal("revoked room leaked")
	}
	forged := guest
	forged.CredentialHash = strings.Repeat("0", 64)
	f.reject(forged, Command{Action: "overview"}, "workspace_unauthorized")
}
