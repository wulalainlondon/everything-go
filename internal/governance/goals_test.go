package governance

import (
	"path/filepath"
	"testing"

	"everything-go/internal/protocol"
)

func TestGoalStateStorePersistsAndRejectsStaleUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "goals.json")
	s := NewGoalStateStore(path)
	complete := protocol.Goal{ThreadID: "t1", Objective: "ship", Status: "complete", UpdatedAt: 20}
	if !s.Apply(protocol.NewGoalUpdate("s1", complete)) {
		t.Fatal("first update should change snapshot")
	}
	if s.Apply(protocol.NewGoalUpdate("s1", protocol.Goal{ThreadID: "t1", Status: "active", UpdatedAt: 10})) {
		t.Fatal("older update must not replace complete state")
	}

	reloaded := NewGoalStateStore(path).Snapshot()
	if reloaded.Revision != 1 || len(reloaded.Items) != 1 || reloaded.Items[0].Goal == nil {
		t.Fatalf("bad reloaded snapshot: %+v", reloaded)
	}
	if got := reloaded.Items[0].Goal.Status; got != "complete" {
		t.Fatalf("reloaded status=%q, want complete", got)
	}
}

func TestGoalStateStorePersistsClearTombstone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "goals.json")
	s := NewGoalStateStore(path)
	s.Apply(protocol.NewGoalUpdate("s1", protocol.Goal{ThreadID: "t1", Status: "active", UpdatedAt: 1}))
	s.Apply(protocol.NewGoalCleared("s1"))
	snapshot := NewGoalStateStore(path).Snapshot()
	if snapshot.Revision != 2 || len(snapshot.Items) != 1 || snapshot.Items[0].Goal != nil {
		t.Fatalf("clear tombstone was not persisted: %+v", snapshot)
	}
}
