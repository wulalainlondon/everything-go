package jarvisrole

import (
	"os"
	"testing"
)

func TestDefaultEffortMigrationCASAuditAndRecovery(t *testing.T) {
	root := t.TempDir()
	s, e := Open(root, "authority")
	if e != nil {
		t.Fatal(e)
	}
	a := &Assignment{Partner: "partner", Target: Target{"authority", "session", "thread", 0}, SourceRevision: 4}
	op, e := s.Apply("human", "phone", 1, "r_original", "assign", "hash", 0, a)
	if e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(s.path)
	if e != nil {
		t.Fatal(e)
	}
	for _, kind := range []string{"source", "epoch", "revision", "thread", "config", "reason", "nonadjacent"} {
		source, after, epoch, revision, config := uint64(4), uint64(5), uint64(1), uint64(1), uint64(0)
		thread, reason := "thread", "provider_default_effort_observed"
		switch kind {
		case "source":
			source = 3
		case "epoch":
			epoch = 9
		case "revision":
			revision = 9
		case "thread":
			thread = "other"
		case "config":
			config = 1
		case "reason":
			reason = "arbitrary_metadata"
		case "nonadjacent":
			after = 8
		}
		if e = s.MigrateDefaultEffort("session", thread, config, source, after, epoch, revision, reason); e == nil {
			t.Fatal("CAS accepted", kind)
		}
		now, _ := os.ReadFile(s.path)
		if string(now) != string(before) {
			t.Fatal("failed CAS changed journal", kind)
		}
	}
	if e = s.MigrateDefaultEffort("session", "thread", 0, 4, 5, 1, 1, "provider_default_effort_observed"); e != nil {
		t.Fatal(e)
	}
	reopened, e := Open(root, "authority")
	if e != nil {
		t.Fatal(e)
	}
	rev, current := reopened.Snapshot()
	if rev != 2 || current.SourceRevision != 5 || current.Epoch != 1 || current.Target != a.Target {
		t.Fatal(rev, current)
	}
	if got, ok := reopened.Lookup("human", "r_original"); !ok || got != op {
		t.Fatal("assign receipt changed")
	}
	if len(reopened.state.WitnessMigrations) != 1 {
		t.Fatal("audit missing")
	}
	audit := reopened.state.WitnessMigrations[0]
	if audit.Before != 4 || audit.After != 5 || audit.Epoch != 1 || audit.Revision != 2 || audit.Reason != "provider_default_effort_observed" || audit.At <= 0 {
		t.Fatal(audit)
	}
}
