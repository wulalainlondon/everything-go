package agentactivity

import (
	"fmt"
	"strings"
	"testing"
)

func TestNativeMailboxJournalCannotReopenExitOrInventRound(t *testing.T) {
	base := `{"threadId":"root","turnId":"parent","item":{"id":"wait","type":"collabAgentToolCall","tool":"wait","status":"completed","senderThreadId":"root"}}`
	var j WaitJournal
	j.Observe("item/completed", []byte(base), "root")
	j.Observe("item/started", []byte(strings.Replace(base, `"completed"`, `"inProgress"`, 1)), "root")
	v := j.Snapshot()
	if len(v) != 1 || v[0].Phase != "exited" {
		t.Fatal(v)
	}
	j.Lost = true
	j.Entries["other"] = NativeMailboxWait{"other", "parent", "blocking"}
	for _, w := range j.Snapshot() {
		if w.CallID == "other" && w.Phase != "unknown" {
			t.Fatal("source gap became wait satisfaction")
		}
	}
}

func TestNativeMailboxFullWindowRetiresAndNeverReopensEvictedCall(t *testing.T) {
	event := func(id, status string) []byte {
		return []byte(`{"threadId":"root","turnId":"parent","item":{"id":"` + id + `","type":"collabAgentToolCall","tool":"wait","status":"` + status + `","senderThreadId":"root"}}`)
	}
	var j WaitJournal
	for i := 0; i < 128; i++ {
		j.Observe("item/started", event(fmt.Sprint(i), "inProgress"), "root")
	}
	j.Observe("item/completed", event("0", "completed"), "root")
	if j.Entries["0"].Phase != "exited" {
		t.Fatal("full window dropped exit")
	}
	for i := 128; i < 2200; i++ {
		j.Observe("item/completed", event(fmt.Sprint(i), "completed"), "root")
	}
	j.Observe("item/started", event("0", "inProgress"), "root")
	if j.Entries["0"].Phase == "blocking" {
		t.Fatal("evicted call reopened")
	}
	if len(j.Entries) > 128 || len(j.tombstones) > 2048 {
		t.Fatal("unbounded journal")
	}
	j.Observe("item/completed", event("1", "completed"), "root")
	if j.Entries["1"].Phase != "exited" {
		t.Fatal("tombstone saturation dropped existing exit")
	}
}
