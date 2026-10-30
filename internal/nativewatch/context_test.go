package nativewatch

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func contextFixture(t *testing.T) (*TurnWatcher, string, func(string), *[]ContextActivity) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	events := []ContextActivity{}
	w := NewTurnWatcher()
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	appendRow := func(body string) {
		t.Helper()
		f, e := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		_, e = f.WriteString(body)
		if e == nil {
			e = f.Sync()
		}
		f.Close()
		if e != nil {
			t.Fatal(e)
		}
	}
	appendRow(`{"type":"session_meta","payload":{"id":"root"}}` + "\n" + fmt.Sprintf(`{"timestamp":%q,"type":"event_msg","payload":{"type":"task_started","turn_id":"turn"}}`, stamp) + "\n")
	w.Track(NativeSession{ID: "s", Backend: "codex", ResumeID: "root", Path: path})
	return w, path, appendRow, &events
}
func TestContextWatcherInitialHistoryLiveCompletionAndReplayIdentity(t *testing.T) {
	w, path, appendRow, events := contextFixture(t)
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	row := fmt.Sprintf(`{"timestamp":%q,"type":"compacted","payload":{"message":"private summary not exported"}}`, stamp) + "\n"
	appendRow(row)
	appendRow(fmt.Sprintf(`{"timestamp":%q,"type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"total_tokens":0},"model_context_window":1000}}}`, stamp) + "\n")
	emit := func(a ContextActivity) { *events = append(*events, a) }
	w.poll(func(TurnActivity) {}, emit)
	if len(*events) != 2 || !(*events)[0].Historical || (*events)[0].Completed || (*events)[1].Used == nil || *(*events)[1].Used != 0 {
		t.Fatalf("initial history must invalidate without counting, then establish actual zero usage: %+v", *events)
	}
	row = fmt.Sprintf(`{"timestamp":%q,"type":"compacted","payload":{"message":"new synthetic completion"}}`, time.Now().UTC().Format(time.RFC3339Nano)) + "\n"
	appendRow(row)
	w.poll(func(TurnActivity) {}, emit)
	if len(*events) != 3 || !(*events)[2].Completed {
		t.Fatalf("new durable compaction missing: %+v", *events)
	}
	id := (*events)[2].EventID
	appendRow(row)
	w.poll(func(TurnActivity) {}, emit)
	if len(*events) != 4 || (*events)[3].EventID != id {
		t.Fatal("same original row replay acquired new identity")
	}
	// A new watcher/restored process must not count old history again.
	restarted := NewTurnWatcher()
	restarted.Track(NativeSession{ID: "s", Backend: "codex", ResumeID: "root", Path: path})
	before := len(*events)
	restarted.poll(func(TurnActivity) {}, emit)
	for _, a := range (*events)[before:] {
		if a.Completed {
			t.Fatal("restart replay counted history")
		}
	}
}
func TestContextWatcherRejectsForeignChildPartialAndUncorrelatedRows(t *testing.T) {
	for _, header := range []string{`{"type":"session_meta","payload":{"id":"foreign"}}`, `{"type":"session_meta","payload":{"id":"root","source":{"subagent":{"depth":1}}}}`} {
		path := filepath.Join(t.TempDir(), "x.jsonl")
		os.WriteFile(path, []byte(header+"\n"), 0600)
		if HasContextSource(path, "root") {
			t.Fatal("foreign/child root accepted")
		}
	}
	w, _, appendRow, events := contextFixture(t)
	emit := func(a ContextActivity) { *events = append(*events, a) }
	w.poll(func(TurnActivity) {}, emit)
	appendRow(`{"timestamp":"2026-10-30T00:00:00Z","type":"compacted","payload":{}}`)
	w.poll(func(TurnActivity) {}, emit)
	if len(*events) != 0 {
		t.Fatal("partial final row counted")
	}
	appendRow("\n")
	w.poll(func(TurnActivity) {}, emit)
	if len(*events) != 1 || !(*events)[0].Completed {
		t.Fatal("committed partial completion lost")
	}
}

func TestContextRootLongTurnMetadataDoesNotNeedRecentLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "long.jsonl")
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	os.WriteFile(path, []byte(`{"type":"session_meta","payload":{"id":"root"}}`+"\n"+fmt.Sprintf(`{"timestamp":%q,"type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"total_tokens":50},"model_context_window":1000}}}`, stamp)+"\n"), 0600)
	w := NewTurnWatcher()
	w.Track(NativeSession{ID: "s", ResumeID: "root", Backend: "codex", Path: path})
	var events []ContextActivity
	emit := func(a ContextActivity) { events = append(events, a) }
	w.poll(func(TurnActivity) {}, emit)
	if len(events) != 1 || events[0].Used == nil || *events[0].Used != 50 {
		t.Fatal("bounded tail lost root usage without recent task_started")
	}
	row := fmt.Sprintf(`{"timestamp":%q,"type":"compacted","payload":{}}`+"\n", stamp)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	f.WriteString(row)
	f.Close()
	w.poll(func(TurnActivity) {}, emit)
	if len(events) != 2 || !events[1].Completed {
		t.Fatal("root durable compaction lost without recent task_started")
	}
}
