package core

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// forkProv is a history provider that also locates a transcript file (the fork
// capability). An empty path or resume id makes TranscriptPath report absent.
type forkProv struct{ path string }

func (p *forkProv) LoadHistory(string, history.Opts) (*history.Result, error) {
	return &history.Result{Kind: "snapshot"}, nil
}
func (p *forkProv) ResumableSessions(int) ([]history.ResumableSession, error) { return nil, nil }
func (p *forkProv) TranscriptPath(resumeID string) (string, bool) {
	if resumeID == "" || p.path == "" {
		return "", false
	}
	return p.path, true
}

type forkExec struct {
	fakeExec
	prov *forkProv
}

func (e *forkExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.prov, true
}
func (e *forkExec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.prov}
}

func writeJSONL(t *testing.T, dir, name string, lines []string) string {
	t.Helper()
	var buf []byte
	for _, l := range lines {
		buf = append(buf, l...)
		buf = append(buf, '\n')
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFindForkOffsetByLine(t *testing.T) {
	dir := t.TempDir()
	lines := []string{`{"uuid":"a"}`, `{"uuid":"b"}`, `{"uuid":"c"}`}
	p := writeJSONL(t, dir, "s.jsonl", lines)
	want := int64(len(lines[0]) + 1 + len(lines[1]) + 1) // bytes through line 2 incl. newlines

	off, ok := findForkOffset(p, "claude:abc:line:2")
	if !ok || off != want {
		t.Fatalf("line offset = %d ok=%v, want %d", off, ok, want)
	}
	if _, ok := findForkOffset(p, "claude:abc:line:99"); ok {
		t.Fatal("out-of-range line must not match")
	}
}

func TestFindForkOffsetByUUID(t *testing.T) {
	dir := t.TempDir()
	lines := []string{`{"uuid":"a"}`, `{"uuid":"b"}`, `{"uuid":"c"}`}
	p := writeJSONL(t, dir, "s.jsonl", lines)
	want := int64(len(lines[0]) + 1 + len(lines[1]) + 1)
	off, ok := findForkOffset(p, "b")
	if !ok || off != want {
		t.Fatalf("uuid offset = %d ok=%v, want %d", off, ok, want)
	}
	if _, ok := findForkOffset(p, "zzz"); ok {
		t.Fatal("unknown uuid must not match")
	}
}

func newForkHub(t *testing.T, transcript string) (*Hub, *forkExec) {
	h, _ := newTestHub(t)
	fe := &forkExec{fakeExec: fakeExec{sink: h}, prov: &forkProv{path: transcript}}
	h.SetExecutor(fe)
	return h, fe
}

func TestForkFullCopy(t *testing.T) {
	dir := t.TempDir()
	src := writeJSONL(t, dir, "parent.jsonl", []string{`{"uuid":"a"}`, `{"uuid":"b"}`})
	h, _ := newForkHub(t, src)

	parent := h.registry.Create("p1", "Parent", dir, "claude", "", "", "resume-parent")
	_ = parent
	c := newDeviceClient(h, "dev", 64)
	h.addClient(c)
	h.registerLatest(c)

	h.handleFork(c, clientproto.NewAppV1().ParseCommand(protocol.Inbound{Type: "fork_session", SessionID: "p1"}))

	ev := waitForType(t, c, "session_forked")
	newID, _ := ev["session_id"].(string)
	if newID == "" || ev["parent_session_id"] != "p1" || ev["name"] != "Parent (fork)" {
		t.Fatalf("session_forked payload wrong: %+v", ev)
	}
	// New session exists with a fresh resume id and a copied transcript file.
	fs, ok := h.registry.Get(newID)
	if !ok {
		t.Fatal("fork session not registered")
	}
	rid := fs.ResumeID()
	if rid == "" || rid == "resume-parent" {
		t.Fatalf("fork must get a new resume id, got %q", rid)
	}
	forkFile := filepath.Join(dir, rid+".jsonl")
	got, err := os.ReadFile(forkFile)
	if err != nil {
		t.Fatalf("fork transcript missing: %v", err)
	}
	orig, _ := os.ReadFile(src)
	if string(got) != string(orig) {
		t.Fatal("full fork should copy the entire transcript")
	}
}

func TestForkTruncated(t *testing.T) {
	dir := t.TempDir()
	lines := []string{`{"uuid":"a"}`, `{"uuid":"b"}`, `{"uuid":"c"}`}
	src := writeJSONL(t, dir, "parent.jsonl", lines)
	h, _ := newForkHub(t, src)
	h.registry.Create("p1", "P", dir, "claude", "", "", "resume-parent")
	c := newDeviceClient(h, "dev", 64)
	h.addClient(c)
	h.registerLatest(c)

	h.handleFork(c, clientproto.NewAppV1().ParseCommand(protocol.Inbound{Type: "fork_session", SessionID: "p1", ForkAfterMessageID: "claude:x:line:2", Name: "Cut"}))

	ev := waitForType(t, c, "session_forked")
	if ev["name"] != "Cut" {
		t.Fatalf("custom fork name lost: %v", ev["name"])
	}
	fs, _ := h.registry.Get(ev["session_id"].(string))
	got, err := os.ReadFile(filepath.Join(dir, fs.ResumeID()+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	want := lines[0] + "\n" + lines[1] + "\n"
	if string(got) != want {
		t.Fatalf("truncated fork content = %q, want %q", got, want)
	}
}

func TestForkParentBusy(t *testing.T) {
	dir := t.TempDir()
	src := writeJSONL(t, dir, "p.jsonl", []string{`{"uuid":"a"}`})
	h, _ := newForkHub(t, src)
	parent := h.registry.Create("p1", "P", dir, "claude", "", "", "resume-parent")
	// Put a turn in flight: the worker flips Idle→Streaming before running fn,
	// and fn blocks, so the parent stays streaming until we release it.
	release := make(chan struct{})
	defer close(release)
	parent.Submit(func() { <-release })
	deadline := time.After(time.Second)
	for !parent.IsStreaming() {
		select {
		case <-deadline:
			t.Fatal("parent never entered streaming")
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	c := newDeviceClient(h, "dev", 64)
	h.addClient(c)
	h.registerLatest(c)

	h.handleFork(c, clientproto.NewAppV1().ParseCommand(protocol.Inbound{Type: "fork_session", SessionID: "p1"}))
	ev := waitForType(t, c, "fork_error")
	if ev["reason"] != "parent_busy" {
		t.Fatalf("expected parent_busy, got %v", ev["reason"])
	}
}

func TestForkNoHistory(t *testing.T) {
	h, _ := newForkHub(t, "")                                       // provider reports no transcript
	h.registry.Create("p1", "P", t.TempDir(), "claude", "", "", "") // empty resume id
	c := newDeviceClient(h, "dev", 64)
	h.addClient(c)
	h.registerLatest(c)

	h.handleFork(c, clientproto.NewAppV1().ParseCommand(protocol.Inbound{Type: "fork_session", SessionID: "p1"}))
	ev := waitForType(t, c, "fork_error")
	if ev["reason"] != "no_history_file" {
		t.Fatalf("expected no_history_file, got %v", ev["reason"])
	}
}

func TestForkUnknownSession(t *testing.T) {
	h, _ := newForkHub(t, "")
	c := newDeviceClient(h, "dev", 64)
	h.addClient(c)
	h.registerLatest(c)
	h.handleFork(c, clientproto.NewAppV1().ParseCommand(protocol.Inbound{Type: "fork_session", SessionID: "ghost"}))
	ev := waitForType(t, c, "error")
	if msg, _ := ev["message"].(string); msg == "" {
		t.Fatalf("expected an error event for unknown session, got %+v", ev)
	}
}
