package core

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type ownershipExec struct {
	*fakeExec
	sendErr, goalErr, claimErr, releaseErr error
	goalCalls, claimCalls, releaseCalls    atomic.Int32
}

func (f *ownershipExec) Send(_ context.Context, _ *session.Session, _, _ string, _ []backend.ImageAttachment, _ []backend.FileAttachment) error {
	return f.sendErr
}
func (f *ownershipExec) SetGoal(context.Context, *session.Session, string, string, *int) error {
	return f.goalErr
}
func (f *ownershipExec) GetGoal(context.Context, *session.Session) error {
	f.goalCalls.Add(1)
	return f.goalErr
}
func (f *ownershipExec) ClearGoal(context.Context, *session.Session) error { return f.goalErr }
func (f *ownershipExec) ClaimSession(context.Context, *session.Session) error {
	f.claimCalls.Add(1)
	return f.claimErr
}
func (f *ownershipExec) ReleaseSession(context.Context, *session.Session) error {
	f.releaseCalls.Add(1)
	return f.releaseErr
}

func newControlTestHub(t *testing.T, dir string) (*Hub, *fakeExec) {
	t.Helper()
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", DataDir: dir, CodexRemote: "unix://"}, governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	fe := &fakeExec{sink: h}
	h.SetExecutor(fe)
	return h, fe
}

func TestDesktopHandoffWaitsForTurnAndRejectsNewMobileWrites(t *testing.T) {
	dir := t.TempDir()
	h, fe := newControlTestHub(t, dir)
	c := newTestClient(h)
	s := h.registry.Create("s1", "codex", dir, backend.Codex, "", "", "thread-1")
	started := make(chan struct{}, 1)
	fe.onSend = func(_ *session.Session, _, _ string) { started <- struct{}{} }

	route(h, c, `{"type":"message","session_id":"s1","request_id":"r1","content":"first"}`)
	<-started
	waitState(t, s, session.Streaming)
	route(h, c, `{"type":"handoff_to_desktop","session_id":"s1"}`)
	pending := waitForType(t, c, "session_control")
	if pending["state"] != governance.ControlPending {
		t.Fatalf("pending event=%v", pending)
	}
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r2","content":"must reject"}`)
	rejected := waitForType(t, c, "error")
	if rejected["code"] != "session_controlled_by_desktop" {
		t.Fatalf("write rejection=%v", rejected)
	}

	h.Emit(protocol.NewDone("s1", "r1"))
	active := waitForType(t, c, "session_control")
	if active["owner"] != governance.ControlDesktop || active["state"] != governance.ControlActive {
		t.Fatalf("active handoff=%v", active)
	}
	command, _ := active["desktop_command"].(string)
	if command != "codex resume thread-1 --remote unix://" {
		t.Fatalf("desktop command=%q", command)
	}
}

func TestDesktopLeaseSurvivesRestartAndExplicitReclaim(t *testing.T) {
	dir := t.TempDir()
	h1, _ := newControlTestHub(t, dir)
	h1.registry.Create("s1", "codex", dir, backend.Codex, "", "", "thread-1")
	c1 := newTestClient(h1)
	route(h1, c1, `{"type":"handoff_to_desktop","session_id":"s1"}`)
	waitForType(t, c1, "session_control")
	waitForType(t, c1, "session_control")

	h2, fe2 := newControlTestHub(t, dir)
	h2.registry.Create("s1", "codex", dir, backend.Codex, "", "", "thread-1")
	c2 := newTestClient(h2)
	blockedCall := make(chan struct{}, 1)
	fe2.onSend = func(_ *session.Session, _, _ string) { blockedCall <- struct{}{} }
	route(h2, c2, `{"type":"message","session_id":"s1","request_id":"blocked","content":"no"}`)
	if event := waitForType(t, c2, "error"); event["code"] != "session_controlled_by_desktop" {
		t.Fatalf("restart write was not rejected: %v", event)
	}
	select {
	case <-blockedCall:
		t.Fatal("desktop-owned turn reached executor")
	default:
	}

	route(h2, c2, `{"type":"reclaim_from_desktop","session_id":"s1"}`)
	reclaimed := waitForType(t, c2, "session_control")
	if reclaimed["owner"] != governance.ControlMobile || !strings.Contains(reclaimed["message"].(string), "reclaimed") {
		t.Fatalf("reclaim event=%v", reclaimed)
	}
	allowedCall := make(chan struct{}, 1)
	fe2.onSend = func(_ *session.Session, _, _ string) { allowedCall <- struct{}{} }
	route(h2, c2, `{"type":"message","session_id":"s1","request_id":"allowed","content":"yes"}`)
	select {
	case <-allowedCall:
	case <-time.After(2 * time.Second):
		t.Fatal("reclaimed mobile turn did not reach executor")
	}
}

func TestActiveWriterBecomesDesktopControlWithoutGoalErrorStorm(t *testing.T) {
	dir := t.TempDir()
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", DataDir: dir, CodexRemote: "unix://"}, governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	exec := &ownershipExec{fakeExec: &fakeExec{sink: h}, goalErr: fmt.Errorf("resume: %w", backend.ErrThreadActiveWriter)}
	h.SetExecutor(exec)
	c := newTestClient(h)
	h.registry.Create("s1", "codex", dir, backend.Codex, "", "", "thread-1")

	route(h, c, `{"type":"codex_goal_get","session_id":"s1"}`)
	control := waitForType(t, c, "session_control")
	if control["owner"] != governance.ControlDesktop || control["thread_id"] != "thread-1" {
		t.Fatalf("external writer control=%v", control)
	}
	if exec.goalCalls.Load() != 1 {
		t.Fatalf("goal calls=%d, want 1", exec.goalCalls.Load())
	}

	// Repeated read-only refreshes may continue, but the idempotent ownership
	// transition must not produce another control warning or a red error.
	route(h, c, `{"type":"codex_goal_get","session_id":"s1"}`)
	time.Sleep(30 * time.Millisecond)
	if exec.goalCalls.Load() != 2 {
		t.Fatalf("goal calls=%d, want 2", exec.goalCalls.Load())
	}
	select {
	case data := <-c.send:
		if strings.Contains(string(data), `"type":"error"`) {
			t.Fatalf("goal refresh emitted red error: %s", data)
		}
	default:
	}
}

func TestActiveWriterRejectsMobileTurnAndSafeReclaimProbesWriter(t *testing.T) {
	dir := t.TempDir()
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", DataDir: dir, CodexRemote: "unix://"}, governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	exec := &ownershipExec{fakeExec: &fakeExec{sink: h}, sendErr: fmt.Errorf("send: %w", backend.ErrThreadActiveWriter)}
	h.SetExecutor(exec)
	c := newTestClient(h)
	h.registry.Create("s1", "codex", dir, backend.Codex, "", "", "thread-1")

	route(h, c, `{"type":"message","session_id":"s1","request_id":"r1","content":"must not race desktop"}`)
	control := waitForType(t, c, "session_control")
	if control["owner"] != governance.ControlDesktop {
		t.Fatalf("control=%v", control)
	}
	rejected := waitForType(t, c, "error")
	if rejected["code"] != "session_controlled_by_desktop" || rejected["request_id"] != "r1" {
		t.Fatalf("turn rejection=%v", rejected)
	}

	exec.claimErr = fmt.Errorf("claim: %w", backend.ErrThreadActiveWriter)
	route(h, c, `{"type":"reclaim_from_desktop","session_id":"s1"}`)
	stillDesktop := waitForType(t, c, "session_control")
	if stillDesktop["owner"] != governance.ControlDesktop || !strings.Contains(stillDesktop["message"].(string), "still owns") {
		t.Fatalf("unsafe reclaim result=%v", stillDesktop)
	}
	if h.controls.MobileMayWrite("s1") {
		t.Fatal("failed ownership probe released mobile writes")
	}

	exec.claimErr = nil
	route(h, c, `{"type":"reclaim_from_desktop","session_id":"s1"}`)
	reclaimed := waitForType(t, c, "session_control")
	if reclaimed["owner"] != governance.ControlMobile || !h.controls.MobileMayWrite("s1") || exec.claimCalls.Load() != 2 {
		t.Fatalf("successful reclaim=%v calls=%d", reclaimed, exec.claimCalls.Load())
	}
}

func TestDesktopHandoffDetachesBridgeBeforeLeaseCompletes(t *testing.T) {
	dir := t.TempDir()
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", DataDir: dir, CodexRemote: "unix://"}, governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	exec := &ownershipExec{fakeExec: &fakeExec{sink: h}}
	h.SetExecutor(exec)
	c := newTestClient(h)
	h.registry.Create("s1", "codex", dir, backend.Codex, "", "", "thread-1")

	route(h, c, `{"type":"handoff_to_desktop","session_id":"s1"}`)
	waitForType(t, c, "session_control")
	active := waitForType(t, c, "session_control")
	if exec.releaseCalls.Load() != 1 || active["owner"] != governance.ControlDesktop {
		t.Fatalf("handoff active=%v releaseCalls=%d", active, exec.releaseCalls.Load())
	}

	exec.releaseErr = errors.New("unsubscribe failed")
	h.controls.Reclaim("s1", "thread-1")
	route(h, c, `{"type":"handoff_to_desktop","session_id":"s1"}`)
	waitForType(t, c, "session_control")
	failed := waitForType(t, c, "session_control")
	if failed["owner"] != governance.ControlMobile || !strings.Contains(failed["message"].(string), "failed") {
		t.Fatalf("failed detach incorrectly completed handoff: %v", failed)
	}
}
