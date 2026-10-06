package goexec

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func TestClaudeStopLetsWrapperReapItsOwnedChild(t *testing.T) {
	dir := t.TempDir()
	childFile := filepath.Join(dir, "child.pid")
	wrapper := filepath.Join(dir, "claude-wrapper")
	script := "#!/bin/bash\ncleanup() { kill \"$CHILD\" 2>/dev/null || true; wait \"$CHILD\" 2>/dev/null || true; exit 0; }\ntrap cleanup INT TERM EXIT\nsleep 60 &\nCHILD=$!\necho \"$CHILD\" > \"" + childFile + "\"\nwait \"$CHILD\"\n"
	if err := os.WriteFile(wrapper, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	sink := &capSink{}
	c := NewClaude(sink, wrapper)
	s := session.NewRegistry().Create("qa-wrapper", "QA", dir, "claude", "sonnet", "read-only", "")
	c.mu.Lock()
	p, err := c.spawn(s)
	if err == nil {
		c.procs[s.ID] = p
		p.beginTurn("r_wrapper_stop", false)
	}
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer p.cancel()
	var pid int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(childFile)
		if err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
			if pid > 0 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid <= 0 {
		t.Fatal("wrapper child did not start")
	}
	child, _ := os.FindProcess(pid)
	defer child.Kill() // only this test's isolated child if the regression occurs
	if err := c.Stop(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if err := child.Signal(syscall.Signal(0)); err == nil {
		t.Fatal("wrapper exited but its owned child still runs; cleanup trap was bypassed")
	}
}

func TestClaudeStopFailureKeepsProcessBindingAndEmitsNoTerminal(t *testing.T) {
	sink := &capSink{}
	backend := NewClaude(sink, "unused")
	current := session.NewRegistry().Create("s1", "test", t.TempDir(), "claude", "", "", "")
	cancelled := false
	process := &proc{reqID: "r1", cancel: func() { cancelled = true }, exited: make(chan struct{})}
	backend.procs[current.ID] = process
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := backend.Stop(ctx, current); err == nil {
		t.Fatal("unconfirmed exit accepted")
	}
	if !cancelled || backend.procs[current.ID] != process {
		t.Fatal("lost pending process binding; cannot retry stop")
	}
	if sink.count(func(event any) bool { _, ok := event.(protocol.Stopped); return ok }) != 0 {
		t.Fatal("unconfirmed exit emitted stopped")
	}
	close(process.exited)
}
func TestClaudeStopWaitsForActualChildExit(t *testing.T) {
	sink := &capSink{}
	backend := NewClaude(sink, "unused")
	current := session.NewRegistry().Create("s1", "test", t.TempDir(), "claude", "", "", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	command := exec.CommandContext(ctx, "/bin/sh", "-c", "exec sleep 60")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &proc{cmd: command, reqID: "r1", cancel: cancel, exited: make(chan struct{})}
	backend.procs[current.ID] = process
	go backend.watchProc(current, process)
	if err := backend.Stop(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	select {
	case <-process.exited:
	case <-time.After(time.Second):
		t.Fatal("stop returned before process exit")
	}
	if sink.count(func(event any) bool {
		value, ok := event.(protocol.Stopped)
		return ok && value.SessionID == "s1" && value.RequestID == "r1"
	}) != 1 {
		t.Fatal("missing correlated confirmed stop")
	}
	backend.mu.Lock()
	bound := backend.procs[current.ID]
	backend.mu.Unlock()
	if bound != nil {
		t.Fatal("manually stopped process restarted")
	}
}

func TestClaudeStopPrivateGroupLeavesUnrelatedProcessAlive(t *testing.T) {
	// Only two test-owned processes; no real provider/daemon/device/workload.
	sentinel := exec.Command("/bin/sh", "-c", "exec sleep 60")
	if err := sentinel.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { sentinel.Process.Kill(); sentinel.Wait() }()
	sink := &capSink{}
	backend := NewClaude(sink, "/bin/sh")
	current := session.NewRegistry().Create("private-group-fixture", "QA", t.TempDir(), "claude", "", "read-only", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	command := exec.CommandContext(ctx, "/bin/sh", "-c", "exec sleep 60")
	configureOwnedProcessGroup(command)
	command.Cancel = func() error { return signalOwnedProcessGroup(command) }
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &proc{cmd: command, reqID: "r_private_stop", cancel: cancel, exited: make(chan struct{})}
	backend.procs[current.ID] = process
	go backend.watchProc(current, process)
	if err := backend.Stop(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	if err := sentinel.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatal("unrelated process was signalled", err)
	}
}
