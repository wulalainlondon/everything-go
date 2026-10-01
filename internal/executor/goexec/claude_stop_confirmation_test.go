package goexec

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

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
