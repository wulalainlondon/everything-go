package goexec

import (
	"encoding/json"
	"everything-go/internal/lifecycle"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestLifecycleLateCallbackKeepsCapturedEpochAndKnownKillIsSeparate(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	dir := t.TempDir()
	c.SetDataDir(dir)
	c.rpc.transportEpoch.Store(99)
	c.recordLifecycle(lifecycle.Event{Kind: "transport_disconnected", Owner: "provider_connection", Epoch: 7, Session: "original-session", Thread: "original-thread", Request: "original-request", Reason: "old_connection_closed"})
	child := exec.Command("sleep", "20")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	identity := lifecycle.ObserveProcess(child.Process.Pid)
	c.killOwnedProcess(child, identity, 8, "own_fixture_stop")
	_ = child.Wait()
	code, signal := processExitEvidence(child.ProcessState)
	c.recordLifecycle(lifecycle.Event{Kind: "process_exited", Owner: "bridge_stdio_child", Epoch: 8, Process: &identity, ExitCode: code, Signal: signal, Reason: "owned_wait"})
	raw, err := os.ReadFile(filepath.Join(dir, "native-lifecycle-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []lifecycle.Event
	if json.Unmarshal(raw, &rows) != nil || len(rows) != 4 {
		t.Fatal(string(raw))
	}
	if rows[0].Epoch != 7 || rows[0].Thread != "original-thread" || rows[0].Session != "original-session" || rows[0].Boot == "" {
		t.Fatal("old callback reassigned", rows[0])
	}
	if rows[1].Kind != "process_kill_attempt" || rows[2].Kind != "process_kill_result" || rows[3].Kind != "process_exited" || rows[3].Signal == "" {
		t.Fatal("kill/exit conflated", rows)
	}
	if identity.Observation != "os_metadata_observed" || identity.Start == "" || identity.Executable == "" || identity.Group == 0 {
		t.Fatal("owned identity absent", identity)
	}
}
