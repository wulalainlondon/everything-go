//go:build !windows

package instances

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestManagerRecoversAndStopsVerifiedChildAfterRestart(t *testing.T) {
	storeDir := t.TempDir()
	childDir := filepath.Join(storeDir, "child")
	if err := os.MkdirAll(childDir, 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/bash", "-c", "trap 'exit 0' TERM; while true; do sleep 1; done", childDir)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	go func() { _ = cmd.Wait() }()
	if err := os.WriteFile(filepath.Join(childDir, "bridge.pid"), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(childDir, ".bridge_state"), []byte("enabled"), 0o600); err != nil {
		t.Fatal(err)
	}

	m := New(storeDir, "/bin/bash")
	if _, code := m.Upsert(Instance{Name: "child", Port: 9444, DataDir: childDir}); code != "" {
		t.Fatal(code)
	}
	status := m.List()
	if len(status) != 1 || status[0].State != "running" || status[0].BridgePID == nil {
		t.Fatalf("status = %+v", status)
	}
	if code := m.Stop("child"); code != "" {
		t.Fatalf("stop = %q", code)
	}
	deadline := time.Now().Add(time.Second)
	for processAlive(cmd.Process.Pid) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if processAlive(cmd.Process.Pid) {
		t.Fatal("verified child still alive")
	}
}
