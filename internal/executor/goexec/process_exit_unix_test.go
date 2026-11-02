//go:build !windows

package goexec

import (
	"os/exec"
	"testing"
)

func TestOwnedProcessExitEvidenceKeepsExitCodeAndSignalSeparate(t *testing.T) {
	for _, scenario := range []struct {
		command string
		code    int
		signal  bool
	}{
		{"exit 7", 7, false}, {"kill -TERM $$", -1, true},
	} {
		cmd := exec.Command("sh", "-c", scenario.command)
		_ = cmd.Run()
		code, signal := processExitEvidence(cmd.ProcessState)
		if code == nil || *code != scenario.code || (signal != "") != scenario.signal {
			t.Fatal(code, signal)
		}
	}
	if code, signal := processExitEvidence(nil); code != nil || signal != "" {
		t.Fatal("invented unknown process status")
	}
}
