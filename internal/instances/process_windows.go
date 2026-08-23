//go:build windows

package instances

import (
	"os"
	"os/exec"
)

func configureDetached(_ *exec.Cmd) {}
func processAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	return err == nil && p.Signal(os.Signal(nil)) == nil
}
func terminateProcess(proc *os.Process) error { return proc.Kill() }
