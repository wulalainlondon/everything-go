//go:build !windows

package goexec

import (
	"os/exec"
	"syscall"
)

// Every executor-owned Claude process receives a fresh private process group.
// Shared provider daemons are never members of this group or signal targets.
func configureOwnedProcessGroup(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }
func signalOwnedProcessGroup(cmd *exec.Cmd) error {
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err == nil && pgid == cmd.Process.Pid {
		return syscall.Kill(-pgid, syscall.SIGTERM)
	}
	// No guessed group: only the exact owned PID may be the fallback target.
	return cmd.Process.Signal(syscall.SIGTERM)
}
