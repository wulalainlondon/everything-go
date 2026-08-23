//go:build !windows

package instances

import (
	"os"
	"os/exec"
	"syscall"
	"time"
)

func configureDetached(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} }

func processAlive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func terminateProcess(proc *os.Process) error {
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !processAlive(proc.Pid) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return proc.Kill()
}
