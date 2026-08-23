//go:build !windows

package instances

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func configureDetached(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} }

func processAlive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func processMatches(pid int, exePath, dataDir string) bool {
	out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=").Output()
	if err != nil {
		return false
	}
	command := string(out)
	return exePath != "" && strings.Contains(command, exePath) && dataDir != "" && strings.Contains(command, dataDir)
}

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
