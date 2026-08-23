//go:build windows

package instances

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func configureDetached(_ *exec.Cmd) {}
func processAlive(pid int) bool {
	return exec.Command("powershell", "-NoProfile", "-Command", "Get-Process -Id "+strconv.Itoa(pid)+" -ErrorAction Stop | Out-Null").Run() == nil
}
func processMatches(pid int, exePath, dataDir string) bool {
	script := "(Get-CimInstance Win32_Process -Filter 'ProcessId=" + strconv.Itoa(pid) + "').CommandLine"
	out, err := exec.Command("powershell", "-NoProfile", "-Command", script).Output()
	if err != nil {
		return false
	}
	command := strings.ToLower(string(out))
	return exePath != "" && strings.Contains(command, strings.ToLower(exePath)) && dataDir != "" && strings.Contains(command, strings.ToLower(dataDir))
}
func terminateProcess(proc *os.Process) error { return proc.Kill() }
