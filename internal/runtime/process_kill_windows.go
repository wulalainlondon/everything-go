//go:build windows

package runtime

import "os"

func KillProcess(pid int, _ bool) (bool, string) {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false, "process_not_found"
	}
	if err := proc.Kill(); err != nil {
		return false, "kill_failed: " + err.Error()
	}
	return true, ""
}
