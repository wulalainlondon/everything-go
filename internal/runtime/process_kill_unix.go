//go:build !windows

package runtime

import "syscall"

func KillProcess(pid int, force bool) (bool, string) {
	sig := syscall.SIGTERM
	if force {
		sig = syscall.SIGKILL
	}
	err := syscall.Kill(pid, sig)
	if err == nil {
		return true, ""
	}
	switch err {
	case syscall.ESRCH:
		return false, "process_not_found"
	case syscall.EPERM:
		return false, "permission_denied"
	default:
		return false, "kill_failed: " + err.Error()
	}
}
