//go:build !windows

package goexec

import (
	"os"
	"syscall"
)

func processExitEvidence(state *os.ProcessState) (*int, string) {
	if state == nil {
		return nil, ""
	}
	code := state.ExitCode()
	signal := ""
	if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		signal = status.Signal().String()
	}
	return &code, signal
}
