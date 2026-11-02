//go:build windows

package goexec

import "os"

func processExitEvidence(state *os.ProcessState) (*int, string) {
	if state == nil {
		return nil, ""
	}
	code := state.ExitCode()
	return &code, ""
}
