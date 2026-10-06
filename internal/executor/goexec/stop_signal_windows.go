//go:build windows

package goexec

import (
	"os"
	"os/exec"
)

// Preserve the existing Windows interruption/fallback behavior.
func configureOwnedProcessGroup(*exec.Cmd)        {}
func signalOwnedProcessGroup(cmd *exec.Cmd) error { return cmd.Process.Signal(os.Interrupt) }
