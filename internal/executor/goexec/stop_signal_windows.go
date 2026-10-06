//go:build windows

package goexec

import "os"

// Preserve the existing Windows interruption/fallback behavior.
func ownedProcessTerminationSignal() os.Signal { return os.Interrupt }
