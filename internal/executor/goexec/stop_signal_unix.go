//go:build !windows

package goexec

import (
	"os"
	"syscall"
)

// SIGINT can be ignored by a non-interactive shell and its asynchronous child.
// TERM follows the wrapper's cleanup/reap path before the existing WaitDelay.
// This signal targets only the executor-owned process, never a shared daemon.
func ownedProcessTerminationSignal() os.Signal { return syscall.SIGTERM }
