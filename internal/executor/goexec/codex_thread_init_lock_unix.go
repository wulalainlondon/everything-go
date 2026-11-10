//go:build !windows

package goexec

// Initialization uses the same nonblocking OS file-lock primitive, but does not
// enter the daemon maintenance/recovery policy or change its ownership.
func lockCodexThreadInit(path string) (func(), error) { return lockCodexRecovery(path) }
