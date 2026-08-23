//go:build windows

package netsvc

import "syscall"

func setBroadcastOptions(fd uintptr) error {
	handle := syscall.Handle(fd)
	_ = syscall.SetsockoptInt(handle, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	return syscall.SetsockoptInt(handle, syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
}
