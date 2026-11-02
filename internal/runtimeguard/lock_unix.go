//go:build !windows

// Package runtimeguard prevents two service writers sharing one data directory.
package runtimeguard

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// Acquire must run before durable service stores are opened. Do not unlink the
// lock on release: another process may already be waiting on the same inode.
func Acquire(dir string) (func(), error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, ".go-service-owner.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("runtime_data_directory_in_use: %w", err)
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
}
