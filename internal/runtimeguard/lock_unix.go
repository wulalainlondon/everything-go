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
	// A marker name can be renamed or removed while its old inode is locked.
	// Keep the original directory itself locked as the exclusion anchor.
	anchor, err := os.OpenFile(dir, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(anchor.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		anchor.Close()
		return nil, fmt.Errorf("runtime_data_directory_in_use: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, ".go-service-owner.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		anchor.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		anchor.Close()
		return nil, fmt.Errorf("runtime_lock_marker_not_regular")
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		anchor.Close()
		return nil, fmt.Errorf("runtime_data_directory_in_use: %w", err)
	}
	return func() { _ = f.Close(); _ = anchor.Close() }, nil
}
