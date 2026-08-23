//go:build !windows

package main

import (
	"os"
	"path/filepath"
	"syscall"
)

func reexecSelf(exePath string, args, env []string) error { return syscall.Exec(exePath, args, env) }

func acquireIndexLock(dataDir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dataDir, "everything_go_indexer.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
}
