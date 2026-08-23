//go:build windows

package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
)

func reexecSelf(exePath string, args, env []string) error {
	cmd := exec.Command(exePath, args[1:]...)
	cmd.Env, cmd.Stdout, cmd.Stderr = env, os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	os.Exit(0)
	return nil
}

// Windows denies deleting an open file, which provides a non-blocking
// process-wide lock without adding a platform-specific dependency.
func acquireIndexLock(dataDir string) (func(), error) {
	name := filepath.Join(dataDir, "everything_go_indexer.lock")
	f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, errors.New("indexer already running")
	}
	return func() { _ = f.Close(); _ = os.Remove(name) }, nil
}
