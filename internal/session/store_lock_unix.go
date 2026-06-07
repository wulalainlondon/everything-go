//go:build !windows

package session

import (
	"os"
	"path/filepath"
	"syscall"
)

func (st *Store) lock() (func(), error) {
	if err := os.MkdirAll(filepath.Dir(st.path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(st.path+".lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
