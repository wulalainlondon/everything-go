package core

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func ensureMediaDirectory(root, path string) error {
	rel, e := filepath.Rel(root, path)
	if e != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return errors.New("media directory outside store")
	}
	if e = os.MkdirAll(root, 0700); e != nil {
		return e
	}
	current := root
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		current = filepath.Join(current, part)
		if e = os.Mkdir(current, 0700); e != nil && !os.IsExist(e) {
			return e
		}
		info, e := os.Lstat(current)
		if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("media directory symlink/replacement")
		}
	}
	return nil
}
func openMediaPart(path string) (*os.File, error) {
	f, e := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return nil, e
	}
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("media part not regular")
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok && st.Nlink != 1 {
		f.Close()
		return nil, errors.New("media part aliases another owner")
	}
	return f, nil
}
