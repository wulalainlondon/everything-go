//go:build !windows

package lifecycle

import (
	"os"
	"syscall"
)

func openJournal(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}
func privateOwner(s os.FileInfo) bool {
	v, ok := s.Sys().(*syscall.Stat_t)
	return ok && v.Uid == uint32(os.Getuid()) && v.Nlink == 1
}
