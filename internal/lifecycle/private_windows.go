//go:build windows

package lifecycle

import (
	"errors"
	"os"
)

func openJournal(path string) (*os.File, error) {
	return nil, errors.New("private_lifecycle_journal_unsupported")
}
func privateOwner(s os.FileInfo) bool { return false }
