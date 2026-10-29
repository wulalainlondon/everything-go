package goexec

import (
	"fmt"
	"os"
	"path/filepath"
)

// validateExistingProviderConnection is also used at the last local-subject
// writer fence. An unavailable endpoint/account source never falls back to
// daemon management, private login or an unguarded ordinary send.
func (c *Codex) validateExistingProviderConnection() error {
	if c.appServerMode != "daemon" || c.appSandbox != nil || !filepath.IsAbs(c.appServerSocket) || !filepath.IsAbs(c.subjectAuthPath) {
		return fmt.Errorf("existing provider requires daemon mode, explicit absolute socket and original subject auth path")
	}
	socket, err := os.Lstat(c.appServerSocket)
	if err != nil || socket.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("existing provider socket unavailable")
	}
	auth, err := os.Lstat(c.subjectAuthPath)
	if err != nil || !auth.Mode().IsRegular() || auth.Size() <= 0 || auth.Size() >= 1<<20 {
		return fmt.Errorf("existing provider subject auth source unavailable")
	}
	return nil
}
