package goexec

import "errors"

var (
	errProcDead   = errors.New("app-server process died")
	errNoThreadID = errors.New("codex thread/start returned no thread id")
)
