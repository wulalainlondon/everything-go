package goexec

import "errors"

var (
	errProcDead   = errors.New("app-server process died")
	errNoThreadID = errors.New("codex thread/start returned no thread id")
)

// imageInputError is definitive local preparation failure, never a native ACK.
// Keep private runtime paths out of the product error text.
type imageInputError struct{ cause error }

func (e *imageInputError) Error() string { return "圖片未能準備，未送出" }
func (e *imageInputError) Unwrap() error { return e.cause }
