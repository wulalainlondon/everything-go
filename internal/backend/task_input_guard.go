package backend

import (
	"context"
	"errors"
)

type taskInputGuardKey struct{}

// Task input checks run at the existing provider's transport writer boundary,
// after awaits/locks and before the first original input bytes are written.
func WithTaskInputGuard(ctx context.Context, guard func() error) context.Context {
	return context.WithValue(ctx, taskInputGuardKey{}, guard)
}
func CheckTaskInputGuard(ctx context.Context) error {
	if ctx == nil {
		return errors.New("task_input_guard_missing")
	}
	if guard, ok := ctx.Value(taskInputGuardKey{}).(func() error); ok {
		return guard()
	}
	return nil
}
func HasTaskInputGuard(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	_, ok := ctx.Value(taskInputGuardKey{}).(func() error)
	return ok
}
