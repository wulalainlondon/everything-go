package backend

import (
	"context"
	"errors"
	"everything-go/internal/session"
)

// ContextResetExecutor is deliberately separate from Clear. Export is a bounded,
// complete read-only native transcript; reset detaches only the exact old
// generation and retains the logical session and its historical generations.
type ContextExport struct {
	Provider         string           `json:"provider"`
	ThreadID         string           `json:"thread_id"`
	SourceGeneration string           `json:"source_generation"`
	Messages         []map[string]any `json:"messages"`
	Complete         bool             `json:"complete"`
}
type ContextResetExecutor interface {
	ExportContext(context.Context, *session.Session) (ContextExport, error)
	ResetContext(context.Context, *session.Session, string) (string, error)
}

type resetAuthorizerKey struct{}

// The original HTTP principal/source authorizer is invoked by each provider
// after every idle wait, at the actual destructive writer boundary.
func WithContextResetGuard(ctx context.Context, guard func() bool) context.Context {
	return context.WithValue(ctx, resetAuthorizerKey{}, guard)
}
func CheckContextResetGuard(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	guard, ok := ctx.Value(resetAuthorizerKey{}).(func() bool)
	if !ok || guard == nil || !guard() {
		return errors.New("reset_original_authority_changed_before_effect")
	}
	return nil
}
