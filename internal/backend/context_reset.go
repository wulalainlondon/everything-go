package backend

import (
	"context"
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
