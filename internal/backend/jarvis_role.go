package backend

import (
	"context"
	"everything-go/internal/session"
)

// Role epoch is supplied by the host and captured at the ORIGINAL native turn
// or voice start. A model cannot supply or refresh its own authority witness.
type SessionRoleProvider interface{ SessionRoleEpoch(*session.Session) uint64 }

// ProviderDefaultEffortObserved is internal host evidence. It is never decoded
// from a client message and the Session-owned receipt has no public constructor.
type ProviderDefaultEffortObserved struct {
	Transition *session.DefaultEffortTransition
}
type ResidentObservation struct {
	State      string `json:"state"`
	Thread     string `json:"thread_id"`
	CheckedAt  int64  `json:"checked_at"`
	FreshUntil int64  `json:"fresh_until"`
	Reason     string `json:"reason"`
	ToolState  string `json:"tool_state"`
	Recovered  bool   `json:"recovered"`
	Generating bool   `json:"model_generating"`
}
type ResidentExecutor interface {
	MaintainResident(context.Context, *session.Session, string, uint64, func() bool) (ResidentObservation, error)
}

type roleEpochKey struct{}

func WithRoleEpoch(ctx context.Context, epoch uint64) context.Context {
	return context.WithValue(ctx, roleEpochKey{}, epoch)
}
func RoleEpoch(ctx context.Context) uint64 { v, _ := ctx.Value(roleEpochKey{}).(uint64); return v }

func ContextRoleEpoch(ctx context.Context) (uint64, bool) {
	v, ok := ctx.Value(roleEpochKey{}).(uint64)
	return v, ok
}
