package backend

import (
	"context"
	"everything-go/internal/taskapi"
)

// TaskInvocationState is supplied by a server-owned native/process registry.
// No JSON/tool argument or observed runtime directory can construct authority.
type TaskInvocationState struct {
	ProviderVersion                 string
	Namespace                       string
	SchemaHash                      string
	Registered, Loaded, ActiveLease bool
	Invocation                      taskapi.Invocation
	Context                         taskapi.VerifiedContext
}

// Original executor/tool transport implements this after owner-reviewed wiring.
// It must validate actual native call or MCP process generation, grant/run/epoch
// and lease. B4 builds provide only this port and isolated fixtures.
type TaskInvocationAuthority interface {
	LookupTaskInvocation(context.Context, taskapi.Invocation) (TaskInvocationState, error)
}
type TaskWorkerProfile struct{ Backend, Model, Effort string }
type TaskWorkerPlan struct {
	Profile                 TaskWorkerProfile
	ProviderVersion         string
	Parameters              map[string]any
	Scope                   taskapi.ChildScope
	NativeConsumptionProven bool
}
