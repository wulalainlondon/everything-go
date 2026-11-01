package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/agentactivity"
	"everything-go/internal/session"
	"time"
)

// ObserveHostedActivity is an original-owner internal port. It has no wire
// command or public registration endpoint. Only an authenticated Hub may supply
// authority and the original session. It never starts or recovers a transport.
// First read registers capture for later original native events; an empty source
// is partial coverage, never proof of no children or successful restoration.
func (c *Codex) ObserveHostedActivity(ctx context.Context, s *session.Session, authority string) (agentactivity.NativeProbe, error) {
	c.startMu.Lock()
	mode := c.appServerMode
	c.startMu.Unlock()
	ss := s.Snapshot()
	if mode != "daemon" || s.State() == session.Closed || ss.Hidden || ss.Backend != "codex" || c.rpc == nil || !c.rpc.hasWriter() {
		return agentactivity.NativeProbe{}, agentactivity.ErrNativeSource
	}
	rpc := hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: authority}}
	scope := rpc.Scope()
	st := c.state(s.ID)
	st.mu.Lock()
	if st.hostedActivity == nil || st.hostedActivityScope != scope {
		st.hostedActivity = agentactivity.NewNativeSource(scope)
		st.hostedActivityScope = scope
	}
	source := st.hostedActivity
	st.mu.Unlock()
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return source.Probe(bounded, rpc)
}

// Capture occurs before retired parent-turn filtering. A child activity can
// arrive after its parent generation ends; its exact native parent turn stays
// attached. It is never reassigned to the latest request or assistant reply.
func (c *Codex) observeHostedNativeItem(s *session.Session, raw json.RawMessage) {
	st := c.state(s.ID)
	st.mu.Lock()
	source, scope := st.hostedActivity, st.hostedActivityScope
	st.mu.Unlock()
	if source == nil {
		return
	}
	rpc := hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: scope.Authority}}
	if rpc.Scope() != scope {
		return
	}
	_ = source.Observe(scope, raw) // Invalid shapes never enter the source.
}

type hostedActivityRPC struct{ codexVisibilityRPC }

func (r hostedActivityRPC) Scope() agentactivity.NativeScope {
	r.c.startMu.Lock()
	mode := r.c.appServerMode
	r.c.startMu.Unlock()
	s := r.codexVisibilityRPC.Scope()
	ss := r.s.Snapshot()
	if mode != "daemon" || r.s.State() == session.Closed || ss.Hidden || ss.Backend != "codex" || r.c.rpc == nil || !r.c.rpc.hasWriter() {
		return agentactivity.NativeScope{}
	}
	return agentactivity.NativeScope{Authority: s.Authority, Session: s.SessionID, RootThread: s.RootActorID, Epoch: s.Epoch}
}
func (r hostedActivityRPC) ListTurns(ctx context.Context, id string) (json.RawMessage, error) {
	if ctx.Err() != nil || !r.c.rpc.hasWriter() {
		return nil, agentactivity.ErrNativeSource
	}
	epoch := r.c.rpc.transportEpoch.Load()
	// A metadata failure retires only this read. It must not enter the original
	// model recovery scheduler or interrupt healthy work on the shared provider.
	return r.c.rpc.requestAtEpochContext(ctx, "thread/turns/list", map[string]any{"threadId": id, "limit": 16, "itemsView": "notLoaded", "sortDirection": "desc"}, time.Second, &epoch)
}
