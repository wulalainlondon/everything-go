package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/agentactivity"
	"everything-go/internal/session"
	"strconv"
	"strings"
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
func (r hostedActivityRPC) ReadThread(ctx context.Context, p agentactivity.NativeReadProof, id string) (json.RawMessage, error) {
	return r.readAtProof(ctx, p, "thread/read", map[string]any{"threadId": id, "includeTurns": false})
}
func (r hostedActivityRPC) ListTurns(ctx context.Context, p agentactivity.NativeReadProof, id string) (json.RawMessage, error) {
	return r.readAtProof(ctx, p, "thread/turns/list", map[string]any{"threadId": id, "limit": 16, "itemsView": "notLoaded", "sortDirection": "desc"})
}

// Scope is never sampled again as a substitute for the proof which authorized
// this read. The original transport, registry and session identity are fenced
// through bytes. All local locks are nonblocking and released before waiting
// for a response. In particular, no Scope()/hasWriter() runs under rpc.wmu.
func (r hostedActivityRPC) readAtProof(ctx context.Context, p agentactivity.NativeReadProof, method string, params any) (json.RawMessage, error) {
	m, ok := params.(map[string]any)
	if !ok {
		return nil, agentactivity.ErrNativeSource
	}
	id, _ := m["threadId"].(string)
	turn, _ := m["turnId"].(string)
	if id == "" {
		return nil, agentactivity.ErrNativeSource
	}
	switch method {
	case "thread/read":
	case "thread/turns/list":
		if id == p.Scope.RootThread {
			return nil, agentactivity.ErrNativeSource
		}
	case "thread/items/list":
		if turn == "" {
			return nil, agentactivity.ErrNativeSource
		}
	default:
		return nil, agentactivity.ErrNativeSource
	}
	parts := strings.Split(p.Scope.Epoch, ":")
	if len(parts) != 3 || p.Scope.Authority != r.authority || p.Scope.Session != r.s.ID || r.c.rpc == nil {
		return nil, agentactivity.ErrNativeChanged
	}
	epoch, e := strconv.ParseUint(parts[1], 10, 64)
	if e != nil {
		return nil, agentactivity.ErrNativeChanged
	}
	config, e := strconv.ParseUint(parts[2], 10, 64)
	if e != nil {
		return nil, agentactivity.ErrNativeChanged
	}
	guard := func() (release func(), err error) {
		var releases []func()
		releaseAll := func() {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
		}
		ok := false
		defer func() {
			if !ok {
				releaseAll()
			}
		}()
		if !r.c.startMu.TryLock() {
			return nil, agentactivity.ErrNativeChanged
		}
		releases = append(releases, r.c.startMu.Unlock)
		if r.c.appServerMode != "daemon" || !r.c.mu.TryLock() {
			return nil, agentactivity.ErrNativeChanged
		}
		releases = append(releases, r.c.mu.Unlock)
		if r.c.metadataObserverBoot != parts[0] || parts[0] == "" {
			return nil, agentactivity.ErrNativeChanged
		}
		unlock, e := r.s.AcquireNativeObservation(p.Scope.RootThread, config, "codex")
		if e != nil {
			return nil, agentactivity.ErrNativeChanged
		}
		releases = append(releases, unlock)
		unlock, e = p.AcquireFor(id, turn)
		if e != nil {
			return nil, e
		}
		releases = append(releases, unlock)
		ok = true
		return releaseAll, nil
	}
	// Timeout/cancel retires only this correlation, never model recovery.
	return r.c.rpc.requestAtEpochGuarded(ctx, method, params, time.Second, &epoch, guard)
}
