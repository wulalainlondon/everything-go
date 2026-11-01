package goexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"everything-go/internal/activitywire"
	"everything-go/internal/agentactivity"
	"everything-go/internal/session"
	"sort"
	"strings"
	"time"
)

func (c *Codex) observeHostedWait(s *session.Session, method string, raw []byte) {
	st := c.state(s.ID)
	st.mu.Lock()
	scope := st.hostedActivityScope
	active := st.hostedActivity != nil
	st.mu.Unlock()
	if !active || (hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: scope.Authority}}).Scope() != scope {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.hostedActivityScope == scope {
		if _, e := agentactivity.DecodeNativeMailboxWait(method, raw, scope.RootThread); e == nil {
			st.hostedWaits.Observe(method, raw, scope.RootThread)
			st.hostedViewRevision++
		}
	}
}

// HostedActivitySnapshot is called only by the original authenticated Hub.
// This projection does not choose a reply by timestamp, body, or latest request.
func (c *Codex) HostedActivitySnapshot(ctx context.Context, s *session.Session, authority string) (activitywire.Snapshot, error) {
	// Register original capture before recovery; it never starts a daemon/model.
	_, e := c.ObserveHostedActivity(ctx, s, authority)
	if e != nil {
		return activitywire.Snapshot{}, e
	}
	st := c.state(s.ID)
	st.mu.Lock()
	source, scope := st.hostedActivity, st.hostedActivityScope
	if st.hostedRecovery == nil || st.hostedRecovery.scope != scope {
		st.hostedRecovery = &hostedRecovery{scope: scope}
	}
	recovery := st.hostedRecovery
	st.mu.Unlock()
	recovered := c.recoverHostedPrefix(ctx, s, recovery, source)
	if recovered == "source_changed" || recovered == "native_source_rejected" {
		return activitywire.Snapshot{}, agentactivity.ErrNativeSource
	}
	rpc := hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: authority}}
	st.mu.Lock()
	viewRevision := st.hostedViewRevision
	waits := st.hostedWaits.Snapshot()
	liveViews := map[string]activitywire.ParentView{}
	for k, v := range st.hostedBoundaries.views {
		liveViews[k] = v
	}
	st.mu.Unlock()
	recovery.mu.Lock()
	recoveryRevision := recovery.revision
	recoveryStats := recovery.stats
	views := map[string]activitywire.ParentView{}
	for k, v := range recovery.boundaries.views {
		views[k] = v
	}
	recovery.mu.Unlock()
	for k, v := range liveViews {
		if _, exists := views[k]; !exists {
			views[k] = v
		}
	}
	probe, e := source.Probe(ctx, rpc)
	if e != nil {
		return activitywire.Snapshot{}, e
	}
	c.historyRequestMu.Lock()
	journal, je := c.loadTurnRequests(scope.RootThread)
	c.historyRequestMu.Unlock()
	out := activitywire.Snapshot{Type: "agent_activity_snapshot", Schema: "agent-activity.v2", Authority: authority, SessionID: s.ID, RootThreadID: scope.RootThread, SourceEpoch: scope.Epoch, ConfigRevision: int64(s.Snapshot().ConfigRevision), Revision: int64(probe.SourceRevision), NativeSourceRevision: int64(probe.SourceRevision), ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Availability: "live", Coverage: probe.Coverage, Actors: []activitywire.Actor{}, Origins: []activitywire.Origin{}, Waits: []activitywire.Wait{}, Recovery: recovered, ParentViews: []activitywire.ParentView{}, Capabilities: []string{"direct_native_actors", "actual_native_rounds", "generic_mailbox_wait", "creation_model", "public_final_read", "partial_inventory"}}
	out.RecoveryStats = recoveryStats
	origins := map[string]bool{}
	for _, a := range probe.Actors {
		key := sha256.Sum256([]byte(strings.Join([]string{scope.Authority, scope.Session, scope.RootThread, a.ThreadID}, "\x00")))
		actor := activitywire.Actor{ID: hex.EncodeToString(key[:]), ThreadID: a.ThreadID, Path: a.AgentPath, Activities: []activitywire.Activity{}, Rounds: []activitywire.Round{}, RoundsTruncated: a.RoundsTruncated}
		for _, v := range a.Activities {
			actor.Activities = append(actor.Activities, activitywire.Activity{ID: v.ID, ParentTurnID: v.ParentTurn, Kind: v.Kind, AtMS: v.CompletedAtMS, CreationModel: v.ResolvedCreationModel, CreationEffort: v.ResolvedCreationEffort})
			origins[v.ParentTurn] = true
		}
		sort.Slice(actor.Activities, func(i, j int) bool {
			a, b := actor.Activities[i], actor.Activities[j]
			if a.AtMS == b.AtMS {
				return a.ID < b.ID
			}
			return a.AtMS < b.AtMS
		})
		for _, v := range a.Rounds {
			actor.Rounds = append(actor.Rounds, activitywire.Round{ID: v.ID, Status: v.Status, RootChainTurnID: v.RootChainTurn, ParentAssociation: v.ParentAssociation, StartedAt: v.StartedAt, CompletedAt: v.CompletedAt})
		}
		out.Actors = append(out.Actors, actor)
	}
	st.mu.Lock()
	current := st.hostedActivityScope == scope && st.hostedActivity == source && st.hostedViewRevision == viewRevision
	st.mu.Unlock()
	recovery.mu.Lock()
	sameRecovery := recovery.revision == recoveryRevision
	recovery.mu.Unlock()
	if !current || !sameRecovery || rpc.Scope() != scope || source.ValidateProbe(probe) != nil {
		return activitywire.Snapshot{}, agentactivity.ErrNativeChanged
	}
	for _, view := range views {
		out.ParentViews = append(out.ParentViews, view)
		origins[view.TurnID] = true
	}
	sort.Slice(out.ParentViews, func(i, j int) bool { return out.ParentViews[i].TurnID < out.ParentViews[j].TurnID })
	for _, w := range waits {
		out.Waits = append(out.Waits, activitywire.Wait{ID: w.CallID, ParentTurnID: w.ParentTurn, State: w.Phase})
		origins[w.ParentTurn] = true
	}
	// Ambiguous original journal mappings stay unlinked; reads never repair them.
	counts := map[string]int{}
	if je == nil {
		for _, id := range journal.Requests {
			counts[id]++
		}
	}
	for turn := range origins {
		var request *string
		if je == nil {
			id := journal.Requests[turn]
			if strings.HasPrefix(id, "r_") && counts[id] == 1 {
				request = &id
			}
		}
		out.Origins = append(out.Origins, activitywire.Origin{TurnID: turn, RequestID: request})
	}
	sort.Slice(out.Origins, func(i, j int) bool { return out.Origins[i].TurnID < out.Origins[j].TurnID })
	out.AggregateObservedActors = int64(len(out.Actors))
	st.mu.Lock()
	current = st.hostedActivity == source && st.hostedActivityScope == scope && st.hostedViewRevision == viewRevision
	st.mu.Unlock()
	recovery.mu.Lock()
	sameRecovery = recovery.revision == recoveryRevision
	recovery.mu.Unlock()
	if !current || !sameRecovery || source.ValidateProbe(probe) != nil || rpc.Scope() != scope {
		return activitywire.Snapshot{}, agentactivity.ErrNativeChanged
	}
	return out, nil
}

func (c *Codex) HostedActivityScope(s *session.Session, authority string) agentactivity.NativeScope {
	scope := (hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: authority}}).Scope()
	st := c.state(s.ID)
	st.mu.Lock()
	source := st.hostedActivity
	bound := st.hostedActivityScope
	st.mu.Unlock()
	if source == nil || bound != scope || !source.Usable(scope) {
		return agentactivity.NativeScope{}
	}
	return scope
}

func (c *Codex) HostedActivityReport(ctx context.Context, s *session.Session, authority string, expected agentactivity.NativeScope, child, turn string) (agentactivity.NativePublicReturn, string, error) {
	rpc := hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: authority}}
	if rpc.Scope() != expected {
		return agentactivity.NativePublicReturn{}, "", agentactivity.ErrNativeChanged
	}
	st := c.state(s.ID)
	st.mu.Lock()
	source := st.hostedActivity
	scope := st.hostedActivityScope
	st.mu.Unlock()
	if source == nil || scope != expected {
		return agentactivity.NativePublicReturn{}, "", agentactivity.ErrNativeChanged
	}
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return source.PublicText(bounded, rpc, child, turn)
}
