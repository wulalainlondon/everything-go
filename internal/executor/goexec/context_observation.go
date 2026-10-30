package goexec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/nativewatch"
	"everything-go/internal/session"
)

func contextEvidenceID(parts ...string) string {
	b, _ := json.Marshal(parts)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (c *Codex) contextCompletion(s *session.Session, thread, turn, item string, failed ...bool) {
	snap := s.Snapshot()
	if snap.Backend != "codex" || snap.ResumeID != thread || thread == "" || turn == "" || item == "" {
		return
	}
	if nativewatch.HasContextSource(c.findCodexSessionFile(thread), thread) {
		return
	} // Native watcher owns durable compacted-row identity.
	aliases := []string{}
	state := "completed"
	if len(failed) > 0 && failed[0] {
		state = "failed"
	}
	c.maintenanceMu.Lock()
	r, ok := c.maintenance[s.ID]
	c.maintenanceMu.Unlock()
	if ok && r.ThreadID == thread && r.TurnID == turn && r.OperationID != "" {
		aliases = append(aliases, contextEvidenceID("codex_maintenance", r.OperationID))
		state = "observed"
	}
	c.sink.Emit(backend.ContextTelemetry{SessionID: s.ID, Backend: "codex", NativeGeneration: thread, SourceRevision: snap.SourceRevision, EventID: contextEvidenceID("codex_item", thread, turn, item), CompletionState: state, Aliases: aliases})
}
func (c *Codex) maintenanceContextCompletion(r backend.Maintenance) {
	if (r.State != "completed" && r.State != "failed" && r.State != "unknown") || r.OperationID == "" || r.ThreadID == "" || r.TurnID == "" {
		return
	}
	c.mu.Lock()
	s := c.threadToSession[r.ThreadID]
	if s == nil {
		s = c.activeThreadOwner[r.ThreadID]
	}
	c.mu.Unlock()
	if s == nil {
		return
	}
	snap := s.Snapshot()
	if snap.ResumeID != r.ThreadID || snap.Backend != "codex" {
		return
	}
	c.sink.Emit(backend.ContextTelemetry{SessionID: s.ID, Backend: "codex", NativeGeneration: r.ThreadID, SourceRevision: snap.SourceRevision, EventID: contextEvidenceID("codex_maintenance", r.OperationID), CompletionState: r.State})
}

// Pointer fields distinguish an explicit native zero from an absent report.
func codexContextUsage(params json.RawMessage) (*int, *int) {
	type usage struct {
		Last *struct {
			Total *int `json:"totalTokens"`
			Other *int `json:"total_tokens"`
		} `json:"last"`
		Max      *int `json:"modelContextWindow"`
		OtherMax *int `json:"model_context_window"`
	}
	var p struct {
		Token *usage `json:"tokenUsage"`
		Usage *usage `json:"usage"`
	}
	if json.Unmarshal(params, &p) != nil {
		return nil, nil
	}
	u := p.Token
	if u == nil {
		u = p.Usage
	}
	if u == nil {
		return nil, nil
	}
	var used *int
	if u.Last != nil {
		used = u.Last.Total
		if used == nil {
			used = u.Last.Other
		}
	}
	max := u.Max
	if max == nil {
		max = u.OtherMax
	}
	if used != nil && *used < 0 {
		used = nil
	}
	if max != nil && *max <= 0 {
		max = nil
	}
	return used, max
}
func (c *Codex) contextUsage(s *session.Session, thread, turn string, params json.RawMessage) {
	snap := s.Snapshot()
	if snap.ResumeID != thread || thread == "" || turn == "" || snap.Backend != "codex" {
		return
	}
	if nativewatch.HasContextSource(c.findCodexSessionFile(thread), thread) {
		return
	}
	st := c.state(s.ID)
	st.mu.Lock()
	live := st.currentTurnID == turn || st.observedTurnID == turn || st.compactActive && st.compactTurnID == turn
	st.mu.Unlock()
	if !live {
		return
	}
	used, max := codexContextUsage(params)
	if used == nil && max == nil {
		return
	}
	var canonical any
	_ = json.Unmarshal(params, &canonical)
	raw, _ := json.Marshal(canonical)
	c.sink.Emit(backend.ContextTelemetry{SessionID: s.ID, Backend: "codex", NativeGeneration: thread, SourceRevision: snap.SourceRevision, EventID: contextEvidenceID("codex_usage", thread, turn, string(raw)), Origin: "codex_native_token_usage", MaxOrigin: "codex_native_token_usage", UsedTokens: used, MaxTokens: max})
}
