package goexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/history"
	"everything-go/internal/session"
	"os"
	"time"
)

func exportFileContext(provider, thread, path string, load func(string, history.Opts) (*history.Result, error)) (backend.ContextExport, error) {
	if path == "" || thread == "" {
		return backend.ContextExport{}, errors.New("history_source_missing")
	}
	before, e := os.Stat(path)
	if e != nil {
		return backend.ContextExport{}, e
	}
	if before.Size() > int64(history.LoadMaxBytes()) {
		return backend.ContextExport{}, errors.New("history_full_export_exceeds_limit")
	}
	p, e := load(thread, history.Opts{Mode: "snapshot", Limit: 10000, IncludeThinking: true})
	if e != nil {
		return backend.ContextExport{}, e
	}
	after, e := os.Stat(path)
	if e != nil || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) || p == nil || p.HasMoreBefore || p.HasMoreAfter {
		return backend.ContextExport{}, errors.New("history_incomplete_or_changed")
	}
	// Unsupported opaque tools can outlive a terminal (e.g. background exec
	// or another service). A transcript saying "done" is not a settlement
	// receipt. Keep reset blocked until typed external-effect coverage exists.
	for _, m := range p.Messages {
		if blocks, ok := m["blocks"].([]map[string]any); ok {
			for _, b := range blocks {
				if b["type"] == "tool_call" && !(provider == "claude" && (b["name"] == "Read" || b["name"] == "Glob" || b["name"] == "Grep")) {
					return backend.ContextExport{}, errors.New("opaque_tool_effect_coverage_unavailable")
				}
			}
		}
		if blocks, ok := m["blocks"].([]any); ok {
			for _, b := range blocks {
				if v, ok := b.(map[string]any); ok && v["type"] == "tool_call" && !(provider == "claude" && (v["name"] == "Read" || v["name"] == "Glob" || v["name"] == "Grep")) {
					return backend.ContextExport{}, errors.New("opaque_tool_effect_coverage_unavailable")
				}
			}
		}
	}
	raw, _ := json.Marshal(p.Messages)
	digest := sha256.Sum256(raw)
	return backend.ContextExport{Provider: provider, ThreadID: thread, SourceGeneration: hex.EncodeToString(digest[:]), Messages: p.Messages, Complete: true}, nil
}
func (c *Codex) contextIdle(s *session.Session) error {
	st := c.state(s.ID)
	st.mu.Lock()
	busy := st.turnActive || st.compactActive || st.stopping || st.turnStartPending || st.observedTurnID != ""
	st.mu.Unlock()
	if busy {
		return errors.New("native_context_busy")
	}
	if s.ResumeID() == "" {
		return errors.New("native_context_uninitialized")
	}
	if e := c.ensureServer(); e != nil {
		return e
	}
	epoch := c.rpc.transportEpoch.Load()
	raw, e := c.rpc.requestAtEpoch("thread/read", map[string]any{"threadId": s.ResumeID(), "includeTurns": false}, 5*time.Second, &epoch)
	if e != nil {
		return e
	}
	var p struct {
		Thread struct {
			ID     string `json:"id"`
			Status struct {
				Type string `json:"type"`
			} `json:"status"`
		} `json:"thread"`
	}
	if c.rpc.transportEpoch.Load() != epoch || json.Unmarshal(raw, &p) != nil || p.Thread.ID != s.ResumeID() || p.Thread.Status.Type != "idle" {
		return errors.New("native_idle_not_confirmed")
	}
	return nil
}
func (c *Codex) ExportContext(ctx context.Context, s *session.Session) (backend.ContextExport, error) {
	if e := ctx.Err(); e != nil {
		return backend.ContextExport{}, e
	}
	if e := c.contextIdle(s); e != nil {
		return backend.ContextExport{}, e
	}
	path, e := c.findCodexSessionFileChecked(s.ResumeID())
	if e != nil {
		return backend.ContextExport{}, e
	}
	return exportFileContext("codex", s.ResumeID(), path, c.LoadHistory)
}
func (c *Codex) ResetContext(ctx context.Context, s *session.Session, old string) (string, error) {
	if !s.ContextResetReserved() || old == "" || s.ResumeID() != old {
		return "", errors.New("reset_generation_not_reserved")
	}
	before := s.Snapshot()
	epoch := c.rpc.transportEpoch.Load()
	if e := c.contextIdle(s); e != nil {
		return "", e
	}
	if after := s.Snapshot(); after.SourceRevision != before.SourceRevision || after.ResumeID != old || !s.ContextResetReserved() {
		return "", errors.New("reset_first_effect_source_changed")
	}
	// Do not use legacy Clear (which ignores errors/forgets historical IDs).
	// Unsubscribe is acknowledged before local detach; a timeout remains unknown.
	if _, e := c.rpc.requestAtEpochGuarded(ctx, "thread/unsubscribe", map[string]any{"threadId": old}, 15*time.Second, &epoch, func() (func(), error) {
		if e := backend.CheckContextResetGuard(ctx); e != nil {
			return nil, e
		}
		now := s.Snapshot()
		if now.SourceRevision != before.SourceRevision || now.ResumeID != old || !s.ContextResetReserved() {
			return nil, errors.New("reset_first_effect_source_changed")
		}
		st := c.state(s.ID)
		st.mu.Lock()
		if st.turnActive || st.compactActive || st.stopping || st.turnStartPending || st.observedTurnID != "" {
			st.mu.Unlock()
			return nil, errors.New("native_context_busy_before_effect")
		}
		return st.mu.Unlock, nil
	}); e != nil {
		return "", e
	}
	c.forgetThread(s, c.state(s.ID), old)
	if e := s.DetachReservedContext(old); e != nil {
		return "", e
	}
	return "awaiting_initialization", nil
}
func (c *Claude) claudeContextIdle(s *session.Session) error {
	c.mu.Lock()
	p := c.procs[s.ID]
	c.mu.Unlock()
	if p != nil {
		p.mu.Lock()
		busy := p.reqID != ""
		p.mu.Unlock()
		if busy {
			return errors.New("native_context_busy")
		}
	}
	if len(c.PendingInteractions(s.ID)) != 0 {
		return errors.New("pending_native_interaction")
	}
	return nil
}
func (c *Claude) ExportContext(ctx context.Context, s *session.Session) (backend.ContextExport, error) {
	if e := c.claudeContextIdle(s); e != nil {
		return backend.ContextExport{}, e
	}
	path, e := c.findSessionFileChecked(s.ResumeID())
	if e != nil {
		return backend.ContextExport{}, e
	}
	return exportFileContext("claude", s.ResumeID(), path, c.LoadHistory)
}
func (c *Claude) ResetContext(ctx context.Context, s *session.Session, old string) (string, error) {
	if !s.ContextResetReserved() || s.ResumeID() != old || old == "" {
		return "", errors.New("reset_generation_not_reserved")
	}
	if e := c.claudeContextIdle(s); e != nil {
		return "", e
	}
	c.mu.Lock()
	p := c.procs[s.ID]
	c.mu.Unlock()
	if e := backend.CheckContextResetGuard(ctx); e != nil {
		return "", e
	}
	if s.ResumeID() != old || !s.ContextResetReserved() {
		return "", errors.New("reset_first_effect_source_changed")
	}
	if p != nil {
		c.mu.Lock()
		if c.procs[s.ID] != p {
			c.mu.Unlock()
			return "", errors.New("provider_owner_changed_before_effect")
		}
		p.mu.Lock()
		if p.reqID != "" {
			p.mu.Unlock()
			c.mu.Unlock()
			return "", errors.New("provider_busy_before_effect")
		}
		if e := backend.CheckContextResetGuard(ctx); e != nil {
			p.mu.Unlock()
			c.mu.Unlock()
			return "", e
		}
		p.cancel()
		p.mu.Unlock()
		c.mu.Unlock()
		select {
		case <-p.exited:
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(5 * time.Second):
			return "", errors.New("provider_release_unconfirmed")
		}
		if p.stopErr != nil {
			return "", p.stopErr
		}
		c.mu.Lock()
		if c.procs[s.ID] == p {
			delete(c.procs, s.ID)
		}
		c.mu.Unlock()
	}
	if e := s.DetachReservedContext(old); e != nil {
		return "", e
	}
	return "awaiting_initialization", nil
}
