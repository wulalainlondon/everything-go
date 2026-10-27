package core

import (
	"context"
	"everything-go/internal/messagequeue"
)

func (h *Hub) unifiedWorkEffective(ctx context.Context, l messagequeue.WorkLink) (map[string]any, error) {
	e, found, err := h.messageQueue.EffectiveInput(ctx, l.SessionID, l.RequestID)
	if err != nil {
		return nil, err
	}
	var content, hash, original string
	var version uint64
	if found {
		content, hash, original, version = e.Content, e.EffectiveHash, e.PayloadHash, e.ContentRevision
	} else if l.Kind == "controller" {
		d, ok, err := h.dispatches.Get(ctx, l.NativeID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		content, err = h.dispatches.EffectiveContent(ctx, d.SessionID, d.ID, d.Content)
		if err != nil {
			return nil, err
		}
		original = d.IntentHash
		v, err := h.dispatches.UnifiedQueue(ctx, d.SessionID, nil)
		if err != nil {
			return nil, err
		}
		for _, r := range v.Items {
			if r.ID == d.ID {
				version, hash = r.ContentRevision, r.ContentHash
			}
		}
	} else if l.Kind == "delegation" {
		d, ok, err := h.delegations.ByID(ctx, l.NativeID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		content, err = h.delegations.EffectiveInstruction(ctx, d.ID, d.Instruction)
		if err != nil {
			return nil, err
		}
		original = d.IntentHash
		v, err := h.delegations.UnifiedQueue(ctx, nil)
		if err != nil {
			return nil, err
		}
		for _, r := range v.Items {
			if r.ID == d.ID {
				version, hash = r.ContentRevision, r.ContentHash
			}
		}
	}
	if version == 0 {
		return nil, nil
	}
	body := []rune(content)
	truncated := len(body) > 240
	if truncated {
		body = body[:240]
	}
	return map[string]any{"content_revision": version, "content_hash": hash, "original_hash": original, "body_preview": string(body), "body_truncated": truncated}, nil
}
func (h *Hub) unifiedDirectReplyFinal(ctx context.Context, r unifiedResolved) {
	if r.Canonical != nil || !r.HasEntry || r.Entry.State != messagequeue.Completed {
		return
	}
	if target, ok := h.registry.Get(r.Target.SessionID); ok {
		_, _ = h.jarvisResultSeal(ctx, target, r.Locator.RequestID, "")
	}
}
