package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/messagequeue"
)

// New direct replies keep their original metadata relation; the existing
// first-start fact/outbox is reused. ACK/actor running are never a start proof.
func (h *Hub) unifiedReplyStartDecision(ctx context.Context, manager string, device string, generation uint64, p messagequeue.NativeStart) (bool, error) {
	scope, gen, body, found, e := h.messageQueue.UnifiedReplySource(ctx, p.SessionID, p.RequestID)
	if e != nil || !found {
		return false, e
	}
	if scope != "device:"+device+"\x00"+manager || gen != generation {
		return false, nil
	}
	var reply struct {
		Source jarvisTarget `json:"source"`
		Target jarvisTarget `json:"target"`
	}
	if json.Unmarshal(body, &reply) != nil {
		return false, nil
	}
	if reply.Source.SessionID != manager || reply.Target.SessionID != p.SessionID || reply.Target.ThreadID != p.ThreadID {
		return false, nil
	}
	if e = h.unifiedTarget(reply.Source, false); e != nil {
		return false, nil
	}
	if e = h.unifiedTarget(reply.Target, false); e != nil {
		return false, nil
	}
	return true, nil
}
