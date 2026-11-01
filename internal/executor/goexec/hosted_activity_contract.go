package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/agentactivity"
)

// This original-owner adapter is internal only. No new Hub command is exposed.
// The caller NativeSource.PublicReturn first proves child membership and the
// exact completed Turn.id via the same original scope/transport.
func (r hostedActivityRPC) LatestTurnItem(ctx context.Context, p agentactivity.NativeReadProof, id, turn string) (json.RawMessage, error) {
	return r.readAtProof(ctx, p, "thread/items/list", map[string]any{"threadId": id, "turnId": turn, "limit": 1, "sortDirection": "desc"})
}
