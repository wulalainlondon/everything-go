package agentactivity

import (
	"context"
	"encoding/json"
)

// NativePublicRPC requests only the latest item of an already-proven child
// native turn. An unsupported/non-final latest item remains unavailable; this
// port never scans backwards through reasoning/tools to hunt for a report.
// The provider can transport other fields; none of their content is decoded.
type NativePublicRPC interface {
	NativeRPC
	LatestTurnItem(context.Context, NativeReadProof, string, string) (json.RawMessage, error)
}

// NativePublicReturn is an exact native item reference, not canonical QA, a
// sealed task result, delivery proof or a content hash. Text stays at the owner.
type NativePublicReturn struct {
	Scope                           NativeScope
	SourceRevision                  uint64
	ChildThread, NativeTurn, ItemID string
}

func (s *NativeSource) PublicReturn(ctx context.Context, rpc NativePublicRPC, child, turn string) (out NativePublicReturn, err error) {
	defer func() {
		if err != nil {
			out = NativePublicReturn{}
		}
	}()
	probe, err := s.Probe(ctx, rpc)
	if err != nil {
		return out, err
	}
	proven := false
	for _, a := range probe.Actors {
		if a.ThreadID != child {
			continue
		}
		for _, r := range a.Rounds {
			if r.ID == turn && r.Status == "completed" {
				proven = true
			}
		}
	}
	if !proven {
		return out, ErrNativeSource
	}
	raw, err := rpc.LatestTurnItem(ctx, NativeReadProof{Scope: probe.Scope, Revision: probe.SourceRevision, source: s, publicChild: child, publicTurn: turn}, child, turn)
	if err != nil {
		return out, ErrNativeSource
	}
	var page struct {
		Data *[]struct {
			TurnID string
			Item   struct {
				ID, Type string
				Phase    *string
			}
		}
		NextCursor json.RawMessage
	}
	if len(raw) > 65536 || !uniqueJSON(raw) || json.Unmarshal(raw, &page) != nil || page.Data == nil || len(*page.Data) != 1 {
		return out, ErrNativeSource
	}
	entry := (*page.Data)[0]
	if entry.TurnID != turn || !shortID(entry.Item.ID) || entry.Item.Type != "agentMessage" || entry.Item.Phase == nil || *entry.Item.Phase != "final_answer" {
		return out, ErrNativeSource
	}
	s.mu.Lock()
	changed := s.scope != probe.Scope || s.revision != probe.SourceRevision || s.failed
	s.mu.Unlock()
	if changed || rpc.Scope() != probe.Scope || ctx.Err() != nil {
		return out, ErrNativeChanged
	}
	return NativePublicReturn{Scope: probe.Scope, SourceRevision: probe.SourceRevision, ChildThread: child, NativeTurn: turn, ItemID: entry.Item.ID}, nil
}

// A hosted mailbox wait does not name one particular child/QA. The receiver
// arrays in the result cannot retrospectively become its original targets.
// A completed tool item proves this wait call exited, not child completion or
// successful satisfaction; timeout/wake attribution requires a separate source.
type NativeMailboxWait struct{ CallID, ParentTurn, Phase string }

func DecodeNativeMailboxWait(method string, raw []byte, root string) (NativeMailboxWait, error) {
	var p struct {
		ThreadID, TurnID string
		Item             struct{ ID, Type, Tool, Status, SenderThreadID string }
	}
	if len(raw) > 65536 || !uniqueJSON(raw) || json.Unmarshal(raw, &p) != nil || p.ThreadID != root || p.Item.SenderThreadID != root || !shortID(p.TurnID) || !shortID(p.Item.ID) || p.Item.Type != "collabAgentToolCall" || p.Item.Tool != "wait" {
		return NativeMailboxWait{}, ErrNativeSource
	}
	phase := ""
	if method == "item/started" && p.Item.Status == "inProgress" {
		phase = "blocking"
	}
	if method == "item/completed" {
		switch p.Item.Status {
		case "completed", "failed", "interrupted":
			phase = "exited"
		}
	}
	if phase == "" {
		return NativeMailboxWait{}, ErrNativeSource
	}
	return NativeMailboxWait{p.Item.ID, p.TurnID, phase}, nil
}
