package governance

import (
	"encoding/json"
	"sync"

	"everything-go/internal/protocol"
)

// offlineCap bounds the offline buffer, mirroring the Python bridge's 10k cap.
const offlineCap = 10000

// OfflineBuffer holds session-scoped events emitted while no client is
// connected, so a reconnecting client can recover them. This is a transport
// journal, not the canonical transcript: replaceable snapshots and cumulative
// stream deltas are compacted while interaction/error events remain reliable.
type OfflineBuffer struct {
	mu     sync.Mutex
	events []any
}

func NewOfflineBuffer() *OfflineBuffer { return &OfflineBuffer{} }

// Append buffers one event. text_chunk merges into the previous entry when it
// shares the same session+request; other types drop the oldest entry at cap.
//
// When a terminal event (done/stopped/error) arrives, the buffered streaming
// content of that turn (text_chunk/thinking_chunk/tool_*/media/document/
// todo_update for the same session+request) is collapsed away first: once a
// turn has ended its content is fully recoverable from history (the client
// fetches a snapshot on open), so replaying those chunks on reconnect is
// redundant and only makes a finished turn briefly animate as if it were still
// streaming. Only an in-flight turn (no terminal event buffered yet) keeps its
// chunks, which is the legitimate offline-catch-up case.
func (b *OfflineBuffer) Append(event any) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Goal is durable state, not an animation stream. While offline, retain only
	// the newest state transition for each session so frequent goal usage cannot
	// grow the replay journal without bound.
	if sid, ok := goalSession(event); ok {
		for i := len(b.events) - 1; i >= 0; i-- {
			if existingSID, isGoal := goalSession(b.events[i]); isGoal && existingSID == sid {
				b.removeAt(i)
				break
			}
		}
	}

	// Text/thinking chunks are append-only deltas. Merge them even when tool
	// events are interleaved; retaining one accumulated chunk per turn preserves
	// the recoverable content without turning a long Goal into thousands of
	// replay entries.
	switch e := event.(type) {
	case protocol.TextChunk:
		e.Content = b.takeTextContent(e.SessionID, e.RequestID) + e.Content
		event = e
	case protocol.ThinkingChunk:
		e.Content = b.takeThinkingContent(e.SessionID, e.RequestID) + e.Content
		event = e
	case protocol.ToolResult:
		// ToolResult is the emitter's cumulative output snapshot, not a delta.
		// Keep only the newest snapshot for this tool.
		b.removeToolEvents(e.SessionID, e.ToolUseID, false)
	case protocol.ToolEnd:
		// A completed tool is already represented in history. Remove its transient
		// start/output state immediately instead of waiting for the whole (possibly
		// multi-hour) Goal turn to finish.
		b.removeToolEvents(e.SessionID, e.ToolUseID, true)
		return
	case protocol.TodoUpdate:
		b.removeTodoUpdate(e.SessionID, e.RequestID)
	case protocol.UsageReport:
		b.removeUsageReports()
	}

	// A turn is identified by its request id; without one we cannot tell turns
	// apart, so leave such (rare/legacy) events untouched rather than risk
	// collapsing a different turn's content.
	if sid, rid, ok := terminalEvent(event); ok && sid != "" && rid != "" {
		b.collapseTurnContent(sid, rid)
	}

	if len(b.events) >= offlineCap {
		// Drop the oldest to make room (bounded memory under a long outage).
		b.events = b.events[1:]
	}
	b.events = append(b.events, event)
}

func (b *OfflineBuffer) takeTextContent(sessionID, requestID string) string {
	for i := len(b.events) - 1; i >= 0; i-- {
		if e, ok := b.events[i].(protocol.TextChunk); ok && e.SessionID == sessionID && e.RequestID == requestID {
			b.removeAt(i)
			return e.Content
		}
	}
	return ""
}

func (b *OfflineBuffer) takeThinkingContent(sessionID, requestID string) string {
	for i := len(b.events) - 1; i >= 0; i-- {
		if e, ok := b.events[i].(protocol.ThinkingChunk); ok && e.SessionID == sessionID && e.RequestID == requestID {
			b.removeAt(i)
			return e.Content
		}
	}
	return ""
}

// removeToolEvents removes transient events for a tool. When includeStart is
// false, a ToolStart remains and only older result snapshots are replaced.
func (b *OfflineBuffer) removeToolEvents(sessionID, toolUseID string, includeStart bool) {
	for i := len(b.events) - 1; i >= 0; i-- {
		remove := false
		stop := false
		switch e := b.events[i].(type) {
		case protocol.ToolStart:
			remove = includeStart && e.SessionID == sessionID && e.ToolUseID == toolUseID
			stop = remove
		case protocol.ToolResult:
			remove = e.SessionID == sessionID && e.ToolUseID == toolUseID
		case protocol.ToolEnd:
			remove = e.SessionID == sessionID && e.ToolUseID == toolUseID
		}
		if remove {
			b.removeAt(i)
			if !includeStart || stop {
				return
			}
		}
	}
}

func (b *OfflineBuffer) removeTodoUpdate(sessionID, requestID string) {
	for i := len(b.events) - 1; i >= 0; i-- {
		if e, ok := b.events[i].(protocol.TodoUpdate); ok && e.SessionID == sessionID && e.RequestID == requestID {
			b.removeAt(i)
			return
		}
	}
}

func (b *OfflineBuffer) removeUsageReports() {
	for i := len(b.events) - 1; i >= 0; i-- {
		if _, ok := b.events[i].(protocol.UsageReport); ok {
			b.removeAt(i)
			return
		}
	}
}

func (b *OfflineBuffer) removeAt(index int) {
	copy(b.events[index:], b.events[index+1:])
	b.events[len(b.events)-1] = nil
	b.events = b.events[:len(b.events)-1]
}

func goalSession(event any) (string, bool) {
	switch e := event.(type) {
	case protocol.GoalUpdate:
		return e.SessionID, true
	case protocol.GoalCleared:
		return e.SessionID, true
	default:
		return "", false
	}
}

// collapseTurnContent drops buffered streaming-content events for a turn that
// has just terminated, keyed by exact (session, request) so concurrent turns in
// other sessions/requests are untouched. Order of survivors is preserved.
func (b *OfflineBuffer) collapseTurnContent(sessionID, requestID string) {
	kept := b.events[:0]
	for _, e := range b.events {
		if sid, rid, ok := streamingContentEvent(e); ok && sid == sessionID && rid == requestID {
			continue
		}
		kept = append(kept, e)
	}
	b.events = kept
}

// streamingContentEvent reports whether the event is per-turn streaming content
// that history will faithfully reproduce (so it need not be replayed once the
// turn ends), and returns its session+request key.
func streamingContentEvent(event any) (sessionID, requestID string, ok bool) {
	switch e := event.(type) {
	case protocol.TextChunk:
		return e.SessionID, e.RequestID, true
	case protocol.ThinkingChunk:
		return e.SessionID, e.RequestID, true
	case protocol.ToolStart:
		return e.SessionID, e.RequestID, true
	case protocol.ToolResult:
		return e.SessionID, e.RequestID, true
	case protocol.ToolEnd:
		return e.SessionID, e.RequestID, true
	case protocol.TodoUpdate:
		return e.SessionID, e.RequestID, true
	default:
		return "", "", false
	}
}

// terminalEvent reports whether the event ends a turn, and returns its
// session+request key.
func terminalEvent(event any) (sessionID, requestID string, ok bool) {
	switch e := event.(type) {
	case protocol.Done:
		return e.SessionID, e.RequestID, true
	case protocol.Stopped:
		return e.SessionID, e.RequestID, true
	case protocol.Error:
		return e.SessionID, e.RequestID, true
	default:
		return "", "", false
	}
}

// Drain returns all buffered events and clears the buffer.
func (b *OfflineBuffer) Drain() []any {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) == 0 {
		return nil
	}
	out := b.events
	b.events = nil
	return out
}

// Peek returns up to limit oldest events without removing them. Reliable
// replay uses Peek + Commit so a disconnect before the client ACK cannot lose
// the remainder of the journal.
func (b *OfflineBuffer) Peek(limit int) []any {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) == 0 || limit <= 0 {
		return nil
	}
	if limit > len(b.events) {
		limit = len(b.events)
	}
	out := make([]any, limit)
	copy(out, b.events[:limit])
	return out
}

// PeekSized returns an oldest-first batch bounded by both event count and
// encoded event bytes. The first event is always returned, even if it alone is
// larger than maxBytes, so one oversized result cannot permanently block the
// journal. The enclosing replay envelope adds a small fixed overhead.
func (b *OfflineBuffer) PeekSized(limit, maxBytes int) []any {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) == 0 || limit <= 0 || maxBytes <= 0 {
		return nil
	}
	if limit > len(b.events) {
		limit = len(b.events)
	}
	out := make([]any, 0, limit)
	used := 0
	for _, event := range b.events[:limit] {
		encoded, err := json.Marshal(event)
		size := len(encoded)
		if err != nil {
			// Let the normal replay marshal path report/drop malformed events.
			size = 0
		}
		if len(out) > 0 && used+size > maxBytes {
			break
		}
		out = append(out, event)
		used += size
	}
	return out
}

// Commit removes count oldest events after a replay ACK. It returns the number
// actually removed (count is clamped defensively).
func (b *OfflineBuffer) Commit(count int) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if count <= 0 || len(b.events) == 0 {
		return 0
	}
	if count > len(b.events) {
		count = len(b.events)
	}
	oldLen := len(b.events)
	copy(b.events, b.events[count:])
	newLen := oldLen - count
	for i := newLen; i < oldLen; i++ {
		b.events[i] = nil
	}
	b.events = b.events[:newLen]
	return count
}

// Len reports the number of buffered events.
func (b *OfflineBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events)
}
