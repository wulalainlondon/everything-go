package governance

import (
	"sync"

	"everything-go/internal/protocol"
)

// offlineCap bounds the offline buffer, mirroring the Python bridge's 10k cap.
const offlineCap = 10000

// OfflineBuffer holds session-scoped events emitted while no client is
// connected, so a reconnecting client can recover them. Consecutive text_chunk
// events for the same session+request are merged into one entry (matching the
// Python send_event offline path), keeping a long unattended stream bounded.
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

	if tc, ok := event.(protocol.TextChunk); ok && len(b.events) > 0 {
		if last, ok := b.events[len(b.events)-1].(protocol.TextChunk); ok &&
			last.SessionID == tc.SessionID && last.RequestID == tc.RequestID {
			last.Content += tc.Content
			b.events[len(b.events)-1] = last
			return
		}
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
	case protocol.Media:
		return e.SessionID, e.RequestID, true
	case protocol.Document:
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

// Len reports the number of buffered events.
func (b *OfflineBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events)
}
