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

	if len(b.events) >= offlineCap {
		// Drop the oldest to make room (bounded memory under a long outage).
		b.events = b.events[1:]
	}
	b.events = append(b.events, event)
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
