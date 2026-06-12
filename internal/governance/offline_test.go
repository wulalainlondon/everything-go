package governance

import (
	"testing"

	"everything-go/internal/protocol"
)

func TestOfflineBufferDrainOrder(t *testing.T) {
	b := NewOfflineBuffer()
	// Two distinct in-flight turns (no terminal events) drain in order.
	b.Append(protocol.NewToolStart("s1", "r1", "t1", "Bash", "ls"))
	b.Append(protocol.NewToolStart("s2", "r1", "t2", "Bash", "pwd"))
	got := b.Drain()
	if len(got) != 2 {
		t.Fatalf("want 2 events, got %d", len(got))
	}
	if ts, ok := got[0].(protocol.ToolStart); !ok || ts.SessionID != "s1" {
		t.Fatalf("first event should be s1 ToolStart, got %T", got[0])
	}
	if ts, ok := got[1].(protocol.ToolStart); !ok || ts.SessionID != "s2" {
		t.Fatalf("second event should be s2 ToolStart, got %T", got[1])
	}
	if b.Len() != 0 {
		t.Fatal("drain must empty the buffer")
	}
}

// A terminated turn's streaming content is collapsed so reconnect replay does
// not re-animate a finished session as if it were streaming. Only the terminal
// marker survives; the content is recoverable from history.
func TestOfflineBufferCollapsesCompletedTurnContent(t *testing.T) {
	b := NewOfflineBuffer()
	b.Append(protocol.NewTextChunk("s1", "r1", "Hello "))
	b.Append(protocol.NewToolStart("s1", "r1", "t1", "Bash", "ls"))
	b.Append(protocol.NewTextChunk("s1", "r1", "world"))
	b.Append(protocol.NewDone("s1", "r1"))

	got := b.Drain()
	if len(got) != 1 {
		t.Fatalf("completed turn should collapse to just its terminal event, got %d: %v", len(got), got)
	}
	if _, ok := got[0].(protocol.Done); !ok {
		t.Fatalf("survivor should be Done, got %T", got[0])
	}
}

// Collapsing is keyed by (session, request): an in-flight turn and other
// sessions are not disturbed when one turn terminates.
func TestOfflineBufferCollapseScopedToTurn(t *testing.T) {
	b := NewOfflineBuffer()
	b.Append(protocol.NewTextChunk("s1", "r1", "done-turn"))
	b.Append(protocol.NewTextChunk("s1", "r2", "live-turn")) // different request, still in-flight
	b.Append(protocol.NewTextChunk("s2", "r1", "other-session"))
	b.Append(protocol.NewStopped("s1", "r1")) // terminate only s1/r1

	got := b.Drain()
	// s1/r1 chunk collapsed; survivors: s1/r2 chunk, s2/r1 chunk, s1/r1 Stopped.
	if len(got) != 3 {
		t.Fatalf("only the terminated turn's content should collapse, got %d: %v", len(got), got)
	}
	if tc, ok := got[0].(protocol.TextChunk); !ok || tc.RequestID != "r2" {
		t.Fatalf("first survivor should be the in-flight s1/r2 chunk, got %T %+v", got[0], got[0])
	}
	if tc, ok := got[1].(protocol.TextChunk); !ok || tc.SessionID != "s2" {
		t.Fatalf("second survivor should be the s2 chunk, got %T %+v", got[1], got[1])
	}
	if _, ok := got[2].(protocol.Stopped); !ok {
		t.Fatalf("third survivor should be Stopped, got %T", got[2])
	}
}

func TestOfflineBufferMergesTextChunks(t *testing.T) {
	b := NewOfflineBuffer()
	b.Append(protocol.NewTextChunk("s1", "r1", "Hello "))
	b.Append(protocol.NewTextChunk("s1", "r1", "world"))
	got := b.Drain()
	if len(got) != 1 {
		t.Fatalf("consecutive text_chunks (same session+req) should merge to 1, got %d", len(got))
	}
	tc := got[0].(protocol.TextChunk)
	if tc.Content != "Hello world" {
		t.Fatalf("merged content = %q, want %q", tc.Content, "Hello world")
	}
}

func TestOfflineBufferDoesNotMergeAcrossSessionOrRequest(t *testing.T) {
	b := NewOfflineBuffer()
	b.Append(protocol.NewTextChunk("s1", "r1", "a"))
	b.Append(protocol.NewTextChunk("s2", "r1", "b")) // different session
	b.Append(protocol.NewTextChunk("s1", "r2", "c")) // different request
	if got := b.Drain(); len(got) != 3 {
		t.Fatalf("different session/request must not merge, got %d", len(got))
	}
}

func TestOfflineBufferTextChunkDoesNotMergeIntoNonText(t *testing.T) {
	b := NewOfflineBuffer()
	b.Append(protocol.NewDone("s1", "r1"))
	b.Append(protocol.NewTextChunk("s1", "r1", "late"))
	if got := b.Drain(); len(got) != 2 {
		t.Fatalf("text_chunk must not merge into a non-text last entry, got %d", len(got))
	}
}

func TestOfflineBufferCapDropsOldest(t *testing.T) {
	b := NewOfflineBuffer()
	// Append cap+5 distinct (non-mergeable) events; oldest must be evicted.
	for i := 0; i < offlineCap+5; i++ {
		b.Append(protocol.NewToolEnd("s1", "r1", "tool-"+itoaTest(i)))
	}
	if b.Len() != offlineCap {
		t.Fatalf("buffer should cap at %d, got %d", offlineCap, b.Len())
	}
	got := b.Drain()
	first := got[0].(protocol.ToolEnd)
	// The first 5 (tool-0..tool-4) were evicted; oldest survivor is tool-5.
	if first.ToolUseID != "tool-5" {
		t.Fatalf("oldest survivor = %q, want tool-5", first.ToolUseID)
	}
}

func TestOfflineBufferDrainEmpty(t *testing.T) {
	b := NewOfflineBuffer()
	if got := b.Drain(); got != nil {
		t.Fatalf("drain of empty buffer should be nil, got %v", got)
	}
}

func itoaTest(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
