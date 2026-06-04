package governance

import (
	"testing"

	"everything-go/internal/protocol"
)

func TestOfflineBufferDrainOrder(t *testing.T) {
	b := NewOfflineBuffer()
	b.Append(protocol.NewToolStart("s1", "r1", "t1", "Bash", "ls"))
	b.Append(protocol.NewDone("s1", "r1"))
	got := b.Drain()
	if len(got) != 2 {
		t.Fatalf("want 2 events, got %d", len(got))
	}
	if _, ok := got[0].(protocol.ToolStart); !ok {
		t.Fatalf("first event should be ToolStart, got %T", got[0])
	}
	if _, ok := got[1].(protocol.Done); !ok {
		t.Fatalf("second event should be Done, got %T", got[1])
	}
	if b.Len() != 0 {
		t.Fatal("drain must empty the buffer")
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
