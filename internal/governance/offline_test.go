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

func TestOfflineBufferMergesInterleavedTextAndThinkingChunks(t *testing.T) {
	b := NewOfflineBuffer()
	b.Append(protocol.NewTextChunk("s1", "r1", "Hello "))
	b.Append(protocol.NewToolStart("s1", "r1", "t1", "Bash", "sleep 1"))
	b.Append(protocol.NewThinkingChunk("s1", "r1", "first "))
	b.Append(protocol.NewTextChunk("s1", "r1", "world"))
	b.Append(protocol.NewThinkingChunk("s1", "r1", "second"))
	got := b.Drain()
	if len(got) != 3 {
		t.Fatalf("want tool + merged text + merged thinking, got %d: %#v", len(got), got)
	}
	if text, ok := got[1].(protocol.TextChunk); !ok || text.Content != "Hello world" {
		t.Fatalf("merged text = %#v", got[1])
	}
	if thinking, ok := got[2].(protocol.ThinkingChunk); !ok || thinking.Content != "first second" {
		t.Fatalf("merged thinking = %#v", got[2])
	}
}

func TestOfflineBufferCoalescesToolResultsAndDropsCompletedTool(t *testing.T) {
	b := NewOfflineBuffer()
	b.Append(protocol.NewToolStart("s1", "r1", "t1", "Bash", "long command"))
	b.Append(protocol.NewToolResult("s1", "r1", "t1", "first"))
	b.Append(protocol.NewToolResult("s1", "r1", "t1", "first\nsecond"))
	if got := b.Peek(10); len(got) != 2 {
		t.Fatalf("running tool should retain start + latest result, got %d: %#v", len(got), got)
	}
	b.Append(protocol.NewToolEnd("s1", "r1", "t1"))
	if b.Len() != 0 {
		t.Fatalf("completed tool transport events should be recoverable from history, got %#v", b.Drain())
	}
}

func TestOfflineBufferKeepsOnlyLatestTodoAndUsageSnapshots(t *testing.T) {
	b := NewOfflineBuffer()
	b.Append(protocol.NewTodoUpdate("s1", "r1", []protocol.TodoItem{{Content: "old"}}))
	b.Append(protocol.NewTodoUpdate("s1", "r1", []protocol.TodoItem{{Content: "new"}}))
	b.Append(protocol.NewUsageReport(nil, nil, nil))
	b.Append(protocol.NewUsageReport(&protocol.UsageWindow{}, nil, nil))
	got := b.Drain()
	if len(got) != 2 {
		t.Fatalf("want latest todo + usage snapshots, got %d: %#v", len(got), got)
	}
	if todo := got[0].(protocol.TodoUpdate); todo.Todos[0].Content != "new" {
		t.Fatalf("todo snapshot = %#v", todo)
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
		b.Append(protocol.Error{Type: "error", SessionID: "s1", RequestID: "r" + itoaTest(i), Message: "failure"})
	}
	if b.Len() != offlineCap {
		t.Fatalf("buffer should cap at %d, got %d", offlineCap, b.Len())
	}
	got := b.Drain()
	first := got[0].(protocol.Error)
	// The first 5 (tool-0..tool-4) were evicted; oldest survivor is tool-5.
	if first.RequestID != "r5" {
		t.Fatalf("oldest survivor = %q, want r5", first.RequestID)
	}
}

func TestOfflineBufferPeekSizedBoundsEncodedPayload(t *testing.T) {
	b := NewOfflineBuffer()
	for i := 0; i < 10; i++ {
		b.Append(protocol.Error{Type: "error", RequestID: itoaTest(i), Message: "1234567890"})
	}
	got := b.PeekSized(10, 140)
	if len(got) == 0 || len(got) >= 10 {
		t.Fatalf("byte-bounded batch should contain some but not all events, got %d", len(got))
	}
	if b.Len() != 10 {
		t.Fatal("PeekSized must be non-destructive")
	}

	huge := NewOfflineBuffer()
	huge.Append(protocol.Error{Type: "error", Message: string(make([]byte, 1024))})
	if got := huge.PeekSized(10, 10); len(got) != 1 {
		t.Fatalf("oversized head event must still make progress, got %d", len(got))
	}
}

func TestOfflineBufferLongGoalToolStreamStaysBounded(t *testing.T) {
	b := NewOfflineBuffer()
	for i := 0; i < 2000; i++ {
		id := "tool-" + itoaTest(i)
		b.Append(protocol.NewTextChunk("s1", "r1", "x"))
		b.Append(protocol.NewToolStart("s1", "r1", id, "Bash", "command"))
		for delta := 0; delta < 5; delta++ {
			b.Append(protocol.NewToolResult("s1", "r1", id, "cumulative output"))
		}
		b.Append(protocol.NewToolEnd("s1", "r1", id))
	}
	if b.Len() != 1 {
		t.Fatalf("completed tools should not fill replay; want merged text only, got %d", b.Len())
	}
}

func TestOfflineBufferDrainEmpty(t *testing.T) {
	b := NewOfflineBuffer()
	if got := b.Drain(); got != nil {
		t.Fatalf("drain of empty buffer should be nil, got %v", got)
	}
}

func TestOfflineBufferPeekCommitDoesNotLoseUnackedEvents(t *testing.T) {
	b := NewOfflineBuffer()
	for i := 0; i < 130; i++ {
		b.Append(protocol.NewDone("s1", itoaTest(i)))
	}
	first := b.Peek(64)
	if len(first) != 64 || b.Len() != 130 {
		t.Fatalf("peek must be non-destructive: batch=%d remaining=%d", len(first), b.Len())
	}
	if removed := b.Commit(64); removed != 64 || b.Len() != 66 {
		t.Fatalf("commit removed=%d remaining=%d", removed, b.Len())
	}
	next := b.Peek(64)
	if got := next[0].(protocol.Done).RequestID; got != "64" {
		t.Fatalf("next batch starts at %q, want 64", got)
	}
}

func TestOfflineBufferCoalescesGoalStatePerSession(t *testing.T) {
	b := NewOfflineBuffer()
	b.Append(protocol.NewGoalUpdate("s1", protocol.Goal{ThreadID: "t1", Status: "active", UpdatedAt: 1}))
	b.Append(protocol.NewDone("s1", "r1"))
	b.Append(protocol.NewGoalUpdate("s1", protocol.Goal{ThreadID: "t1", Status: "complete", UpdatedAt: 2}))
	b.Append(protocol.NewGoalUpdate("s2", protocol.Goal{ThreadID: "t2", Status: "active", UpdatedAt: 1}))
	got := b.Drain()
	if len(got) != 3 {
		t.Fatalf("want done + newest s1 goal + s2 goal, got %d: %#v", len(got), got)
	}
	if goal, ok := got[1].(protocol.GoalUpdate); !ok || goal.Goal.Status != "complete" {
		t.Fatalf("s1 should retain only complete goal, got %#v", got[1])
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
