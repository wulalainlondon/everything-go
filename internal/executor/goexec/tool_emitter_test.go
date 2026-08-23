package goexec

import (
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
)

func TestToolEmitterAccumulatesBySessionRequestAndTool(t *testing.T) {
	sink := &capSink{}
	em := newToolEmitter(sink)

	em.Start("s1", "r1", "toolA", "Bash", "ls")
	em.Delta("s1", "r1", "toolA", "a")
	em.Delta("s2", "r1", "toolA", "x")
	em.Delta("s1", "r1", "toolA", "b")
	em.End("s1", "r1", "toolA")

	var results []protocol.ToolResult
	for _, e := range sink.events {
		if tr, ok := e.(protocol.ToolResult); ok {
			results = append(results, tr)
		}
	}
	if len(results) != 3 {
		t.Fatalf("want 3 tool_result events, got %d: %+v", len(results), results)
	}
	if results[0].SessionID != "s1" || results[0].Output != "a" {
		t.Fatalf("first s1 delta wrong: %+v", results[0])
	}
	if results[1].SessionID != "s2" || results[1].Output != "x" {
		t.Fatalf("s2 delta should not inherit s1 output: %+v", results[1])
	}
	if results[2].SessionID != "s1" || results[2].Output != "ab" {
		t.Fatalf("second s1 delta should accumulate: %+v", results[2])
	}
}

func TestToolEmitterResultEndOrder(t *testing.T) {
	sink := &capSink{}
	em := newToolEmitter(sink)

	em.ResultEnd("s1", "r1", "toolA", "done")

	if len(sink.events) != 2 {
		t.Fatalf("want result+end, got %d events", len(sink.events))
	}
	if _, ok := sink.events[0].(protocol.ToolResult); !ok {
		t.Fatalf("first event should be ToolResult, got %T", sink.events[0])
	}
	if _, ok := sink.events[1].(protocol.ToolEnd); !ok {
		t.Fatalf("second event should be ToolEnd, got %T", sink.events[1])
	}
}

func TestToolEmitterDeltaAccumulatorIsBounded(t *testing.T) {
	sink := &capSink{}
	em := newToolEmitter(sink)
	chunk := make([]byte, backend.MaxToolResultOutputBytes)
	for i := range chunk {
		chunk[i] = 'x'
	}

	em.Delta("s1", "r1", "toolA", string(chunk))
	em.Delta("s1", "r1", "toolA", string(chunk))
	em.Delta("s1", "r1", "toolA", "tail-that-should-not-grow-buffer")

	var last protocol.ToolResult
	for _, e := range sink.events {
		if tr, ok := e.(protocol.ToolResult); ok {
			last = tr
		}
	}
	if len(last.Output) > backend.MaxToolResultOutputBytes {
		t.Fatalf("tool output length = %d, want <= %d", len(last.Output), backend.MaxToolResultOutputBytes)
	}
	if got := last.Output[len(last.Output)-len(backend.ToolResultTruncatedMark):]; got != backend.ToolResultTruncatedMark {
		t.Fatalf("missing truncation marker: %q", got)
	}
}
