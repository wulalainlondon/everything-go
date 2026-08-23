package backend

import (
	"testing"

	"everything-go/internal/protocol"
)

func TestBackendEventsUseCurrentWireRuntimeTypes(t *testing.T) {
	if _, ok := any(NewDone("s1", "r1")).(protocol.Done); !ok {
		t.Fatalf("NewDone must remain protocol.Done-compatible")
	}
	if _, ok := any(NewStopped("s1", "r1")).(protocol.Stopped); !ok {
		t.Fatalf("NewStopped must remain protocol.Stopped-compatible")
	}
	if _, ok := any(NewError("s1", "r1", ErrTurn, "bad")).(protocol.Error); !ok {
		t.Fatalf("NewError must remain protocol.Error-compatible")
	}
	if _, ok := any(NewSessionCommandStarted("s1", "compact_s1", 0)).(protocol.SessionCommandStarted); !ok {
		t.Fatalf("NewSessionCommandStarted must remain protocol.SessionCommandStarted-compatible")
	}
}

func TestNewErrorCarriesRequestID(t *testing.T) {
	err := NewError("s1", "r1", ErrBackendUnavailable, "down")
	if err.Type != "error" || err.SessionID != "s1" || err.RequestID != "r1" ||
		err.Code != ErrBackendUnavailable || err.Message != "down" {
		t.Fatalf("bad backend error: %+v", err)
	}
}

func TestNewToolResultTruncatesHugeOutput(t *testing.T) {
	huge := make([]byte, MaxToolResultOutputBytes+1024)
	for i := range huge {
		huge[i] = 'x'
	}
	ev := NewToolResult("s1", "r1", "t1", string(huge))
	if len(ev.Output) > MaxToolResultOutputBytes {
		t.Fatalf("tool output length = %d, want <= %d", len(ev.Output), MaxToolResultOutputBytes)
	}
	if got := ev.Output[len(ev.Output)-len(ToolResultTruncatedMark):]; got != ToolResultTruncatedMark {
		t.Fatalf("missing truncation marker: %q", got)
	}
}
