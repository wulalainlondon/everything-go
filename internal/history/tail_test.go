package history

import (
	"strings"
	"testing"
)

func TestStreamTailLinesSmallKeepsAllWithLineNos(t *testing.T) {
	// Blank lines must still advance the line counter so ids stay stable.
	in := "a\n\nb\nc\n"
	lines, truncated, err := StreamTailLines(strings.NewReader(in), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if truncated {
		t.Fatal("small input should not truncate")
	}
	want := []TailLine{{1, []byte("a")}, {3, []byte("b")}, {4, []byte("c")}}
	if len(lines) != len(want) {
		t.Fatalf("got %d lines, want %d", len(lines), len(want))
	}
	for i, w := range want {
		if lines[i].LineNo != w.LineNo || string(lines[i].Data) != string(w.Data) {
			t.Fatalf("line %d = {%d,%q}, want {%d,%q}", i, lines[i].LineNo, lines[i].Data, w.LineNo, w.Data)
		}
	}
}

func TestStreamTailLinesTruncatesToTail(t *testing.T) {
	// 10 lines of ~10 bytes each; a tiny budget keeps only the most recent,
	// and their line numbers must be the true absolute positions.
	var b strings.Builder
	for i := 1; i <= 10; i++ {
		b.WriteString(strings.Repeat("x", 9))
		b.WriteByte('\n')
	}
	lines, truncated, err := StreamTailLines(strings.NewReader(b.String()), 25)
	if err != nil {
		t.Fatal(err)
	}
	if !truncated {
		t.Fatal("expected truncation under a tiny budget")
	}
	// Budget 25 / ~9 bytes ⇒ last 2-3 lines; the last must be line 10.
	last := lines[len(lines)-1]
	if last.LineNo != 10 {
		t.Fatalf("last retained line = %d, want 10", last.LineNo)
	}
	if lines[0].LineNo <= 1 {
		t.Fatalf("tail should drop early lines, first kept = %d", lines[0].LineNo)
	}
}

func TestStreamTailLinesHandlesHugeSingleLine(t *testing.T) {
	// A single line far larger than the budget must be kept (never dropped to
	// zero) and must not error — the failure mode the old Scanner cap had.
	huge := strings.Repeat("y", 5<<20)
	lines, truncated, err := StreamTailLines(strings.NewReader(huge+"\n"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 || lines[0].LineNo != 1 || len(lines[0].Data) != len(huge) {
		t.Fatalf("huge single line not retained intact: n=%d", len(lines))
	}
	_ = truncated
}
