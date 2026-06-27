package goexec

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// A runaway transcript must load only its recent tail (bounded memory) while
// keeping ABSOLUTE line numbers, so message ids stay stable across the
// whole-file vs tail-streamed paths.
func TestLoadClaudeHistoryTailTruncatesPreservingLineNos(t *testing.T) {
	t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "2048")
	path := filepath.Join(t.TempDir(), "sess.jsonl")
	var b strings.Builder
	for i := 1; i <= 50; i++ {
		b.WriteString(`{"type":"user","timestamp":"2026-01-01T00:00:00Z","message":{"content":"msg `)
		b.WriteString(strconv.Itoa(i))
		b.WriteByte(' ')
		b.WriteString(strings.Repeat("x", 120))
		b.WriteString(`"}}`)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	msgs, truncated := loadClaudeHistoryMessages(path, "abc", 0)
	if !truncated {
		t.Fatal("expected truncation under a tiny budget")
	}
	if len(msgs) == 0 {
		t.Fatal("should still return the most recent messages")
	}
	if len(msgs) >= 50 {
		t.Fatalf("expected a truncated tail window, got all %d messages", len(msgs))
	}
	// The newest message is physical line 50 — tail mode must NOT renumber from 1.
	lastID, _ := msgs[len(msgs)-1]["source_message_id"].(string)
	if !strings.HasSuffix(lastID, ":line:50") {
		t.Fatalf("last message id = %q, want suffix :line:50", lastID)
	}
}
