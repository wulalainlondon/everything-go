package goexec

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A large transcript must load with peak heap bounded to ~the byte budget, not
// the file size — the whole point of the fix (an 826MB file used to spike the
// bridge to ~3.4GB). Generates a ~150MB fixture and samples peak HeapInuse.
func TestLoadClaudeHistoryBoundedPeakMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("generates a 150MB fixture")
	}
	path := filepath.Join(t.TempDir(), "big.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	line := `{"type":"user","timestamp":"2026-01-01T00:00:00Z","message":{"content":"` + strings.Repeat("x", 2000) + `"}}` + "\n"
	for i := 0; i < 75000; i++ { // ~150MB
		if _, err := f.WriteString(line); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()
	fi, _ := os.Stat(path)
	fileMB := float64(fi.Size()) / 1e6

	var peak atomic.Uint64
	done := make(chan struct{})
	go func() {
		var ms runtime.MemStats
		for {
			select {
			case <-done:
				return
			default:
				runtime.ReadMemStats(&ms)
				for old := peak.Load(); ms.HeapInuse > old && !peak.CompareAndSwap(old, ms.HeapInuse); old = peak.Load() {
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()
	msgs, truncated := loadClaudeHistoryMessages(path, "abc", 0)
	close(done)
	peakMB := float64(peak.Load()) / 1e6
	t.Logf("file=%.0fMB peak_heap=%.0fMB messages=%d truncated=%v", fileMB, peakMB, len(msgs), truncated)

	if !truncated {
		t.Fatal("a 150MB file must truncate under the 32MB budget")
	}
	if peakMB > 400 {
		t.Fatalf("peak heap %.0fMB not bounded (file was %.0fMB) — tail-streaming failed", peakMB, fileMB)
	}
}

// scanCwdAndName (resumable-sessions list) must read only the file's head, not
// the whole thing — it used to os.ReadFile every transcript, spiking the bridge
// to multiple GB on giant sessions when get_resumable_sessions ran.
func TestScanCwdAndNameBoundedMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("generates a 150MB fixture")
	}
	path := filepath.Join(t.TempDir(), "big.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	// cwd + first user text live on line 1 — the scan must stop there.
	f.WriteString(`{"type":"user","cwd":"/home/x/proj","message":{"content":"first user message here"}}` + "\n")
	bulk := strings.Repeat("x", 2000)
	for i := 0; i < 75000; i++ { // ~150MB of trailing content that must never be read
		f.WriteString(`{"type":"assistant","message":{"content":"` + bulk + `"}}` + "\n")
	}
	f.Close()

	var peak atomic.Uint64
	done := make(chan struct{})
	go func() {
		var ms runtime.MemStats
		for {
			select {
			case <-done:
				return
			default:
				runtime.ReadMemStats(&ms)
				for old := peak.Load(); ms.HeapInuse > old && !peak.CompareAndSwap(old, ms.HeapInuse); old = peak.Load() {
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()
	cwd, name := scanCwdAndName(path)
	close(done)
	peakMB := float64(peak.Load()) / 1e6
	t.Logf("cwd=%q name=%q peak_heap=%.0fMB", cwd, name, peakMB)

	if cwd != "/home/x/proj" || name != "first user message here" {
		t.Fatalf("scan returned cwd=%q name=%q", cwd, name)
	}
	if peakMB > 100 {
		t.Fatalf("peak heap %.0fMB — scan read far more than the head of a 150MB file", peakMB)
	}
}

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
