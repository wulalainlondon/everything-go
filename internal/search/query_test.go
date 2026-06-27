package search

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildFTSMatch(t *testing.T) {
	cases := map[string]string{
		"hello world":     `"hello" "world"`,
		`"exact phrase"`:  `"exact phrase"`,
		"foo OR bar":      `"foo" OR "bar"`,
		"foo AND":         `"foo"`,           // trailing operator dropped
		"OR foo":          `"foo"`,           // leading operator dropped
		`drop;the"quotes`: `"dropthequotes"`, // special chars stripped
	}
	for in, want := range cases {
		if got := buildFTSMatch(in); got != want {
			t.Errorf("buildFTSMatch(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestShortCJKTokens(t *testing.T) {
	// 1-2 char CJK runs need the LIKE fallback; 3+ go via trigram.
	if got := shortCJKTokens("連線"); len(got) != 1 || got[0] != "連線" {
		t.Fatalf("2-char CJK should be flagged, got %v", got)
	}
	if got := shortCJKTokens("穩定度"); len(got) != 0 {
		t.Fatalf("3-char CJK should NOT be flagged, got %v", got)
	}
	if got := shortCJKTokens("hello"); len(got) != 0 {
		t.Fatalf("ASCII should not be CJK-flagged, got %v", got)
	}
}

func TestCollectWarnings(t *testing.T) {
	w := collectWarnings("go is fun")
	if len(w) == 0 {
		t.Fatal("short ASCII tokens 'go','is' should warn")
	}
	if len(collectWarnings("everything works")) != 0 {
		t.Fatal("3+ char ASCII should not warn")
	}
}

// newTestIndex builds an in-memory-ish index on a temp file and inserts rows
// directly so query behavior can be tested without scanning real JSONL.
func newTestIndex(t *testing.T) *Index {
	t.Helper()
	idx, err := New(filepath.Join(t.TempDir(), "test_search.db"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { idx.Close() })
	return idx
}

func seed(t *testing.T, idx *Index, sessionID, content, ts string) {
	t.Helper()
	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()
	_, err := idx.db.Exec(`INSERT OR IGNORE INTO sessions(session_id, source, source_path, project_dir, display_name, last_ts, msg_count, backend) VALUES(?,?,?,?,?,?,?,?)`,
		sessionID, "claude", "/p/"+sessionID, "/p", "name "+sessionID, ts, 1, "claude")
	if err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := idx.db.Exec(`INSERT INTO messages(session_id, msg_uuid, role, ts, content) VALUES(?,?,?,?,?)`,
		sessionID, sessionID+":"+ts, "user", ts, content); err != nil {
		t.Fatalf("seed message: %v", err)
	}
}

func TestSearchASCIITrigram(t *testing.T) {
	idx := newTestIndex(t)
	seed(t, idx, "s1", "the quick brown fox jumps", "2026-01-01T00:00:00Z")
	resp := idx.Search("brown", Filters{}, 10, 0)
	if resp.ReturnedCount != 1 {
		t.Fatalf("want 1 hit for 'brown', got %d (warnings=%v)", resp.ReturnedCount, resp.Warnings)
	}
	if !strings.Contains(resp.Hits[0].Snippet, "<<brown>>") {
		t.Fatalf("snippet should highlight match: %q", resp.Hits[0].Snippet)
	}
}

func TestSearchCJKTrigram(t *testing.T) {
	idx := newTestIndex(t)
	seed(t, idx, "s1", "今天天氣很好我想出去玩程式設計", "2026-01-01T00:00:00Z")
	resp := idx.Search("程式設計", Filters{}, 10, 0)
	if resp.ReturnedCount != 1 {
		t.Fatalf("want 1 hit for 4-char CJK, got %d", resp.ReturnedCount)
	}
	if len(resp.Warnings) != 0 {
		t.Fatalf("3+ char CJK should not warn, got %v", resp.Warnings)
	}
}

func TestSearchCJKShortLikeFallback(t *testing.T) {
	idx := newTestIndex(t)
	seed(t, idx, "s1", "我想討論連線穩定度的問題", "2026-01-01T00:00:00Z")
	// LIKE fallback bounds an unfiltered scan to the last 90 days (Python parity);
	// pass an explicit Since so this test is independent of wall-clock time.
	resp := idx.Search("連線", Filters{Since: "2020-01-01T00:00:00Z"}, 10, 0)
	if resp.ReturnedCount != 1 {
		t.Fatalf("LIKE fallback should find 2-char CJK, got %d", resp.ReturnedCount)
	}
	if len(resp.Warnings) == 0 {
		t.Fatal("short CJK should emit a fallback warning")
	}
}

func TestSearchEmptyQuery(t *testing.T) {
	idx := newTestIndex(t)
	resp := idx.Search("   ", Filters{}, 10, 0)
	if resp.ReturnedCount != 0 || len(resp.Warnings) == 0 {
		t.Fatalf("blank query should return 0 hits with a warning, got %+v", resp)
	}
}

func TestSearchContextRoundTrip(t *testing.T) {
	idx := newTestIndex(t)
	seed(t, idx, "s1", "first message", "2026-01-01T00:00:01Z")
	seed(t, idx, "s1", "target message here", "2026-01-01T00:00:02Z")
	seed(t, idx, "s1", "third message", "2026-01-01T00:00:03Z")

	hit := idx.Search("target", Filters{}, 1, 0)
	if hit.ReturnedCount != 1 {
		t.Fatalf("setup search failed: %d hits", hit.ReturnedCount)
	}
	ctx := idx.GetContext("s1", hit.Hits[0].MsgUUID, 5)
	if len(ctx.Messages) != 3 {
		t.Fatalf("context should include all 3 messages, got %d", len(ctx.Messages))
	}
	var targets int
	for _, m := range ctx.Messages {
		if m.IsTarget {
			targets++
		}
	}
	if targets != 1 {
		t.Fatalf("exactly one message should be the target, got %d", targets)
	}
}

func TestListSessionsPagination(t *testing.T) {
	idx := newTestIndex(t)
	seed(t, idx, "s1", "a", "2026-01-01T00:00:01Z")
	seed(t, idx, "s2", "b", "2026-01-01T00:00:02Z")
	seed(t, idx, "s3", "c", "2026-01-01T00:00:03Z")
	page := idx.ListSessions("", 2, "", false)
	if len(page.Items) != 2 {
		t.Fatalf("limit 2 should return 2 items, got %d", len(page.Items))
	}
	if page.NextCursor == nil {
		t.Fatal("should have a next cursor when more remain")
	}
	// Newest-first: s3 then s2.
	if page.Items[0].SessionID != "s3" {
		t.Fatalf("expected newest-first ordering, got %s first", page.Items[0].SessionID)
	}
}

func TestHealthReadyFromMarkReady(t *testing.T) {
	idx := newTestIndex(t)
	// Empty index, ingest runs out-of-process, no MarkReady yet → not ready.
	if idx.Health().Ready {
		t.Fatal("empty index should report not ready")
	}
	idx.MarkReady() // bridge calls this after the first child indexer run
	if !idx.Health().Ready {
		t.Fatal("MarkReady should make Health ready")
	}
}

func TestHealthReadyDerivedFromMessages(t *testing.T) {
	idx := newTestIndex(t)
	seed(t, idx, "s1", "hello world", "2026-01-01T00:00:00Z")
	// A populated DB reports ready even without the in-memory MarkReady flag, so
	// the bridge surfaces a usable index immediately after a restart.
	if !idx.Health().Ready {
		t.Fatal("index with messages should report ready without MarkReady")
	}
}

func TestSetIndexingStatus(t *testing.T) {
	idx := newTestIndex(t)
	idx.SetIndexing(true)
	if got := idx.Health().IngestProgress["status"]; got != "ingesting" {
		t.Fatalf("status while child indexer runs = %v, want ingesting", got)
	}
	idx.SetIndexing(false)
	if got := idx.Health().IngestProgress["status"]; got != "ready" {
		t.Fatalf("status after child indexer exits = %v, want ready", got)
	}
}
