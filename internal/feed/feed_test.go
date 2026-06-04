package feed

import (
	"strings"
	"testing"
)

func TestFeedPushListFetch(t *testing.T) {
	s := New(t.TempDir())

	id1, dup, err := s.Push("First", "<h1>one</h1>", "pocket_gamer", "http://x/1", "k1", "html")
	if err != nil || dup || id1 == "" {
		t.Fatalf("push1: id=%q dup=%v err=%v", id1, dup, err)
	}
	id2, _, _ := s.Push("Second", "# two", "weekly", "", "k2", "markdown")

	// dedup: same key → same id, no new entry
	idDup, dup, _ := s.Push("First again", "<h1>dup</h1>", "x", "", "k1", "html")
	if !dup || idDup != id1 {
		t.Fatalf("dedup should return id1=%s, got id=%s dup=%v", id1, idDup, dup)
	}

	list := s.List()
	if len(list) != 2 {
		t.Fatalf("want 2 items, got %d", len(list))
	}
	// newest first (id2 pushed after id1)
	if list[0].FeedID != id2 {
		t.Errorf("expected newest (id2) first, got %s", list[0].FeedID)
	}
	for _, m := range list {
		if m.ClientDedupKey != "" {
			t.Errorf("client_dedup_key must be stripped on publish, got %q", m.ClientDedupKey)
		}
	}
	if list[1].ContentType != "html" || list[0].ContentType != "markdown" {
		t.Errorf("content types wrong: %v", list)
	}

	// fetch body
	html, ct, ok := s.Fetch(id2)
	if !ok || html != "# two" || ct != "markdown" {
		t.Fatalf("fetch id2: html=%q ct=%q ok=%v", html, ct, ok)
	}

	// mark read
	if m, ok := s.MarkRead(id1); !ok || !m.Read {
		t.Fatalf("mark read failed: %+v ok=%v", m, ok)
	}

	// delete → excluded from list + not fetchable
	if _, ok := s.Delete(id1); !ok {
		t.Fatal("delete failed")
	}
	if _, _, ok := s.Fetch(id1); ok {
		t.Error("deleted item should not be fetchable")
	}
	if got := s.List(); len(got) != 1 || got[0].FeedID != id2 {
		t.Errorf("after delete, list should be [id2], got %d items", len(got))
	}
}

func TestFeedPushOversize(t *testing.T) {
	s := New(t.TempDir())
	big := strings.Repeat("x", 5*1024*1024+1)
	if _, _, err := s.Push("big", big, "", "", "", "html"); err == nil {
		t.Fatal("oversize push should error")
	}
}

func TestFeedPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	id, _, _ := s.Push("persist", "<p>hi</p>", "", "", "", "html")
	// reopen
	s2 := New(dir)
	list := s2.List()
	if len(list) != 1 || list[0].FeedID != id {
		t.Fatalf("feed index did not persist across reopen: %d items", len(list))
	}
	if html, _, ok := s2.Fetch(id); !ok || html != "<p>hi</p>" {
		t.Fatalf("article body did not persist: html=%q ok=%v", html, ok)
	}
}
