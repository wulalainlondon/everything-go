package history

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestHistoryCacheMemoryHitAndMiss(t *testing.T) {
	c := NewMemoryCache()
	key := FileKey{Path: "/tmp/a.jsonl", MtimeNS: 10, Size: 20}
	msgs := []map[string]any{{"role": "user", "content": "hello"}}
	c.Save("claude:abc", key, msgs)

	got, ok := c.Load("claude:abc", key)
	if !ok || len(got) != 1 || got[0]["content"] != "hello" {
		t.Fatalf("cache hit failed ok=%v got=%+v", ok, got)
	}
	if _, ok := c.Load("claude:abc", FileKey{Path: "/tmp/a.jsonl", MtimeNS: 11, Size: 20}); ok {
		t.Fatal("cache must miss when file key changes")
	}
}

func TestHistoryCacheSQLitePersistsAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	key := FileKey{Path: "/tmp/a.jsonl", MtimeNS: 10, Size: 20}

	c1, err := NewSQLiteCache(path)
	if err != nil {
		t.Fatal(err)
	}
	c1.Save("claude:abc", key, []map[string]any{{"role": "assistant", "content": "cached"}})
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}

	c2, err := NewSQLiteCache(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	got, ok := c2.Load("claude:abc", key)
	if !ok || len(got) != 1 || got[0]["content"] != "cached" {
		t.Fatalf("sqlite cache miss ok=%v got=%+v", ok, got)
	}
}

func TestHistoryCacheCorruptSQLiteJSONMisses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	c, err := NewSQLiteCache(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(
		`INSERT INTO history_cache(cache_name,path,mtime_ns,size,messages_json,updated_at) VALUES(?,?,?,?,?,?)`,
		"claude:bad", "/tmp/a.jsonl", 1, 2, "{bad json", 123,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Load("claude:bad", FileKey{Path: "/tmp/a.jsonl", MtimeNS: 1, Size: 2}); ok {
		t.Fatal("corrupt cached JSON should miss")
	}
}
