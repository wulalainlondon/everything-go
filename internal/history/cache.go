package history

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type FileKey struct {
	Path    string
	MtimeNS int64
	Size    int64
}

type cacheEntry struct {
	key      FileKey
	messages []map[string]any
}

type Cache struct {
	mu  sync.Mutex
	mem map[string]cacheEntry
	db  *sql.DB
}

var (
	defaultCacheOnce sync.Once
	defaultCache     *Cache
)

func DefaultCache() *Cache {
	defaultCacheOnce.Do(func() {
		path := defaultCachePath()
		cache, err := NewSQLiteCache(path)
		if err != nil {
			defaultCache = NewMemoryCache()
			return
		}
		defaultCache = cache
	})
	return defaultCache
}

func defaultCachePath() string {
	base, err := os.UserCacheDir()
	if err != nil || base == "" {
		base = os.TempDir()
	}
	return filepath.Join(base, "everything-go", "history_cache.sqlite")
}

func NewMemoryCache() *Cache {
	return &Cache{mem: map[string]cacheEntry{}}
}

func NewSQLiteCache(path string) (*Cache, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	c := &Cache{mem: map[string]cacheEntry{}, db: db}
	if err := c.initDB(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return c, nil
}

func (c *Cache) initDB() error {
	if c.db == nil {
		return nil
	}
	_, err := c.db.Exec(`
CREATE TABLE IF NOT EXISTS history_cache (
	cache_name TEXT PRIMARY KEY,
	path TEXT NOT NULL,
	mtime_ns INTEGER NOT NULL,
	size INTEGER NOT NULL,
	messages_json TEXT NOT NULL,
	updated_at INTEGER NOT NULL
)`)
	return err
}

func (c *Cache) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

func (c *Cache) Load(cacheName string, key FileKey) ([]map[string]any, bool) {
	if c == nil || cacheName == "" {
		return nil, false
	}
	c.mu.Lock()
	if ent, ok := c.mem[cacheName]; ok && sameFileKey(ent.key, key) {
		messages := cloneMessages(ent.messages)
		c.mu.Unlock()
		return messages, true
	}
	c.mu.Unlock()
	if c.db == nil {
		return nil, false
	}

	var raw string
	err := c.db.QueryRow(
		`SELECT messages_json FROM history_cache WHERE cache_name=? AND path=? AND mtime_ns=? AND size=?`,
		cacheName, key.Path, key.MtimeNS, key.Size,
	).Scan(&raw)
	if err != nil {
		return nil, false
	}
	var messages []map[string]any
	if json.Unmarshal([]byte(raw), &messages) != nil {
		return nil, false
	}
	c.mu.Lock()
	c.mem[cacheName] = cacheEntry{key: key, messages: cloneMessages(messages)}
	c.mu.Unlock()
	return messages, true
}

func (c *Cache) Save(cacheName string, key FileKey, messages []map[string]any) {
	if c == nil || cacheName == "" {
		return
	}
	stored := cloneMessages(messages)
	c.mu.Lock()
	c.mem[cacheName] = cacheEntry{key: key, messages: stored}
	c.mu.Unlock()
	if c.db == nil {
		return
	}
	raw, err := json.Marshal(stored)
	if err != nil {
		return
	}
	_, _ = c.db.Exec(
		`INSERT INTO history_cache(cache_name,path,mtime_ns,size,messages_json,updated_at)
VALUES(?,?,?,?,?,?)
ON CONFLICT(cache_name) DO UPDATE SET
	path=excluded.path,
	mtime_ns=excluded.mtime_ns,
	size=excluded.size,
	messages_json=excluded.messages_json,
	updated_at=excluded.updated_at`,
		cacheName, key.Path, key.MtimeNS, key.Size, string(raw), time.Now().Unix(),
	)
}

func (c *Cache) SaveAsync(cacheName string, key FileKey, messages []map[string]any) {
	if c == nil {
		return
	}
	snapshot := cloneMessages(messages)
	go c.Save(cacheName, key, snapshot)
}

func sameFileKey(a, b FileKey) bool {
	return a.Path == b.Path && a.MtimeNS == b.MtimeNS && a.Size == b.Size
}

func cloneMessages(messages []map[string]any) []map[string]any {
	if messages == nil {
		return nil
	}
	out := make([]map[string]any, len(messages))
	for i, m := range messages {
		cp := make(map[string]any, len(m))
		for k, v := range m {
			cp[k] = v
		}
		out[i] = cp
	}
	return out
}
