// Package feed is the Go port of bridge/handlers/feed_ops.py: a small store for
// HTML/markdown articles pushed from local pipelines to the mobile app's feed.
// Metadata lives in feed/index.json; article bodies are individual files under
// feed/articles/. The app lists metadata (feed_list), opens a body on demand
// (feed_fetch → feed_detail), and marks read / deletes (feed_updated).
package feed

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

func randHex(n int) string {
	b := make([]byte, (n+1)/2)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)[:n]
}

// maxArticleBytes mirrors the Python 5 MB cap.
const maxArticleBytes = 5 * 1024 * 1024

// gcAge hard-deletes soft-deleted entries older than this (Python: 7 days).
const gcAge = 7 * 24 * time.Hour

// Meta is one feed entry. JSON tags match app feed.ts FeedMetaSchema; the
// client_dedup_key is internal and stripped before publishing to clients.
type Meta struct {
	FeedID         string   `json:"feed_id"`
	Title          string   `json:"title"`
	Source         string   `json:"source"`
	URL            string   `json:"url"`
	ContentType    string   `json:"content_type"`
	ClientDedupKey string   `json:"client_dedup_key,omitempty"`
	CreatedAt      float64  `json:"created_at"`
	Read           bool     `json:"read"`
	Deleted        bool     `json:"deleted"`
	DeletedAt      *float64 `json:"deleted_at"`
}

// Store holds the feed registry and persists it. Safe for concurrent use.
type Store struct {
	dir string // <data_dir>/feed
	mu  sync.Mutex
	reg map[string]*Meta
}

// New opens (or creates) the feed store rooted at dataDir, loading any existing
// index.json.
func New(dataDir string) *Store {
	s := &Store{dir: filepath.Join(dataDir, "feed"), reg: map[string]*Meta{}}
	s.load()
	return s
}

func (s *Store) indexPath() string { return filepath.Join(s.dir, "index.json") }
func (s *Store) articlePath(id string) string {
	return filepath.Join(s.dir, "articles", id+".html")
}

func (s *Store) load() {
	data, err := os.ReadFile(s.indexPath())
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &s.reg) // best-effort; corrupt index → empty
	if s.reg == nil {
		s.reg = map[string]*Meta{}
	}
}

// caller holds s.mu
func (s *Store) save() {
	_ = os.MkdirAll(s.dir, 0o755)
	if data, err := json.Marshal(s.reg); err == nil {
		_ = os.WriteFile(s.indexPath(), data, 0o644)
	}
}

func nowSecs() float64 { return float64(time.Now().UnixNano()) / 1e9 }

func validContentType(ct string) string {
	if ct == "markdown" {
		return "markdown"
	}
	return "html" // default + sanitize unknown, like Python
}

// Push stores a new article. Returns the feed_id, whether it was a dedup hit
// (existing entry returned, nothing new stored), and any error (oversize body).
func (s *Store) Push(title, html, source, url, dedupKey, contentType string) (id string, deduped bool, err error) {
	if len(html) > maxArticleBytes {
		return "", false, errors.New("Feed article exceeds 5 MB limit")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if dedupKey != "" {
		for _, m := range s.reg {
			if m.ClientDedupKey == dedupKey {
				return m.FeedID, true, nil
			}
		}
	}
	if source == "" {
		source = "pipeline"
	}
	id = "feed_" + randHex(12)
	s.reg[id] = &Meta{
		FeedID: id, Title: title, Source: source, URL: url,
		ContentType: validContentType(contentType), ClientDedupKey: dedupKey,
		CreatedAt: nowSecs(), Read: false, Deleted: false,
	}
	ap := s.articlePath(id)
	if e := os.MkdirAll(filepath.Dir(ap), 0o755); e != nil {
		delete(s.reg, id)
		return "", false, e
	}
	if e := os.WriteFile(ap, []byte(html), 0o644); e != nil {
		delete(s.reg, id)
		return "", false, e
	}
	s.save()
	return id, false, nil
}

// List returns non-deleted entries newest-first, with the dedup key stripped.
// It first hard-deletes expired soft-deleted entries (GC).
func (s *Store) List() []Meta {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gcLocked()
	out := make([]Meta, 0, len(s.reg))
	for _, m := range s.reg {
		if m.Deleted {
			continue
		}
		pub := *m
		pub.ClientDedupKey = ""
		out = append(out, pub)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt > out[j].CreatedAt })
	return out
}

// Fetch returns an article body. ok is false if missing or soft-deleted.
func (s *Store) Fetch(id string) (html, contentType string, ok bool) {
	s.mu.Lock()
	m := s.reg[id]
	if m == nil || m.Deleted {
		s.mu.Unlock()
		return "", "", false
	}
	ct := m.ContentType
	s.mu.Unlock()
	data, err := os.ReadFile(s.articlePath(id))
	if err != nil {
		return "", "", false
	}
	return string(data), ct, true
}

// MarkRead flags an entry read and returns its published meta. ok=false if absent.
func (s *Store) MarkRead(id string) (Meta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.reg[id]
	if m == nil {
		return Meta{}, false
	}
	m.Read = true
	s.save()
	pub := *m
	pub.ClientDedupKey = ""
	return pub, true
}

// Delete soft-deletes an entry and returns its published meta. ok=false if absent.
func (s *Store) Delete(id string) (Meta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.reg[id]
	if m == nil {
		return Meta{}, false
	}
	m.Deleted = true
	t := nowSecs()
	m.DeletedAt = &t
	s.save()
	pub := *m
	pub.ClientDedupKey = ""
	return pub, true
}

// gcLocked hard-deletes soft-deleted entries older than gcAge. Caller holds mu.
func (s *Store) gcLocked() {
	cutoff := nowSecs() - gcAge.Seconds()
	changed := false
	for id, m := range s.reg {
		if m.DeletedAt != nil && *m.DeletedAt < cutoff {
			_ = os.Remove(s.articlePath(id))
			delete(s.reg, id)
			changed = true
		}
	}
	if changed {
		s.save()
	}
}
