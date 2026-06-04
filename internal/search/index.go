package search

import (
	"database/sql"
	"log"
	"sync"
	"time"
)

// Index owns the search database and the background ingest loop. One Index is
// shared by the whole bridge; queries run concurrently against the WAL DB while
// a single goroutine serializes writes through writeMu.
type Index struct {
	db      *sql.DB
	path    string
	sources []source

	writeMu sync.Mutex

	mu        sync.Mutex
	ready     bool
	lastError string
}

// New opens (creating if needed) the search index at dbPath and registers the
// Claude + Codex sources. It does not start ingesting — call Start.
func New(dbPath string) (*Index, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return nil, err
	}
	return &Index{
		db:      db,
		path:    dbPath,
		sources: []source{newClaudeSource(), newCodexSource()},
	}, nil
}

// Start runs an initial full ingest, then re-scans every interval to pick up new
// messages. Runs until the process exits.
func (idx *Index) Start(interval time.Duration) {
	go func() {
		t0 := time.Now()
		n := idx.ingestAll()
		idx.mu.Lock()
		idx.ready = true
		idx.mu.Unlock()
		log.Printf("[search] initial ingest: %d messages in %s", n, time.Since(t0).Round(time.Millisecond))

		for {
			time.Sleep(interval)
			if added := idx.ingestAll(); added > 0 {
				log.Printf("[search] incremental ingest: +%d messages", added)
			}
		}
	}()
}

func (idx *Index) isReady() bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.ready
}

func (idx *Index) Close() error { return idx.db.Close() }
