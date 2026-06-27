package search

import (
	"database/sql"
	"sync"
	"time"
)

// Index owns the search database. One Index is shared by the whole bridge and
// queries run concurrently against the WAL DB. Ingestion does NOT run in this
// process — it happens in a short-lived `--mode=index` child (see RunOnce), so
// the resident bridge only ever reads.
type Index struct {
	db      *sql.DB
	path    string
	sources []source

	writeMu sync.Mutex

	mu       sync.Mutex
	ready    bool
	progress ingestProgress
}

type ingestProgress struct {
	status        string
	filesTotal    int
	filesDone     int
	currentFile   string
	currentSource string
	lastAdded     int
	lastError     string
	cycleStarted  time.Time
	cycleDone     time.Time
}

// New opens (creating if needed) the search index at dbPath and registers the
// Claude + Codex sources. It does not ingest — the bridge issues read-only
// queries while the `--mode=index` child calls RunOnce.
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

// RunOnce ingests every source's new content to completion and returns the
// number of messages added. It is the body of the `--mode=index` child: a
// short-lived process that does the heap-heavy parse and then exits, handing all
// of its memory back to the OS so the resident bridge stays lightweight.
func (idx *Index) RunOnce() int { return idx.ingestAll() }

// MarkReady marks the index queryable. The bridge calls it after the first
// successful child indexer run; Health also derives readiness from the DB.
func (idx *Index) MarkReady() {
	idx.mu.Lock()
	idx.ready = true
	idx.mu.Unlock()
}

// SetIndexing records whether a child indexer is currently running so Health()
// can surface ingest activity to the app.
func (idx *Index) SetIndexing(on bool) {
	idx.setProgress(func(p *ingestProgress) {
		if on {
			p.status = "ingesting"
			p.cycleStarted = time.Now()
			p.cycleDone = time.Time{}
		} else {
			p.status = "ready"
			p.cycleDone = time.Now()
		}
	})
}

func (idx *Index) isReady() bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.ready
}

func (idx *Index) snapshotProgress() ingestProgress {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.progress
}

func (idx *Index) setProgress(fn func(*ingestProgress)) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	fn(&idx.progress)
}

func (idx *Index) Close() error { return idx.db.Close() }
