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

const (
	ingestBatchFiles = 20
	ingestBatchTime  = 2 * time.Second
	ingestBatchPause = 250 * time.Millisecond
)

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

// Start ingests in bounded background batches, then re-scans every interval to
// pick up new messages. The first full pass no longer monopolizes startup.
func (idx *Index) Start(interval time.Duration) {
	go idx.ingestLoop(interval)
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

func (idx *Index) ingestLoop(interval time.Duration) {
	first := true
	for {
		t0 := time.Now()
		jobs := idx.discoverJobs()
		idx.setProgress(func(p *ingestProgress) {
			p.status = "ingesting"
			p.filesTotal = len(jobs)
			p.filesDone = 0
			p.currentFile = ""
			p.currentSource = ""
			p.lastAdded = 0
			p.lastError = ""
			p.cycleStarted = t0
			p.cycleDone = time.Time{}
		})

		total := 0
		for len(jobs) > 0 {
			var added int
			jobs, added = idx.ingestBatch(jobs, ingestBatchFiles, ingestBatchTime)
			total += added
			if len(jobs) > 0 {
				time.Sleep(ingestBatchPause)
			}
		}

		idx.mu.Lock()
		idx.ready = true
		idx.progress.status = "ready"
		idx.progress.currentFile = ""
		idx.progress.currentSource = ""
		idx.progress.lastAdded = total
		idx.progress.cycleDone = time.Now()
		idx.mu.Unlock()

		if first {
			log.Printf("[search] initial ingest: %d messages in %s", total, time.Since(t0).Round(time.Millisecond))
			first = false
		} else if total > 0 {
			log.Printf("[search] incremental ingest: +%d messages in %s", total, time.Since(t0).Round(time.Millisecond))
		}

		time.Sleep(interval)
	}
}

func (idx *Index) setProgress(fn func(*ingestProgress)) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	fn(&idx.progress)
}

func (idx *Index) Close() error { return idx.db.Close() }
