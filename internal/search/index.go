package search

import (
	"database/sql"
	"log"
	"sync"
	"time"

	"everything-go/internal/sourcepolicy"
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
func (idx *Index) RunOnce() int {
	if removed, err := idx.pruneExcludedCodexSessions(); err != nil {
		log.Printf("[search] prune excluded Codex sessions: %v", err)
	} else if removed > 0 {
		log.Printf("[search] pruned %d excluded Codex session(s)", removed)
	}
	if removed, err := idx.pruneFrameworkNoiseMessages(); err != nil {
		log.Printf("[search] prune framework-noise messages: %v", err)
	} else if removed > 0 {
		log.Printf("[search] pruned %d framework-noise message(s)", removed)
	}
	return idx.ingestAll()
}

func (idx *Index) pruneFrameworkNoiseMessages() (int64, error) {
	tx, err := idx.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	result, err := tx.Exec(`
		DELETE FROM messages
		WHERE role='user'
		  AND session_id IN (SELECT session_id FROM sessions WHERE source='codex')
		  AND lower(ltrim(content)) LIKE '<recommended_plugins>%'`)
	if err != nil {
		return 0, err
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if _, err := tx.Exec(`
		UPDATE sessions
		SET msg_count=(SELECT COUNT(*) FROM messages WHERE messages.session_id=sessions.session_id)
		WHERE source='codex'`); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return removed, nil
}

func (idx *Index) pruneExcludedCodexSessions() (int, error) {
	cwdGlobs := sourcepolicy.CodexIgnoreCWDGlobs()
	namePrefixes := sourcepolicy.CodexIgnoreNamePrefixes()
	if len(cwdGlobs) == 0 && len(namePrefixes) == 0 {
		return 0, nil
	}
	rows, err := idx.db.Query(
		"SELECT session_id, source_path, COALESCE(cwd,''), COALESCE(display_name,'') FROM sessions WHERE source='codex'",
	)
	if err != nil {
		return 0, err
	}
	type excludedRow struct {
		sessionID, sourcePath string
	}
	var excluded []excludedRow
	for rows.Next() {
		var sessionID, sourcePath, cwd, name string
		if err := rows.Scan(&sessionID, &sourcePath, &cwd, &name); err != nil {
			rows.Close()
			return 0, err
		}
		if sourcepolicy.IgnoreCodexSession(cwd, name, cwdGlobs, namePrefixes) {
			excluded = append(excluded, excludedRow{sessionID: sessionID, sourcePath: sourcePath})
		}
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if len(excluded) == 0 {
		return 0, nil
	}
	tx, err := idx.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, row := range excluded {
		if _, err := tx.Exec("DELETE FROM messages WHERE session_id=?", row.sessionID); err != nil {
			return 0, err
		}
		if _, err := tx.Exec("DELETE FROM sessions WHERE session_id=?", row.sessionID); err != nil {
			return 0, err
		}
		if row.sourcePath != "" {
			if _, err := tx.Exec("DELETE FROM ingest_state WHERE source_path=?", row.sourcePath); err != nil {
				return 0, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(excluded), nil
}

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
