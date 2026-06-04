// Package search is the Go port of the Python bridge's SQLite FTS5 search
// subsystem (bridge/search/**). It indexes Claude and Codex conversation JSONL
// into an FTS5 trigram index so the app can full-text search history, with a
// LIKE fallback for 1–2 character CJK terms the trigram tokenizer cannot match.
//
// Pure-Go via modernc.org/sqlite — no cgo, preserving the CGO_ENABLED=0 static
// build goal. Trigram tokenizer, bm25(), snippet() and window functions were all
// verified available in modernc.org/sqlite v1.51.
package search

import (
	"database/sql"

	_ "modernc.org/sqlite"
)

// schemaVersion mirrors bridge/search/db/schema.py SCHEMA_VERSION.
const schemaVersion = 1

// ddl is 1:1 with bridge/search/db/schema.py: external-content FTS5 over the
// messages table, trigram tokenizer (remove_diacritics 0 keeps CJK intact),
// sync triggers, and the sessions / ingest_state bookkeeping tables.
const ddl = `
CREATE TABLE IF NOT EXISTS schema_meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  session_id      TEXT PRIMARY KEY,
  source          TEXT NOT NULL CHECK (source IN ('claude', 'codex', 'ollama')),
  source_path     TEXT NOT NULL,
  project_dir     TEXT NOT NULL,
  cwd             TEXT,
  display_name    TEXT,
  first_ts        TEXT,
  last_ts         TEXT,
  msg_count       INTEGER NOT NULL DEFAULT 0 CHECK (msg_count >= 0),
  backend         TEXT,
  is_pinned       INTEGER NOT NULL DEFAULT 0 CHECK (is_pinned IN (0, 1)),
  is_hidden       INTEGER NOT NULL DEFAULT 0 CHECK (is_hidden IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_sessions_pinned_ts
  ON sessions(is_hidden, is_pinned DESC, last_ts DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_project_pinned_ts
  ON sessions(is_hidden, project_dir, is_pinned DESC, last_ts DESC);

CREATE TABLE IF NOT EXISTS messages (
  rowid           INTEGER PRIMARY KEY,
  session_id      TEXT NOT NULL,
  msg_uuid        TEXT NOT NULL,
  parent_uuid     TEXT,
  role            TEXT NOT NULL,
  ts              TEXT NOT NULL,
  is_subagent     INTEGER NOT NULL DEFAULT 0 CHECK (is_subagent IN (0, 1)),
  content         TEXT NOT NULL,
  UNIQUE(session_id, msg_uuid)
);

CREATE INDEX IF NOT EXISTS idx_messages_session_ts ON messages(session_id, ts);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
  content,
  content='messages',
  content_rowid='rowid',
  tokenize='trigram remove_diacritics 0',
  columnsize=0,
  detail=full
);

CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
  INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
END;
CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
  INSERT INTO messages_fts(messages_fts, rowid, content) VALUES('delete', old.rowid, old.content);
END;
CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
  INSERT INTO messages_fts(messages_fts, rowid, content) VALUES('delete', old.rowid, old.content);
  INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
END;

CREATE TABLE IF NOT EXISTS ingest_state (
  source_path     TEXT PRIMARY KEY,
  file_size       INTEGER NOT NULL,
  last_mtime      REAL NOT NULL,
  last_offset     INTEGER NOT NULL,
  head_sha256     TEXT,
  last_ingest_at  REAL NOT NULL,
  msg_extracted   INTEGER NOT NULL DEFAULT 0 CHECK (msg_extracted >= 0),
  errors          INTEGER NOT NULL DEFAULT 0 CHECK (errors >= 0)
);

INSERT INTO schema_meta(key, value) VALUES ('schema_version', '1')
ON CONFLICT(key) DO UPDATE SET value = excluded.value;
`

// openDB opens (creating if needed) the search database with the WAL/pragma
// suite applied via DSN, and ensures the schema exists.
func openDB(path string) (*sql.DB, error) {
	dsn := "file:" + path +
		"?_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=foreign_keys(ON)" +
		"&_pragma=cache_size(-65536)" +
		"&_pragma=temp_store(MEMORY)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(ddl); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
