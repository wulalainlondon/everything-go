package search

import (
	"database/sql"
	"log"
	"os"
	"time"
)

// ingestFile brings one file's messages into the index incrementally. It honors
// the stored byte offset, detects rotation/truncation via head signature + size,
// upserts the session row, inserts new messages, and records ingest_state.
// Mirrors bridge/search/ingest/single_file.py (condensed).
func (idx *Index) ingestFile(src source, path string) (extracted int, err error) {
	info, statErr := os.Stat(path)
	if statErr != nil {
		return 0, statErr
	}
	size := info.Size()
	mtime := float64(info.ModTime().UnixNano()) / 1e9
	headSHA := src.headSignature(path)

	// Read prior ingest state.
	var startOffset int64
	var prevSHA string
	row := idx.db.QueryRow(
		"SELECT last_offset, head_sha256 FROM ingest_state WHERE source_path = ?", path)
	if scanErr := row.Scan(&startOffset, &prevSHA); scanErr == sql.ErrNoRows {
		startOffset, prevSHA = 0, ""
	} else if scanErr != nil {
		return 0, scanErr
	}

	rotated := prevSHA != "" && prevSHA != headSHA
	truncated := startOffset > size
	if rotated || truncated {
		startOffset = 0 // re-ingest from the top
	}
	if startOffset == size && !rotated {
		return 0, nil // nothing new
	}

	msgs, finalOffset := src.iterMessages(path, startOffset)
	meta := src.sessionMeta(path)
	sid := src.sessionIDFor(path)

	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()

	tx, err := idx.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	if len(msgs) > 0 {
		lastTS := meta.FirstTS
		for _, m := range msgs {
			if m.Timestamp > lastTS {
				lastTS = m.Timestamp
			}
		}
		// On rotation, reset the running msg_count to this batch's size.
		if rotated || startOffset == 0 {
			_, _ = tx.Exec(`
				INSERT INTO sessions(session_id, source, source_path, project_dir, cwd,
					display_name, first_ts, last_ts, msg_count, backend)
				VALUES(?,?,?,?,?,?,?,?,?,?)
				ON CONFLICT(session_id) DO UPDATE SET
					source_path = excluded.source_path,
					project_dir = excluded.project_dir,
					cwd = COALESCE(NULLIF(excluded.cwd,''), sessions.cwd),
					display_name = CASE WHEN sessions.display_name IS NULL OR sessions.display_name=''
						THEN excluded.display_name ELSE sessions.display_name END,
					first_ts = CASE WHEN sessions.first_ts IS NULL OR sessions.first_ts=''
						THEN excluded.first_ts ELSE sessions.first_ts END,
					last_ts = CASE WHEN excluded.last_ts > sessions.last_ts
						THEN excluded.last_ts ELSE sessions.last_ts END,
					msg_count = excluded.msg_count,
					backend = excluded.backend`,
				sid, src.name(), path, meta.ProjectDir, meta.Cwd,
				meta.DisplayName, meta.FirstTS, lastTS, len(msgs), src.name())
		} else {
			_, _ = tx.Exec(`
				INSERT INTO sessions(session_id, source, source_path, project_dir, cwd,
					display_name, first_ts, last_ts, msg_count, backend)
				VALUES(?,?,?,?,?,?,?,?,?,?)
				ON CONFLICT(session_id) DO UPDATE SET
					cwd = COALESCE(NULLIF(excluded.cwd,''), sessions.cwd),
					display_name = CASE WHEN sessions.display_name IS NULL OR sessions.display_name=''
						THEN excluded.display_name ELSE sessions.display_name END,
					last_ts = CASE WHEN excluded.last_ts > sessions.last_ts
						THEN excluded.last_ts ELSE sessions.last_ts END,
					msg_count = sessions.msg_count + excluded.msg_count,
					backend = excluded.backend`,
				sid, src.name(), path, meta.ProjectDir, meta.Cwd,
				meta.DisplayName, meta.FirstTS, lastTS, len(msgs), src.name())
		}

		stmt, perr := tx.Prepare(`
			INSERT INTO messages(session_id, msg_uuid, parent_uuid, role, ts, is_subagent, content)
			VALUES(?,?,?,?,?,?,?)
			ON CONFLICT(session_id, msg_uuid) DO NOTHING`)
		if perr != nil {
			return 0, perr
		}
		for _, m := range msgs {
			sub := 0
			if m.IsSubagent {
				sub = 1
			}
			if _, e := stmt.Exec(m.SessionID, m.MsgUUID, nullStr(m.ParentUUID),
				m.Role, m.Timestamp, sub, m.Text); e != nil {
				stmt.Close()
				return 0, e
			}
		}
		stmt.Close()
	}

	now := float64(time.Now().UnixNano()) / 1e9
	_, _ = tx.Exec(`
		INSERT INTO ingest_state(source_path, file_size, last_mtime, last_offset,
			head_sha256, last_ingest_at, msg_extracted, errors)
		VALUES(?,?,?,?,?,?,?,0)
		ON CONFLICT(source_path) DO UPDATE SET
			file_size = excluded.file_size,
			last_mtime = excluded.last_mtime,
			last_offset = excluded.last_offset,
			head_sha256 = excluded.head_sha256,
			last_ingest_at = excluded.last_ingest_at,
			msg_extracted = ingest_state.msg_extracted + excluded.msg_extracted`,
		path, size, mtime, finalOffset, headSHA, now, len(msgs))

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(msgs), nil
}

// ingestAll scans every source's files once. Returns total messages added.
func (idx *Index) ingestAll() int {
	total := 0
	for _, src := range idx.sources {
		if !src.enabled() {
			continue
		}
		for _, path := range src.discover() {
			n, err := idx.ingestFile(src, path)
			if err != nil {
				log.Printf("[search] ingest %s: %v", path, err)
				continue
			}
			total += n
		}
	}
	return total
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
