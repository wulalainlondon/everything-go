package messagequeue

import (
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/protocol"
	"fmt"
)

type ContextScope struct {
	Authority, SessionID, Backend, Generation string
	SourceRevision                            uint64 `json:"-"`
}

func (c ContextScope) key() (string, error) {
	if c.Authority == "" || c.SessionID == "" || c.Generation == "" || (c.Backend != "codex" && c.Backend != "claude") {
		return "", errors.New("invalid context scope")
	}
	b, _ := json.Marshal(c)
	return string(b), nil
}
func (s *Store) installContextObservations() error {
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS context_observation_scopes(scope TEXT PRIMARY KEY,tracked_since INTEGER NOT NULL,usage BLOB,usage_at INTEGER NOT NULL DEFAULT 0,usage_source_revision INTEGER NOT NULL DEFAULT 0,invalidated_at INTEGER NOT NULL DEFAULT 0,as_of INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS context_usage_seen(scope TEXT NOT NULL,event_id TEXT NOT NULL,PRIMARY KEY(scope,event_id));
 CREATE TABLE IF NOT EXISTS context_compaction_operations(scope TEXT NOT NULL,canonical TEXT NOT NULL,state TEXT NOT NULL,observed_at INTEGER NOT NULL,PRIMARY KEY(scope,canonical));
 CREATE TABLE IF NOT EXISTS context_compaction_aliases(scope TEXT NOT NULL,alias TEXT NOT NULL,canonical TEXT NOT NULL,observed_at INTEGER NOT NULL,PRIMARY KEY(scope,alias));`)
	return err
}

// RecordContextObservation commits dedupe, completion and invalidation together.
// Duplicate/replayed observations never refresh the original report time.
func (s *Store) RecordContextObservation(c ContextScope, id string, usage *protocol.ContextUsageObservation, completionState string, aliases []string, at int64) error {
	key, err := c.key()
	if err != nil {
		return err
	}
	if id == "" || len(id) > 512 || at <= 0 || len(aliases) > 8 {
		return errors.New("invalid context evidence")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`INSERT OR IGNORE INTO context_observation_scopes(scope,tracked_since,as_of) VALUES(?,?,?)`, key, at, at); err != nil {
		return err
	}
	if completionState != "" {
		if completionState != "observed" && completionState != "completed" && completionState != "failed" && completionState != "unknown" {
			return errors.New("invalid completion state")
		}
		canonical := ""
		all := append([]string{id}, aliases...)
		for _, alias := range all {
			if alias == "" || len(alias) > 512 {
				return errors.New("invalid context alias")
			}
			var found string
			err = tx.QueryRow(`SELECT canonical FROM context_compaction_aliases WHERE scope=? AND alias=?`, key, alias).Scan(&found)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil {
				if canonical != "" && canonical != found {
					return errors.New("context completion identity conflict")
				}
				canonical = found
			}
		}
		if canonical == "" {
			canonical = id
			if len(aliases) > 0 {
				canonical = aliases[0]
			}
		}
		var previous string
		err = tx.QueryRow(`SELECT state FROM context_compaction_operations WHERE scope=? AND canonical=?`, key, canonical).Scan(&previous)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		next := previous
		if next == "" {
			next = "observed"
		}
		// Item aliases cannot manufacture the terminal of a managed operation.
		if id == canonical {
			if previous == "failed" && completionState == "completed" {
				return errors.New("failed compaction cannot become completed")
			}
			if previous == "completed" && completionState != "completed" {
				return errors.New("conflicting compaction terminal")
			}
			next = completionState
		}
		if _, err = tx.Exec(`INSERT INTO context_compaction_operations VALUES(?,?,?,?) ON CONFLICT(scope,canonical) DO UPDATE SET state=excluded.state,observed_at=excluded.observed_at`, key, canonical, next, at); err != nil {
			return err
		}
		isNew := next == "completed" && previous != "completed"
		for _, alias := range all {
			if _, err = tx.Exec(`INSERT OR IGNORE INTO context_compaction_aliases VALUES(?,?,?,?)`, key, alias, canonical, at); err != nil {
				return err
			}
		}
		if isNew {
			if _, err = tx.Exec(`UPDATE context_observation_scopes SET invalidated_at=?,as_of=? WHERE scope=?`, at, at, key); err != nil {
				return err
			}
		}
	} else if usage != nil {
		result, e := tx.Exec(`INSERT OR IGNORE INTO context_usage_seen VALUES(?,?)`, key, id)
		if e != nil {
			return e
		}
		n, _ := result.RowsAffected()
		if n > 0 {
			var lastReport int64
			if e = tx.QueryRow(`SELECT usage_at FROM context_observation_scopes WHERE scope=?`, key).Scan(&lastReport); e != nil {
				return e
			}
			if usage.ReportedAtMS != nil && *usage.ReportedAtMS < lastReport {
				return tx.Commit()
			}
			raw, e := json.Marshal(usage)
			if e != nil {
				return e
			}
			reported := at
			if usage.ReportedAtMS != nil {
				reported = *usage.ReportedAtMS
			}
			if _, err = tx.Exec(`UPDATE context_observation_scopes SET usage=?,usage_at=?,usage_source_revision=?,invalidated_at=0,as_of=? WHERE scope=?`, raw, reported, c.SourceRevision, at, key); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

type ContextRecord struct {
	TrackedSince, AsOf, UsageAt, InvalidatedAt int64
	Completed                                  int
	UsageRevision                              uint64
	Usage                                      *protocol.ContextUsageObservation
}

func (s *Store) ContextObservation(c ContextScope) (ContextRecord, bool, error) {
	var v ContextRecord
	key, err := c.key()
	if err != nil {
		return v, false, err
	}
	var raw []byte
	tx, err := s.db.Begin()
	if err != nil {
		return v, false, err
	}
	defer tx.Rollback()
	err = tx.QueryRow(`SELECT tracked_since,as_of,usage,usage_at,usage_source_revision,invalidated_at FROM context_observation_scopes WHERE scope=?`, key).Scan(&v.TrackedSince, &v.AsOf, &raw, &v.UsageAt, &v.UsageRevision, &v.InvalidatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return v, false, nil
	}
	if err != nil {
		return v, false, err
	}
	if len(raw) > 0 {
		var u protocol.ContextUsageObservation
		if err = json.Unmarshal(raw, &u); err != nil {
			return v, false, fmt.Errorf("context usage unavailable: %w", err)
		}
		v.Usage = &u
	}
	err = tx.QueryRow(`SELECT count(*) FROM context_compaction_operations WHERE scope=? AND state='completed'`, key).Scan(&v.Completed)
	if err != nil {
		return v, false, err
	}
	err = tx.Commit()
	return v, true, err
}
