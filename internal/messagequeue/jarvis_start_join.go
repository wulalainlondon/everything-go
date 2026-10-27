package messagequeue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
)

// These progress rows live next to the original facts/outbox. No fact is pruned
// or silently lost because it was outside a clock window or a latest-N query.
func (s *Store) installStartJoin() error {
	_, e := s.db.Exec(`CREATE TABLE IF NOT EXISTS jarvis_start_join_rotation(id INTEGER PRIMARY KEY,last_sub INTEGER NOT NULL);INSERT OR IGNORE INTO jarvis_start_join_rotation VALUES(1,0);CREATE TABLE IF NOT EXISTS jarvis_start_join_progress(scope TEXT NOT NULL,manager TEXT NOT NULL,policy_hash TEXT NOT NULL,last_seq INTEGER NOT NULL,PRIMARY KEY(scope,manager,policy_hash));`)
	return e
}
func (s *Store) StartJoinSubscribers(ctx context.Context, limit int) ([]NoticeSubscriber, error) {
	if limit < 1 || limit > 100 {
		limit = 20
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	var after int64
	if e = tx.QueryRowContext(ctx, `SELECT last_sub FROM jarvis_start_join_rotation WHERE id=1`).Scan(&after); e != nil {
		return nil, e
	}
	rows, e := tx.QueryContext(ctx, `SELECT rowid,scope,manager,input,enabled FROM jarvis_notice_subscribers WHERE enabled=1 AND rowid>? ORDER BY rowid LIMIT ?`, after, limit)
	if e != nil {
		return nil, e
	}
	out := []NoticeSubscriber{}
	last := int64(0)
	for rows.Next() {
		var sub NoticeSubscriber
		if e = rows.Scan(&last, &sub.Scope, &sub.Manager, &sub.Input, &sub.Enabled); e != nil {
			rows.Close()
			return nil, e
		}
		out = append(out, sub)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	if _, e = tx.ExecContext(ctx, `UPDATE jarvis_start_join_rotation SET last_sub=? WHERE id=1`, last); e != nil {
		return nil, e
	}
	return out, tx.Commit()
}

type SequencedNativeStart struct {
	Sequence int64
	Start    NativeStart
}

func (s *Store) StartJoinFacts(ctx context.Context, sub NoticeSubscriber, limit int) ([]SequencedNativeStart, error) {
	if limit < 1 || limit > 100 {
		limit = 100
	}
	var after int64
	e := s.db.QueryRowContext(ctx, `SELECT last_seq FROM jarvis_start_join_progress WHERE scope=? AND manager=? AND policy_hash=?`, sub.Scope, sub.Manager, workHash(sub.Input)).Scan(&after)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	rows, e := s.db.QueryContext(ctx, `SELECT rowid,payload FROM task_native_starts WHERE rowid>? ORDER BY rowid LIMIT ?`, after, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []SequencedNativeStart{}
	for rows.Next() {
		var p SequencedNativeStart
		var raw []byte
		if e = rows.Scan(&p.Sequence, &raw); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(raw, &p.Start); e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Store) CommitStartJoin(ctx context.Context, sub NoticeSubscriber, p SequencedNativeStart, outcome string) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var input []byte
	var enabled bool
	if e = tx.QueryRowContext(ctx, `SELECT input,enabled FROM jarvis_notice_subscribers WHERE scope=? AND manager=?`, sub.Scope, sub.Manager).Scan(&input, &enabled); e != nil {
		return e
	}
	if !enabled || string(input) != string(sub.Input) {
		return ErrWorkConflict
	}
	var previous int64
	e = tx.QueryRowContext(ctx, `SELECT last_seq FROM jarvis_start_join_progress WHERE scope=? AND manager=? AND policy_hash=?`, sub.Scope, sub.Manager, workHash(sub.Input)).Scan(&previous)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	if p.Sequence <= previous {
		return tx.Commit()
	}
	if outcome != "" {
		raw, _ := json.Marshal(p.Start)
		receipt, _ := json.Marshal(map[string]any{"state": outcome, "native_fact_sequence": p.Sequence, "device_received": "unknown", "displayed": "unknown"})
		result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO jarvis_start_push VALUES(?,?,?,?,?,?,?)`, sub.Scope, sub.Manager, p.Start.EventID, outcome, raw, p.Start.ObservedAt+180000, receipt)
		if err != nil {
			return err
		}
		if n, err := result.RowsAffected(); err != nil {
			return err
		} else if n > 0 {
			if err = taskapi.ChangeTx(tx, sub.Manager, p.Start.EventID, "jarvis_notice_outbox"); err != nil {
				return err
			}
		}

	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_start_join_progress VALUES(?,?,?,?) ON CONFLICT(scope,manager,policy_hash) DO UPDATE SET last_seq=excluded.last_seq`, sub.Scope, sub.Manager, workHash(sub.Input), p.Sequence); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) PreparedStartPushes(ctx context.Context) ([]StartPush, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT scope,manager,event_id,payload,expires FROM jarvis_start_push WHERE state='prepared' ORDER BY rowid LIMIT 100`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []StartPush{}
	for rows.Next() {
		var p StartPush
		if e = rows.Scan(&p.Scope, &p.Manager, &p.EventID, &p.Payload, &p.ExpiresAt); e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Store) RetirePreparedStartPush(ctx context.Context, p StartPush, state string) error {
	return s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		r, e := tx.ExecContext(ctx, `UPDATE jarvis_start_push SET state=?,receipt=? WHERE scope=? AND manager=? AND event_id=? AND state='prepared'`, state, []byte(`{"provider_sent":false}`), p.Scope, p.Manager, p.EventID)
		if e != nil {
			return e
		}
		if n, e := r.RowsAffected(); e != nil {
			return e
		} else if n > 0 {
			return taskapi.ChangeTx(tx, p.Manager, p.EventID, "jarvis_notice_retired")
		}
		return nil
	})
}

func (s *Store) HasWorkExecutionLink(ctx context.Context, scope, manager, session, request string) (bool, error) {
	var exists bool
	e := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM jarvis_work_items,json_each(jarvis_work_items.payload,'$.links') AS link WHERE scope=? AND manager=? AND json_extract(link.value,'$.kind')='intake' AND json_extract(link.value,'$.session_id')=? AND json_extract(link.value,'$.request_id')=?)`, scope, manager, session, request).Scan(&exists)
	return exists, e
}

// Receipt reads follow the selected exact execution links, not a latest-N
// window. Thus an old expired/unknown outcome remains inspectable by original ref.
func (s *Store) StartPushesForLinks(ctx context.Context, scope, manager string, links []WorkLink) ([]StartPush, error) {
	out := []StartPush{}
	seen := map[string]bool{}
	for _, l := range links {
		fact, ok, e := s.NativeStart(l.SessionID, l.RequestID)
		if e != nil {
			return nil, e
		}
		if !ok || seen[fact.EventID] {
			continue
		}
		seen[fact.EventID] = true
		p := StartPush{Scope: scope, Manager: manager, EventID: fact.EventID}
		e = s.db.QueryRowContext(ctx, `SELECT state,payload,expires,receipt FROM jarvis_start_push WHERE scope=? AND manager=? AND event_id=?`, scope, manager, fact.EventID).Scan(&p.Status, &p.Payload, &p.ExpiresAt, &p.Receipt)
		if errors.Is(e, sql.ErrNoRows) {
			continue
		}
		if e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return out, nil
}

// Return exact retained links without a presentation page/preview bound.
func (s *Store) WorkExecutionLinks(ctx context.Context, scope, manager, session, request string) ([]WorkLink, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT link.value FROM jarvis_work_items,json_each(jarvis_work_items.payload,'$.links') AS link WHERE scope=? AND manager=? AND json_extract(link.value,'$.session_id')=? AND json_extract(link.value,'$.request_id')=?`, scope, manager, session, request)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []WorkLink{}
	for rows.Next() {
		var raw string
		var link WorkLink
		if e = rows.Scan(&raw); e != nil {
			return nil, e
		}
		if e = json.Unmarshal([]byte(raw), &link); e != nil {
			return nil, e
		}
		out = append(out, link)
	}
	return out, rows.Err()
}
