package messagequeue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type ContextReset struct {
	IntentID            string `json:"intent_id"`
	Scope               string `json:"-"`
	SessionID           string `json:"session_id"`
	ThreadID            string `json:"thread_id"`
	SourceRevision      uint64 `json:"source_revision"`
	ConfigRevision      uint64 `json:"config_revision"`
	Mode                string `json:"mode"`
	BoundaryHash        string `json:"boundary_hash"`
	ArchiveHash         string `json:"archive_hash"`
	Handoff             string `json:"handoff"`
	HandoffHash         string `json:"handoff_hash"`
	HandoffRevision     uint64 `json:"handoff_revision"`
	HandoffBoundaryHash string `json:"handoff_boundary_hash,omitempty"`
	SeedRequestID       string `json:"seed_request_id,omitempty"`
	Phase               string `json:"phase"`
	Revision            uint64 `json:"revision"`
	Error               string `json:"error,omitempty"`
	CreatedAt           int64  `json:"created_at_ms"`
	UpdatedAt           int64  `json:"updated_at_ms"`
	BindingRevision     uint64 `json:"binding_revision,omitempty"`
}

func (s *Store) installContextReset() error {
	_, e := s.db.Exec(`CREATE TABLE IF NOT EXISTS jarvis_context_resets(scope TEXT NOT NULL,intent TEXT NOT NULL,session_id TEXT NOT NULL,input_hash TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(scope,intent));CREATE TABLE IF NOT EXISTS jarvis_context_archives(scope TEXT NOT NULL,intent TEXT NOT NULL,digest TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(scope,intent));CREATE TABLE IF NOT EXISTS jarvis_context_barriers(session_id TEXT PRIMARY KEY,intent TEXT NOT NULL,scope TEXT NOT NULL,active INTEGER NOT NULL);`)
	return e
}
func (s *Store) SaveContextReset(ctx context.Context, scope, inputHash string, in ContextReset, archive json.RawMessage) (ContextReset, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return in, e
	}
	defer tx.Rollback()
	var hash string
	var raw []byte
	e = tx.QueryRowContext(ctx, `SELECT input_hash,payload FROM jarvis_context_resets WHERE scope=? AND intent=?`, scope, in.IntentID).Scan(&hash, &raw)
	if e == nil {
		if hash != inputHash {
			return in, ErrWorkConflict
		}
		e = json.Unmarshal(raw, &in)
		return in, e
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return in, e
	}
	if len(archive) == 0 || len(archive) > 64<<20 || workHash(json.RawMessage(archive)) != in.ArchiveHash {
		return in, errors.New("archive_digest_or_limit_invalid")
	}
	in.Scope = scope
	in.Phase = "prepared"
	in.Revision = 1
	in.CreatedAt = time.Now().UnixMilli()
	in.UpdatedAt = in.CreatedAt
	raw, _ = json.Marshal(in)
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_context_resets VALUES(?,?,?,?,?)`, scope, in.IntentID, in.SessionID, inputHash, raw); e != nil {
		return in, e
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_context_archives VALUES(?,?,?,?)`, scope, in.IntentID, in.ArchiveHash, archive); e != nil {
		return in, e
	}
	if e = tx.Commit(); e != nil {
		return in, e
	}
	return s.ContextReset(ctx, scope, in.IntentID)
}
func (s *Store) ContextReset(ctx context.Context, scope, id string) (ContextReset, error) {
	var v ContextReset
	var raw []byte
	e := s.db.QueryRowContext(ctx, `SELECT payload FROM jarvis_context_resets WHERE scope=? AND intent=?`, scope, id).Scan(&raw)
	if e == nil {
		e = json.Unmarshal(raw, &v)
	}
	v.Scope = scope
	return v, e
}
func (s *Store) ContextArchive(ctx context.Context, scope, id string) (json.RawMessage, string, error) {
	var raw []byte
	var hash string
	e := s.db.QueryRowContext(ctx, `SELECT digest,payload FROM jarvis_context_archives WHERE scope=? AND intent=?`, scope, id).Scan(&hash, &raw)
	if e == nil && workHash(json.RawMessage(raw)) != hash {
		e = errors.New("archive_readback_digest_mismatch")
	}
	return raw, hash, e
}
func (s *Store) ReserveContextReset(ctx context.Context, scope, id string, revision uint64) (ContextReset, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return ContextReset{}, e
	}
	defer tx.Rollback()
	var raw []byte
	if e = tx.QueryRowContext(ctx, `SELECT payload FROM jarvis_context_resets WHERE scope=? AND intent=?`, scope, id).Scan(&raw); e != nil {
		return ContextReset{}, e
	}
	var v ContextReset
	if e = json.Unmarshal(raw, &v); e != nil {
		return v, e
	}
	if v.Phase != "prepared" || v.Revision != revision {
		return v, ErrWorkConflict
	}
	var count int
	if e = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM queue_commands WHERE session_id=? AND state IN ('queued','running','uncertain','steering')`, v.SessionID).Scan(&count); e != nil || count != 0 {
		return v, errors.New("context_queue_not_idle")
	}
	var prior string
	e = tx.QueryRowContext(ctx, `SELECT intent FROM jarvis_context_barriers WHERE session_id=? AND active=1`, v.SessionID).Scan(&prior)
	if e == nil {
		return v, errors.New("context_generation_already_reserved")
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return v, e
	}
	v.Phase = "reserved"
	v.Revision++
	v.UpdatedAt = time.Now().UnixMilli()
	raw, _ = json.Marshal(v)
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_context_barriers VALUES(?,?,?,1) ON CONFLICT(session_id) DO UPDATE SET intent=excluded.intent,scope=excluded.scope,active=1`, v.SessionID, id, scope); e != nil {
		return v, e
	}
	if _, e = tx.ExecContext(ctx, `UPDATE jarvis_context_resets SET payload=? WHERE scope=? AND intent=?`, raw, scope, id); e != nil {
		return v, e
	}
	return v, tx.Commit()
}
func (s *Store) ContextResetPhase(ctx context.Context, scope, id, from, to, reason string) (ContextReset, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return ContextReset{}, e
	}
	defer tx.Rollback()
	var raw []byte
	var v ContextReset
	if e = tx.QueryRowContext(ctx, `SELECT payload FROM jarvis_context_resets WHERE scope=? AND intent=?`, scope, id).Scan(&raw); e != nil {
		return v, e
	}
	if e = json.Unmarshal(raw, &v); e != nil {
		return v, e
	}
	if v.Phase != from {
		return v, ErrWorkConflict
	}
	v.Phase = to
	v.Error = reason
	v.Revision++
	v.UpdatedAt = time.Now().UnixMilli()
	raw, _ = json.Marshal(v)
	if _, e = tx.ExecContext(ctx, `UPDATE jarvis_context_resets SET payload=? WHERE scope=? AND intent=?`, raw, scope, id); e != nil {
		return v, e
	}
	if to == "completed" || to == "blocked" || to == "awaiting_seed" {
		if _, e = tx.ExecContext(ctx, `UPDATE jarvis_context_barriers SET active=0 WHERE session_id=? AND intent=? AND scope=?`, v.SessionID, id, scope); e != nil {
			return v, e
		}
	}
	return v, tx.Commit()
}
func contextBarrierTx(tx *sql.Tx, session string) error {
	var id string
	e := tx.QueryRow(`SELECT intent FROM jarvis_context_barriers WHERE session_id=? AND active=1`, session).Scan(&id)
	if errors.Is(e, sql.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	return errors.New("context_generation_reserved_lookup_original")
}
func (s *Store) ContextBarrier(ctx context.Context, session string) (bool, error) {
	var n int
	e := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jarvis_context_barriers WHERE session_id=? AND active=1`, session).Scan(&n)
	return n > 0, e
}

// A pre-effect rejection is durable under the same original reset intent. It
// cannot race a prepared receipt into a false known-none outcome.
func (s *Store) RejectContextReset(ctx context.Context, scope, inputHash string, in ContextReset, reason string) (ContextReset, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return in, e
	}
	defer tx.Rollback()
	var oldHash string
	var raw []byte
	e = tx.QueryRowContext(ctx, `SELECT input_hash,payload FROM jarvis_context_resets WHERE scope=? AND intent=?`, scope, in.IntentID).Scan(&oldHash, &raw)
	if e == nil {
		if oldHash != inputHash {
			return in, ErrWorkConflict
		}
		e = json.Unmarshal(raw, &in)
		return in, e
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return in, e
	}
	in.Phase = "rejected"
	in.Error = reason
	in.Revision = 1
	in.CreatedAt = time.Now().UnixMilli()
	in.UpdatedAt = in.CreatedAt
	raw, _ = json.Marshal(in)
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_context_resets VALUES(?,?,?,?,?)`, scope, in.IntentID, in.SessionID, inputHash, raw); e != nil {
		return in, e
	}
	return in, tx.Commit()
}
