package automation

// Transport operation receipts live beside the original automation outbox. They
// do not own execution, task state, or provider effects.
import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type WorkbenchOperation struct {
	IntentID      string          `json:"intent_id"`
	Kind          string          `json:"kind"`
	Device        string          `json:"-"`
	InputHash     string          `json:"input_hash"`
	Status        string          `json:"status"`
	SessionID     string          `json:"session_id,omitempty"`
	Revision      uint64          `json:"commit_revision"`
	Input         json.RawMessage `json:"input"`
	AcceptedAtMS  int64           `json:"accepted_at_ms"`
	CommittedAtMS int64           `json:"committed_at_ms"`
}

func (s *Store) installWorkbench(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS jarvis_operations(kind TEXT NOT NULL,intent TEXT NOT NULL,device TEXT NOT NULL,hash TEXT NOT NULL,status TEXT NOT NULL,session TEXT NOT NULL,revision INTEGER NOT NULL,input BLOB NOT NULL,accepted_at_ms INTEGER NOT NULL,committed_at_ms INTEGER NOT NULL,PRIMARY KEY(kind,intent));
 CREATE UNIQUE INDEX IF NOT EXISTS jarvis_created_session ON jarvis_operations(session) WHERE kind='create';
 CREATE TABLE IF NOT EXISTS jarvis_resolution_heads(event_set TEXT PRIMARY KEY,revision INTEGER NOT NULL);`)
	return err
}
func scanWorkbench(row interface{ Scan(...any) error }) (WorkbenchOperation, error) {
	var o WorkbenchOperation
	err := row.Scan(&o.Kind, &o.IntentID, &o.Device, &o.InputHash, &o.Status, &o.SessionID, &o.Revision, &o.Input, &o.AcceptedAtMS, &o.CommittedAtMS)
	return o, err
}
func (s *Store) WorkbenchOperation(ctx context.Context, kind, intent, device string) (WorkbenchOperation, error) {
	if err := s.installWorkbench(ctx); err != nil {
		return WorkbenchOperation{}, err
	}
	return scanWorkbench(s.db.QueryRowContext(ctx, `SELECT kind,intent,device,hash,status,session,revision,input,accepted_at_ms,committed_at_ms FROM jarvis_operations WHERE kind=? AND intent=? AND device=?`, kind, intent, device))
}

// Claim or read back the immutable operation before any registry effect.
func (s *Store) ClaimWorkbench(ctx context.Context, o WorkbenchOperation) (WorkbenchOperation, error) {
	if err := s.installWorkbench(ctx); err != nil {
		return o, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return o, err
	}
	defer tx.Rollback()
	old, err := scanWorkbench(tx.QueryRowContext(ctx, `SELECT kind,intent,device,hash,status,session,revision,input,accepted_at_ms,committed_at_ms FROM jarvis_operations WHERE kind=? AND intent=?`, o.Kind, o.IntentID))
	if err == nil {
		if old.Device != o.Device || old.InputHash != o.InputHash {
			return o, ErrConflict
		}
		return old, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return o, err
	}
	if o.Kind == "create" {
		var other string
		err = tx.QueryRowContext(ctx, `SELECT intent FROM jarvis_operations WHERE kind='create' AND session=?`, o.SessionID).Scan(&other)
		if err == nil {
			return o, ErrConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return o, err
		}
	}
	o.Status = "accepted"
	o.AcceptedAtMS = time.Now().UnixMilli()
	_, err = tx.ExecContext(ctx, `INSERT INTO jarvis_operations VALUES(?,?,?,?,?,?,?,?,?,?)`, o.Kind, o.IntentID, o.Device, o.InputHash, o.Status, o.SessionID, o.Revision, []byte(o.Input), o.AcceptedAtMS, o.CommittedAtMS)
	if err != nil {
		return o, err
	}
	return o, tx.Commit()
}
func (s *Store) CommitWorkbench(ctx context.Context, o WorkbenchOperation) (WorkbenchOperation, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE jarvis_operations SET status='committed',revision=1,committed_at_ms=? WHERE kind=? AND intent=? AND device=? AND hash=? AND status='accepted'`, time.Now().UnixMilli(), o.Kind, o.IntentID, o.Device, o.InputHash)
	if err != nil {
		return o, err
	}
	if _, err = result.RowsAffected(); err != nil {
		return o, err
	}
	return s.WorkbenchOperation(ctx, o.Kind, o.IntentID, o.Device)
}

// Resolution is a human-only compare-and-swap receipt; it never sends a message.
func (s *Store) ResolveWorkbench(ctx context.Context, o WorkbenchOperation, eventSet string, expected uint64) (WorkbenchOperation, error) {
	if err := s.installWorkbench(ctx); err != nil {
		return o, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return o, err
	}
	defer tx.Rollback()
	old, err := scanWorkbench(tx.QueryRowContext(ctx, `SELECT kind,intent,device,hash,status,session,revision,input,accepted_at_ms,committed_at_ms FROM jarvis_operations WHERE kind='resolution' AND intent=?`, o.IntentID))
	if err == nil {
		if old.Device != o.Device || old.InputHash != o.InputHash {
			return o, ErrConflict
		}
		return old, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return o, err
	}
	var rev uint64
	err = tx.QueryRowContext(ctx, `SELECT revision FROM jarvis_resolution_heads WHERE event_set=?`, eventSet).Scan(&rev)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return o, err
	}
	if rev != expected {
		return o, ErrConflict
	}
	o.Kind = "resolution"
	o.Status = "committed"
	o.Revision = rev + 1
	o.AcceptedAtMS = time.Now().UnixMilli()
	o.CommittedAtMS = o.AcceptedAtMS
	_, err = tx.ExecContext(ctx, `INSERT INTO jarvis_operations VALUES(?,?,?,?,?,?,?,?,?,?)`, o.Kind, o.IntentID, o.Device, o.InputHash, o.Status, "", o.Revision, []byte(o.Input), o.AcceptedAtMS, o.CommittedAtMS)
	if err != nil {
		return o, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO jarvis_resolution_heads VALUES(?,?) ON CONFLICT(event_set) DO UPDATE SET revision=excluded.revision`, eventSet, o.Revision)
	if err != nil {
		return o, err
	}
	return o, tx.Commit()
}
func (s *Store) EventAssignment(ctx context.Context, id string) (EventAssignment, error) {
	var a EventAssignment
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload,content FROM event_session_assignments WHERE id=?`, id).Scan(&raw, &a.Content)
	if err != nil {
		return a, err
	}
	err = decodeStoredAssignment(raw, &a)
	return a, err
}

func (s *Store) WorkbenchCreationForSession(ctx context.Context, session string) (WorkbenchOperation, error) {
	if err := s.installWorkbench(ctx); err != nil {
		return WorkbenchOperation{}, err
	}
	return scanWorkbench(s.db.QueryRowContext(ctx, `SELECT kind,intent,device,hash,status,session,revision,input,accepted_at_ms,committed_at_ms FROM jarvis_operations WHERE kind='create' AND session=?`, session))
}
func (s *Store) CurrentEventAssignment(ctx context.Context, source, event string) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT assignment_id FROM event_session_owners WHERE event_id=?`, source+"\x00"+event).Scan(&id)
	return id, err
}

func (s *Store) WorkbenchResolutionForAssignment(ctx context.Context, assignment, device string) (WorkbenchOperation, error) {
	if err := s.installWorkbench(ctx); err != nil {
		return WorkbenchOperation{}, err
	}
	return scanWorkbench(s.db.QueryRowContext(ctx, `SELECT kind,intent,device,hash,status,session,revision,input,accepted_at_ms,committed_at_ms FROM jarvis_operations WHERE kind='resolution' AND device=? AND status='committed' AND json_extract(input,'$.assignment_id')=? ORDER BY committed_at_ms DESC LIMIT 1`, device, assignment))
}

func (s *Store) WorkbenchResolutionRevision(ctx context.Context, eventSet string) (uint64, error) {
	if err := s.installWorkbench(ctx); err != nil {
		return 0, err
	}
	var rev uint64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE((SELECT revision FROM jarvis_resolution_heads WHERE event_set=?),0)`, eventSet).Scan(&rev)
	return rev, err
}
