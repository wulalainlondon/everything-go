package messagequeue

// Overview metadata is a separate projection/annotation store. It never mutates
// queue admissions, native execution, delivery, QA, or attachment acknowledgments.
import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
	"strings"
	"time"
)

type OverviewRecord struct {
	ID        string          `json:"id"`
	Revision  uint64          `json:"revision"`
	UpdatedAt int64           `json:"updated_at_ms"`
	Payload   json.RawMessage `json:"payload"`
}
type OverviewReceipt struct {
	Operation     string            `json:"operation"`
	IntentID      string            `json:"intent_id"`
	InputHash     string            `json:"input_hash"`
	Phase         string            `json:"phase"`
	KnownNoEffect bool              `json:"known_no_effect,omitempty"`
	Error         map[string]string `json:"error,omitempty"`
	Revision      uint64            `json:"revision"`
	Record        OverviewRecord    `json:"record"`
}
type OverviewAsset struct {
	ID               string
	SHA256           string
	MIME             string
	Width, Height    int
	Bytes, Thumbnail []byte
}

func (s *Store) installOverview() error {
	_, e := s.db.Exec(`CREATE TABLE IF NOT EXISTS jarvis_overview_records(scope TEXT NOT NULL,kind TEXT NOT NULL,id TEXT NOT NULL,revision INTEGER NOT NULL,updated INTEGER NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(scope,kind,id)); CREATE TABLE IF NOT EXISTS jarvis_overview_ops(scope TEXT NOT NULL,intent TEXT NOT NULL,hash TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(scope,intent)); CREATE TABLE IF NOT EXISTS jarvis_overview_assets(scope TEXT NOT NULL,id TEXT NOT NULL,sha TEXT NOT NULL,mime TEXT NOT NULL,width INTEGER NOT NULL,height INTEGER NOT NULL,bytes BLOB NOT NULL,thumbnail BLOB NOT NULL,PRIMARY KEY(scope,id));`)
	return e
}
func (s *Store) OverviewRecord(ctx context.Context, scope, kind, id string) (OverviewRecord, error) {
	var v OverviewRecord
	e := taskapi.ReadSQL(ctx, s.db).QueryRowContext(ctx, `SELECT id,revision,updated,payload FROM jarvis_overview_records WHERE scope=? AND kind=? AND id=?`, scope, kind, id).Scan(&v.ID, &v.Revision, &v.UpdatedAt, &v.Payload)
	return v, e
}
func (s *Store) OverviewRecords(ctx context.Context, scope, kind string) ([]OverviewRecord, error) {
	rows, e := taskapi.ReadSQL(ctx, s.db).QueryContext(ctx, `SELECT id,revision,updated,payload FROM jarvis_overview_records WHERE scope=? AND kind=? ORDER BY id`, scope, kind)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []OverviewRecord{}
	for rows.Next() {
		var v OverviewRecord
		if e = rows.Scan(&v.ID, &v.Revision, &v.UpdatedAt, &v.Payload); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) OverviewOperation(ctx context.Context, scope, intent string) (OverviewReceipt, error) {
	var v OverviewReceipt
	var b []byte
	e := s.db.QueryRowContext(ctx, `SELECT payload FROM jarvis_overview_ops WHERE scope=? AND intent=?`, scope, intent).Scan(&b)
	if e == nil {
		e = json.Unmarshal(b, &v)
	}
	return v, e
}
func (s *Store) MutateOverview(ctx context.Context, scope, kind, id, intent, operation, hash string, expected uint64, payload json.RawMessage, asset *OverviewAsset) (OverviewReceipt, error) {
	var out OverviewReceipt
	if scope == "" || id == "" || intent == "" || len(hash) != 64 || len(payload) > 128<<10 {
		return out, errors.New("invalid_overview_metadata")
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return out, e
	}
	defer tx.Rollback()
	var oldHash string
	var raw []byte
	e = tx.QueryRowContext(ctx, `SELECT hash,payload FROM jarvis_overview_ops WHERE scope=? AND intent=?`, scope, intent).Scan(&oldHash, &raw)
	if e == nil {
		if oldHash != hash {
			return out, ErrWorkConflict
		}
		e = json.Unmarshal(raw, &out)
		return out, e
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return out, e
	}
	var revision uint64
	e = tx.QueryRowContext(ctx, `SELECT revision FROM jarvis_overview_records WHERE scope=? AND kind=? AND id=?`, scope, kind, id).Scan(&revision)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return out, e
	}
	if revision != expected {
		return out, ErrWorkConflict
	}
	var metaBytes int64
	if e = tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT SUM(length(payload)) FROM jarvis_overview_records WHERE scope=?),0)+COALESCE((SELECT SUM(length(payload)) FROM jarvis_overview_ops WHERE scope=?),0)`, scope, scope).Scan(&metaBytes); e != nil {
		return out, e
	}
	if metaBytes > 64<<20 {
		return out, errors.New("metadata_capacity_originals_preserved")
	}
	var used int64
	if e = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(length(bytes)+length(thumbnail)),0) FROM jarvis_overview_assets WHERE scope=?`, scope).Scan(&used); e != nil {
		return out, e
	}
	if asset != nil {
		if used+int64(len(asset.Bytes)+len(asset.Thumbnail)) > 128<<20 {
			return out, errors.New("storage_capacity_originals_preserved")
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_overview_assets VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(scope,id) DO NOTHING`, scope, asset.ID, asset.SHA256, asset.MIME, asset.Width, asset.Height, asset.Bytes, asset.Thumbnail); e != nil {
			return out, e
		}
	}
	out = OverviewReceipt{Operation: operation, IntentID: intent, InputHash: hash, Phase: "applied", Revision: revision + 1, Record: OverviewRecord{ID: id, Revision: revision + 1, UpdatedAt: time.Now().UnixMilli(), Payload: payload}}
	if kind == "collection" {
		var object map[string]any
		if e = json.Unmarshal(payload, &object); e != nil {
			return out, e
		}
		object["revision"] = out.Revision
		object["updated_at_ms"] = out.Record.UpdatedAt
		payload, _ = json.Marshal(object)
		out.Record.Payload = payload
	}
	raw, _ = json.Marshal(out)
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_overview_records VALUES(?,?,?,?,?,?) ON CONFLICT(scope,kind,id) DO UPDATE SET revision=excluded.revision,updated=excluded.updated,payload=excluded.payload`, scope, kind, id, out.Revision, out.Record.UpdatedAt, payload); e != nil {
		return out, e
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_overview_ops VALUES(?,?,?,?)`, scope, intent, hash, raw); e != nil {
		return out, e
	}
	if e = taskapi.ChangeTx(tx, "overview", intent, "jarvis_overview_metadata"); e != nil {
		return out, e
	}
	return out, tx.Commit()
}
func (s *Store) OverviewAsset(ctx context.Context, scope, id string) (OverviewAsset, error) {
	var v OverviewAsset
	e := s.db.QueryRowContext(ctx, `SELECT id,sha,mime,width,height,bytes,thumbnail FROM jarvis_overview_assets WHERE scope=? AND id=?`, scope, id).Scan(&v.ID, &v.SHA256, &v.MIME, &v.Width, &v.Height, &v.Bytes, &v.Thumbnail)
	return v, e
}

// A human decision and its exact overview intent receipt commit together. It is
// metadata only; no call into queue enqueue/start/send or native runtime occurs.
func (s *Store) RecordOverviewDecision(ctx context.Context, scope, manager, id, intent, operation, hash string, expected uint64, decision WorkDecision, source json.RawMessage) (OverviewReceipt, error) {
	var out OverviewReceipt
	if len(intent) < 10 || len(intent) > 90 || !strings.HasPrefix(intent, "r_") || len(hash) != 64 {
		return out, errors.New("invalid_intent")
	}
	switch decision.State {
	case "pending", "approved", "declined", "deferred":
	default:
		return out, errors.New("invalid_decision")
	}
	if decision.State != "pending" && (decision.Instruction == "" || decision.SourceRef == "") {
		return out, errors.New("explicit_decision_required")
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return out, e
	}
	defer tx.Rollback()
	var oldHash string
	var raw []byte
	e = tx.QueryRowContext(ctx, `SELECT hash,payload FROM jarvis_overview_ops WHERE scope=? AND intent=?`, scope, intent).Scan(&oldHash, &raw)
	if e == nil {
		if oldHash != hash {
			return out, ErrWorkConflict
		}
		e = json.Unmarshal(raw, &out)
		return out, e
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return out, e
	}
	var revision uint64
	e = tx.QueryRowContext(ctx, `SELECT revision,payload FROM jarvis_work_items WHERE scope=? AND manager=? AND item_id=?`, scope, manager, id).Scan(&revision, &raw)
	if e != nil {
		return out, e
	}
	if revision != expected {
		return out, ErrWorkConflict
	}
	var item WorkItem
	if json.Unmarshal(raw, &item) != nil {
		return out, errors.New("corrupt_metadata")
	}
	item.Decision = &decision
	item.Revision++
	item.UpdatedAt = time.Now().UnixMilli()
	raw, _ = json.Marshal(item)
	if _, e = tx.ExecContext(ctx, `UPDATE jarvis_work_items SET revision=?,payload=?,updated_at=? WHERE scope=? AND manager=? AND item_id=?`, item.Revision, raw, item.UpdatedAt, scope, manager, id); e != nil {
		return out, e
	}
	wrapped, _ := json.Marshal(map[string]any{"item": item, "source": source})
	out = OverviewReceipt{Operation: operation, IntentID: intent, InputHash: hash, Phase: "applied", Revision: item.Revision, Record: OverviewRecord{ID: manager + ":" + id, Revision: item.Revision, UpdatedAt: item.UpdatedAt, Payload: wrapped}}
	receipt, _ := json.Marshal(out)
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_overview_ops VALUES(?,?,?,?)`, scope, intent, hash, receipt); e != nil {
		return out, e
	}
	if e = taskapi.ChangeTx(tx, manager, intent, "jarvis_overview_decision"); e != nil {
		return out, e
	}
	return out, tx.Commit()
}

// Rejections are durable only after a known pre-effect refusal. Commit/storage
// uncertainty never calls this method and cannot be turned into a new intent.
func (s *Store) RejectOverviewIntent(ctx context.Context, scope, intent, operation, hash, code string) (OverviewReceipt, error) {
	out := OverviewReceipt{Operation: operation, IntentID: intent, InputHash: hash, Phase: "rejected", KnownNoEffect: true, Error: map[string]string{"code": code}}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return out, e
	}
	defer tx.Rollback()
	var raw []byte
	var oldHash string
	e = tx.QueryRowContext(ctx, `SELECT hash,payload FROM jarvis_overview_ops WHERE scope=? AND intent=?`, scope, intent).Scan(&oldHash, &raw)
	if e == nil {
		if oldHash != hash {
			return out, ErrWorkConflict
		}
		e = json.Unmarshal(raw, &out)
		return out, e
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return out, e
	}
	raw, _ = json.Marshal(out)
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_overview_ops VALUES(?,?,?,?)`, scope, intent, hash, raw); e != nil {
		return out, e
	}
	if e = taskapi.ChangeTx(tx, "overview", intent, "jarvis_overview_rejected"); e != nil {
		return out, e
	}
	return out, tx.Commit()
}
