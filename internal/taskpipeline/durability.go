package taskpipeline

// The delivery ledger is visual metadata only. Inbox commit does not mean a
// device displayed it, a person read it, or a provider consumed it.
import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
	"os"
)

type Notification struct {
	ID                 string   `json:"id"`
	RecordID           string   `json:"record_id"`
	DefinitionRevision uint64   `json:"definition_revision"`
	EventID            string   `json:"event_id"`
	Destination        Identity `json:"destination"`
	Channel            string   `json:"channel"`
	State              string   `json:"state"`
	Revision           uint64   `json:"revision"`
	Attempt            uint64   `json:"attempt"`
	Reason             string   `json:"reason"`
	CreatedAt          int64    `json:"created_at"`
	UpdatedAt          int64    `json:"updated_at"`
	Fact               Fact     `json:"fact"`
}
type SourceCursor struct {
	Key                string  `json:"key"`
	RecordID           string  `json:"record_id"`
	DefinitionRevision uint64  `json:"definition_revision"`
	NodeID             string  `json:"node_id"`
	Source             Locator `json:"source"`
	Store              string  `json:"store"`
	StoreIdentity      string  `json:"store_identity"`
	ScopeHash          string  `json:"scope_hash"`
	After              uint64  `json:"after_sequence"`
	Watermark          uint64  `json:"observed_watermark"`
	Floor              uint64  `json:"retention_floor"`
	State              string  `json:"state"`
	Reason             string  `json:"reason"`
	FactHash           string  `json:"fact_hash"`
	Revision           uint64  `json:"revision"`
	UpdatedAt          int64   `json:"updated_at"`
}
type TriggerPolicy struct {
	Kind             string   `json:"kind"`
	Revision         uint64   `json:"revision"`
	Enabled          bool     `json:"enabled"`
	OperatorDisabled bool     `json:"operator_disabled"`
	EffectiveEnabled bool     `json:"effective_enabled"`
	Scope            Identity `json:"scope"`
}
type DeliveryReceipt struct {
	Kind         string         `json:"kind"`
	IntentID     string         `json:"intent_id"`
	InputHash    string         `json:"input_hash"`
	Operation    string         `json:"operation"`
	State        string         `json:"state"`
	Notification *Notification  `json:"notification"`
	Policy       *TriggerPolicy `json:"policy"`
}

func installDurability(db *sql.DB) error {
	_, e := db.Exec(`
 CREATE TABLE IF NOT EXISTS task_pipeline_notifications(id TEXT PRIMARY KEY,record_id TEXT NOT NULL,state TEXT NOT NULL,notification BLOB NOT NULL);
 CREATE INDEX IF NOT EXISTS task_pipeline_notifications_record ON task_pipeline_notifications(record_id,id);
 CREATE TABLE IF NOT EXISTS task_pipeline_visual_inbox(notification_id TEXT PRIMARY KEY,record_id TEXT NOT NULL,committed_at INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS task_pipeline_source_cursors(key TEXT PRIMARY KEY,record_id TEXT NOT NULL,cursor BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS task_pipeline_trigger_policies(scope TEXT NOT NULL,manager TEXT NOT NULL,revision INTEGER NOT NULL,enabled INTEGER NOT NULL,PRIMARY KEY(scope,manager));
 CREATE TABLE IF NOT EXISTS task_pipeline_delivery_operations(scope TEXT NOT NULL,manager TEXT NOT NULL,intent TEXT NOT NULL,operation TEXT NOT NULL,input_hash TEXT NOT NULL,receipt BLOB NOT NULL,PRIMARY KEY(scope,manager,intent));
 CREATE TABLE IF NOT EXISTS task_pipeline_event_retention(record_id TEXT PRIMARY KEY,floor INTEGER NOT NULL);
 `)
	return e
}
func OperatorDisabled() bool { return os.Getenv("BRIDGE_TASK_PIPELINE_DISABLE_NEW_TRIGGERS") == "1" }
func policy(ctx context.Context, q reader, scope Scope) (TriggerPolicy, error) {
	p := TriggerPolicy{Kind: "trigger_policy", Enabled: true, Scope: scope.Manager, OperatorDisabled: OperatorDisabled()}
	e := q.QueryRowContext(ctx, `SELECT revision,enabled FROM task_pipeline_trigger_policies WHERE scope=? AND manager=?`, ScopeKey(scope), ManagerKey(scope)).Scan(&p.Revision, &p.Enabled)
	if e != nil && e != sql.ErrNoRows {
		return p, e
	}
	p.EffectiveEnabled = p.Enabled && !p.OperatorDisabled
	return p, nil
}
func (s *Store) TriggerPolicy(ctx context.Context, scope Scope) (TriggerPolicy, error) {
	return policy(ctx, taskapi.ReadSQL(ctx, s.db), scope)
}
func triggersAllowed(ctx context.Context, q reader, scope Scope) error {
	p, e := policy(ctx, q, scope)
	if e != nil {
		return e
	}
	if !p.EffectiveEnabled {
		return errors.New("pipeline_triggers_disabled")
	}
	return nil
}
func (s *Store) TriggersAllowed(ctx context.Context, scope Scope) error {
	return triggersAllowed(ctx, taskapi.ReadSQL(ctx, s.db), scope)
}
func queueNotificationTx(ctx context.Context, tx *sql.Tx, r Record, ev Event) error {
	if r.Watch == nil || r.Watch.Mode != "notify_only" || (ev.State != "mission_applied" && ev.State != "evidence_verified") {
		return nil
	}
	if !PredicateMet(r.Watch.Predicate, ev.Fact) && !PredicateMet(Predicate{Kind: "terminal"}, ev.Fact) {
		return nil
	}
	n := Notification{ID: "pn_" + digest(struct {
		Record    string
		Version   uint64
		Source    Locator
		Predicate Predicate
	}{r.ID, r.DefinitionRevision, r.Watch.Source, r.Watch.Predicate}), RecordID: r.ID, DefinitionRevision: r.DefinitionRevision, EventID: ev.ID, Destination: r.Watch.Destination, Channel: "visual", State: "pending", Revision: 1, CreatedAt: now(), UpdatedAt: now(), Fact: ev.Fact}
	_, e := tx.ExecContext(ctx, `INSERT OR IGNORE INTO task_pipeline_notifications VALUES(?,?,?,?)`, n.ID, n.RecordID, n.State, encode(n))
	return e
}
func (s *Store) PendingNotifications(ctx context.Context, limit int) ([]Notification, error) {
	if limit < 1 || limit > 100 {
		limit = 32
	}
	rows, e := s.db.QueryContext(ctx, `SELECT notification FROM task_pipeline_notifications WHERE state='pending' ORDER BY id LIMIT ?`, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Notification{}
	for rows.Next() {
		var b []byte
		var n Notification
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &n); e != nil {
			return nil, e
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// DeliverNotification atomically commits the local visual inbox and outbox ACK.
// A crash cannot create an unqueryable issued visual effect. Retry reuses the
// original notification/event identity; it never invokes an actor/model writer.
func (s *Store) DeliverNotification(ctx context.Context, n Notification, check func(context.Context) error) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var raw []byte
	if e = tx.QueryRowContext(ctx, `SELECT notification FROM task_pipeline_notifications WHERE id=?`, n.ID).Scan(&raw); e != nil {
		return e
	}
	var cur Notification
	if e = json.Unmarshal(raw, &cur); e != nil {
		return e
	}
	if cur.State == "inbox_committed" || cur.State == "displayed" {
		return nil
	}
	if cur.Revision != n.Revision || cur.State != "pending" {
		return ErrConflict
	}
	if check == nil {
		return ErrScope
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO task_pipeline_visual_inbox VALUES(?,?,?)`, cur.ID, cur.RecordID, now()); e != nil {
		return e
	}
	cur.State = "inbox_committed"
	cur.Revision++
	cur.Attempt++
	cur.Reason = ""
	cur.UpdatedAt = now()
	if _, e = tx.ExecContext(ctx, `UPDATE task_pipeline_notifications SET state=?,notification=? WHERE id=?`, cur.State, encode(cur), cur.ID); e != nil {
		return e
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) NotificationFailed(ctx context.Context, n Notification, reason string) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var raw []byte
	if e = tx.QueryRowContext(ctx, `SELECT notification FROM task_pipeline_notifications WHERE id=? AND state='pending'`, n.ID).Scan(&raw); e != nil {
		return e
	}
	var cur Notification
	if e = json.Unmarshal(raw, &cur); e != nil {
		return e
	}
	if cur.Revision != n.Revision {
		return ErrConflict
	}
	cur.State = "failed"
	cur.Reason = reason
	cur.Revision++
	cur.Attempt++
	cur.UpdatedAt = now()
	_, e = tx.ExecContext(ctx, `UPDATE task_pipeline_notifications SET state=?,notification=? WHERE id=?`, cur.State, encode(cur), cur.ID)
	if e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) Notifications(ctx context.Context, scope Scope, id, after string, limit int) ([]Notification, string, error) {
	if _, _, found, e := s.Get(ctx, scope, id); e != nil || !found {
		if e == nil {
			e = ErrScope
		}
		return nil, "", e
	}
	if limit < 1 || limit > 100 {
		limit = 50
	}
	rows, e := s.db.QueryContext(ctx, `SELECT notification FROM task_pipeline_notifications WHERE record_id=? AND id>? ORDER BY id LIMIT ?`, id, after, limit+1)
	if e != nil {
		return nil, "", e
	}
	defer rows.Close()
	out := []Notification{}
	next := ""
	for rows.Next() {
		var b []byte
		var n Notification
		if e = rows.Scan(&b); e != nil {
			return nil, "", e
		}
		if e = json.Unmarshal(b, &n); e != nil {
			return nil, "", e
		}
		if len(out) == limit {
			next = out[len(out)-1].ID
			break
		}
		out = append(out, n)
	}
	return out, next, rows.Err()
}
func (s *Store) DeliveryOperation(ctx context.Context, scope Scope, intent, op, hash string) (DeliveryReceipt, bool, error) {
	var b []byte
	var savedOp, savedHash string
	e := s.db.QueryRowContext(ctx, `SELECT operation,input_hash,receipt FROM task_pipeline_delivery_operations WHERE scope=? AND manager=? AND intent=?`, ScopeKey(scope), ManagerKey(scope), intent).Scan(&savedOp, &savedHash, &b)
	if e == sql.ErrNoRows {
		return DeliveryReceipt{}, false, nil
	}
	if e != nil {
		return DeliveryReceipt{}, false, e
	}
	if op != savedOp || hash != savedHash {
		return DeliveryReceipt{}, false, ErrConflict
	}
	var r DeliveryReceipt
	e = json.Unmarshal(b, &r)
	return r, e == nil, e
}
func (s *Store) MutateDelivery(ctx context.Context, scope Scope, intent, op, hash, record, id string, expected uint64, enabled bool, check func(context.Context) error) (DeliveryReceipt, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return DeliveryReceipt{}, e
	}
	defer tx.Rollback()
	var b []byte
	var oldOp, oldHash string
	e = tx.QueryRowContext(ctx, `SELECT operation,input_hash,receipt FROM task_pipeline_delivery_operations WHERE scope=? AND manager=? AND intent=?`, ScopeKey(scope), ManagerKey(scope), intent).Scan(&oldOp, &oldHash, &b)
	if e == nil {
		if oldOp != op || oldHash != hash {
			return DeliveryReceipt{}, ErrConflict
		}
		var r DeliveryReceipt
		e = json.Unmarshal(b, &r)
		return r, e
	}
	if e != sql.ErrNoRows {
		return DeliveryReceipt{}, e
	}
	var other int
	if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_operations WHERE scope=? AND manager=? AND intent=?`, ScopeKey(scope), ManagerKey(scope), intent).Scan(&other); e != nil {
		return DeliveryReceipt{}, e
	}
	if other != 0 {
		return DeliveryReceipt{}, ErrConflict
	}
	if check == nil {
		return DeliveryReceipt{}, ErrScope
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return DeliveryReceipt{}, e
	}
	r := DeliveryReceipt{Kind: "delivery_receipt", IntentID: intent, InputHash: hash, Operation: op, State: "applied"}
	if op == "trigger_policy_set" {
		p, e := policy(ctx, tx, scope)
		if e != nil {
			return r, e
		}
		if p.Revision != expected {
			return r, ErrConflict
		}
		p.Enabled = enabled
		p.Revision++
		p.EffectiveEnabled = enabled && !p.OperatorDisabled
		_, e = tx.ExecContext(ctx, `INSERT INTO task_pipeline_trigger_policies VALUES(?,?,?,?) ON CONFLICT(scope,manager) DO UPDATE SET revision=excluded.revision,enabled=excluded.enabled`, ScopeKey(scope), ManagerKey(scope), p.Revision, enabled)
		if e != nil {
			return r, e
		}
		r.Policy = &p
	} else {
		if _, _, found, e := get(ctx, tx, scope, record); e != nil || !found {
			if e == nil {
				e = ErrScope
			}
			return r, e
		}
		var n Notification
		if e = tx.QueryRowContext(ctx, `SELECT notification FROM task_pipeline_notifications WHERE id=? AND record_id=?`, id, record).Scan(&b); e != nil {
			return r, e
		}
		if e = json.Unmarshal(b, &n); e != nil {
			return r, e
		}
		if n.Revision != expected {
			return r, ErrConflict
		}
		switch op {
		case "notification_ack":
			if n.Destination != scope.Manager {
				return r, ErrScope
			}
			if n.State != "inbox_committed" && n.State != "displayed" {
				return r, ErrConflict
			}
			n.State = "displayed"
		case "notification_retry":
			if n.State != "failed" {
				return r, ErrConflict
			}
			n.State = "pending"
		default:
			return r, ErrScope
		}
		n.Revision++
		n.UpdatedAt = now()
		n.Reason = ""
		if _, e = tx.ExecContext(ctx, `UPDATE task_pipeline_notifications SET state=?,notification=? WHERE id=?`, n.State, encode(n), id); e != nil {
			return r, e
		}
		r.Notification = &n
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO task_pipeline_delivery_operations VALUES(?,?,?,?,?,?)`, ScopeKey(scope), ManagerKey(scope), intent, op, hash, encode(r)); e != nil {
		return r, e
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return r, e
	}
	return r, tx.Commit()
}
func SourceCursorKey(scope Scope, id string, version uint64, node string, loc Locator, store, identity string) string {
	return "psc_" + digest(struct {
		Scope, Record   string
		Version         uint64
		Node            string
		Source          Locator
		Store, Identity string
	}{ScopeKey(scope), id, version, node, loc, store, identity})
}
func (s *Store) SourceCursors(ctx context.Context, scope Scope, id string) ([]SourceCursor, error) {
	if _, _, found, e := s.Get(ctx, scope, id); e != nil || !found {
		if e == nil {
			e = ErrScope
		}
		return nil, e
	}
	rows, e := s.db.QueryContext(ctx, `SELECT cursor FROM task_pipeline_source_cursors WHERE record_id=? ORDER BY key`, id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []SourceCursor{}
	for rows.Next() {
		var b []byte
		var c SourceCursor
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &c); e != nil {
			return nil, e
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func putCursorsTx(ctx context.Context, tx *sql.Tx, r Record, i Issuer, cursors []SourceCursor) error {
	for _, c := range cursors {
		matched := false
		for _, step := range Steps(r) {
			if step.ID == c.NodeID && step.Source != nil && *step.Source == c.Source {
				matched = true
			}
		}
		if !matched {
			return ErrScope
		}
		if c.RecordID != r.ID || c.DefinitionRevision != r.DefinitionRevision || c.ScopeHash != ScopeKey(i.Scope) || c.Key != SourceCursorKey(i.Scope, r.ID, r.DefinitionRevision, c.NodeID, c.Source, c.Store, c.StoreIdentity) {
			return ErrScope
		}
		var raw []byte
		e := tx.QueryRowContext(ctx, `SELECT cursor FROM task_pipeline_source_cursors WHERE key=?`, c.Key).Scan(&raw)
		if e != nil && e != sql.ErrNoRows {
			return e
		}
		if e == sql.ErrNoRows && c.Revision != 0 {
			return ErrConflict
		}
		if e == nil {
			var old SourceCursor
			if e = json.Unmarshal(raw, &old); e != nil {
				return e
			}
			if old.Revision != c.Revision || c.After < old.After {
				return ErrConflict
			}
		}
		c.Revision++
		c.UpdatedAt = now()
		if _, e = tx.ExecContext(ctx, `INSERT INTO task_pipeline_source_cursors VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET cursor=excluded.cursor`, c.Key, r.ID, encode(c)); e != nil {
			return e
		}
	}
	return nil
}

// Event retention is a read-window floor, not deletion of referenced proofs.
// Outstanding ingress/seals/claims remain immutable and queryable by exact ID.
func (s *Store) EventFloor(ctx context.Context, id string) (uint64, error) {
	var floor uint64
	e := s.db.QueryRowContext(ctx, `SELECT floor FROM task_pipeline_event_retention WHERE record_id=?`, id).Scan(&floor)
	if e == sql.ErrNoRows {
		return 0, nil
	}
	return floor, e
}
func (s *Store) RetainEventWindow(ctx context.Context, scope Scope, id string, floor uint64, check func(context.Context) error) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, _, found, e := get(ctx, tx, scope, id); e != nil || !found {
		if e == nil {
			e = ErrScope
		}
		return e
	}
	if check == nil {
		return ErrScope
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return e
	}
	var max uint64
	if e = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM task_pipeline_events WHERE record_id=?`, id).Scan(&max); e != nil {
		return e
	}
	if floor > max {
		return ErrConflict
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO task_pipeline_event_retention VALUES(?,?) ON CONFLICT(record_id) DO UPDATE SET floor=MAX(floor,excluded.floor)`, id, floor)
	if e != nil {
		return e
	}
	return tx.Commit()
}

func (s *Store) EventWatermark(ctx context.Context, id string) (uint64, error) {
	var n uint64
	e := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM task_pipeline_events WHERE record_id=?`, id).Scan(&n)
	return n, e
}

// New claims can use a sampled source only after all its committed pages are
// current. Existing accepted effects use their original receipt path instead.
func (s *Store) TriggerSourceGuard(ctx context.Context, r Record) error {
	rows, e := taskapi.ReadSQL(ctx, s.db).QueryContext(ctx, `SELECT cursor FROM task_pipeline_source_cursors WHERE record_id=? AND json_extract(cursor,'$.definition_revision')=?`, r.ID, r.DefinitionRevision)
	if e != nil {
		return e
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var c SourceCursor
		if e = rows.Scan(&raw); e != nil {
			return e
		}
		if e = json.Unmarshal(raw, &c); e != nil {
			return e
		}
		if c.State != "current" && c.State != "snapshot_recovered" {
			return errors.New("pipeline_source_evidence_gap")
		}
	}
	return rows.Err()
}
