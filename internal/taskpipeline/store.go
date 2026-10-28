package taskpipeline

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
	"fmt"
	"time"
)

var ErrConflict = errors.New("pipeline_revision_conflict")
var ErrScope = errors.New("pipeline_scope_changed")
var ErrUnknown = errors.New("pipeline_original_effect_unknown")

type Store struct {
	proof             *NativeProvenance
	proofContinuation string
	db                *sql.DB
	txContext         func(context.Context, *sql.Tx) context.Context
	admissionGuard    func(*sql.Tx, string) error
}

func (s *Store) WithProof(proof *NativeProvenance, continuation ...string) *Store {
	clone := *s
	clone.proof = proof
	if len(continuation) > 0 {
		clone.proofContinuation = continuation[0]
	}
	return &clone
}
func New(db *sql.DB) *Store { return &Store{db: db} }
func NewWithContext(db *sql.DB, binder func(context.Context, *sql.Tx) context.Context, guard ...func(*sql.Tx, string) error) *Store {
	s := &Store{db: db, txContext: binder}
	if len(guard) > 0 {
		s.admissionGuard = guard[0]
	}
	return s
}
func (s *Store) context(ctx context.Context, tx *sql.Tx) context.Context {
	if s.txContext != nil {
		return s.txContext(ctx, tx)
	}
	return ctx
}
func Install(db *sql.DB) error {
	_, e := db.Exec(`
CREATE TABLE IF NOT EXISTS task_pipeline_scan_cursors(kind TEXT PRIMARY KEY,after_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS task_pipeline_native_inputs(session_id TEXT NOT NULL,request_id TEXT NOT NULL,thread_id TEXT NOT NULL,token TEXT NOT NULL,revision INTEGER NOT NULL,input_hash TEXT NOT NULL,PRIMARY KEY(session_id,request_id));
CREATE TABLE IF NOT EXISTS task_pipeline_records(id TEXT PRIMARY KEY,scope TEXT NOT NULL,manager TEXT NOT NULL,revision INTEGER NOT NULL,definition_revision INTEGER NOT NULL,state TEXT NOT NULL,record BLOB NOT NULL,issuer BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS task_pipeline_scope ON task_pipeline_records(scope,manager,id);
CREATE TABLE IF NOT EXISTS task_pipeline_operations(scope TEXT NOT NULL,manager TEXT NOT NULL,intent TEXT NOT NULL,operation TEXT NOT NULL,input_hash TEXT NOT NULL,receipt BLOB NOT NULL,PRIMARY KEY(scope,manager,intent));
CREATE TABLE IF NOT EXISTS task_pipeline_events(seq INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL UNIQUE,record_id TEXT NOT NULL,node_id TEXT NOT NULL,source_key TEXT NOT NULL,event BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS task_pipeline_events_record ON task_pipeline_events(record_id,seq);
CREATE TABLE IF NOT EXISTS task_pipeline_outbox(id TEXT PRIMARY KEY,record_id TEXT NOT NULL,definition_revision INTEGER NOT NULL,node_id TEXT NOT NULL,state TEXT NOT NULL,continuation BLOB NOT NULL,UNIQUE(record_id,definition_revision,node_id));
CREATE TABLE IF NOT EXISTS task_pipeline_authorizations(record_id TEXT NOT NULL,definition_revision INTEGER NOT NULL,issuer BLOB NOT NULL,PRIMARY KEY(record_id,definition_revision));
CREATE TABLE IF NOT EXISTS task_pipeline_revisions(record_id TEXT NOT NULL,revision INTEGER NOT NULL,record BLOB NOT NULL,PRIMARY KEY(record_id,revision));
`)
	if e != nil {
		return e
	}
	return installDurability(db)
}
func digest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func ScopeKey(s Scope) string {
	return digest(struct {
		Principal  string
		Generation uint64
	}{s.Principal, s.Generation})
}
func ManagerKey(s Scope) string { return digest(s.Manager) }
func now() int64                { return time.Now().UnixMilli() }
func encode(v any) []byte       { b, _ := json.Marshal(v); return b }
func same(a, b Identity) bool   { return a == b }
func (s *Store) Get(ctx context.Context, scope Scope, id string) (Record, Issuer, bool, error) {
	return get(ctx, s.db, scope, id)
}

type reader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func get(ctx context.Context, q reader, scope Scope, id string) (r Record, i Issuer, found bool, e error) {
	var raw, ir []byte
	e = q.QueryRowContext(ctx, `SELECT record,issuer FROM task_pipeline_records WHERE id=? AND scope=? AND manager=?`, id, ScopeKey(scope), ManagerKey(scope)).Scan(&raw, &ir)
	if e == sql.ErrNoRows {
		return r, i, false, nil
	}
	if e != nil {
		return
	}
	e = json.Unmarshal(raw, &r)
	if e == nil {
		e = json.Unmarshal(ir, &i)
	}
	found = e == nil
	return
}
func (s *Store) List(ctx context.Context, scope Scope, kind, cursor string, limit int) ([]Record, string, error) {
	if limit < 1 || limit > 50 {
		limit = 20
	}
	rows, e := s.db.QueryContext(ctx, `SELECT record FROM task_pipeline_records WHERE scope=? AND manager=? AND id>? AND (?='' OR json_extract(record,'$.kind')=?) ORDER BY id LIMIT ?`, ScopeKey(scope), ManagerKey(scope), cursor, kind, kind, limit+1)
	if e != nil {
		return nil, "", e
	}
	defer rows.Close()
	out := []Record{}
	next := ""
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, "", e
		}
		var r Record
		if e = json.Unmarshal(b, &r); e != nil {
			return nil, "", e
		}
		if kind != "" && r.Kind != kind {
			continue
		}
		if len(out) == limit {
			next = out[len(out)-1].ID
			break
		}
		out = append(out, r)
	}
	return out, next, rows.Err()
}
func (s *Store) Operation(ctx context.Context, scope Scope, intent, op, hash string) (Receipt, bool, error) {
	var raw []byte
	var savedOp, savedHash string
	e := s.db.QueryRowContext(ctx, `SELECT operation,input_hash,receipt FROM task_pipeline_operations WHERE scope=? AND manager=? AND intent=?`, ScopeKey(scope), ManagerKey(scope), intent).Scan(&savedOp, &savedHash, &raw)
	if e == sql.ErrNoRows {
		return Receipt{}, false, nil
	}
	if e != nil {
		return Receipt{}, false, e
	}
	if savedOp != op || savedHash != hash {
		return Receipt{}, false, ErrConflict
	}
	var r Receipt
	e = json.Unmarshal(raw, &r)
	return r, e == nil, e
}
func save(ctx context.Context, tx *sql.Tx, scope Scope, r Record, i Issuer) error {
	_, e := tx.ExecContext(ctx, `INSERT INTO task_pipeline_records(id,scope,manager,revision,definition_revision,state,record,issuer) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,definition_revision=excluded.definition_revision,state=excluded.state,record=excluded.record,issuer=excluded.issuer`, r.ID, ScopeKey(scope), ManagerKey(scope), r.Revision, r.DefinitionRevision, r.State, encode(r), encode(i))
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO task_pipeline_revisions(record_id,revision,record) VALUES(?,?,?)`, r.ID, r.Revision, encode(r))
	return e
}

// Mutate shares one SQL transaction for definition, original operation receipt,
// checkpoint and all future-edge retirement. Repeating a key returns old bytes.
func (s *Store) Mutate(ctx context.Context, scope Scope, issuer Issuer, intent, op, hash, id string, expected uint64, definition *Record, checkpoint *Checkpoint, initial []Fact, check func(context.Context) error, reportedIntent ...string) (Receipt, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return Receipt{}, e
	}
	defer tx.Rollback()
	var raw []byte
	var oldOp, oldHash string
	e = tx.QueryRowContext(ctx, `SELECT operation,input_hash,receipt FROM task_pipeline_operations WHERE scope=? AND manager=? AND intent=?`, ScopeKey(scope), ManagerKey(scope), intent).Scan(&oldOp, &oldHash, &raw)
	if e == nil {
		if oldOp != op || oldHash != hash {
			return Receipt{}, ErrConflict
		}
		var old Receipt
		e = json.Unmarshal(raw, &old)
		return old, e
	}
	if e != sql.ErrNoRows {
		return Receipt{}, e
	}
	var other int
	if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_delivery_operations WHERE scope=? AND manager=? AND intent=?`, ScopeKey(scope), ManagerKey(scope), intent).Scan(&other); e != nil {
		return Receipt{}, e
	}
	if other != 0 {
		return Receipt{}, ErrConflict
	}
	if check == nil {
		return Receipt{}, ErrScope
	}
	if s.admissionGuard != nil && (definition != nil || op == "pipeline_enable") {
		if e = s.admissionGuard(tx, scope.Manager.Session); e != nil {
			return Receipt{}, e
		}
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return Receipt{}, e
	}
	newIssuer := issuer
	var r Record
	var exists bool
	if definition != nil && id == "" {
		r = *definition
		r.ID = "pl_" + digest(struct{ Scope, Manager, Intent string }{ScopeKey(scope), ManagerKey(scope), intent})[:32]
		var count int
		if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_records WHERE scope=? AND manager=?`, ScopeKey(scope), ManagerKey(scope)).Scan(&count); e != nil {
			return Receipt{}, e
		}
		if count >= 128 {
			return Receipt{}, errors.New("pipeline_record_limit")
		}
		r.Manager = scope.Manager
		r.Revision = 1
		r.DefinitionRevision = 1
		r.CreatedAt = now()
		r.UpdatedAt = r.CreatedAt
		r.AccessMode = "owner"
		r.Steps = []StepState{}
		if r.Kind == "pipeline" {
			r.State = "disabled"
		} else {
			r.State = "active"
		}
		if r.Kind == "mission" {
			for _, step := range Steps(r) {
				if step.Source == nil && step.Action == nil && step.WorkItem == nil && r.Mission.RecoveryAction == nil {
					r.State = "blocked"
					r.BlockedReason = "source_unbound"
				}
			}
		}
		id = r.ID
	} else {
		r, issuer, exists, e = get(ctx, tx, scope, id)
		if e != nil {
			return Receipt{}, e
		}
		if !exists {
			return Receipt{}, errors.New("pipeline_not_found")
		}
		if r.Revision != expected {
			return Receipt{}, ErrConflict
		}
		if definition != nil && definition.Kind != r.Kind {
			return Receipt{}, ErrScope
		}
		if op != "mission_checkpoint" && len(op) > len(r.Kind) && op[:len(r.Kind)] != r.Kind {
			return Receipt{}, ErrScope
		}
		r.Revision++
		r.UpdatedAt = now()
		if definition != nil {
			issuer = newIssuer
			r.Mission = definition.Mission
			r.Watch = definition.Watch
			r.Pipeline = definition.Pipeline
			r.DefinitionRevision++
			r.Steps = []StepState{}
			r.Checkpoint = nil
			r.NoProgressCount = 0
			r.State = "active"
			r.BlockedReason = ""
		}
	}
	switch op {
	case "mission_pause", "watch_pause", "pipeline_pause":
		r.State = "paused"
	case "mission_cancel", "watch_cancel", "pipeline_cancel":
		r.State = "cancelled"
		if _, e = tx.ExecContext(ctx, `UPDATE task_pipeline_notifications SET state='retired',notification=json_set(notification,'$.state','retired','$.revision',json_extract(notification,'$.revision')+1,'$.reason','original_rule_cancelled') WHERE record_id=? AND state IN ('pending','failed')`, r.ID); e != nil {
			return Receipt{}, e
		}
	case "pipeline_enable":
		r.State = "active"
	case "mission_checkpoint":
		if checkpoint == nil {
			return Receipt{}, errors.New("checkpoint_required")
		}
		r.Checkpoint = checkpoint
	}
	if definition != nil || r.State != "active" {
		if _, e = tx.ExecContext(ctx, `UPDATE task_pipeline_outbox SET state='retired',continuation=json_set(continuation,'$.state','retired','$.reason','definition_retired') WHERE record_id=? AND state='ready'`, id); e != nil {
			return Receipt{}, e
		}
	}
	if definition != nil {
		if _, e = tx.ExecContext(ctx, `INSERT INTO task_pipeline_authorizations VALUES(?,?,?)`, r.ID, r.DefinitionRevision, encode(issuer)); e != nil {
			return Receipt{}, e
		}
	}
	if definition != nil {
		if e = seedTx(ctx, tx, r, initial); e != nil {
			return Receipt{}, e
		}
	}
	if e = save(ctx, tx, scope, r, issuer); e != nil {
		return Receipt{}, e
	}
	wireIntent := intent
	if len(reportedIntent) > 0 {
		wireIntent = reportedIntent[0]
	}
	var nativeContinuation *Continuation
	if s.proof != nil && s.proofContinuation != "" {
		var raw []byte
		if e = tx.QueryRowContext(ctx, `SELECT continuation FROM task_pipeline_outbox WHERE id=? AND record_id=?`, s.proofContinuation, r.ID).Scan(&raw); e != nil {
			return Receipt{}, e
		}
		var c Continuation
		if e = json.Unmarshal(raw, &c); e != nil {
			return Receipt{}, e
		}
		expectedRequest := c.RequestID
		if c.OriginalReceipt != nil {
			expectedRequest = c.OriginalReceipt.Request
		}
		if expectedRequest != s.proof.RequestID || c.Action.Target != s.proof.Source {
			return Receipt{}, ErrScope
		}
		if e = appendNativeOperation(&c, wireIntent, op, hash); e != nil {
			return Receipt{}, e
		}
		if _, e = tx.ExecContext(ctx, `UPDATE task_pipeline_outbox SET continuation=? WHERE id=?`, encode(c), c.ID); e != nil {
			return Receipt{}, e
		}
		nativeContinuation = &c
	}
	receipt := Receipt{Continuation: nativeContinuation, Provenance: s.proof, Kind: "operation_receipt", IntentID: wireIntent, InputHash: hash, Operation: op, State: "applied", Record: &r, NoEffect: true}
	if _, e = tx.ExecContext(ctx, `INSERT INTO task_pipeline_operations VALUES(?,?,?,?,?,?)`, ScopeKey(scope), ManagerKey(scope), intent, op, hash, encode(receipt)); e != nil {
		return Receipt{}, e
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return Receipt{}, e
	}
	e = tx.Commit()
	return receipt, e
}
func (s *Store) Active(ctx context.Context, limit int) ([]struct {
	Record Record
	Issuer Issuer
}, error) {
	cursor, e := s.scanCursor(ctx, "records")
	if e != nil {
		return nil, e
	}
	var count int
	if e = s.db.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_records WHERE state='active' AND id>?`, cursor).Scan(&count); e != nil {
		return nil, e
	}
	if count == 0 {
		cursor = ""
	}
	rows, e := s.db.QueryContext(ctx, `SELECT record,issuer FROM task_pipeline_records WHERE state='active' AND id>? ORDER BY id LIMIT ?`, cursor, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []struct {
		Record Record
		Issuer Issuer
	}{}
	for rows.Next() {
		var b, i []byte
		if e = rows.Scan(&b, &i); e != nil {
			return nil, e
		}
		var v struct {
			Record Record
			Issuer Issuer
		}
		if e = json.Unmarshal(b, &v.Record); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(i, &v.Issuer); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}
	rows.Close()
	if len(out) > 0 {
		if e = s.saveScanCursor(ctx, "records", out[len(out)-1].Record.ID); e != nil {
			return nil, e
		}
	}
	return out, nil
}
func (s *Store) Events(ctx context.Context, scope Scope, id string, after uint64, limit int) ([]Event, uint64, bool, error) {
	if _, _, found, e := s.Get(ctx, scope, id); e != nil || !found {
		return nil, after, false, e
	}
	floor, e := s.EventFloor(ctx, id)
	if e != nil {
		return nil, after, false, e
	}
	if after < floor {
		return nil, after, false, taskapi.Failure("cursor_expired", "known_none", "refresh_snapshot")
	}
	if limit < 1 || limit > 100 {
		limit = 50
	}
	rows, e := s.db.QueryContext(ctx, `SELECT seq,event FROM task_pipeline_events WHERE record_id=? AND seq>? ORDER BY seq LIMIT ?`, id, after, limit+1)
	if e != nil {
		return nil, after, false, e
	}
	defer rows.Close()
	out := []Event{}
	more := false
	for rows.Next() {
		var seq uint64
		var b []byte
		if e = rows.Scan(&seq, &b); e != nil {
			return nil, after, false, e
		}
		if len(out) == limit {
			more = true
			break
		}
		var ev Event
		if e = json.Unmarshal(b, &ev); e != nil {
			return nil, after, false, e
		}
		ev.Sequence = seq
		out = append(out, ev)
		after = seq
	}
	return out, after, more, rows.Err()
}
func (s *Store) Continuation(ctx context.Context, scope Scope, id string) (Continuation, bool, error) {
	var b []byte
	var record string
	e := s.db.QueryRowContext(ctx, `SELECT record_id,continuation FROM task_pipeline_outbox WHERE id=?`, id).Scan(&record, &b)
	if e == sql.ErrNoRows {
		return Continuation{}, false, nil
	}
	if e != nil {
		return Continuation{}, false, e
	}
	if _, _, f, e := s.Get(ctx, scope, record); e != nil || !f {
		return Continuation{}, false, e
	}
	var c Continuation
	e = json.Unmarshal(b, &c)
	return c, e == nil, e
}

type GuardStatus struct {
	Kind         string   `json:"kind"`
	Outstanding  uint64   `json:"outstanding"`
	Unknown      uint64   `json:"unknown"`
	ActiveClaims uint64   `json:"active_claims"`
	Ready        bool     `json:"ready"`
	Reasons      []string `json:"reasons"`
}

func (s *Store) Guard(ctx context.Context, session string) (GuardStatus, error) {
	return s.guard(ctx, session, nil)
}
func (s *Store) ScopedGuard(ctx context.Context, scope Scope) (GuardStatus, error) {
	return s.guard(ctx, scope.Manager.Session, &scope)
}
func (s *Store) guard(ctx context.Context, session string, scope *Scope) (GuardStatus, error) {
	g := GuardStatus{Kind: "guard_status", Ready: true, Reasons: []string{}}
	rows, e := s.db.QueryContext(ctx, `SELECT r.record,o.state,r.scope,r.manager FROM task_pipeline_records r LEFT JOIN task_pipeline_outbox o ON o.record_id=r.id WHERE r.state NOT IN ('paused','cancelled','blocked','done','expired','disabled') OR o.state NOT IN ('checkpointed','retired','rejected','no_progress')`)
	if e != nil {
		return g, e
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var b []byte
		var state sql.NullString
		var sk, mk string
		if e = rows.Scan(&b, &state, &sk, &mk); e != nil {
			return g, e
		}
		if scope != nil && (sk != ScopeKey(*scope) || mk != ManagerKey(*scope)) {
			continue
		}
		var r Record
		if e = json.Unmarshal(b, &r); e != nil {
			return g, e
		}
		if session != "" && !Touches(r, session) {
			continue
		}
		if !seen[r.ID] {
			g.Outstanding++
			seen[r.ID] = true
		}
		if state.Valid && (state.String == "issued" || state.String == "unknown" || !KnownContinuationState(state.String)) {
			g.Unknown++
		}
		if state.String == "issued" {
			g.ActiveClaims++
		}
	}

	if e = rows.Err(); e != nil {
		return g, e
	}
	rows.Close()
	notices, e := s.db.QueryContext(ctx, `SELECT r.record,n.state,r.scope,r.manager FROM task_pipeline_notifications n JOIN task_pipeline_records r ON r.id=n.record_id WHERE n.state NOT IN ('inbox_committed','displayed','retired')`)
	if e != nil {
		return g, e
	}
	defer notices.Close()
	for notices.Next() {
		var raw []byte
		var state, sk, mk string
		if e = notices.Scan(&raw, &state, &sk, &mk); e != nil {
			return g, e
		}
		if scope != nil && (sk != ScopeKey(*scope) || mk != ManagerKey(*scope)) {
			continue
		}
		var r Record
		if e = json.Unmarshal(raw, &r); e != nil {
			return g, e
		}
		if session != "" && !Touches(r, session) {
			continue
		}
		if !seen[r.ID] {
			g.Outstanding++
			seen[r.ID] = true
		}
		if state != "pending" && state != "failed" {
			g.Unknown++
		}
	}
	if e = notices.Err(); e != nil {
		return g, e
	}
	g.Ready = g.Outstanding == 0 && g.Unknown == 0
	if !g.Ready {
		g.Reasons = append(g.Reasons, "pipeline_outstanding_or_unknown")
	}
	return g, rows.Err()
}
func validateRecord(r Record) error {
	switch r.Kind {
	case "mission":
		if r.Mission == nil || r.Mission.Goal == "" || len(r.Mission.Steps) == 0 {
			return errors.New("mission_steps_required")
		}
	case "watch":
		if r.Watch == nil || r.Watch.Source.Request == "" || r.Watch.Source.RoundID == "" {
			return errors.New("exact_source_round_required")
		}
	case "pipeline":
		if r.Pipeline == nil || len(r.Pipeline.Nodes) == 0 {
			return errors.New("pipeline_nodes_required")
		}
	default:
		return fmt.Errorf("invalid_record_kind")
	}
	return nil
}

func (s *Store) OriginalDefinition(ctx context.Context, id string, version uint64) (Record, Issuer, error) {
	var b, ir []byte
	e := s.db.QueryRowContext(ctx, `SELECT r.record,a.issuer FROM task_pipeline_revisions r JOIN task_pipeline_authorizations a ON a.record_id=r.record_id AND a.definition_revision=? WHERE r.record_id=? AND json_extract(r.record,'$.definition_revision')=? ORDER BY r.revision LIMIT 1`, version, id, version).Scan(&b, &ir)
	var r Record
	var i Issuer
	if e != nil {
		return r, i, e
	}
	if e = json.Unmarshal(b, &r); e != nil {
		return r, i, e
	}
	e = json.Unmarshal(ir, &i)
	return r, i, e
}
func (s *Store) ExactContinuation(ctx context.Context, id string) (Continuation, bool, error) {
	var b []byte
	var state string
	e := s.db.QueryRowContext(ctx, `SELECT state,continuation FROM task_pipeline_outbox WHERE id=?`, id).Scan(&state, &b)
	if e == sql.ErrNoRows {
		return Continuation{}, false, nil
	}
	if e != nil {
		return Continuation{}, false, e
	}
	var c Continuation
	e = json.Unmarshal(b, &c)
	c.State = state
	return c, e == nil, e
}
func (s *Store) CheckEventIDs(ctx context.Context, scope Scope, id string, events []string) error {
	if _, _, f, e := s.Get(ctx, scope, id); e != nil || !f {
		return ErrScope
	}
	for _, event := range events {
		var n int
		if e := s.db.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_events WHERE record_id=? AND event_id=? AND json_extract(event,'$.state') IN ('evidence_verified','mission_applied')`, id, event).Scan(&n); e != nil {
			return e
		}
		if n != 1 {
			return errors.New("unverified_checkpoint_event")
		}
	}
	return nil
}

func Touches(r Record, session string) bool {
	if r.Manager.Session == session {
		return true
	}
	for _, s := range Steps(r) {
		if s.Source != nil && s.Source.Session == session {
			return true
		}
		if s.Action != nil && s.Action.Target.Session == session {
			return true
		}
	}
	return r.Watch != nil && (r.Watch.Destination.Session == session || (r.Watch.Action != nil && r.Watch.Action.Target.Session == session))
}

// GuardTx runs in the destructive generation writer's transaction, so a watch
// registration and context reset cannot both pass independent idle snapshots.
func GuardTx(ctx context.Context, tx *sql.Tx, session string) error {
	rows, e := tx.QueryContext(ctx, `SELECT r.record FROM task_pipeline_records r WHERE r.state NOT IN ('paused','cancelled','blocked','done','expired','disabled') OR EXISTS(SELECT 1 FROM task_pipeline_outbox o WHERE o.record_id=r.id AND o.state NOT IN ('checkpointed','retired','rejected','no_progress')) OR EXISTS(SELECT 1 FROM task_pipeline_notifications n WHERE n.record_id=r.id AND n.state NOT IN ('inbox_committed','displayed','retired'))`)
	if e != nil {
		return e
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return e
		}
		var r Record
		if e = json.Unmarshal(b, &r); e != nil {
			return e
		}
		if Touches(r, session) {
			return errors.New("pipeline_outstanding_or_unknown")
		}
	}
	return rows.Err()
}

func (s *Store) scanCursor(ctx context.Context, kind string) (string, error) {
	var cursor string
	e := s.db.QueryRowContext(ctx, `SELECT after_id FROM task_pipeline_scan_cursors WHERE kind=?`, kind).Scan(&cursor)
	if e == sql.ErrNoRows {
		return "", nil
	}
	return cursor, e
}
func (s *Store) saveScanCursor(ctx context.Context, kind, cursor string) error {
	_, e := s.db.ExecContext(ctx, `INSERT INTO task_pipeline_scan_cursors VALUES(?,?) ON CONFLICT(kind) DO UPDATE SET after_id=excluded.after_id`, kind, cursor)
	return e
}

func KnownContinuationState(state string) bool {
	switch state {
	case "ready", "issued", "unknown", "queued", "native_started", "checkpoint_applied", "waiting_evidence", "blocked", "checkpointed", "retired", "rejected", "no_progress":
		return true
	}
	return false
}

func appendNativeOperation(c *Continuation, intent, op, hash string) error {
	for _, r := range c.NativeOperations {
		if r.IntentID == intent {
			if r.Operation != op || r.InputHash != hash {
				return ErrConflict
			}
			return nil
		}
	}
	if len(c.NativeOperations) >= 32 {
		return errors.New("native_operation_limit")
	}
	c.NativeOperations = append(c.NativeOperations, NativeOperationRef{intent, op, hash})
	return nil
}

// NativeProjection locates only the server's original native-owner namespace.
// It never accepts a principal from a client or scans another device partition.
func (s *Store) NativeProjection(ctx context.Context, manager Identity, id string) (Record, Issuer, bool, error) {
	scope := Scope{Principal: "session:" + manager.Session, Manager: manager}
	r, i, found, e := s.Get(ctx, scope, id)
	if e != nil || !found {
		return r, i, found, e
	}
	if (i.Kind != "native_tool" && i.Kind != "mcp_process_binding") || i.Scope != scope || r.Manager != manager || i.SourceRequest == "" {
		return Record{}, Issuer{}, false, ErrScope
	}
	r.AccessMode = "read_only_projection"
	return r, i, true, nil
}

type ReadRecord struct {
	Record     Record
	Issuer     Issuer
	Projection bool
}

// ReadList merges ONLY own paired rows and the exact manager's native-owner
// rows in one stable key order. Revoked rows are returned for the gateway to
// reject the whole page; skipping them could falsely assert complete/empty.
func (s *Store) ReadList(ctx context.Context, scope Scope, kind, cursor string, limit int) ([]ReadRecord, string, error) {
	if limit < 1 || limit > 50 {
		limit = 20
	}
	native := Scope{Principal: "session:" + scope.Manager.Session, Manager: scope.Manager}
	rows, e := s.db.QueryContext(ctx, `SELECT record,issuer,scope FROM task_pipeline_records WHERE manager=? AND (scope=? OR (scope=? AND json_extract(issuer,'$.Kind') IN ('native_tool','mcp_process_binding'))) AND id>? AND (?='' OR json_extract(record,'$.kind')=?) ORDER BY id LIMIT ?`, ManagerKey(scope), ScopeKey(scope), ScopeKey(native), cursor, kind, kind, limit+1)
	if e != nil {
		return nil, "", e
	}
	defer rows.Close()
	out := []ReadRecord{}
	next := ""
	for rows.Next() {
		var raw, ir []byte
		var sk string
		if e = rows.Scan(&raw, &ir, &sk); e != nil {
			return nil, "", e
		}
		var item ReadRecord
		if e = json.Unmarshal(raw, &item.Record); e != nil {
			return nil, "", e
		}
		if e = json.Unmarshal(ir, &item.Issuer); e != nil {
			return nil, "", e
		}
		if len(out) == limit {
			next = out[len(out)-1].Record.ID
			break
		}
		item.Projection = sk != ScopeKey(scope)
		item.Record.AccessMode = "owner"
		if item.Projection {
			if item.Issuer.Scope != native || item.Record.Manager != scope.Manager || item.Issuer.SourceRequest == "" {
				return nil, "", ErrScope
			}
			item.Record.AccessMode = "read_only_projection"
		}
		out = append(out, item)
	}
	return out, next, rows.Err()
}
