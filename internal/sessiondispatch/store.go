// Package sessiondispatch owns explicit controller grants and durable cross-session receipts.
package sessiondispatch

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/controllerresults"
	"everything-go/internal/taskapi"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unicode/utf8"
)

type Grant struct {
	RoleEpoch    uint64   `json:"role_epoch,omitempty"`
	RoleSessions []string `json:"role_sessions,omitempty"`
	Enabled      bool     `json:"enabled"`
	Local        bool     `json:"local"`
	Instances    []string `json:"instances"`
	Sessions     []string `json:"sessions"`
	Steer        bool     `json:"steer"`
	Revision     uint64   `json:"revision"`
}

func (g Grant) AllowsInstance(local, instance string) bool {
	for _, id := range g.RoleSessions {
		if len(id) > len(instance) && id[:len(instance)+1] == instance+":" {
			return true
		}
	}
	if !g.Enabled {
		return false
	}
	if instance == local && g.Local {
		return true
	}
	for _, id := range g.Instances {
		if id == instance {
			return true
		}
	}
	return false
}

func (g Grant) Allows(local, instance, session string) bool {
	for _, id := range g.RoleSessions {
		if id == instance+":"+session {
			return true
		}
	}
	if !g.Enabled {
		return false
	}
	allowed := instance == local && g.Local
	for _, id := range g.Instances {
		if id == instance {
			allowed = true
		}
	}
	if !allowed {
		return false
	}
	if len(g.Sessions) == 0 {
		return true
	}
	for _, id := range g.Sessions {
		if id == instance+":"+session {
			return true
		}
	}
	return false
}

type Record struct {
	RoleEpoch                uint64 `json:"role_epoch,omitempty"`
	ParentExecutionRequestID string `json:"parent_execution_request_id,omitempty"`
	ParentConfigRevision     uint64 `json:"parent_config_revision"`
	ParentConfigKnown        bool   `json:"parent_config_known,omitempty"`
	ParentThreadID           string `json:"parent_thread_id"`
	ID                       string `json:"dispatch_id"`
	ParentID                 string `json:"parent_session_id"`
	OriginRequestID          string `json:"origin_request_id"`
	TurnID                   string `json:"turn_id"`
	ToolCallID               string `json:"tool_call_id"`
	VoiceID                  string `json:"voice_id,omitempty"`
	InstanceID               string `json:"instance_id"`
	SessionID                string `json:"target_session_id"`
	ThreadID                 string `json:"thread_id"`
	ConfigRevision           uint64 `json:"config_revision"`
	RequestID                string `json:"request_id"`
	ExecutionRequestID       string `json:"execution_request_id,omitempty"`
	ExecutionTurnID          string `json:"execution_turn_id,omitempty"`
	Mode                     string `json:"mode"`
	Content                  string `json:"content"`
	IntentHash               string `json:"intent_hash"`
	State                    string `json:"state"`
	Error                    string `json:"error,omitempty"`
	Result                   string `json:"result,omitempty"`
	DeliveryState            string `json:"delivery_state"`
	DeliveryError            string `json:"delivery_error,omitempty"`
	CreatedAt                int64  `json:"created_at"`
	UpdatedAt                int64  `json:"updated_at"`
	TerminalAt               int64  `json:"terminal_at,omitempty"`
}
type Store struct {
	db        *sql.DB
	roleMu    sync.RWMutex
	roleGrant func(context.Context, string, Grant) Grant
}

func (s *Store) SetRoleGrantResolver(f func(context.Context, string, Grant) Grant) {
	s.roleMu.Lock()
	s.roleGrant = f
	s.roleMu.Unlock()
}

func (s *Store) SealResult(ctx context.Context, session, request, text string) error {
	if len(text) > 256*1024 {
		text = text[:256*1024]
		for !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
	}
	_, e := s.db.ExecContext(ctx, `UPDATE session_dispatches SET payload=json_set(payload,'$.result',?,'$.updated_at',?) WHERE json_extract(payload,'$.target_session_id')=? AND json_extract(payload,'$.request_id')=?`, text, time.Now().UnixMilli(), session, request)
	return e
}

func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("dispatch_data_dir_required")
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	file, e := os.OpenFile(filepath.Join(dir, "everything_go_session_dispatch.db"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	file.Close()
	db, e := sql.Open("sqlite", "file:"+filepath.Join(dir, "everything_go_session_dispatch.db")+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)")
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	_, e = db.Exec(`CREATE TABLE IF NOT EXISTS controller_grants(parent_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, payload TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS session_dispatches(id TEXT PRIMARY KEY,parent_id TEXT NOT NULL,origin_request TEXT NOT NULL,tool_call TEXT NOT NULL,intent_hash TEXT NOT NULL,state TEXT NOT NULL,payload TEXT NOT NULL,UNIQUE(parent_id,origin_request,tool_call));`)
	if e != nil {
		db.Close()
		return nil, e
	}
	if err := taskapi.InstallUnified(db); err != nil {
		db.Close()
		return nil, err
	}
	if err := taskapi.InstallJournal(db); err != nil {
		db.Close()
		return nil, err
	}
	if err := controllerresults.Install(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}
func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Grant(ctx context.Context, parent string) (Grant, error) {
	g, e := s.BaseGrant(ctx, parent)
	if e != nil {
		return g, e
	}
	s.roleMu.RLock()
	f := s.roleGrant
	s.roleMu.RUnlock()
	if f != nil {
		g = f(ctx, parent, g)
	}
	return g, nil
}
func (s *Store) BaseGrant(ctx context.Context, parent string) (Grant, error) {
	var raw string
	g := Grant{Instances: []string{}, Sessions: []string{}}
	e := s.db.QueryRowContext(ctx, `SELECT payload FROM controller_grants WHERE parent_id=?`, parent).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return g, nil
	}
	if e != nil {
		return g, e
	}
	e = json.Unmarshal([]byte(raw), &g)
	g.RoleEpoch = 0
	g.RoleSessions = nil
	if g.Instances == nil {
		g.Instances = []string{}
	}
	if g.Sessions == nil {
		g.Sessions = []string{}
	}
	return g, e
}
func (s *Store) SetGrant(ctx context.Context, parent string, g Grant, expected uint64) (Grant, error) {
	g.RoleEpoch = 0
	g.RoleSessions = nil
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return Grant{}, e
	}
	defer tx.Rollback()
	var revision uint64
	e = tx.QueryRowContext(ctx, `SELECT revision FROM controller_grants WHERE parent_id=?`, parent).Scan(&revision)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return Grant{}, e
	}
	if revision != expected {
		return Grant{}, errors.New("controller_grant_revision_conflict")
	}
	g.Revision = revision + 1
	if g.Instances == nil {
		g.Instances = []string{}
	}
	if g.Sessions == nil {
		g.Sessions = []string{}
	}
	raw, _ := json.Marshal(g)
	_, e = tx.ExecContext(ctx, `INSERT INTO controller_grants VALUES(?,?,?) ON CONFLICT(parent_id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload`, parent, g.Revision, string(raw))
	if e != nil {
		return Grant{}, e
	}
	return g, tx.Commit()
}
func (s *Store) Get(ctx context.Context, id string) (Record, bool, error) {
	var raw string
	var r Record
	e := s.db.QueryRowContext(ctx, `SELECT payload FROM session_dispatches WHERE id=?`, id).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return r, false, nil
	}
	if e != nil {
		return r, false, e
	}
	e = json.Unmarshal([]byte(raw), &r)
	return r, e == nil, e
}
func (s *Store) ByOrigin(ctx context.Context, parent, request, call string) (Record, bool, error) {
	var id string
	e := s.db.QueryRowContext(ctx, `SELECT id FROM session_dispatches WHERE parent_id=? AND origin_request=? AND tool_call=?`, parent, request, call).Scan(&id)
	if errors.Is(e, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	if e != nil {
		return Record{}, false, e
	}
	return s.Get(ctx, id)
}
func (s *Store) Create(ctx context.Context, r Record) (bool, error) {
	if r.ID == "" || r.ParentID == "" || r.RequestID == "" || r.IntentHash == "" {
		return false, errors.New("dispatch_identity_invalid")
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return false, e
	}
	defer tx.Rollback()
	var existing string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM session_dispatches WHERE parent_id=? AND origin_request=? AND tool_call=?`, r.ParentID, r.OriginRequestID, r.ToolCallID).Scan(&existing); err == nil {
		return false, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	var count int
	e = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM session_dispatches WHERE parent_id=? AND state IN ('prepared','pending','accepted','queued','running','steering','steered','uncertain')`, r.ParentID).Scan(&count)
	if e != nil {
		return false, e
	}
	if count >= 8 {
		return false, errors.New("controller_pending_limit")
	}
	r.CreatedAt = time.Now().UnixMilli()
	r.UpdatedAt = r.CreatedAt
	if r.State == "" {
		r.State = "prepared"
	}
	if r.DeliveryState == "" {
		r.DeliveryState = "pending"
	}
	raw, _ := json.Marshal(r)
	result, e := tx.ExecContext(ctx, `INSERT OR IGNORE INTO session_dispatches VALUES(?,?,?,?,?,?,?)`, r.ID, r.ParentID, r.OriginRequestID, r.ToolCallID, r.IntentHash, r.State, string(raw))
	if e != nil {
		return false, e
	}
	n, _ := result.RowsAffected()
	if n == 1 {
		if e = taskapi.ChangeTx(tx, r.SessionID, r.RequestID, "controller_admission"); e != nil {
			return false, e
		}
	}
	return n == 1, tx.Commit()
}
func (s *Store) Put(ctx context.Context, r Record) error {
	r.UpdatedAt = time.Now().UnixMilli()
	terminal := r.State == "completed" || r.State == "failed" || r.State == "cancelled" || r.State == "rejected"
	if terminal && r.TerminalAt == 0 {
		r.TerminalAt = r.UpdatedAt
	}
	raw, e := json.Marshal(r)
	if e != nil {
		return e
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	res, e := tx.ExecContext(ctx, `UPDATE session_dispatches SET state=?,payload=json_set(CASE WHEN json_extract(payload,'$.result')!='' AND ?='' THEN json_set(?,'$.result',json_extract(payload,'$.result')) ELSE ? END,'$.terminal_at',COALESCE(NULLIF(json_extract(payload,'$.terminal_at'),0),?)) WHERE id=? AND parent_id=? AND intent_hash=? AND (state NOT IN ('completed','failed','cancelled','rejected') OR state=?)`, r.State, r.Result, string(raw), string(raw), r.TerminalAt, r.ID, r.ParentID, r.IntentHash, r.State)
	if e != nil {
		return e
	}
	n, e := res.RowsAffected()
	if e != nil {
		return e
	}
	if n > 0 {
		if e = taskapi.ChangeTx(tx, r.SessionID, r.RequestID, "controller_state"); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (s *Store) List(ctx context.Context, parent string) ([]Record, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT payload FROM session_dispatches WHERE parent_id=? ORDER BY rowid DESC LIMIT 100`, parent)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		var raw string
		var r Record
		if e = rows.Scan(&raw); e != nil {
			return nil, e
		}
		if e = json.Unmarshal([]byte(raw), &r); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *Store) Pending(ctx context.Context) ([]Record, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT payload FROM session_dispatches WHERE state IN ('prepared','pending','accepted','queued','running','steering','steered','uncertain') OR json_extract(payload,'$.delivery_state') IN ('pending','queued') ORDER BY COALESCE((SELECT rank FROM unified_queue_ranks WHERE queue_key=json_extract(session_dispatches.payload,'$.target_session_id') AND item_id=session_dispatches.id),rowid),rowid LIMIT 200`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		var raw string
		var r Record
		if e = rows.Scan(&raw); e != nil {
			return nil, e
		}
		if e = json.Unmarshal([]byte(raw), &r); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// HasTargetReceipt checks an exact canonical dispatch tuple without exposing history.
func (s *Store) HasTargetReceipt(ctx context.Context, instance, session, request string) (bool, error) {
	var found bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM session_dispatches WHERE json_extract(payload,'$.instance_id')=? AND json_extract(payload,'$.target_session_id')=? AND json_extract(payload,'$.request_id')=?)`, instance, session, request).Scan(&found)
	return found, err
}

// Exact original target lookup is not limited by the UI's latest-100 list.
func (s *Store) ByTargetRequest(ctx context.Context, parent, target, request string) (Record, bool, error) {
	var raw string
	var v Record
	e := s.db.QueryRowContext(ctx, `SELECT payload FROM session_dispatches WHERE parent_id=? AND json_extract(payload,'$.target_session_id')=? AND json_extract(payload,'$.request_id')=? ORDER BY rowid DESC LIMIT 1`, parent, target, request).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return v, false, nil
	}
	if e == nil {
		e = json.Unmarshal([]byte(raw), &v)
	}
	return v, e == nil, e
}

// A destructive-generation guard needs complete outstanding coverage, not the
// latest-100 presentation list or the coordinator's rotating work page.
func (s *Store) ContextUnsettled(ctx context.Context, parent string) (bool, bool, error) {
	var execution, delivery bool
	e := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM session_dispatches WHERE parent_id=? AND state NOT IN ('completed','failed','cancelled','rejected')),EXISTS(SELECT 1 FROM session_dispatches WHERE parent_id=? AND COALESCE(json_extract(payload,'$.delivery_state'),'pending') NOT IN ('delivered','not_requested'))`, parent, parent).Scan(&execution, &delivery)
	return execution, delivery, e
}

// IndexRecords exposes original controller receipts for the scoped TaskAPI
// adapter. It does not create an admission journal or change original IDs.
// Overflow is explicit, never an empty or falsely complete task catalog.
func (s *Store) IndexRecords(ctx context.Context) ([]Record, error) {
	rows, e := s.db.QueryContext(ctx, "SELECT payload FROM session_dispatches ORDER BY rowid LIMIT 4097")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		var r Record
		if e = json.Unmarshal(b, &r); e != nil {
			return nil, e
		}
		out = append(out, r)
		if len(out) > 4096 {
			return nil, errors.New("controller_index_range_exceeded")
		}
	}
	return out, rows.Err()
}

func (s *Store) Results() *controllerresults.Store { return controllerresults.New(s.db) }

// Complete bounded source scan, separate from the presentation latest-100 list.
func (s *Store) ListForResults(ctx context.Context, parent string) ([]Record, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT payload FROM session_dispatches WHERE parent_id=? ORDER BY rowid LIMIT 4097`, parent)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		var b []byte
		var r Record
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &r); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	if len(out) > 4096 {
		return nil, errors.New("controller_result_capacity")
	}
	return out, rows.Err()
}
