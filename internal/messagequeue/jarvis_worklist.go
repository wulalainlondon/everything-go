package messagequeue

// Work items are user metadata. They never enqueue, execute, or replace the
// canonical queue/Task API state. Scope is the original paired credential epoch.
import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
	"fmt"
	"strings"
	"time"
)

type WorkLink struct {
	Kind           string `json:"kind"`
	Authority      string `json:"authority_instance_id"`
	SessionID      string `json:"session_id"`
	ThreadID       string `json:"thread_id"`
	ConfigRevision uint64 `json:"config_revision"`
	RequestID      string `json:"request_id"`
	NativeID       string `json:"native_id,omitempty"`
}
type WorkPlan struct {
	Path      string   `json:"path"`
	SHA256    string   `json:"sha256"`
	Revision  uint64   `json:"revision"`
	Checklist []string `json:"checklist"`
}
type WorkDecision struct {
	State       string `json:"state"`
	SourceRef   string `json:"source_ref"`
	Instruction string `json:"instruction"`
}
type WorkItem struct {
	ID             string        `json:"item_id"`
	Kind           string        `json:"kind"`
	Title          string        `json:"title"`
	Body           string        `json:"body"`
	RecordingState string        `json:"recording_state"`
	SourceRefs     []string      `json:"source_refs"`
	Plan           *WorkPlan     `json:"plan_ref,omitempty"`
	Decision       *WorkDecision `json:"decision,omitempty"`
	Links          []WorkLink    `json:"links"`
	Revision       uint64        `json:"revision"`
	CreatedAt      int64         `json:"created_at_ms"`
	UpdatedAt      int64         `json:"updated_at_ms"`
}
type WorkMutation struct {
	IntentID         string `json:"intent_id"`
	ExpectedRevision uint64 `json:"expected_revision"`
	WorkItem
}

var ErrWorkConflict = errors.New("work_revision_or_intent_conflict")

func workHash(v any) string {
	b, _ := json.Marshal(v)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func (s *Store) installWorklist() error {
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS jarvis_work_items(scope TEXT NOT NULL,manager TEXT NOT NULL,item_id TEXT NOT NULL,revision INTEGER NOT NULL,payload BLOB NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(scope,manager,item_id));
 CREATE TABLE IF NOT EXISTS jarvis_work_ops(scope TEXT NOT NULL,intent TEXT NOT NULL,manager TEXT NOT NULL,input_hash TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(scope,intent));
 CREATE TABLE IF NOT EXISTS task_native_starts(session_id TEXT NOT NULL,request_id TEXT NOT NULL,event_id TEXT NOT NULL,payload BLOB NOT NULL,observed_at INTEGER NOT NULL,PRIMARY KEY(session_id,request_id),UNIQUE(event_id));
 CREATE TABLE IF NOT EXISTS task_native_start_attempts(session_id TEXT NOT NULL,request_id TEXT NOT NULL,token TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(session_id,request_id,token));
 CREATE TABLE IF NOT EXISTS jarvis_notices(scope TEXT NOT NULL,manager TEXT NOT NULL,event_id TEXT NOT NULL,channel TEXT NOT NULL,state TEXT NOT NULL,payload BLOB NOT NULL,expires_at INTEGER NOT NULL,PRIMARY KEY(scope,manager,event_id,channel));`)
	return err
}
func validWork(in WorkMutation) bool {
	if len(in.IntentID) < 10 || len(in.IntentID) > 90 || !strings.HasPrefix(in.IntentID, "r_") || len(in.ID) < 1 || len(in.ID) > 120 || strings.TrimSpace(in.Title) == "" || len([]rune(in.Title)) > 180 || len(in.Body) > 32000 || len(in.Links) > 100 || len(in.SourceRefs) > 30 {
		return false
	}
	switch in.Kind {
	case "discussion_note", "planning", "execution", "incoming_share", "context_reset":
	default:
		return false
	}
	switch in.RecordingState {
	case "recorded", "investigating", "plan_draft", "plan_ready", "awaiting_user_decision":
	default:
		return false
	}
	if in.RecordingState == "plan_ready" || in.RecordingState == "awaiting_user_decision" {
		if in.Plan == nil || len(in.Plan.SHA256) != 64 || in.Plan.Path == "" || in.Plan.Revision == 0 || len(in.Plan.Checklist) < 6 {
			return false
		}
	}
	if in.Plan != nil {
		seen := map[string]bool{}
		for _, v := range in.Plan.Checklist {
			v = strings.TrimSpace(v)
			if v == "" || seen[v] || len(v) > 240 {
				return false
			}
			seen[v] = true
		}
	}
	if in.Decision != nil {
		switch in.Decision.State {
		case "pending", "approved", "declined", "deferred":
		default:
			return false
		}
		if in.Decision.State != "pending" && (in.Decision.SourceRef == "" || in.Decision.Instruction == "") {
			return false
		}
	}
	return true
}
func (s *Store) MutateWork(ctx context.Context, scope, manager string, in WorkMutation) (WorkItem, error) {
	if scope == "" || manager == "" || !validWork(in) {
		return WorkItem{}, errors.New("invalid_work_item")
	}
	// Server revision/timestamps cannot be supplied to force a merge.
	in.Revision = 0
	in.CreatedAt = 0
	in.UpdatedAt = 0
	hash := workHash(struct {
		Manager string
		Input   WorkMutation
	}{manager, in})
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return WorkItem{}, e
	}
	defer tx.Rollback()
	var priorHash string
	var raw []byte
	e = tx.QueryRowContext(ctx, `SELECT input_hash,payload FROM jarvis_work_ops WHERE scope=? AND intent=?`, scope, in.IntentID).Scan(&priorHash, &raw)
	if e == nil {
		if priorHash != hash {
			return WorkItem{}, ErrWorkConflict
		}
		var old WorkItem
		e = json.Unmarshal(raw, &old)
		return old, e
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return WorkItem{}, e
	}
	var old WorkItem
	var revision uint64
	e = tx.QueryRowContext(ctx, `SELECT revision,payload FROM jarvis_work_items WHERE scope=? AND manager=? AND item_id=?`, scope, manager, in.ID).Scan(&revision, &raw)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return WorkItem{}, e
	}
	if revision != in.ExpectedRevision {
		return WorkItem{}, ErrWorkConflict
	}
	if e == nil && json.Unmarshal(raw, &old) != nil {
		return WorkItem{}, errors.New("corrupt_work_item")
	}
	var bytes int64
	if e := tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT SUM(length(payload)) FROM jarvis_work_items WHERE scope=?),0)+COALESCE((SELECT SUM(length(payload)) FROM jarvis_work_ops WHERE scope=?),0)`, scope, scope).Scan(&bytes); e != nil {
		return WorkItem{}, e
	}
	if bytes > 64<<20 {
		return WorkItem{}, errors.New("metadata_capacity_reached_original_records_preserved")
	}
	// Links are append-only: historical failed/unknown rounds cannot disappear.
	if revision > 0 {
		for _, l := range old.Links {
			found := false
			for _, n := range in.Links {
				if l == n {
					found = true
				}
			}
			if !found {
				return WorkItem{}, errors.New("historical_link_removal_forbidden")
			}
		}
	}
	out := in.WorkItem
	out.Revision = revision + 1
	out.CreatedAt = old.CreatedAt
	if out.CreatedAt == 0 {
		out.CreatedAt = time.Now().UnixMilli()
	}
	out.UpdatedAt = time.Now().UnixMilli()
	if out.Links == nil {
		out.Links = []WorkLink{}
	}
	if out.SourceRefs == nil {
		out.SourceRefs = []string{}
	}
	raw, _ = json.Marshal(out)
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_work_items VALUES(?,?,?,?,?,?) ON CONFLICT(scope,manager,item_id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload,updated_at=excluded.updated_at`, scope, manager, out.ID, out.Revision, raw, out.UpdatedAt); e != nil {
		return WorkItem{}, e
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_work_ops VALUES(?,?,?,?,?)`, scope, in.IntentID, manager, hash, raw); e != nil {
		return WorkItem{}, e
	}
	return out, tx.Commit()
}
func (s *Store) WorkItems(ctx context.Context, scope, manager string, after string, limit int) ([]WorkItem, string, error) {
	if limit < 1 || limit > 30 {
		limit = 30
	}
	rows, e := s.db.QueryContext(ctx, `SELECT payload FROM jarvis_work_items WHERE scope=? AND manager=? AND item_id>? ORDER BY item_id LIMIT ?`, scope, manager, after, limit+1)
	if e != nil {
		return nil, "", e
	}
	defer rows.Close()
	out := []WorkItem{}
	next := ""
	for rows.Next() {
		var raw []byte
		if e = rows.Scan(&raw); e != nil {
			return nil, "", e
		}
		var v WorkItem
		if e = json.Unmarshal(raw, &v); e != nil {
			return nil, "", e
		}
		out = append(out, v)
	}
	if e = rows.Err(); e != nil {
		return nil, "", e
	}
	if len(out) > limit {
		out = out[:limit]
		next = out[len(out)-1].ID
	}
	return out, next, nil
}
func (s *Store) WorkItem(ctx context.Context, scope, manager, id string) (WorkItem, error) {
	var raw []byte
	var v WorkItem
	e := s.db.QueryRowContext(ctx, `SELECT payload FROM jarvis_work_items WHERE scope=? AND manager=? AND item_id=?`, scope, manager, id).Scan(&raw)
	if e == nil {
		e = json.Unmarshal(raw, &v)
	}
	return v, e
}

// Typed live proof. Recovery/history/generic runtime are intentionally absent.
type NativeStart struct {
	SessionID  string `json:"session_id"`
	RequestID  string `json:"request_id"`
	Provider   string `json:"provider"`
	ThreadID   string `json:"thread_id"`
	Token      string `json:"token"`
	TokenKind  string `json:"token_kind"`
	ProofKind  string `json:"proof_kind"`
	ObservedAt int64  `json:"observed_at_ms"`
	EventID    string `json:"event_id"`
}

func (s *Store) RecordNativeStart(p NativeStart) (bool, error) {
	if p.SessionID == "" || p.RequestID == "" || p.ThreadID == "" || p.Token == "" || p.ObservedAt <= 0 {
		return false, errors.New("invalid_native_start")
	}
	if !(p.Provider == "codex" && p.TokenKind == "turn_id" && p.ProofKind == "live_turn_start_in_progress") && !(p.Provider == "claude" && p.TokenKind == "message_uuid" && p.ProofKind == "live_assistant_after_input_ack") {
		return false, errors.New("unsupported_start_proof")
	}
	p.EventID = "start_" + workHash([]string{p.SessionID, p.RequestID, "first_started"})
	raw, _ := json.Marshal(p)
	created := false
	err := s.TaskJournal().Transaction(context.Background(), func(tx *sql.Tx) error {
		var state string
		if e := tx.QueryRow(`SELECT state FROM queue_commands WHERE session_id=? AND request_id=?`, p.SessionID, p.RequestID).Scan(&state); e != nil {
			return e
		}
		if state != string(Running) {
			return errors.New("non_live_queue_request")
		}
		if p.Provider == "codex" {
			var thread, token string
			err := tx.QueryRow(`SELECT thread_id,turn_id FROM task_native_acceptance WHERE session_id=? AND request_id=?`, p.SessionID, p.RequestID).Scan(&thread, &token)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err != nil || thread != p.ThreadID || token != p.Token {
				var count int
				if e := tx.QueryRow(`SELECT COUNT(*) FROM recovery_attempts a JOIN recovery_runs r ON a.session_id=r.session_id AND a.request_id=r.request_id AND a.attempt_id=json_extract(r.payload,'$.last_attempt_id') WHERE r.session_id=? AND r.request_id=? AND r.state NOT IN ('cancelled','failed_permanent','disabled') AND json_extract(a.payload,'$.thread_id')=? AND json_extract(a.payload,'$.turn_id')=? AND json_extract(a.payload,'$.acceptance')='accepted'`, p.SessionID, p.RequestID, p.ThreadID, p.Token).Scan(&count); e != nil || count != 1 {
					return errors.New("missing_exact_native_acceptance")
				}
			}
		}

		if p.Provider == "claude" {
			var thread, token string
			if e := tx.QueryRow(`SELECT conversation_id,token FROM task_api_provider_evidence WHERE session_id=? AND request_id=?`, p.SessionID, p.RequestID).Scan(&thread, &token); e != nil {
				return e
			}
			if thread != p.ThreadID || token != p.Token {
				return ErrNativeConflict
			}
		}
		if _, e := tx.Exec(`INSERT OR IGNORE INTO task_native_start_attempts VALUES(?,?,?,?)`, p.SessionID, p.RequestID, p.Token, raw); e != nil {
			return e
		}
		r, e := tx.Exec(`INSERT OR IGNORE INTO task_native_starts VALUES(?,?,?,?,?)`, p.SessionID, p.RequestID, p.EventID, raw, p.ObservedAt)
		if e != nil {
			return e
		}
		n, e := r.RowsAffected()
		if e != nil {
			return e
		}
		created = n > 0
		if !created {
			return nil
		}
		return taskapi.ChangeTx(tx, p.SessionID, p.RequestID, "native_live_started")
	})
	return created, err
}
func (s *Store) NativeStart(session, request string) (NativeStart, bool, error) {
	var raw []byte
	var out NativeStart
	e := s.db.QueryRow(`SELECT payload FROM task_native_starts WHERE session_id=? AND request_id=?`, session, request).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return out, false, nil
	}
	if e == nil {
		e = json.Unmarshal(raw, &out)
	}
	return out, e == nil, e
}

type WorkNotice struct {
	EventID   string         `json:"event_id"`
	Channel   string         `json:"channel"`
	State     string         `json:"state"`
	Start     NativeStart    `json:"start"`
	ExpiresAt int64          `json:"expires_at_ms"`
	Receipts  map[string]any `json:"receipts"`
}

func (s *Store) WorkNotices(ctx context.Context, scope, manager string, links []WorkLink) ([]WorkNotice, error) {
	out := []WorkNotice{}
	seen := map[string]bool{}
	for _, l := range links {
		p, ok, e := s.NativeStart(l.SessionID, l.RequestID)
		if e != nil {
			return nil, e
		}
		if !ok || seen[p.EventID] {
			continue
		}
		seen[p.EventID] = true
		n := WorkNotice{EventID: p.EventID, Channel: "visual", State: "available", Start: p, ExpiresAt: p.ObservedAt + 24*60*60*1000, Receipts: map[string]any{"device_received": "unknown", "displayed": "unknown", "opened": "unknown", "explicit_read": "unknown"}}
		if time.Now().UnixMilli() > n.ExpiresAt {
			n.State = "expired"
		}
		raw, _ := json.Marshal(n)
		if _, e = s.db.ExecContext(ctx, `INSERT OR IGNORE INTO jarvis_notices VALUES(?,?,?,?,?,?,?)`, scope, manager, n.EventID, n.Channel, n.State, raw, n.ExpiresAt); e != nil {
			return nil, e
		}
		var old []byte
		if e = s.db.QueryRowContext(ctx, `SELECT payload FROM jarvis_notices WHERE scope=? AND manager=? AND event_id=? AND channel='visual'`, scope, manager, n.EventID).Scan(&old); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(old, &n); e != nil {
			return nil, e
		}
		if time.Now().UnixMilli() > n.ExpiresAt {
			n.State = "expired"
		}
		out = append(out, n)
	}
	return out, nil
}
func (s *Store) WorkCursor(scope, manager, after string) string {
	return fmt.Sprintf("%s.%s", workHash([]string{scope, manager}), after)
}

func (s *Store) WorkOperation(ctx context.Context, scope, manager, id string) (WorkItem, error) {
	var raw []byte
	var v WorkItem
	e := s.db.QueryRowContext(ctx, `SELECT payload FROM jarvis_work_ops WHERE scope=? AND manager=? AND intent=?`, scope, manager, id).Scan(&raw)
	if e == nil {
		e = json.Unmarshal(raw, &v)
	}
	return v, e
}

type NoticeSubscriber struct {
	Scope   string          `json:"-"`
	Manager string          `json:"manager"`
	Input   json.RawMessage `json:"input"`
	Enabled bool            `json:"enabled"`
}
type StartPush struct {
	Scope     string          `json:"-"`
	Manager   string          `json:"manager"`
	EventID   string          `json:"event_id"`
	Status    string          `json:"status"`
	Payload   json.RawMessage `json:"payload"`
	ExpiresAt int64           `json:"expires_at_ms"`
	Receipt   json.RawMessage `json:"receipt"`
}

func (s *Store) installStartPush() error {
	_, e := s.db.Exec(`CREATE TABLE IF NOT EXISTS jarvis_notice_policy_revisions(scope TEXT NOT NULL,manager TEXT NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(scope,manager));CREATE TABLE IF NOT EXISTS jarvis_notice_policy_ops(scope TEXT NOT NULL,intent TEXT NOT NULL,manager TEXT NOT NULL,hash TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(scope,intent));CREATE TABLE IF NOT EXISTS jarvis_notice_subscribers(scope TEXT NOT NULL,manager TEXT NOT NULL,input BLOB NOT NULL,enabled INTEGER NOT NULL,PRIMARY KEY(scope,manager));CREATE TABLE IF NOT EXISTS jarvis_start_push(scope TEXT NOT NULL,manager TEXT NOT NULL,event_id TEXT NOT NULL,state TEXT NOT NULL,payload BLOB NOT NULL,expires INTEGER NOT NULL,receipt BLOB NOT NULL,PRIMARY KEY(scope,manager,event_id));`)
	return e
}
func (s *Store) SubscribeStart(ctx context.Context, v NoticeSubscriber) error {
	_, e := s.db.ExecContext(ctx, `INSERT INTO jarvis_notice_subscribers VALUES(?,?,?,?) ON CONFLICT(scope,manager) DO UPDATE SET input=excluded.input,enabled=excluded.enabled`, v.Scope, v.Manager, []byte(v.Input), v.Enabled)
	return e
}
func (s *Store) StartSubscribers(ctx context.Context) ([]NoticeSubscriber, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT scope,manager,input,enabled FROM jarvis_notice_subscribers WHERE enabled=1 LIMIT 500`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []NoticeSubscriber{}
	for rows.Next() {
		var v NoticeSubscriber
		if e = rows.Scan(&v.Scope, &v.Manager, &v.Input, &v.Enabled); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) RecentNativeStarts(ctx context.Context, since int64) ([]NativeStart, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT payload FROM task_native_starts WHERE observed_at>? ORDER BY observed_at DESC LIMIT 100`, since)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []NativeStart{}
	for rows.Next() {
		var raw []byte
		var p NativeStart
		if e = rows.Scan(&raw); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(raw, &p); e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Store) PrepareStartPush(ctx context.Context, p StartPush) error {
	_, e := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO jarvis_start_push VALUES(?,?,?,'prepared',?,?,?)`, p.Scope, p.Manager, p.EventID, []byte(p.Payload), p.ExpiresAt, []byte(`{}`))
	return e
}
func (s *Store) ClaimStartPush(ctx context.Context, p StartPush) (bool, error) {
	r, e := s.db.ExecContext(ctx, `UPDATE jarvis_start_push SET state='issued' WHERE scope=? AND manager=? AND event_id=? AND state='prepared' AND expires>?`, p.Scope, p.Manager, p.EventID, time.Now().UnixMilli())
	if e != nil {
		return false, e
	}
	n, e := r.RowsAffected()
	return n == 1, e
}
func (s *Store) FinishStartPush(ctx context.Context, p StartPush, state string, receipt json.RawMessage) error {
	_, e := s.db.ExecContext(ctx, `UPDATE jarvis_start_push SET state=?,receipt=? WHERE scope=? AND manager=? AND event_id=? AND state='issued'`, state, []byte(receipt), p.Scope, p.Manager, p.EventID)
	return e
}
func (s *Store) StartPushes(ctx context.Context, scope, manager string) ([]StartPush, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT event_id,state,payload,expires,receipt FROM jarvis_start_push WHERE scope=? AND manager=? ORDER BY rowid DESC LIMIT 100`, scope, manager)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []StartPush{}
	for rows.Next() {
		v := StartPush{Scope: scope, Manager: manager}
		if e = rows.Scan(&v.EventID, &v.Status, &v.Payload, &v.ExpiresAt, &v.Receipt); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

type NoticePolicyReceipt struct {
	IntentID          string `json:"intent_id"`
	Revision          uint64 `json:"revision"`
	Enabled           bool   `json:"enabled"`
	PushSchemaVersion int    `json:"push_schema_version"`
}

func (s *Store) SetNoticePolicy(ctx context.Context, v NoticeSubscriber, intent string, expected uint64) (NoticePolicyReceipt, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return NoticePolicyReceipt{}, e
	}
	defer tx.Rollback()
	hash := workHash(v)
	var oldHash string
	var raw []byte
	e = tx.QueryRowContext(ctx, `SELECT hash,payload FROM jarvis_notice_policy_ops WHERE scope=? AND intent=?`, v.Scope, intent).Scan(&oldHash, &raw)
	if e == nil {
		var old NoticePolicyReceipt
		if hash != oldHash {
			return old, ErrWorkConflict
		}
		e = json.Unmarshal(raw, &old)
		return old, e
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return NoticePolicyReceipt{}, e
	}
	var rev uint64
	e = tx.QueryRowContext(ctx, `SELECT revision FROM jarvis_notice_policy_revisions WHERE scope=? AND manager=?`, v.Scope, v.Manager).Scan(&rev)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return NoticePolicyReceipt{}, e
	}
	if rev != expected {
		return NoticePolicyReceipt{}, ErrWorkConflict
	}
	out := NoticePolicyReceipt{intent, rev + 1, v.Enabled, 1}
	raw, _ = json.Marshal(out)
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_notice_subscribers VALUES(?,?,?,?) ON CONFLICT(scope,manager) DO UPDATE SET input=excluded.input,enabled=excluded.enabled`, v.Scope, v.Manager, []byte(v.Input), v.Enabled); e != nil {
		return out, e
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_notice_policy_revisions VALUES(?,?,?) ON CONFLICT(scope,manager) DO UPDATE SET revision=excluded.revision`, v.Scope, v.Manager, out.Revision); e != nil {
		return out, e
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO jarvis_notice_policy_ops VALUES(?,?,?,?,?)`, v.Scope, intent, v.Manager, hash, raw); e != nil {
		return out, e
	}
	return out, tx.Commit()
}
func (s *Store) CurrentNoticePolicy(ctx context.Context, scope, manager string) (NoticeSubscriber, NoticePolicyReceipt, error) {
	v := NoticeSubscriber{Scope: scope, Manager: manager}
	var receipt NoticePolicyReceipt
	e := s.db.QueryRowContext(ctx, `SELECT s.input,s.enabled,r.revision FROM jarvis_notice_subscribers s JOIN jarvis_notice_policy_revisions r ON s.scope=r.scope AND s.manager=r.manager WHERE s.scope=? AND s.manager=?`, scope, manager).Scan(&v.Input, &v.Enabled, &receipt.Revision)
	receipt.Enabled = v.Enabled
	receipt.PushSchemaVersion = 1
	return v, receipt, e
}
func (s *Store) NoticePolicyOperation(ctx context.Context, scope, manager, intent string) (NoticePolicyReceipt, error) {
	var raw []byte
	var v NoticePolicyReceipt
	e := s.db.QueryRowContext(ctx, `SELECT payload FROM jarvis_notice_policy_ops WHERE scope=? AND manager=? AND intent=?`, scope, manager, intent).Scan(&raw)
	if e == nil {
		e = json.Unmarshal(raw, &v)
	}
	return v, e
}
