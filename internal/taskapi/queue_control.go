package taskapi

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
)

// QueueControl metadata lives on each ORIGINAL owner's connection. It stores
// neither execution state nor callbacks and cannot admit, wake or schedule work.
const queueControlDDL = `
CREATE TABLE IF NOT EXISTS unified_queue_ranks(queue_key TEXT NOT NULL,item_id TEXT NOT NULL,rank INTEGER NOT NULL,PRIMARY KEY(queue_key,item_id));
CREATE TABLE IF NOT EXISTS unified_queue_operations(scope TEXT NOT NULL,generation INTEGER NOT NULL,operation TEXT NOT NULL,intent TEXT NOT NULL,input_hash TEXT NOT NULL,receipt BLOB NOT NULL,PRIMARY KEY(scope,generation,operation,intent));
CREATE TABLE IF NOT EXISTS unified_version_bindings(queue_key TEXT NOT NULL,item_id TEXT NOT NULL,version INTEGER NOT NULL,binding BLOB NOT NULL,PRIMARY KEY(queue_key,item_id,version));
CREATE TABLE IF NOT EXISTS unified_input_versions(queue_key TEXT NOT NULL,item_id TEXT NOT NULL,version INTEGER NOT NULL,payload BLOB NOT NULL,content TEXT NOT NULL,hash TEXT NOT NULL,original_hash TEXT NOT NULL,PRIMARY KEY(queue_key,item_id,version));
CREATE TABLE IF NOT EXISTS unified_documents(id TEXT PRIMARY KEY,owner_session TEXT NOT NULL,owner_request TEXT NOT NULL,seal_id TEXT NOT NULL,hash TEXT NOT NULL,path_identity TEXT NOT NULL,manifest BLOB NOT NULL,body BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS unified_document_grants(id TEXT PRIMARY KEY,document TEXT NOT NULL,owner_scope TEXT NOT NULL,owner_generation INTEGER NOT NULL,consumer BLOB NOT NULL,purpose TEXT NOT NULL,expires INTEGER NOT NULL,revoked INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS unified_reply_payloads(item_id TEXT PRIMARY KEY,payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS unified_reply_sources(session_id TEXT NOT NULL,request_id TEXT NOT NULL,scope TEXT NOT NULL,generation INTEGER NOT NULL,body BLOB NOT NULL,PRIMARY KEY(session_id,request_id));
CREATE TABLE IF NOT EXISTS unified_replies(scope TEXT NOT NULL,generation INTEGER NOT NULL,intent TEXT NOT NULL,input_hash TEXT NOT NULL,body BLOB NOT NULL,PRIMARY KEY(scope,generation,intent));`

func InstallUnified(db *sql.DB) error { _, e := db.Exec(queueControlDDL + facadeDDL); return e }

type QueueRow struct {
	ID              string `json:"item_id"`
	State           string `json:"state"`
	Rank            int64  `json:"-"`
	Sequence        int64  `json:"audit_sequence"`
	ContentRevision uint64 `json:"content_revision"`
	ContentHash     string `json:"content_hash"`
	Eligible        bool   `json:"eligible"`
	Barrier         string `json:"barrier_reason,omitempty"`
	Parent          string `json:"owner_session_id"`
	Session         string `json:"session_id"`
	Request         string `json:"request_id"`
}
type QueueView struct {
	Revision       uint64     `json:"revision"`
	MembershipHash string     `json:"membership_hash"`
	Items          []QueueRow `json:"items"`
}
type QueueMutation struct {
	PublicHandles                                                      map[string]string
	GrantRevisions                                                     map[string]uint64
	AllowAttachments                                                   bool
	Binding                                                            []byte
	RoleSession                                                        string
	RoleRevision                                                       uint64
	Scope                                                              string
	Generation                                                         uint64
	Intent, Hash, Operation, Item, Relative, Placement, MembershipHash string
	ExpectedRevision, ExpectedContentRevision                          uint64
	ExpectedHash, Content                                              string
	Payload                                                            []byte
	Revalidate                                                         func(context.Context) error
}
type MutationReceipt struct {
	Intent string          `json:"intent_id"`
	Hash   string          `json:"input_hash"`
	State  string          `json:"state"`
	Data   json.RawMessage `json:"data"`
}

func Digest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func RawDigest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func QueueFingerprint(rows []QueueRow) QueueView {
	if rows == nil {
		rows = []QueueRow{}
	}
	for i := range rows {
		if rows[i].Rank == 0 {
			rows[i].Rank = rows[i].Sequence
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Rank == rows[j].Rank {
			return rows[i].Sequence < rows[j].Sequence
		}
		return rows[i].Rank < rows[j].Rank
	})
	hash := Digest(rows)
	return QueueView{MembershipHash: hash, Items: rows}
}
func LoadRanks(ctx context.Context, tx *sql.Tx, key string, rows []QueueRow) error {
	for i := range rows {
		var rank int64
		e := tx.QueryRowContext(ctx, "SELECT rank FROM unified_queue_ranks WHERE queue_key=? AND item_id=?", key, rows[i].ID).Scan(&rank)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if e == nil {
			rows[i].Rank = rank
		}
	}
	return nil
}

// Revisions are full membership fingerprints, not a mutable timestamp. Revision
// derives from all canonical status/version/order values and is <= JS safe int.
func ViewRevision(v QueueView) uint64 {
	h := sha256.Sum256([]byte(v.MembershipHash))
	var n uint64
	for _, b := range h[:6] {
		n = n<<8 | uint64(b)
	}
	return n
}
func OriginalOperation(ctx context.Context, tx *sql.Tx, m QueueMutation) (MutationReceipt, bool, error) {
	var hash string
	var b []byte
	e := tx.QueryRowContext(ctx, "SELECT input_hash,receipt FROM unified_queue_operations WHERE scope=? AND generation=? AND operation=? AND intent=?", m.Scope, m.Generation, m.Operation, m.Intent).Scan(&hash, &b)
	if errors.Is(e, sql.ErrNoRows) {
		return MutationReceipt{}, false, nil
	}
	if e != nil {
		return MutationReceipt{}, false, e
	}
	if m.Hash != "" && hash != m.Hash {
		return MutationReceipt{}, true, Failure("idempotency_conflict", "known_receipt", "lookup_original")
	}
	var r MutationReceipt
	e = json.Unmarshal(b, &r)
	return r, true, e
}
func SaveOperation(ctx context.Context, tx *sql.Tx, m QueueMutation, data any) (MutationReceipt, error) {
	raw, e := json.Marshal(data)
	if e != nil {
		return MutationReceipt{}, e
	}
	r := MutationReceipt{m.Intent, m.Hash, "applied", raw}
	b, _ := json.Marshal(r)
	_, e = tx.ExecContext(ctx, "INSERT INTO unified_queue_operations VALUES(?,?,?,?,?,?)", m.Scope, m.Generation, m.Operation, m.Intent, m.Hash, b)
	return r, e
}
func CheckQueueMutation(ctx context.Context, tx *sql.Tx, key string, rows []QueueRow, m QueueMutation) (QueueView, error) {
	if m.Revalidate == nil {
		return QueueView{}, Failure("caller_unbound", "known_none", "refresh_identity")
	}
	if e := m.Revalidate(ctx); e != nil {
		return QueueView{}, e
	}
	if e := LoadRanks(ctx, tx, key, rows); e != nil {
		return QueueView{}, e
	}
	v := QueueFingerprint(rows)
	v.Revision = ViewRevision(v)
	if v.Revision != m.ExpectedRevision || m.MembershipHash != "" && v.MembershipHash != m.MembershipHash {
		return v, Failure("stale_revision", "known_none", "refresh_snapshot")
	}
	return v, nil
}
func MoveRanks(ctx context.Context, tx *sql.Tx, key string, v QueueView, m QueueMutation) ([]string, error) {
	a, b := -1, -1
	for i, r := range v.Items {
		if r.ID == m.Item {
			a = i
		}
		if r.ID == m.Relative {
			b = i
		}
	}
	if a < 0 || b < 0 || a == b {
		return nil, Failure("invalid_argument", "known_none", "refresh_snapshot")
	}
	lo, hi := a, b
	if lo > hi {
		lo, hi = hi, lo
	}
	for _, r := range v.Items[lo : hi+1] {
		if !r.Eligible || r.State != "queued" {
			return nil, Failure("queue_barrier", "known_none", "refresh_snapshot")
		}
	}
	out := append([]QueueRow{}, v.Items...)
	moving := out[a]
	out = append(out[:a], out[a+1:]...)
	idx := 0
	for i, r := range out {
		if r.ID == m.Relative {
			idx = i
			break
		}
	}
	if m.Placement == "after" {
		idx++
	} else if m.Placement != "before" {
		return nil, Failure("invalid_argument", "known_none", "correct_input")
	}
	out = append(out, QueueRow{})
	copy(out[idx+1:], out[idx:])
	out[idx] = moving
	ids := []string{}
	for i, r := range out {
		ids = append(ids, r.ID)
		if _, e := tx.ExecContext(ctx, "INSERT INTO unified_queue_ranks VALUES(?,?,?) ON CONFLICT(queue_key,item_id) DO UPDATE SET rank=excluded.rank", key, r.ID, int64(i+1)); e != nil {
			return nil, e
		}
	}
	return ids, nil
}

func PublicQueueOrder(ids []string, m QueueMutation) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id
		if mapped := m.PublicHandles[id]; mapped != "" {
			out[i] = mapped
		}
	}
	return out
}
