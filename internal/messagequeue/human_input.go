package messagequeue

// Human input reservations and their operation receipts share the ORIGINAL
// queue database. Reserving an upload is not Enqueue: no prepared payload exists
// until the exact ready seal joins the original queue transaction.
import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
	"fmt"
	"time"
)

const HumanInputLimit = 64
const HumanEditTTL = 120 * time.Second

type HumanBinding struct {
	Device     string `json:"device"`
	Authority  string `json:"authority"`
	Session    string `json:"session"`
	Thread     string `json:"thread"`
	Config     uint64 `json:"config"`
	Epoch      uint64 `json:"epoch"`
	Generation uint64 `json:"generation"`
}
type HumanLease struct {
	ID           string `json:"id"`
	Generation   uint64 `json:"generation"`
	Expires      int64  `json:"expires"`
	Boot         string `json:"boot"`
	BaseState    string `json:"base_state"`
	BaseRevision uint64 `json:"base_revision"`
	BaseProof    string `json:"base_proof"`
}
type HumanInput struct {
	Origin              string          `json:"origin_kind,omitempty"`
	ObservationRevision uint64          `json:"observation_revision"`
	WireHash            string          `json:"wire_hash,omitempty"`
	ID                  string          `json:"request_id"`
	Binding             HumanBinding    `json:"binding"`
	State               string          `json:"state"`
	Sequence            int64           `json:"sequence"`
	Position            int64           `json:"position"`
	Revision            uint64          `json:"revision"`
	ContentRevision     uint64          `json:"content_revision"`
	IntentHash          string          `json:"intent_hash"`
	EffectiveHash       string          `json:"effective_hash"`
	Content             string          `json:"content"`
	Assets              json.RawMessage `json:"assets"`
	ReadyProof          string          `json:"ready_proof"`
	Predecessor         string          `json:"predecessor"`
	CancelGeneration    uint64          `json:"cancel_generation"`
	Lease               *HumanLease     `json:"lease,omitempty"`
}
type HumanOperation struct {
	Origin           string          `json:"origin_kind,omitempty"`
	OriginalReserve  *HumanOperation `json:"original_reserve,omitempty"`
	QueueRevision    *uint64         `json:"queue_revision,omitempty"`
	MembershipHash   string          `json:"membership_hash,omitempty"`
	ID               string          `json:"operation_id"`
	Kind             string          `json:"kind"`
	Request          string          `json:"request_id"`
	ExpectedRevision uint64          `json:"expected_revision,omitempty"`
	ContentRevision  uint64          `json:"content_revision,omitempty"`
	ExpectedHash     string          `json:"expected_hash,omitempty"`
	IntentHash       string          `json:"intent_hash,omitempty"`
	Content          string          `json:"content,omitempty"`
	Assets           json.RawMessage `json:"assets,omitempty"`
	Predecessor      string          `json:"predecessor,omitempty"`
	Relative         string          `json:"relative,omitempty"`
	Placement        string          `json:"placement,omitempty"`
	LeaseID          string          `json:"lease_id,omitempty"`
	LeaseGeneration  uint64          `json:"lease_generation,omitempty"`
	ReadyProof       string          `json:"ready_proof,omitempty"`
}
type HumanReceipt struct {
	Body          json.RawMessage `json:"operation,omitempty"`
	Operation     string          `json:"operation_id"`
	Kind          string          `json:"kind"`
	Hash          string          `json:"input_hash"`
	Status        string          `json:"status"`
	Code          string          `json:"code,omitempty"`
	Input         *HumanInput     `json:"input,omitempty"`
	QueueRevision uint64          `json:"queue_revision"`
}
type HumanSeal struct {
	Boot         string
	Now          time.Time
	Clock        func() (string, time.Time)
	Prepared     bool
	Commit       func(func() error) error
	leaseExpires int64
	Binding      HumanBinding
	Operation    HumanOperation
	// Proof is supplied ONLY by the original media admission owner after its
	// manifest/source/cancel-generation checks; never by an unverified client.
	Proof string
}

func installHumanInputs(db *sql.DB) error {
	_, e := db.Exec(`CREATE TABLE IF NOT EXISTS input_reservations(session TEXT NOT NULL,id TEXT NOT NULL,body BLOB NOT NULL,PRIMARY KEY(session,id));
 CREATE TABLE IF NOT EXISTS message_order_nodes(sequence INTEGER PRIMARY KEY AUTOINCREMENT,session TEXT NOT NULL,id TEXT NOT NULL,rank INTEGER NOT NULL DEFAULT 0,UNIQUE(session,id));
 CREATE TABLE IF NOT EXISTS input_operations(device TEXT NOT NULL,authority TEXT NOT NULL,session TEXT NOT NULL,id TEXT NOT NULL,hash TEXT NOT NULL,receipt BLOB NOT NULL,binding BLOB NOT NULL,PRIMARY KEY(device,authority,session,id));
 CREATE TABLE IF NOT EXISTS input_prepared_operations(device TEXT NOT NULL,authority TEXT NOT NULL,session TEXT NOT NULL,id TEXT NOT NULL,body BLOB NOT NULL,PRIMARY KEY(device,authority,session,id));
 CREATE TABLE IF NOT EXISTS input_versions(session TEXT NOT NULL,id TEXT NOT NULL,revision INTEGER NOT NULL,body BLOB NOT NULL,PRIMARY KEY(session,id,revision));`)
	return e
}
func humanHash(v any) string { // normalize nested JSON object order, not struct field order
	b, _ := json.Marshal(v)
	var normal any
	_ = json.Unmarshal(b, &normal)
	return taskapi.Digest(normal)
}
func validHumanBinding(b HumanBinding) bool {
	return b.Device != "" && b.Authority != "" && b.Session != ""
}
func humanSameSource(a, b HumanBinding) bool { return a == b }
func humanNodeTx(tx *sql.Tx, session, id string) (HumanInput, bool, error) {
	var raw []byte
	var v HumanInput
	e := tx.QueryRow(`SELECT body FROM input_reservations WHERE session=? AND id=?`, session, id).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return v, false, nil
	}
	if e != nil {
		return v, false, e
	}
	e = json.Unmarshal(raw, &v)
	return v, e == nil, e
}
func humanSaveTx(tx *sql.Tx, v HumanInput) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = tx.Exec(`INSERT INTO input_reservations VALUES(?,?,?) ON CONFLICT(session,id) DO UPDATE SET body=excluded.body`, v.Binding.Session, v.ID, b)
	return e
}
func humanReceiptTx(tx *sql.Tx, b HumanBinding, id, hash string) (HumanReceipt, bool, error) {
	var v HumanReceipt
	var raw []byte
	var prior string
	var original []byte
	e := tx.QueryRow(`SELECT hash,receipt,binding FROM input_operations WHERE device=? AND authority=? AND session=? AND id=?`, b.Device, b.Authority, b.Session, id).Scan(&prior, &raw, &original)
	if errors.Is(e, sql.ErrNoRows) {
		return v, false, nil
	}
	if e != nil {
		return v, false, e
	}
	var binding HumanBinding
	if json.Unmarshal(original, &binding) != nil || binding != b {
		return HumanReceipt{}, false, ErrConflict
	}
	if hash != "" && hash != prior {
		return v, true, ErrConflict
	}
	e = json.Unmarshal(raw, &v)
	return v, true, e
}
func humanRecordTx(tx *sql.Tx, b HumanBinding, op HumanOperation, v *HumanInput, code string) (HumanReceipt, error) {
	var revision uint64
	_ = tx.QueryRow(`SELECT revision FROM queue_sessions WHERE session_id=?`, b.Session).Scan(&revision)
	if v != nil {
		v.ObservationRevision = revision
	}
	body, _ := json.Marshal(op)
	r := HumanReceipt{Body: body, Operation: op.ID, Kind: op.Kind, Hash: humanHash(op), Status: "committed", Code: code, Input: v, QueueRevision: revision}
	if code != "" {
		r.Status = "rejected_known_none"
	}
	raw, e := json.Marshal(r)
	if e != nil {
		return r, e
	}
	frozen, _ := json.Marshal(b)
	_, e = tx.Exec(`INSERT INTO input_operations(device,authority,session,id,hash,receipt,binding) VALUES(?,?,?,?,?,?,?)`, b.Device, b.Authority, b.Session, op.ID, r.Hash, raw, frozen)
	return r, e
}

// Existing ranks are imported once, in their actual original execution order.
// Every later admission joins this same order; a reserved head cannot be bypassed
// by the legacy message route.
func ensureHumanOrderTx(tx *sql.Tx, session string) error {
	rows, e := tx.Query(`SELECT request_id FROM queue_commands WHERE session_id=? AND state IN ('queued','running','steering','uncertain') ORDER BY COALESCE((SELECT rank FROM unified_queue_ranks WHERE queue_key=queue_commands.session_id AND item_id=request_id),seq),seq`, session)
	if e != nil {
		return e
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, id := range ids {
		if _, e = tx.Exec(`INSERT OR IGNORE INTO message_order_nodes(session,id) VALUES(?,?)`, session, id); e != nil {
			return e
		}
	}
	return nil
}
func orderRowsTx(tx *sql.Tx, session string) ([]string, error) {
	rows, e := tx.Query(`SELECT n.id FROM message_order_nodes n LEFT JOIN queue_commands q ON q.session_id=n.session AND q.request_id=n.id LEFT JOIN input_reservations i ON i.session=n.session AND i.id=n.id LEFT JOIN unified_queue_ranks u ON u.queue_key=n.session AND u.item_id=n.id WHERE n.session=? AND (q.state IN ('queued','running','steering','uncertain') OR (q.request_id IS NULL AND json_extract(i.body,'$.state') NOT IN ('cancelled','failed'))) ORDER BY COALESCE(u.rank,CASE WHEN n.rank=0 THEN n.sequence ELSE n.rank END),n.sequence`, session)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			return nil, e
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func (s *Store) ReadHumanOperation(b HumanBinding, id, hash string) (HumanReceipt, error) {
	tx, e := s.db.Begin()
	if e != nil {
		return HumanReceipt{}, e
	}
	defer tx.Rollback()
	v, found, e := humanReceiptTx(tx, b, id, hash)
	if e != nil {
		return v, e
	}
	if !found {
		return HumanReceipt{Operation: id, Status: "unseen_unknown"}, nil
	}
	return v, nil
}
func (s *Store) HumanInputs(b HumanBinding) ([]HumanInput, error) {
	rows, e := s.db.Query(`SELECT i.body,COALESCE(q.state,''),COALESCE(qs.revision,0),COALESCE(o.rank,0) FROM input_reservations i LEFT JOIN message_order_nodes o ON o.session=i.session AND o.id=i.id LEFT JOIN queue_commands q ON q.session_id=i.session AND q.request_id=i.id LEFT JOIN queue_sessions qs ON qs.session_id=i.session WHERE i.session=?`, b.Session)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []HumanInput{}
	for rows.Next() {
		var raw []byte
		var v HumanInput
		var execution string
		var observation uint64
		var position int64
		if e = rows.Scan(&raw, &execution, &observation, &position); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(raw, &v); e != nil {
			return nil, e
		}
		if humanSameSource(v.Binding, b) {
			v.ObservationRevision = observation
			v.Position = position
			if execution != "" && execution != "queued" {
				v.State = execution
			}
			out = append(out, v)
		}
	}
	return out, rows.Err()
}
func (s *Store) HumanOrder(session string) ([]string, error) {
	tx, e := s.db.Begin()
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	if e = ensureHumanOrderTx(tx, session); e != nil {
		return nil, e
	}
	v, e := orderRowsTx(tx, session)
	if e != nil {
		return nil, e
	}
	return v, tx.Commit()
}

// HumanReady is pure. Lease expiry is applied by a separate bounded recovery
// mutation, never by a read or by an arbitrary local wall clock.
func (s *Store) HumanReady(session, id string) bool {
	tx, e := s.db.Begin()
	if e != nil {
		return false
	}
	defer tx.Rollback()
	v, found, e := humanNodeTx(tx, session, id)
	if e != nil {
		return false
	}
	if found && (v.State != "ready_queued" || v.Lease != nil) {
		return false
	}
	ids, e := orderRowsTx(tx, session)
	if e != nil {
		return false
	}
	for _, other := range ids {
		if other == id {
			return true
		}
		var state string
		e = tx.QueryRow(`SELECT state FROM queue_commands WHERE session_id=? AND request_id=?`, session, other).Scan(&state)
		otherInput, isHuman, lookupErr := humanNodeTx(tx, session, other)
		if lookupErr != nil {
			return false
		}
		if state == "uncertain" && !isHuman {
			continue
		} // preserve legacy recovery; provider readiness still gates native input
		_ = otherInput
		if e != nil || state == "queued" || state == "running" || state == "steering" || (state == "uncertain" && isHuman) {
			return false
		}
	}
	// Old stores/synthetic tests without v2 membership retain old execution.
	return !found
}
func (s *Store) HumanApply(b HumanBinding, op HumanOperation, boot string, now time.Time) (HumanReceipt, error) {
	return s.HumanApplyOrder(b, op, boot, now, nil)
}
func (s *Store) HumanApplyOrder(b HumanBinding, op HumanOperation, boot string, now time.Time, orderCommit func([]string, func() error) error) (HumanReceipt, error) {
	if !validHumanBinding(b) || op.ID == "" || op.Request == "" || len(op.ID) > 160 || len(op.Request) > 160 {
		return HumanReceipt{}, fmt.Errorf("invalid_input")
	}
	tx, e := s.db.Begin()
	if e != nil {
		return HumanReceipt{}, e
	}
	defer tx.Rollback()
	if r, found, e := humanReceiptTx(tx, b, op.ID, humanHash(op)); e != nil || found {
		return r, e
	}
	if e = ensureHumanOrderTx(tx, b.Session); e != nil {
		return HumanReceipt{}, e
	}
	v, found, e := humanNodeTx(tx, b.Session, op.Request)
	if e != nil {
		return HumanReceipt{}, e
	}
	reject := func(code string) (HumanReceipt, error) {
		r, e := humanRecordTx(tx, b, op, nil, code)
		if e != nil {
			return r, e
		}
		return r, tx.Commit()
	}
	if op.Kind == "reserve" {
		if found {
			return reject("request_already_reserved")
		}
		if _, exists, e := lookup(tx, b.Session, op.Request); e != nil {
			return HumanReceipt{}, e
		} else if exists {
			return reject("request_exists")
		}
		ids, e := orderRowsTx(tx, b.Session)
		if e != nil {
			return HumanReceipt{}, e
		}
		_ = ids
		waiting, e := humanWaitingCountTx(tx, b.Session)
		if e != nil {
			return HumanReceipt{}, e
		}
		if waiting >= HumanInputLimit {
			return reject("queue_full")
		}
		if op.Predecessor != "" {
			var present int
			if e = tx.QueryRow(`SELECT COUNT(*) FROM message_order_nodes WHERE session=? AND id=?`, b.Session, op.Predecessor).Scan(&present); e != nil {
				return HumanReceipt{}, e
			}
			if present != 1 {
				return reject("predecessor_unknown")
			}
		}
		if (op.Origin != "" && op.Origin != "formal_intake") || len(op.IntentHash) != 64 || len(op.Content) > 32000 || len(op.Assets) > 262144 {
			return reject("invalid_intent")
		}
		var descriptors []map[string]any
		if len(op.Assets) > 0 && json.Unmarshal(op.Assets, &descriptors) != nil || len(descriptors) > 8 {
			return reject("invalid_assets")
		}
		if _, e = tx.Exec(`INSERT INTO message_order_nodes(session,id) VALUES(?,?)`, b.Session, op.Request); e != nil {
			return HumanReceipt{}, e
		}
		var seq int64
		if e = tx.QueryRow(`SELECT sequence FROM message_order_nodes WHERE session=? AND id=?`, b.Session, op.Request).Scan(&seq); e != nil {
			return HumanReceipt{}, e
		}
		assets := op.Assets
		if len(assets) == 0 {
			assets = json.RawMessage(`[]`)
		}
		v = HumanInput{Origin: op.Origin, ID: op.Request, Binding: b, State: "reserved_waiting_assets", Sequence: seq, Revision: 1, ContentRevision: 1, IntentHash: op.IntentHash, EffectiveHash: humanHash(map[string]any{"content": op.Content, "assets": assets}), Content: op.Content, Assets: assets, Predecessor: op.Predecessor}
	} else {
		if !found && op.Kind == "cancel" && op.OriginalReserve != nil {
			original := op.OriginalReserve
			if original.Kind != "reserve" || original.Request != op.Request || original.ID == "" || original.OriginalReserve != nil || len(original.IntentHash) != 64 || op.ExpectedRevision != 0 {
				return reject("invalid_original_reserve")
			}
			if _, exists, err := lookup(tx, b.Session, op.Request); err != nil {
				return HumanReceipt{}, err
			} else if exists {
				return reject("request_exists")
			}
			// A cancellation fence owns no runnable slot. It durably prevents a
			// delayed original reserve from resurrecting this same business ID.
			assets := original.Assets
			if len(assets) == 0 {
				assets = json.RawMessage(`[]`)
			}
			var descriptors []any
			if json.Unmarshal(assets, &descriptors) != nil || len(descriptors) > 8 {
				return reject("invalid_original_reserve")
			}
			v = HumanInput{Origin: original.Origin, ID: op.Request, Binding: b, State: "cancelled", Revision: 1, ContentRevision: 1, IntentHash: original.IntentHash, Content: original.Content, Assets: assets, EffectiveHash: humanHash(map[string]any{"content": original.Content, "assets": assets}), CancelGeneration: 1}
			if err := humanSaveTx(tx, v); err != nil {
				return HumanReceipt{}, err
			}
			if err := bump(tx, b.Session); err != nil {
				return HumanReceipt{}, err
			}
			r, err := humanRecordTx(tx, b, op, &v, "")
			if err != nil {
				return r, err
			}
			return r, tx.Commit()
		}
		if !found || !humanSameSource(v.Binding, b) {
			return reject("source_unavailable")
		}
		if v.State == "cancelled" {
			return reject("already_cancelled")
		}
		if op.ExpectedRevision != v.Revision {
			return reject("stale_revision")
		}
		if op.Kind == "begin_edit" || op.Kind == "move" {
			var revision uint64
			if e = tx.QueryRow(`SELECT revision FROM queue_sessions WHERE session_id=?`, b.Session).Scan(&revision); e != nil {
				return HumanReceipt{}, e
			}
			ids, e := orderRowsTx(tx, b.Session)
			if e != nil {
				return HumanReceipt{}, e
			}
			if op.QueueRevision == nil || *op.QueueRevision != revision || op.MembershipHash != humanHash(map[string]any{"binding": b, "order": ids}) {
				return reject("queue_membership_changed")
			}
		}

		entry, hasEntry, e := lookup(tx, b.Session, op.Request)
		if e != nil {
			return HumanReceipt{}, e
		}
		if hasEntry && entry.State != Queued {
			return reject("native_boundary")
		}
		var accepted int
		if e = tx.QueryRow(`SELECT COUNT(*) FROM task_native_acceptance WHERE session_id=? AND request_id=?`, b.Session, op.Request).Scan(&accepted); e != nil {
			return HumanReceipt{}, e
		}
		conflicted, err := humanNativeConflictTx(tx, b.Session, op.Request)
		if err != nil {
			return HumanReceipt{}, err
		}
		if accepted > 0 || conflicted {
			return reject("native_boundary")
		}
		leaseOK := v.Lease != nil && v.Lease.ID == op.LeaseID && v.Lease.Generation == op.LeaseGeneration && v.Lease.Boot == boot && now.UnixMilli() < v.Lease.Expires
		switch op.Kind {
		case "begin_edit":
			if v.Lease != nil {
				return reject("edit_busy")
			}
			if op.ContentRevision != v.ContentRevision || op.ExpectedHash != v.EffectiveHash {
				return reject("content_changed")
			}
			v.Lease = &HumanLease{ID: op.ID, Generation: v.Revision + 1, Boot: boot, Expires: now.Add(HumanEditTTL).UnixMilli(), BaseState: v.State, BaseRevision: v.ContentRevision, BaseProof: v.ReadyProof}
		case "renew_edit":
			if !leaseOK {
				return reject("edit_expired")
			}
			v.Lease.Expires = now.Add(HumanEditTTL).UnixMilli()
		case "abort_edit":
			if !leaseOK {
				return reject("edit_expired")
			}
			v.State = v.Lease.BaseState
			v.ReadyProof = v.Lease.BaseProof
			v.Lease = nil
		case "recover_edit":
			if v.Lease == nil || v.Lease.ID != op.LeaseID || v.Lease.Generation != op.LeaseGeneration || v.Lease.Boot == boot {
				return reject("edit_recovery_not_required")
			}
			v.State, v.ReadyProof, v.Lease = v.Lease.BaseState, v.Lease.BaseProof, nil
		case "cancel":
			v.State = "cancelled"
			v.CancelGeneration++
			v.Lease = nil
			if hasEntry {
				if _, e = tx.Exec(`UPDATE queue_commands SET state='cancelled',updated_at=? WHERE session_id=? AND request_id=? AND state='queued'`, now.UnixMilli(), b.Session, op.Request); e != nil {
					return HumanReceipt{}, e
				}
			}
		case "move":
			if v.Lease != nil {
				return reject("edit_busy")
			}
			ids, e := orderRowsTx(tx, b.Session)
			if e != nil {
				return HumanReceipt{}, e
			}
			from, to := -1, -1
			for i, id := range ids {
				if id == op.Request {
					from = i
				}
				if id == op.Relative {
					to = i
				}
			}
			if from < 0 || to < 0 || from == to {
				return reject("membership_changed")
			}
			lo, hi := from, to
			if lo > hi {
				lo, hi = hi, lo
			}
			for _, id := range ids[lo : hi+1] {
				other, ok, e := humanNodeTx(tx, b.Session, id)
				if e != nil {
					return HumanReceipt{}, e
				}
				if !ok || !humanSameSource(other.Binding, b) || other.Lease != nil {
					return reject("queue_barrier")
				}
				entry, exists, err := lookup(tx, b.Session, id)
				if err != nil {
					return HumanReceipt{}, err
				}
				var accepted int
				if err = tx.QueryRow(`SELECT COUNT(*) FROM task_native_acceptance WHERE session_id=? AND request_id=?`, b.Session, id).Scan(&accepted); err != nil {
					return HumanReceipt{}, err
				}
				conflicted, err := humanNativeConflictTx(tx, b.Session, id)
				if err != nil {
					return HumanReceipt{}, err
				}
				if exists && entry.State != Queued || accepted > 0 || conflicted {
					return reject("queue_barrier")
				}
			}
			if op.Placement != "before" && op.Placement != "after" {
				return reject("invalid_placement")
			}
			ordered := []string{}
			for _, id := range ids {
				if id != op.Request {
					ordered = append(ordered, id)
				}
			}
			to = -1
			for i, id := range ordered {
				if id == op.Relative {
					to = i
				}
			}
			if op.Placement == "after" {
				to++
			}
			ordered = append(ordered[:to], append([]string{op.Request}, ordered[to:]...)...)
			for i, id := range ordered {
				if _, e = tx.Exec(`UPDATE message_order_nodes SET rank=? WHERE session=? AND id=?`, i+1, b.Session, id); e != nil {
					return HumanReceipt{}, e
				}
				if _, e = tx.Exec(`INSERT INTO unified_queue_ranks VALUES(?,?,?) ON CONFLICT(queue_key,item_id) DO UPDATE SET rank=excluded.rank`, b.Session, id, i+1); e != nil {
					return HumanReceipt{}, e
				}
			}
		default:
			return reject("unsupported_operation")
		}
		v.Revision++
	}
	if e = humanSaveTx(tx, v); e != nil {
		return HumanReceipt{}, e
	}
	if e = bump(tx, b.Session); e != nil {
		return HumanReceipt{}, e
	}
	r, e := humanRecordTx(tx, b, op, &v, "")
	if e != nil {
		return r, e
	}
	if op.Kind == "move" && orderCommit != nil {
		order, err := orderRowsTx(tx, b.Session)
		if err != nil {
			return HumanReceipt{}, err
		}
		return r, orderCommit(order, tx.Commit)
	}
	return r, tx.Commit()
}

func humanNativeConflictTx(tx *sql.Tx, session, id string) (bool, error) {
	var conflict bool
	err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM task_native_conflicts WHERE session_id=? AND request_id=?) OR EXISTS(SELECT 1 FROM task_provider_conflicts WHERE session_id=? AND request_id=?)`, session, id, session, id).Scan(&conflict)
	return conflict, err
}

// Expiry preserves BASE readiness: an incomplete upload never becomes runnable.
// Boot mismatch is quarantined; an explicit abort/recovery is required.
func (s *Store) ExpireHumanEdits(boot string, now time.Time) error {
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	rows, e := tx.Query(`SELECT body FROM input_reservations WHERE json_extract(body,'$.lease') IS NOT NULL`)
	if e != nil {
		return e
	}
	items := []HumanInput{}
	for rows.Next() {
		var raw []byte
		var v HumanInput
		if e = rows.Scan(&raw); e != nil {
			rows.Close()
			return e
		}
		if e = json.Unmarshal(raw, &v); e != nil {
			rows.Close()
			return e
		}
		items = append(items, v)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, v := range items {
		if v.Lease == nil || v.Lease.Boot != boot || now.UnixMilli() < v.Lease.Expires {
			continue
		}
		v.State = v.Lease.BaseState
		v.ReadyProof = v.Lease.BaseProof
		v.Lease = nil
		v.Revision++
		if e = humanSaveTx(tx, v); e != nil {
			return e
		}
		if e = bump(tx, v.Binding.Session); e != nil {
			return e
		}
	}
	return tx.Commit()
}

func commitHumanEditTx(tx *sql.Tx, e Entry, v *HumanInput, prior Entry) (Entry, error) {
	if e.HumanSeal == nil || v == nil {
		return Entry{}, ErrConflict
	}
	var old, next map[string]any
	effective, err := effectiveInput(context.Background(), tx, prior)
	if err != nil {
		return Entry{}, err
	}
	if json.Unmarshal(effective.Payload, &old) != nil || json.Unmarshal(e.Payload, &next) != nil {
		return Entry{}, ErrConflict
	}
	// Only content and the already-qualified media revision may change. Original
	// owner, target, configuration, role and callback metadata remain immutable.
	for _, key := range []string{"content", "images", "files", "media_body_hash", "media_batch_id", "visual_user_hash", "visual_effective_hash"} {
		delete(old, key)
		delete(next, key)
	}
	if humanHash(old) != humanHash(next) {
		return Entry{}, ErrConflict
	}
	if _, err = tx.Exec(`INSERT OR IGNORE INTO unified_input_versions VALUES(?,?,?,?,?,?,?)`, e.SessionID, e.RequestID, 1, prior.Payload, prior.Content, prior.PayloadHash, prior.PayloadHash); err != nil {
		return Entry{}, err
	}
	if _, err = tx.Exec(`INSERT INTO unified_input_versions VALUES(?,?,?,?,?,?,?)`, e.SessionID, e.RequestID, v.ContentRevision, e.Payload, e.Content, taskapi.RawDigest(e.Payload), prior.PayloadHash); err != nil {
		return Entry{}, err
	}
	if err = bump(tx, e.SessionID); err != nil {
		return Entry{}, err
	}
	e.PayloadHash = prior.PayloadHash
	if err = commitHumanSealTx(tx, e, v); err != nil {
		return Entry{}, err
	}
	return effectiveInput(context.Background(), tx, prior)
}

// Called INSIDE Enqueue's original transaction, after media admission has
// produced its durable ready proof. This is the only first-payload seal.
func beginHumanSealTx(tx *sql.Tx, e Entry) (*HumanInput, error) {
	if e.HumanSeal == nil {
		return nil, nil
	}
	seal := *e.HumanSeal
	if seal.Clock != nil {
		seal.Boot, seal.Now = seal.Clock()
	}
	if conflict, err := humanNativeConflictTx(tx, e.SessionID, e.RequestID); err != nil {
		return nil, err
	} else if conflict {
		return nil, ErrConflict
	}
	op := seal.Operation
	if (op.Kind != "seal" && op.Kind != "commit_edit") || seal.Proof == "" || op.ReadyProof != seal.Proof {
		return nil, fmt.Errorf("media_proof_required")
	}
	if r, ok, err := humanReceiptTx(tx, seal.Binding, op.ID, humanHash(op)); err != nil {
		return nil, err
	} else if ok {
		if r.Status != "committed" {
			return nil, ErrRejected
		}
		return nil, nil
	}
	v, ok, err := humanNodeTx(tx, e.SessionID, e.RequestID)
	if err != nil {
		return nil, err
	}
	if !ok || !humanSameSource(v.Binding, seal.Binding) || v.State == "cancelled" || v.Revision != op.ExpectedRevision || v.ContentRevision != op.ContentRevision || op.IntentHash != v.IntentHash {
		return nil, fmt.Errorf("reservation_changed")
	}
	if op.Kind == "seal" {
		if v.State != "reserved_waiting_assets" || v.Lease != nil || v.Content != op.Content {
			return nil, ErrConflict
		}
	} else {
		if op.ExpectedHash != v.EffectiveHash || v.Lease == nil || v.Lease.ID != op.LeaseID || v.Lease.Generation != op.LeaseGeneration || v.Lease.Boot != seal.Boot || seal.Now.UnixMilli() >= v.Lease.Expires {
			return nil, fmt.Errorf("edit_expired")
		}
		e.HumanSeal.leaseExpires = v.Lease.Expires
		v.ContentRevision++
		v.Content = op.Content
		if len(op.Assets) > 0 {
			v.Assets = op.Assets
		}
		v.EffectiveHash = humanHash(map[string]any{"content": v.Content, "assets": v.Assets})
		v.Lease = nil
	}
	v.State = "ready_queued"
	v.ReadyProof = seal.Proof
	v.Revision++
	return &v, nil
}
func commitHumanSealTx(tx *sql.Tx, e Entry, v *HumanInput) error {
	if v == nil {
		return nil
	}
	v.WireHash = taskapi.RawDigest(e.Payload)
	// Retain version 1 too. A delayed media receipt must remain resolvable after
	// later edits or terminal raw-payload compaction, without borrowing latest.
	if _, err := tx.Exec(`INSERT OR IGNORE INTO unified_input_versions VALUES(?,?,?,?,?,?,?)`, e.SessionID, e.RequestID, v.ContentRevision, e.Payload, e.Content, v.WireHash, e.PayloadHash); err != nil {
		return err
	}
	var frozenHash string
	if err := tx.QueryRow(`SELECT hash FROM unified_input_versions WHERE queue_key=? AND item_id=? AND version=?`, e.SessionID, e.RequestID, v.ContentRevision).Scan(&frozenHash); err != nil {
		return err
	} else if frozenHash != v.WireHash {
		return ErrConflict
	}
	if err := humanSaveTx(tx, *v); err != nil {
		return err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO input_versions VALUES(?,?,?,?)`, v.Binding.Session, v.ID, v.ContentRevision, raw); err != nil {
		return err
	}
	_, err = humanRecordTx(tx, e.HumanSeal.Binding, e.HumanSeal.Operation, v, "")
	return err
}

// A process-monotonic clock is serialized only with its exact boot domain.
// UTC changes cannot extend a live edit lease.
func (s *Store) HumanClock() (string, time.Time) {
	return s.clock.domain, time.UnixMilli(time.Since(s.humanStarted).Milliseconds())
}

// A blank original native thread may acquire ONLY the exact accepted thread
// recorded for this original owner/configuration. A renamed or unrelated thread
// is never adopted from its display name or the current session list.
func (s *Store) HumanThreadCompatible(b HumanBinding, actual string) bool {
	return s.HumanThreadCompatibleContext(context.Background(), b, actual)
}

func (s *Store) HumanThreadCompatibleContext(ctx context.Context, b HumanBinding, actual string) bool {
	if b.Thread == actual {
		return true
	}
	if b.Thread != "" || actual == "" {
		return false
	}
	var count, distinct int
	var q interface{ QueryRow(string, ...any) *sql.Row } = s.db
	if bound, ok := ctx.Value(nativeAcceptanceTransactionKey{}).(nativeAcceptanceTransaction); ok && bound.store == s && bound.tx != nil {
		q = bound.tx
	}
	err := q.QueryRow(`SELECT COUNT(*),COUNT(DISTINCT a.thread_id) FROM task_native_acceptance a JOIN input_reservations i ON i.session=a.session_id AND i.id=a.request_id WHERE i.session=? AND a.thread_id=? AND json_extract(i.body,'$.binding.device')=? AND json_extract(i.body,'$.binding.config')=? AND json_extract(i.body,'$.binding.generation')=? AND json_extract(i.body,'$.binding.epoch')=? AND json_extract(i.body,'$.binding.thread')=''`, b.Session, actual, b.Device, b.Config, b.Generation, b.Epoch).Scan(&count, &distinct)
	return err == nil && count > 0 && distinct == 1
}

// HumanBindingForRequest is a read of the original committed reservation. The
// caller still verifies device/configuration/epoch before adopting a thread.
func (s *Store) HumanBindingForRequest(session, request string) (HumanBinding, bool) {
	tx, err := s.db.Begin()
	if err != nil {
		return HumanBinding{}, false
	}
	defer tx.Rollback()
	v, found, err := humanNodeTx(tx, session, request)
	return v.Binding, found && err == nil
}

func (s *Store) HumanStageAllowed(b HumanBinding, id string, base uint64, hash, lease string, generation uint64, boot string, now time.Time) bool {
	tx, err := s.db.Begin()
	if err != nil {
		return false
	}
	defer tx.Rollback()
	v, found, err := humanNodeTx(tx, b.Session, id)
	if err != nil || !found || v.Binding != b || v.State == "cancelled" || v.ContentRevision != base || v.EffectiveHash != hash || v.Lease == nil || v.Lease.ID != lease || v.Lease.Generation != generation || v.Lease.Boot != boot || now.UnixMilli() >= v.Lease.Expires {
		return false
	}
	entry, exists, err := lookup(tx, b.Session, id)
	if err != nil || exists && entry.State != Queued {
		return false
	}
	var accepted int
	if tx.QueryRow(`SELECT COUNT(*) FROM task_native_acceptance WHERE session_id=? AND request_id=?`, b.Session, id).Scan(&accepted) != nil || accepted > 0 {
		return false
	}
	conflict, err := humanNativeConflictTx(tx, b.Session, id)
	return err == nil && !conflict
}

func (s *Store) HumanCompatibleBindings(current HumanBinding) ([]HumanBinding, error) {
	rows, err := s.db.Query(`SELECT body FROM input_reservations WHERE session=?`, current.Session)
	if err != nil {
		return nil, err
	}
	var candidates []HumanBinding
	for rows.Next() {
		var raw []byte
		var v HumanInput
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, err
		}
		if err = json.Unmarshal(raw, &v); err != nil {
			rows.Close()
			return nil, err
		}
		b := v.Binding
		if b.Device == current.Device && b.Authority == current.Authority && b.Config == current.Config && b.Epoch == current.Epoch && b.Generation == current.Generation {
			candidates = append(candidates, b)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	seen := map[HumanBinding]bool{current: true}
	out := []HumanBinding{current}
	for _, b := range candidates {
		if !seen[b] && s.HumanThreadCompatible(b, current.Thread) {
			seen[b] = true
			out = append(out, b)
		}
	}
	return out, nil
}

func humanWaitingCountTx(tx *sql.Tx, session string) (int, error) {
	var count int
	err := tx.QueryRow(`SELECT COUNT(*) FROM (
 SELECT request_id AS id FROM queue_commands WHERE session_id=? AND state IN ('queued','uncertain')
 UNION SELECT id FROM input_reservations WHERE session=? AND json_extract(body,'$.state') NOT IN ('cancelled','failed')
 AND NOT EXISTS(SELECT 1 FROM queue_commands q WHERE q.session_id=input_reservations.session AND q.request_id=input_reservations.id AND q.state NOT IN ('queued','uncertain'))
 )`, session, session).Scan(&count)
	return count, err
}

// Reserved nodes restore their original empty actor slot before later ready
// messages. Without this, a cold restart could leave B waiting in front of A's
// callback forever after A's upload finally seals.
func (s *Store) HumanReserved() ([]Entry, error) {
	rows, err := s.db.Query(`SELECT i.body FROM input_reservations i LEFT JOIN queue_commands q ON q.session_id=i.session AND q.request_id=i.id WHERE q.request_id IS NULL AND json_extract(i.body,'$.state')!='cancelled'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Entry{}
	for rows.Next() {
		var raw []byte
		var v HumanInput
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		out = append(out, Entry{SessionID: v.Binding.Session, RequestID: v.ID, Content: v.Content, State: State("reserved_waiting_assets")})
	}
	return out, rows.Err()
}

// This digest reveals no private row body or ID. It binds a mutation to the
// complete ORIGINAL order, not merely the caller's visible subset.
func (s *Store) HumanMembership(b HumanBinding) (uint64, string, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, "", err
	}
	defer tx.Rollback()
	var revision uint64
	err = tx.QueryRow(`SELECT revision FROM queue_sessions WHERE session_id=?`, b.Session).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	if err != nil {
		return 0, "", err
	}
	ids, err := orderRowsTx(tx, b.Session)
	if err != nil {
		return 0, "", err
	}
	return revision, humanHash(map[string]any{"binding": b, "order": ids}), nil
}
