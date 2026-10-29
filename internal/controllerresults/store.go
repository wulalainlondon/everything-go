package controllerresults

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db} }
func Install(db *sql.DB) error {
	_, e := db.Exec(`
CREATE TABLE IF NOT EXISTS controller_result_scopes(scope TEXT PRIMARY KEY,payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS controller_result_groups(id TEXT PRIMARY KEY,scope TEXT NOT NULL,identity TEXT NOT NULL,version INTEGER NOT NULL,fact_hash TEXT NOT NULL,payload BLOB NOT NULL,historical INTEGER NOT NULL,legacy_outstanding INTEGER NOT NULL,UNIQUE(scope,identity));
CREATE TABLE IF NOT EXISTS controller_result_members(dispatch_id TEXT PRIMARY KEY,group_id TEXT NOT NULL,payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS controller_result_changes(scope TEXT NOT NULL,seq INTEGER NOT NULL,kind TEXT NOT NULL,PRIMARY KEY(scope,seq));
CREATE TABLE IF NOT EXISTS controller_result_member_pages(id TEXT PRIMARY KEY,scope TEXT NOT NULL,payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS controller_result_reads(id TEXT PRIMARY KEY,scope TEXT NOT NULL,proof BLOB NOT NULL,payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS controller_result_acks(scope TEXT NOT NULL,group_id TEXT NOT NULL,version INTEGER NOT NULL,kind TEXT NOT NULL,PRIMARY KEY(scope,group_id,version,kind));
CREATE TABLE IF NOT EXISTS controller_result_operations(scope TEXT NOT NULL,intent TEXT NOT NULL,hash TEXT NOT NULL,receipt BLOB NOT NULL,PRIMARY KEY(scope,intent));
CREATE TABLE IF NOT EXISTS controller_return_control(scope TEXT PRIMARY KEY,payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS controller_return_batches(id TEXT PRIMARY KEY,scope TEXT NOT NULL,state TEXT NOT NULL,payload BLOB NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS controller_return_one_inflight ON controller_return_batches(scope) WHERE state IN ('frozen','unknown','queued','running');
CREATE TABLE IF NOT EXISTS controller_delivery_links(dispatch_id TEXT NOT NULL,batch_id TEXT NOT NULL,group_id TEXT NOT NULL,version INTEGER NOT NULL,PRIMARY KEY(dispatch_id,batch_id));
CREATE INDEX IF NOT EXISTS controller_result_members_group ON controller_result_members(group_id,dispatch_id);
CREATE INDEX IF NOT EXISTS controller_delivery_coverage ON controller_delivery_links(group_id,version,batch_id);
CREATE INDEX IF NOT EXISTS controller_batches_scope ON controller_return_batches(scope);
CREATE INDEX IF NOT EXISTS controller_result_read_scope ON controller_result_reads(scope);
CREATE INDEX IF NOT EXISTS controller_result_member_pages_scope ON controller_result_member_pages(scope);
CREATE TABLE IF NOT EXISTS controller_result_cohort(dispatch_id TEXT PRIMARY KEY,cohort TEXT NOT NULL);
`)
	return e
}
func now() int64           { return time.Now().UnixMilli() }
func marshal(v any) []byte { b, _ := json.Marshal(v); return b }
func change(ctx context.Context, tx *sql.Tx, scope, kind string) error {
	_, e := tx.ExecContext(ctx, `INSERT INTO controller_result_changes(scope,seq,kind) SELECT ?,COALESCE(max(seq),0)+1,? FROM controller_result_changes WHERE scope=?`, scope, kind, scope)
	return e
}
func getGroup(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id string) (Group, error) {
	var g Group
	var b []byte
	e := q.QueryRowContext(ctx, `SELECT payload FROM controller_result_groups WHERE id=?`, id).Scan(&b)
	if e == nil {
		e = json.Unmarshal(b, &g)
	}
	return g, e
}

// Observe atomically links an immutable original dispatch member. Only actual
// facts change a version/high-water mark; updated_at polling is deliberately absent.
func (s *Store) Observe(ctx context.Context, o Observation) (Group, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return Group{}, e
	}
	defer tx.Rollback()
	sk := o.Scope.Key()
	if _, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO controller_result_scopes VALUES(?,?)`, sk, marshal(o.Scope)); e != nil {
		return Group{}, e
	}

	identity := Hash(struct {
		E          Execution
		Unverified string
	}{o.Execution, func() string {
		if !o.Verified {
			return o.Member.DispatchID
		}
		return ""
	}()})
	id := "crg_" + Hash([]string{sk, identity})
	var existingID string
	e = tx.QueryRowContext(ctx, `SELECT group_id FROM controller_result_members WHERE dispatch_id=?`, o.Member.DispatchID).Scan(&existingID)
	if e == nil && existingID != id {
		return Group{}, ErrConflict
	}
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return Group{}, e
	}
	g, e := getGroup(ctx, tx, id)
	fresh := errors.Is(e, sql.ErrNoRows)
	if e != nil && !fresh {
		return g, e
	}
	state := o.State
	if !o.Verified {
		state = "historical_unverified"
	} else if state == "completed" {
		if o.Final == nil {
			state = "pending_final"
		} else {
			state = "final"
		}
	}
	fact := Hash(struct {
		State string
		Final *Final
	}{state, o.Final})
	changed := fresh
	if fresh {
		g = Group{ID: id, Version: 1, FactHash: fact, Execution: o.Execution, State: state, Final: o.Final, ConflictHashes: []string{}, Unread: !o.Historical, SourceCompletedAt: o.CompletedAt, FirstObservedAt: now(), CommittedAt: now()}
	} else if g.State == "final" && state == "pending_final" {
		// Absence is not evidence of a contradictory final. Native/grant fences still gate every wake.
	} else if g.FactHash != fact {
		// A seal may arrive after terminal. Any incompatible terminal/seal is retained
		// as conflict rather than choosing the latest observation.
		if g.State == "pending_final" && state == "final" {
			g.State = state
			g.Final = o.Final
			g.FactHash = fact
			g.Version++
			g.MissionApplied = false
			g.NativeRead = false
			g.NativeReadAt = 0
			g.NativeReadReceipt = ""
			g.Unread = !o.Historical
			changed = true
		} else if g.State != "conflicted" {
			g.ConflictHashes = []string{g.FactHash, fact}
			g.State = "conflicted"
			g.MissionApplied = false
			g.Version++
			g.FactHash = Hash(g.ConflictHashes)
			changed = true
		} else {
			found := false
			for _, h := range g.ConflictHashes {
				found = found || h == fact
			}
			if !found {
				g.ConflictHashes = append(g.ConflictHashes, fact)
				g.Version++
				g.FactHash = Hash(g.ConflictHashes)
				changed = true
			}
		}
	}
	res, e := tx.ExecContext(ctx, `INSERT OR IGNORE INTO controller_result_members VALUES(?,?,?)`, o.Member.DispatchID, id, marshal(o.Member))
	if e != nil {
		return g, e
	}
	n, _ := res.RowsAffected()
	g.MemberCount += int(n)
	// Late aliases do not reopen unread/ACK or advance the result version.
	_, e = tx.ExecContext(ctx, `INSERT INTO controller_result_groups VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET version=excluded.version,fact_hash=excluded.fact_hash,payload=excluded.payload`, id, sk, identity, g.Version, g.FactHash, marshal(g), o.Historical, o.LegacyOutstanding)
	if e != nil {
		return g, e
	}
	_, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO controller_result_cohort VALUES(?,'v2')`, o.Member.DispatchID)
	if e != nil {
		return g, e
	}
	if n > 0 && !fresh && !changed {
		_, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO controller_delivery_links SELECT ?,l.batch_id,l.group_id,l.version FROM controller_delivery_links l WHERE l.group_id=? AND l.version=?`, o.Member.DispatchID, g.ID, g.Version)
		if e != nil {
			return g, e
		}
	}
	if changed || n > 0 {
		if e = change(ctx, tx, sk, "inbox_changed"); e != nil {
			return g, e
		}
	}
	if e = tx.Commit(); e != nil {
		return g, e
	}
	return g, nil
}
func (s *Store) Cohort(ctx context.Context, id string) (bool, error) {
	var n int
	e := s.db.QueryRowContext(ctx, `SELECT count(*) FROM controller_result_cohort WHERE dispatch_id=?`, id).Scan(&n)
	return n > 0, e
}
func control(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, scope string) (Control, error) {
	c := Control{Mode: "notify_only", Paused: true}
	var b []byte
	e := q.QueryRowContext(ctx, `SELECT payload FROM controller_return_control WHERE scope=?`, scope).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return c, nil
	}
	if e == nil {
		e = json.Unmarshal(b, &c)
	}
	return c, e
}
func (s *Store) Control(ctx context.Context, scope Scope) (Control, error) {
	return control(ctx, s.db, scope.Key())
}
func operation(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, scope, key, hash string) (Receipt, bool, error) {
	var r Receipt
	var b []byte
	var h string
	e := q.QueryRowContext(ctx, `SELECT hash,receipt FROM controller_result_operations WHERE scope=? AND intent=?`, scope, key).Scan(&h, &b)
	if errors.Is(e, sql.ErrNoRows) {
		return r, false, nil
	}
	if e != nil {
		return r, false, e
	}
	if hash != "" && h != hash {
		return r, false, ErrConflict
	}
	e = json.Unmarshal(b, &r)
	return r, true, e
}
func (s *Store) Operation(ctx context.Context, scope Scope, key, hash string) (Receipt, bool, error) {
	return operation(ctx, s.db, scope.Key(), key, hash)
}
func saveOperation(ctx context.Context, tx *sql.Tx, scope string, r Receipt) error {
	_, e := tx.ExecContext(ctx, `INSERT INTO controller_result_operations VALUES(?,?,?,?)`, scope, r.Intent, r.InputHash, marshal(r))
	return e
}
func (s *Store) SetControl(ctx context.Context, scope Scope, expected uint64, c Control, key, hash string, check func() error) (Receipt, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return Receipt{}, e
	}
	defer tx.Rollback()
	sk := scope.Key()
	if e = grantCurrent(ctx, tx, scope); e != nil {
		return Receipt{}, e
	}
	if r, ok, e := operation(ctx, tx, sk, key, hash); ok || e != nil {
		return r, e
	}
	old, e := control(ctx, tx, sk)
	if e != nil {
		return Receipt{}, e
	}
	if old.Revision != expected {
		return Receipt{}, ErrConflict
	}
	if check == nil {
		return Receipt{}, ErrScope
	}
	if e = check(); e != nil {
		return Receipt{}, e
	}
	if c.Mode != "notify_only" && c.Mode != "resume_authorized" {
		return Receipt{}, ErrScope
	}
	if c.Mode == "resume_authorized" && (!c.Paused && (c.Deadline <= now() || c.MaxBatches < 1 || c.MaxBatches > 16 || c.MaxNoProgress < 1 || c.MaxNoProgress > 4)) {
		return Receipt{}, ErrBudget
	}
	c.Revision = old.Revision + 1
	c.Issued = old.Issued
	c.NoProgress = old.NoProgress
	if old.Paused && c.Mode == "resume_authorized" && !c.Paused {
		c.Issued = 0
		c.NoProgress = 0
	}
	c.SourceIntent = key
	if _, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO controller_result_scopes VALUES(?,?)`, sk, marshal(scope)); e != nil {
		return Receipt{}, e
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO controller_return_control VALUES(?,?) ON CONFLICT(scope) DO UPDATE SET payload=excluded.payload`, sk, marshal(c))
	if e != nil {
		return Receipt{}, e
	}
	r := Receipt{Kind: "operation_receipt", Intent: key, Operation: "set_dispatch_return_control", InputHash: hash, State: "applied", Versions: []Version{}, Control: &c}
	if e = saveOperation(ctx, tx, sk, r); e != nil {
		return r, e
	}
	if e = change(ctx, tx, sk, "control_changed"); e != nil {
		return r, e
	}
	return r, tx.Commit()
}
func (s *Store) Read(ctx context.Context, scope Scope, proof ReadProof, cursor string, limit int) (Page, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 50 {
		return Page{}, ErrScope
	}
	sk := scope.Key()
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return Page{}, e
	}
	defer tx.Rollback()
	if e = grantCurrent(ctx, tx, scope); e != nil {
		return Page{}, e
	}
	// Cursor is a server-owned exact scope+snapshot+offset record, not a client
	// invented high-water. Existing read receipt IDs are the snapshot token.
	offset := 0
	high := uint64(0)
	var snapshot []Group
	if cursor != "" {
		token := cursor
		var b []byte
		e = tx.QueryRowContext(ctx, `SELECT payload FROM controller_result_reads WHERE id=? AND scope=?`, token, sk).Scan(&b)
		if e != nil {
			return Page{}, ErrGap
		}
		var saved struct {
			GrantRevision uint64
			Page          Page
			Proof         ReadProof
			Snapshot      []Group
			Offset        int
		}
		if json.Unmarshal(b, &saved) != nil || saved.Proof != proof || saved.GrantRevision != scope.GrantRevision {
			return Page{}, ErrScope
		}
		snapshot = saved.Snapshot
		offset = saved.Offset
		high = saved.Page.HighWater
	} else {
		rows, e := tx.QueryContext(ctx, `SELECT payload FROM controller_result_groups WHERE scope=? ORDER BY rowid LIMIT 4097`, sk)
		if e != nil {
			return Page{}, e
		}
		snapshot = []Group{}
		for rows.Next() {
			var b []byte
			if e = rows.Scan(&b); e != nil {
				rows.Close()
				return Page{}, e
			}
			var g Group
			if e = json.Unmarshal(b, &g); e != nil {
				rows.Close()
				return Page{}, e
			}
			snapshot = append(snapshot, g)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return Page{}, e
		}
		if len(snapshot) > 4096 {
			return Page{}, ErrBudget
		}
		if e = tx.QueryRowContext(ctx, `SELECT COALESCE(max(seq),0) FROM controller_result_changes WHERE scope=?`, sk).Scan(&high); e != nil {
			return Page{}, e
		}
	}
	if offset > len(snapshot) {
		return Page{}, ErrGap
	}
	end := offset + limit
	if end > len(snapshot) {
		end = len(snapshot)
	}
	groups := append([]Group{}, snapshot[offset:end]...)
	unread := 0
	for i := range snapshot {
		g := &snapshot[i]
		var ack, seen bool
		e = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM controller_result_acks WHERE scope=? AND group_id=? AND version=? AND kind='ack'),EXISTS(SELECT 1 FROM controller_result_acks WHERE scope=? AND group_id=? AND version=? AND kind='seen')`, sk, g.ID, g.Version, sk, g.ID, g.Version).Scan(&ack, &seen)
		if e != nil {
			return Page{}, e
		}
		g.Acknowledged = ack
		g.HumanSeen = seen
		g.Unread = g.Unread && !ack
		if g.Unread {
			unread++
		}
	}
	groups = append([]Group{}, snapshot[offset:end]...)
	versions := []Version{}
	for _, g := range groups {
		versions = append(versions, Version{g.ID, g.Version, g.FactHash})
	}
	rr := ReadReceipt{ID: "crr_" + Hash([]any{sk, proof, high, offset, nonce()}), ScopeHash: sk, Versions: versions, Native: proof.Native, CreatedAt: now()}
	if proof.Native {
		for i := range groups {
			g := &groups[i]
			if !g.NativeRead {
				g.NativeRead = true
				g.NativeReadAt = now()
				g.NativeReadReceipt = rr.ID
				current, err := getGroup(ctx, tx, g.ID)
				if err != nil {
					return Page{}, err
				}
				if current.Version != g.Version || current.FactHash != g.FactHash {
					return Page{}, ErrConflict
				}
				current.NativeRead = true
				current.NativeReadAt = g.NativeReadAt
				current.NativeReadReceipt = rr.ID
				if _, err = tx.ExecContext(ctx, `UPDATE controller_result_groups SET payload=? WHERE id=?`, marshal(current), g.ID); err != nil {
					return Page{}, err
				}
				if err = change(ctx, tx, sk, "native_read"); err != nil {
					return Page{}, err
				}
			}
		}
	}
	c, e := control(ctx, tx, sk)
	if e != nil {
		return Page{}, e
	}
	p := Page{Kind: "dispatch_inbox", Groups: groups, ReadReceipt: rr, HighWater: high, HasMore: end < len(snapshot), Complete: end == len(snapshot), Unread: unread, Control: c}
	if p.HasMore {
		p.NextCursor = rr.ID
	}
	saved := struct {
		GrantRevision uint64
		Page          Page
		Proof         ReadProof
		Snapshot      []Group
		Offset        int
	}{scope.GrantRevision, p, proof, snapshot, end}
	_, e = tx.ExecContext(ctx, `INSERT INTO controller_result_reads VALUES(?,?,?,?)`, rr.ID, sk, marshal(proof), marshal(saved))
	if e != nil {
		return p, e
	}
	if _, e = tx.ExecContext(ctx, `DELETE FROM controller_result_reads WHERE scope=? AND id NOT IN (SELECT id FROM controller_result_reads WHERE scope=? ORDER BY rowid DESC LIMIT 1024)`, sk, sk); e != nil {
		return p, e
	}
	return p, tx.Commit()
}
func (s *Store) Mark(ctx context.Context, scope Scope, proof ReadProof, readID string, versions []Version, key, hash, kind string, check func() error, callID ...string) (Receipt, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return Receipt{}, e
	}
	defer tx.Rollback()
	sk := scope.Key()
	if e = grantCurrent(ctx, tx, scope); e != nil {
		return Receipt{}, e
	}
	if r, ok, e := operation(ctx, tx, sk, key, hash); ok || e != nil {
		return r, e
	}
	if len(versions) == 0 || len(versions) > 50 {
		return Receipt{}, ErrScope
	}
	var b []byte
	e = tx.QueryRowContext(ctx, `SELECT payload FROM controller_result_reads WHERE id=? AND scope=?`, readID, sk).Scan(&b)
	if e != nil {
		return Receipt{}, ErrScope
	}
	var saved struct {
		GrantRevision uint64
		Page          Page
		Proof         ReadProof
	}
	if json.Unmarshal(b, &saved) != nil || saved.Proof != proof || saved.GrantRevision != scope.GrantRevision || (kind == "ack" && !proof.Native) || (kind == "seen" && proof.Native) {
		return Receipt{}, ErrScope
	}
	if check == nil {
		return Receipt{}, ErrScope
	}
	if e = check(); e != nil {
		return Receipt{}, e
	}
	seen := map[string]bool{}
	for _, v := range versions {
		if seen[v.ID] {
			return Receipt{}, ErrConflict
		}
		seen[v.ID] = true
		allowed := false
		for _, x := range saved.Page.ReadReceipt.Versions {
			allowed = allowed || x == v
		}
		g, e := getGroup(ctx, tx, v.ID)
		if !allowed || e != nil || g.Version != v.Version || g.FactHash != v.Hash {
			return Receipt{}, ErrConflict
		}
		_, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO controller_result_acks VALUES(?,?,?,?)`, sk, v.ID, v.Version, kind)
		if e != nil {
			return Receipt{}, e
		}
	}
	op := "ack_dispatch_results"
	if kind == "seen" {
		op = "mark_dispatch_results_seen"
	}
	r := Receipt{Kind: "operation_receipt", Intent: key, Operation: op, InputHash: hash, State: "applied", Versions: versions}
	if kind == "ack" {
		call := ""
		if len(callID) > 0 {
			call = callID[0]
		}
		r.NativeProvenance = &NativeProvenance{Source: scope.Parent, Request: proof.Request, Turn: proof.Turn, CallID: call, ReadReceipt: readID}
	}
	if e = saveOperation(ctx, tx, sk, r); e != nil {
		return r, e
	}
	if kind == "ack" || kind == "seen" {
		for _, v := range versions {
			g, err := getGroup(ctx, tx, v.ID)
			if err != nil {
				return r, err
			}
			if kind == "ack" {
				g.Acknowledged = true
				g.Unread = false
				g.ProcessedAt = now()
			} else {
				g.HumanSeen = true
			}
			if _, err = tx.ExecContext(ctx, `UPDATE controller_result_groups SET payload=? WHERE id=?`, marshal(g), g.ID); err != nil {
				return r, err
			}
		}
	}
	if e = change(ctx, tx, sk, kind); e != nil {
		return r, e
	}
	return r, tx.Commit()
}

// Validate against the original grant in this SAME dispatch SQL transaction.
// The verifier never borrows MQ/delegation while this single connection is held.
func grantCurrent(ctx context.Context, tx *sql.Tx, scope Scope) error {
	if scope.GrantRevision == 0 {
		return ErrScope
	}
	var revision uint64
	var enabled bool
	e := tx.QueryRowContext(ctx, `SELECT revision,COALESCE(json_extract(payload,'$.enabled'),0) FROM controller_grants WHERE parent_id=?`, scope.Parent.Session).Scan(&revision, &enabled)
	if e != nil || !enabled || revision != scope.GrantRevision {
		return ErrScope
	}
	return nil
}

func nonce() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}

// A positive canonical conflict quarantines the existing group; missing data
// alone is never promoted to a successful/latest replacement.
func (s *Store) Quarantine(ctx context.Context, dispatch, reason string) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var id, scope string
	e = tx.QueryRowContext(ctx, `SELECT m.group_id,g.scope FROM controller_result_members m JOIN controller_result_groups g ON g.id=m.group_id WHERE m.dispatch_id=?`, dispatch).Scan(&id, &scope)
	if errors.Is(e, sql.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	g, e := getGroup(ctx, tx, id)
	if e != nil {
		return e
	}
	if g.State == "conflicted" {
		return nil
	}
	g.State = "conflicted"
	g.MissionApplied = false
	g.ConflictHashes = []string{g.FactHash, Hash(reason)}
	g.FactHash = Hash(g.ConflictHashes)
	g.Version++
	g.NativeRead = false
	g.NativeReadAt = 0
	g.NativeReadReceipt = ""
	_, e = tx.ExecContext(ctx, `UPDATE controller_result_groups SET version=?,fact_hash=?,payload=? WHERE id=?`, g.Version, g.FactHash, marshal(g), id)
	if e != nil {
		return e
	}
	if e = change(ctx, tx, scope, "inbox_changed"); e != nil {
		return e
	}
	return tx.Commit()
}

// MatchFact uses exact native execution/member provenance and final identity.
// Text, titles, times and similar hashes are never task joins.
func (s *Store) MatchFact(ctx context.Context, scope Scope, target Identity, request, seal, hash string) (Group, bool, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT payload FROM controller_result_groups WHERE scope=? AND json_extract(payload,'$.state')='final' AND json_extract(payload,'$.execution.target.authority_instance_id')=? AND json_extract(payload,'$.execution.target.session_id')=? AND json_extract(payload,'$.execution.target.thread_id')=? AND json_extract(payload,'$.execution.target.config_revision')=? AND json_extract(payload,'$.final.seal_id')=? AND json_extract(payload,'$.final.full_hash')=? AND (json_extract(payload,'$.execution.request_id')=? OR EXISTS(SELECT 1 FROM controller_result_members m WHERE m.group_id=controller_result_groups.id AND json_extract(m.payload,'$.request_id')=? AND json_extract(m.payload,'$.alias_proven')=1)) LIMIT 2`, scope.Key(), target.Authority, target.Session, target.Thread, target.Revision, seal, hash, request, request)
	if e != nil {
		return Group{}, false, e
	}
	defer rows.Close()
	var found Group
	n := 0
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return found, false, e
		}
		if e = json.Unmarshal(b, &found); e != nil {
			return found, false, e
		}
		n++
	}
	if n > 1 {
		return found, false, ErrConflict
	}
	return found, n == 1, rows.Err()
}
func (s *Store) ApplyMission(ctx context.Context, scope Scope, id string, proof MissionApplication) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var sk string
	if e = tx.QueryRowContext(ctx, `SELECT scope FROM controller_result_groups WHERE id=?`, id).Scan(&sk); e != nil || sk != scope.Key() {
		return ErrScope
	}
	g, e := getGroup(ctx, tx, id)
	if e != nil {
		return e
	}
	if g.Version != proof.GroupVersion || g.FactHash != proof.FactHash || g.State != "final" {
		return ErrConflict
	}
	for _, old := range g.MissionApplications {
		if old == proof {
			return nil
		}
	}
	if len(g.MissionApplications) >= 4096 {
		return ErrBudget
	}
	g.MissionApplications = append(g.MissionApplications, proof)
	g.MissionApplied = true
	if _, e = tx.ExecContext(ctx, `UPDATE controller_result_groups SET payload=? WHERE id=?`, marshal(g), g.ID); e != nil {
		return e
	}
	if e = change(ctx, tx, sk, "mission_applied"); e != nil {
		return e
	}
	return tx.Commit()
}
