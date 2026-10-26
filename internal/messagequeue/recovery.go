package messagequeue

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"everything-go/internal/recovery"
)

var ErrRecoveryInputPressure = errors.New("recovery input storage bound; ordinary single send remains available")

func installRecovery(db *sql.DB) error {
	_, e := db.Exec(`CREATE TABLE IF NOT EXISTS recovery_runs(session_id TEXT NOT NULL,request_id TEXT NOT NULL,generation INTEGER NOT NULL,revision INTEGER NOT NULL,state TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(session_id,request_id));
 CREATE TABLE IF NOT EXISTS recovery_file_leases(session_id TEXT NOT NULL,request_id TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(session_id,request_id));
 CREATE TABLE IF NOT EXISTS recovery_inputs(session_id TEXT NOT NULL,request_id TEXT NOT NULL,hash TEXT NOT NULL,lease_until INTEGER NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(session_id,request_id));
 CREATE TABLE IF NOT EXISTS recovery_attempts(session_id TEXT NOT NULL,request_id TEXT NOT NULL,attempt_id TEXT NOT NULL,generation INTEGER NOT NULL,phase TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(session_id,request_id,attempt_id));
 CREATE TABLE IF NOT EXISTS recovery_scopes(scope TEXT PRIMARY KEY,owner TEXT NOT NULL DEFAULT '',lease_until INTEGER NOT NULL DEFAULT 0,cooldown_until INTEGER NOT NULL DEFAULT 0,window_start INTEGER NOT NULL DEFAULT 0,rejections INTEGER NOT NULL DEFAULT 0,last_dispatch INTEGER NOT NULL DEFAULT 0);`)
	return e
}
func recoveryLookup(tx interface{ QueryRow(string, ...any) *sql.Row }, sid, rid string) (recovery.Run, bool, error) {
	var raw []byte
	var r recovery.Run
	e := tx.QueryRow(`SELECT payload FROM recovery_runs WHERE session_id=? AND request_id=?`, sid, rid).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return r, false, nil
	}
	if e == nil {
		e = json.Unmarshal(raw, &r)
	}
	return r, e == nil, e
}
func saveRecovery(tx *sql.Tx, r *recovery.Run) error {
	r.Revision++
	raw, e := json.Marshal(r)
	if e != nil {
		return e
	}
	_, e = tx.Exec(`UPDATE recovery_runs SET generation=?,revision=?,state=?,payload=? WHERE session_id=? AND request_id=?`, r.Generation, r.Revision, r.State, raw, r.Source.Session, r.Source.Request)
	if e != nil {
		return e
	}
	return bump(tx, r.Source.Session)
}
func (s *Store) Recovery(sid, rid string) (recovery.Run, bool, error) {
	return recoveryLookup(s.db, sid, rid)
}
func (s *Store) RecoverySnapshots(sid string) ([]recovery.Run, error) {
	rows, e := s.db.Query(`SELECT payload FROM recovery_runs WHERE session_id=? ORDER BY rowid DESC LIMIT 100`, sid)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []recovery.Run{}
	for rows.Next() {
		var raw []byte
		var r recovery.Run
		if e = rows.Scan(&raw); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(raw, &r); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// BeginRecovery owns the exact canonical queue payload and its run in one DB
// transaction. It cannot backfill failed/unknown work or change a prior input.
func (s *Store) BeginRecovery(source recovery.AutomaticSource, epoch string, now int64, files ...recovery.FileLease) (recovery.Run, error) {
	return s.beginRecovery(source, epoch, now, recovery.ClockAnchor{}, files...)
}

// BeginRecoveryClock binds production admission to verified same-boot elapsed time.
func (s *Store) BeginRecoveryClock(source recovery.AutomaticSource, epoch string, clock recovery.ClockAnchor, files ...recovery.FileLease) (recovery.Run, error) {
	if !clock.Valid() {
		return recovery.Run{}, recovery.ErrClock
	}
	return s.beginRecovery(source, epoch, clock.WallMS, clock, files...)
}

func (s *Store) beginRecovery(source recovery.AutomaticSource, epoch string, now int64, clock recovery.ClockAnchor, files ...recovery.FileLease) (recovery.Run, error) {
	tx, e := s.db.Begin()
	if e != nil {
		return recovery.Run{}, e
	}
	defer tx.Rollback()
	q, found, e := lookup(tx, source.Session, source.Request)
	if e != nil {
		return recovery.Run{}, e
	}
	if !found || q.State != Running || len(q.Payload) == 0 {
		return recovery.Run{}, recovery.ErrGuard
	}
	if r, ok, e := recoveryLookup(tx, source.Session, source.Request); e != nil {
		return r, e
	} else if ok {
		if r.Source != source {
			return r, recovery.ErrGuard
		}
		return r, nil
	}
	if source.Authority == "" || source.Principal == "" || source.Configuration == "" || source.Scope == "" || epoch == "" {
		return recovery.Run{}, recovery.ErrGuard
	}
	if e = retireRecoveryInputsTx(tx); e != nil {
		return recovery.Run{}, e
	}
	var total int64
	if e = tx.QueryRow(`SELECT COALESCE(SUM(length(payload)),0) FROM recovery_inputs`).Scan(&total); e != nil {
		return recovery.Run{}, e
	}
	if total+int64(len(q.Payload)) > recovery.MaxInputBytes {
		return recovery.Run{}, ErrRecoveryInputPressure
	}
	r := recovery.Run{Clock: clock, ClockLast: now, Source: source, Revision: 1, Generation: 1, OwnerEpoch: epoch, State: "ready", InputHash: recovery.Hash(q.Payload), LeaseUntil: now + int64((24*time.Hour)/time.Millisecond), MaxAttempts: 5}
	raw, _ := json.Marshal(r)
	if _, e = tx.Exec(`INSERT INTO recovery_runs VALUES(?,?,?,?,?,?)`, source.Session, source.Request, r.Generation, r.Revision, r.State, raw); e != nil {
		return r, e
	}
	if _, e = tx.Exec(`INSERT INTO recovery_inputs VALUES(?,?,?,?,?)`, source.Session, source.Request, r.InputHash, r.LeaseUntil, q.Payload); e != nil {
		return r, e
	}
	leaseJSON, _ := json.Marshal(files)
	if _, e = tx.Exec(`INSERT INTO recovery_file_leases VALUES(?,?,?)`, source.Session, source.Request, leaseJSON); e != nil {
		return r, e
	}
	if e = bump(tx, source.Session); e != nil {
		return r, e
	}
	return r, tx.Commit()
}
func (s *Store) RecoveryInput(sid, rid string) ([]byte, error) {
	var raw []byte
	var hash string
	e := s.db.QueryRow(`SELECT payload,hash FROM recovery_inputs WHERE session_id=? AND request_id=?`, sid, rid).Scan(&raw, &hash)
	if e != nil {
		return nil, e
	}
	if recovery.Hash(raw) != hash {
		return nil, recovery.ErrGuard
	}
	return raw, nil
}
func checkRecovery(r recovery.Run, generation uint64, epoch string, now int64) error {
	if r.Generation != generation || r.OwnerEpoch != epoch || r.LeaseUntil <= now {
		return recovery.ErrGuard
	}
	return nil
}

// ClaimRecovery commits a dispatching tombstone BEFORE calling the writer. A
// process crash in this state can only reconcile, never acquire another claim.
func (s *Store) ClaimRecovery(sid, rid string, generation uint64, epoch string, transport uint64, inputHash string, now int64) (recovery.Run, recovery.Attempt, error) {
	tx, e := s.db.Begin()
	if e != nil {
		return recovery.Run{}, recovery.Attempt{}, e
	}
	defer tx.Rollback()
	r, ok, e := recoveryLookup(tx, sid, rid)
	if e != nil {
		return r, recovery.Attempt{}, e
	}
	if !ok {
		return r, recovery.Attempt{}, recovery.ErrGuard
	}
	if e = checkRecovery(r, generation, epoch, now); e != nil {
		return r, recovery.Attempt{}, e
	}
	// Even an unsuccessful due-time check records observed clock order. Once a
	// backwards sample is observed, restart cannot make this run dispatchable.
	if r.ClockUnsafe || now < r.ClockLast {
		r.ClockUnsafe = true
		if r.State == "ready" || r.State == "retry_wait" {
			r.State, r.Reason, r.NextAt = "paused_uncertain", "clock_provenance_unavailable", 0
		}
		if e = saveRecovery(tx, &r); e != nil {
			return r, recovery.Attempt{}, e
		}
		if e = tx.Commit(); e != nil {
			return r, recovery.Attempt{}, e
		}
		return r, recovery.Attempt{}, recovery.ErrClock
	}
	r.ClockLast = now
	if r.State != "ready" && r.State != "retry_wait" {
		return r, recovery.Attempt{}, recovery.ErrGuard
	}
	if now < r.NextAt {
		if e = saveRecovery(tx, &r); e != nil {
			return r, recovery.Attempt{}, e
		}
		if e = tx.Commit(); e != nil {
			return r, recovery.Attempt{}, e
		}
		return r, recovery.Attempt{}, recovery.ErrDispatchDeferred
	}
	if (r.Deadline > 0 && now >= r.Deadline) || r.Attempts >= r.MaxAttempts && r.LastAttempt != "" {
		r.State = "paused_budget"
		r.Reason = "retry_budget_exhausted"
		r.NextAt = 0
		if e = saveRecovery(tx, &r); e != nil {
			return r, recovery.Attempt{}, e
		}
		if e = tx.Commit(); e != nil {
			return r, recovery.Attempt{}, e
		}
		return r, recovery.Attempt{}, recovery.ErrDispatchPaused
	}
	var queueState State
	var queueSequence int64
	if e = tx.QueryRow(`SELECT state,seq FROM queue_commands WHERE session_id=? AND request_id=?`, sid, rid).Scan(&queueState, &queueSequence); e != nil {
		return r, recovery.Attempt{}, e
	}
	if queueState != Running {
		return r, recovery.Attempt{}, recovery.ErrGuard
	}
	// A newer ordinary input invalidates the recovery generation even if the
	// same source/configuration later returns (ABA).
	var newer int
	if e = tx.QueryRow(`SELECT count(*) FROM queue_commands WHERE session_id=? AND seq>?`, sid, queueSequence).Scan(&newer); e != nil {
		return r, recovery.Attempt{}, e
	}
	if newer > 0 {
		return r, recovery.Attempt{}, recovery.ErrGuard
	}
	if _, e = tx.Exec(`INSERT OR IGNORE INTO recovery_scopes(scope) VALUES(?)`, r.Source.Scope); e != nil {
		return r, recovery.Attempt{}, e
	}
	var owner string
	var lease, cooldown, last int64
	if e = tx.QueryRow(`SELECT owner,lease_until,cooldown_until,last_dispatch FROM recovery_scopes WHERE scope=?`, r.Source.Scope).Scan(&owner, &lease, &cooldown, &last); e != nil {
		return r, recovery.Attempt{}, e
	}
	isRetry := r.LastAttempt != ""
	// Only previously registered, still eligible same-scope waiters hold FIFO
	// priority. Ordering is the immutable original queue sequence, not which
	// goroutine happens to wake first or a wall clock that can move backwards.
	var olderWaiters int
	if isRetry {
		e = tx.QueryRow(`SELECT count(*) FROM recovery_runs waiting JOIN queue_commands q USING(session_id,request_id)
          WHERE waiting.state='retry_wait' AND q.state IN ('queued','running') AND q.seq<?
          AND json_extract(waiting.payload,'$.source.scope_hash')=? AND json_extract(waiting.payload,'$.scope_waiting')=1
          AND json_extract(waiting.payload,'$.input_lease_until')>? AND (COALESCE(json_extract(waiting.payload,'$.deadline'),0)=0 OR json_extract(waiting.payload,'$.deadline')>?)
          AND NOT EXISTS(SELECT 1 FROM queue_commands newer WHERE newer.session_id=q.session_id AND newer.seq>q.seq)`, queueSequence, r.Source.Scope, now, now).Scan(&olderWaiters)
		if e != nil {
			return r, recovery.Attempt{}, e
		}
	}
	// A dispatched/accepted/unknown probe is held until durable settlement,
	// including restart and cancellation. Wall-clock lease expiry is not proof.
	if isRetry && (olderWaiters > 0 || cooldown > now || owner != "" && owner != sid+":"+rid || last > now-10000) {
		r.ScopeWaiting = true
		r.NextAt = now + 1000
		if r.NextAt < cooldown {
			r.NextAt = cooldown
		}
		if r.NextAt < last+10000 {
			r.NextAt = last + 10000
		}
		result := recovery.ErrDispatchDeferred
		if r.Deadline > 0 && r.NextAt >= r.Deadline {
			r.State = "paused_budget"
			r.Reason = "scope_wait_exceeds_budget"
			r.NextAt = 0
			result = recovery.ErrDispatchPaused
		}
		if e = saveRecovery(tx, &r); e != nil {
			return r, recovery.Attempt{}, e
		}
		if e = tx.Commit(); e != nil {
			return r, recovery.Attempt{}, e
		}
		return r, recovery.Attempt{}, result
	}
	_ = lease // diagnostic timer only; never retires unresolved scope ownership.
	var raw []byte
	var hash string
	if e = tx.QueryRow(`SELECT payload,hash FROM recovery_inputs WHERE session_id=? AND request_id=?`, sid, rid).Scan(&raw, &hash); e != nil {
		return r, recovery.Attempt{}, e
	}
	if hash != r.InputHash || recovery.Hash(raw) != hash {
		return r, recovery.Attempt{}, recovery.ErrGuard
	}
	n := 0
	if r.LastAttempt != "" {
		r.Attempts++
		n = r.Attempts
	}
	a := recovery.Attempt{Kind: r.Kind, ID: fmt.Sprintf("ra_%s_%d", recovery.Hash([]byte(sid + ":" + rid))[:24], n), Number: n, Generation: r.Generation, State: "dispatching", Thread: r.Source.Thread, TransportEpoch: transport, InputHash: inputHash, Acceptance: recovery.AcceptanceUnknown}
	ar, _ := json.Marshal(a)
	if _, e = tx.Exec(`INSERT INTO recovery_attempts VALUES(?,?,?,?,?,?)`, sid, rid, a.ID, a.Generation, a.State, ar); e != nil {
		return r, a, e
	}
	r.LastAttempt = a.ID
	r.ScopeWaiting = false
	r.State = "dispatching"
	r.NextAt = 0
	if isRetry {
		if _, e = tx.Exec(`UPDATE recovery_scopes SET owner=?,lease_until=?,last_dispatch=? WHERE scope=?`, sid+":"+rid, now+30000, now, r.Source.Scope); e != nil {
			return r, a, e
		}
	}
	if e = saveRecovery(tx, &r); e != nil {
		return r, a, e
	}
	return r, a, tx.Commit()
}
func (s *Store) RecoveryAccepted(sid, rid, attempt, thread, turn string, generation uint64, epoch string, now int64) error {
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	r, ok, e := recoveryLookup(tx, sid, rid)
	if e != nil {
		return e
	}
	if !ok || r.LastAttempt != attempt || r.Source.Thread != thread || turn == "" {
		return recovery.ErrGuard
	}
	var raw []byte
	if e = tx.QueryRow(`SELECT payload FROM recovery_attempts WHERE session_id=? AND request_id=? AND attempt_id=?`, sid, rid, attempt).Scan(&raw); e != nil {
		return e
	}
	var a recovery.Attempt
	if e = json.Unmarshal(raw, &a); e != nil {
		return e
	}
	// Preserve exact late acceptance as evidence even when cancelled; it cannot
	// revive a schedule or overwrite a different attempt/native tuple.
	if a.Turn != "" && (a.Turn != turn || a.Thread != thread) {
		return recovery.ErrGuard
	}
	a.Turn = turn
	a.Thread = thread
	a.Acceptance = recovery.Accepted
	a.State = "accepted"
	raw, _ = json.Marshal(a)
	if _, e = tx.Exec(`UPDATE recovery_attempts SET phase=?,payload=? WHERE session_id=? AND request_id=? AND attempt_id=?`, a.State, raw, sid, rid, attempt); e != nil {
		return e
	}
	if checkRecovery(r, generation, epoch, now) == nil && r.State == "dispatching" {
		r.State = "accepted"
		if e = saveRecovery(tx, &r); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (s *Store) ScheduleRecovery(sid, rid string, generation uint64, epoch string, o recovery.AutomaticOutcome, now int64, jitter float64) (recovery.Run, bool, error) {
	tx, e := s.db.Begin()
	if e != nil {
		return recovery.Run{}, false, e
	}
	defer tx.Rollback()
	r, ok, e := recoveryLookup(tx, sid, rid)
	if e != nil {
		return r, false, e
	}
	if !ok {
		return r, false, recovery.ErrGuard
	}
	if e = checkRecovery(r, generation, epoch, now); e != nil {
		return r, false, e
	}
	if r.State != "dispatching" && r.State != "accepted" {
		return r, false, recovery.ErrGuard
	}
	var raw []byte
	if e = tx.QueryRow(`SELECT payload FROM recovery_attempts WHERE session_id=? AND request_id=? AND attempt_id=?`, sid, rid, r.LastAttempt).Scan(&raw); e != nil {
		return r, false, e
	}
	var a recovery.Attempt
	if e = json.Unmarshal(raw, &a); e != nil {
		return r, false, e
	}
	if o.Acceptance == recovery.Accepted && (a.Acceptance != recovery.Accepted || a.Turn != o.Turn || a.Thread != o.Thread) {
		return r, false, recovery.ErrGuard
	}
	r.ClockUnsafe = r.ClockUnsafe || now < r.ClockLast
	r.ClockLast = max(now, r.ClockLast)
	kind, reason := recovery.Recoverable(o)
	a.State = "terminal"
	a.Failure = o.Failure
	a.Terminal = o.Terminal
	a.Acceptance = o.Acceptance
	a.EvidenceHash = o.Canonical.EvidenceHash
	a.EffectsSettled = kind == "continue"
	raw, _ = json.Marshal(a)
	if _, e = tx.Exec(`UPDATE recovery_attempts SET phase=?,payload=? WHERE session_id=? AND request_id=? AND attempt_id=?`, a.State, raw, sid, rid, a.ID); e != nil {
		return r, false, e
	}
	if r.FirstFailureAt == 0 {
		r.FirstFailureAt = now
		r.Deadline = now + int64(30*time.Minute/time.Millisecond)
		if r.LeaseUntil < r.Deadline {
			r.Deadline = r.LeaseUntil
		}
	}
	r.Reason = reason
	r.ScopeWaiting = false
	r.Kind = kind
	r.ParentTurn = o.Turn
	if a.Number > 0 && kind == "continue" {
		if o.Canonical.ProgressHash == "" || o.Canonical.ProgressHash == r.ProgressHash {
			r.NoProgress++
		} else {
			r.NoProgress = 0
		}
	}
	if o.Canonical.ProgressHash != "" {
		r.ProgressHash = o.Canonical.ProgressHash
	}
	if r.ClockUnsafe {
		r.State, r.Reason, r.NextAt = "paused_uncertain", "clock_provenance_unavailable", 0
	} else if kind == "" {
		r.State = "paused_uncertain"
		if reason == "waiting_user" {
			r.State = "waiting_user"
		}
	} else if r.Attempts >= r.MaxAttempts || now >= r.Deadline || r.NoProgress >= 2 {
		r.State = "paused_budget"
		r.Reason = "recovery_budget_exhausted"
	} else {
		r.NextAt = now + recovery.Delay(r.Attempts+1, jitter, o.RetryAfter).Milliseconds()
		r.State = "retry_wait"
		if r.NextAt >= r.Deadline {
			r.State = "paused_budget"
			r.Reason = "retry_after_exceeds_budget"
		}
	}
	if a.Acceptance == recovery.Rejected || a.Acceptance == recovery.Accepted && a.Terminal {
		if _, e = tx.Exec(`UPDATE recovery_scopes SET owner='',lease_until=0 WHERE scope=? AND owner=?`, r.Source.Scope, sid+":"+rid); e != nil {
			return r, false, e
		}
	}
	if _, e = tx.Exec(`UPDATE recovery_scopes SET window_start=CASE WHEN window_start<? THEN ? ELSE window_start END,rejections=CASE WHEN window_start<? THEN 1 ELSE rejections+1 END WHERE scope=?`, now-60000, now, now-60000, r.Source.Scope); e != nil {
		return r, false, e
	}
	if _, e = tx.Exec(`UPDATE recovery_scopes SET cooldown_until=MAX(cooldown_until,?) WHERE scope=? AND rejections>=3`, now+60000, r.Source.Scope); e != nil {
		return r, false, e
	}
	if r.State == "retry_wait" {
		var cooldown, last int64
		if e = tx.QueryRow(`SELECT cooldown_until,last_dispatch FROM recovery_scopes WHERE scope=?`, r.Source.Scope).Scan(&cooldown, &last); e != nil {
			return r, false, e
		}
		if r.NextAt < cooldown {
			r.NextAt = cooldown
		}
		if r.NextAt < last+10000 {
			r.NextAt = last + 10000
		}
		if r.NextAt >= r.Deadline {
			r.State = "paused_budget"
			r.Reason = "scope_cooldown_exceeds_budget"
		}
	}
	if e = saveRecovery(tx, &r); e != nil {
		return r, false, e
	}
	return r, r.State == "retry_wait", tx.Commit()
}
func (s *Store) InvalidateRecovery(sid, rid, reason string) error {
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	rows, e := tx.Query(`SELECT payload FROM recovery_runs WHERE session_id=?`+func() string {
		if rid != "" {
			return ` AND request_id=?`
		}
		return ""
	}(), func() []any {
		if rid != "" {
			return []any{sid, rid}
		}
		return []any{sid}
	}()...)
	if e != nil {
		return e
	}
	var runs []recovery.Run
	for rows.Next() {
		var raw []byte
		if e = rows.Scan(&raw); e != nil {
			rows.Close()
			return e
		}
		var r recovery.Run
		if e = json.Unmarshal(raw, &r); e != nil {
			rows.Close()
			return e
		}
		runs = append(runs, r)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, r := range runs {
		if r.State == "native_completed" || r.State == "cancelled" || r.State == "failed_permanent" {
			continue
		}
		r.Generation++
		r.ScopeWaiting = false
		r.State = "cancelled"
		if reason == "disabled" {
			r.State = "disabled"
		}
		r.Reason = reason
		r.NextAt = 0
		if e = saveRecovery(tx, &r); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (s *Store) FinishRecovery(sid, rid, status string) error {
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	r, ok, e := recoveryLookup(tx, sid, rid)
	if e != nil {
		return e
	}
	if !ok {
		return nil
	}
	// A completed native turn requires its committed exact acceptance receipt.
	// Persistence failure may never be projected as a logical success.
	if status == "completed" || status == "cancelled" {
		if r.LastAttempt != "" {
			var raw []byte
			if e = tx.QueryRow(`SELECT payload FROM recovery_attempts WHERE session_id=? AND request_id=? AND attempt_id=?`, sid, rid, r.LastAttempt).Scan(&raw); e != nil {
				return e
			}
			var a recovery.Attempt
			if e = json.Unmarshal(raw, &a); e != nil {
				return e
			}
			if status == "completed" && (a.Acceptance != recovery.Accepted || a.Turn == "" || a.Thread != r.Source.Thread) {
				return recovery.ErrGuard
			}
			if a.Acceptance == recovery.Accepted {
				a.Terminal, a.State = true, "terminal"
				if status == "completed" {
					a.EffectsSettled = true
				}
				raw, _ = json.Marshal(a)
				if _, e = tx.Exec(`UPDATE recovery_attempts SET phase='terminal',payload=? WHERE session_id=? AND request_id=? AND attempt_id=?`, raw, sid, rid, a.ID); e != nil {
					return e
				}
				if _, e = tx.Exec(`UPDATE recovery_scopes SET owner='',lease_until=0 WHERE scope=? AND owner=?`, r.Source.Scope, sid+":"+rid); e != nil {
					return e
				}
			}
		} else if status == "completed" {
			return recovery.ErrGuard
		}
	}
	// Native completed is deliberately distinct from objective verification.
	r.NativeStatus = status
	r.ScopeWaiting = false
	r.ObjectiveVerified = false
	r.NextAt = 0
	if status == "disabled" {
		r.State = "disabled"
		r.Reason = "provider_subject_unavailable"
	} else if status == "completed" {
		r.State = "native_completed"
		r.Reason = "objective_verification_not_available"
	} else if status == "cancelled" {
		r.State = "cancelled"
	} else if r.State != "paused_uncertain" && r.State != "paused_budget" && r.State != "waiting_user" && r.State != "disabled" && r.State != "cancelled" {
		r.State = "failed_permanent"
	}
	if e = saveRecovery(tx, &r); e != nil {
		return e
	}
	if e = retireRecoveryInputsTx(tx); e != nil {
		return e
	}
	return tx.Commit()
}

// Retire only immutable input copies whose logical queue is terminal and whose
// exact last dispatch is proven rejected or accepted-terminal. Expiry alone is
// never evidence that an unknown/native-active dispatch ended. Hashes, leases,
// run generations and every attempt/tombstone remain in the original database.
func retireRecoveryInputsTx(tx *sql.Tx) error {
	rows, e := tx.Query(`SELECT r.payload FROM recovery_runs r JOIN recovery_inputs i USING(session_id,request_id) JOIN queue_commands q USING(session_id,request_id) WHERE length(i.payload)>0 AND r.state IN ('native_completed','cancelled','failed_permanent','disabled') AND q.state IN ('completed','cancelled','failed')`)
	if e != nil {
		return e
	}
	var runs []recovery.Run
	for rows.Next() {
		var raw []byte
		if e = rows.Scan(&raw); e != nil {
			rows.Close()
			return e
		}
		var r recovery.Run
		if e = json.Unmarshal(raw, &r); e != nil {
			rows.Close()
			return e
		}
		runs = append(runs, r)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, r := range runs {
		if r.LastAttempt != "" {
			var raw []byte
			if e = tx.QueryRow(`SELECT payload FROM recovery_attempts WHERE session_id=? AND request_id=? AND attempt_id=?`, r.Source.Session, r.Source.Request, r.LastAttempt).Scan(&raw); e != nil {
				return e
			}
			var a recovery.Attempt
			if e = json.Unmarshal(raw, &a); e != nil {
				return e
			}
			if a.Acceptance != recovery.Rejected && !(a.Acceptance == recovery.Accepted && a.Terminal && a.EffectsSettled) {
				continue
			}
		}
		if _, e = tx.Exec(`UPDATE recovery_scopes SET owner='',lease_until=0 WHERE scope=? AND owner=?`, r.Source.Scope, r.Source.Session+":"+r.Source.Request); e != nil {
			return e
		}
		if _, e = tx.Exec(`UPDATE recovery_inputs SET payload=X'' WHERE session_id=? AND request_id=?`, r.Source.Session, r.Source.Request); e != nil {
			return e
		}
	}
	return nil
}

// Called inside the original queue restart transaction, before its quarantine.
func recoverAutomaticTx(tx *sql.Tx) error {
	// Only a persisted explicit rejected/failed-terminal retry_wait is resumable.
	// Anything ever dispatching at the crash has an unknown submit tombstone.
	rows, e := tx.Query(`SELECT payload FROM recovery_runs WHERE state NOT IN ('native_completed','cancelled','failed_permanent','disabled')`)
	if e != nil {
		return e
	}
	var runs []recovery.Run
	for rows.Next() {
		var raw []byte
		if e = rows.Scan(&raw); e != nil {
			rows.Close()
			return e
		}
		var r recovery.Run
		if e = json.Unmarshal(raw, &r); e != nil {
			rows.Close()
			return e
		}
		runs = append(runs, r)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, r := range runs {
		if r.State == "retry_wait" {
			if _, e = tx.Exec(`UPDATE queue_commands SET state='queued' WHERE session_id=? AND request_id=? AND state IN ('running','uncertain') AND length(payload)>0`, r.Source.Session, r.Source.Request); e != nil {
				return e
			}
		} else if r.State == "dispatching" || r.State == "accepted" || r.State == "ready" {
			r.State = "reconciling"
			r.Reason = "restart_submit_outcome_unknown"
		}
		if e = saveRecovery(tx, &r); e != nil {
			return e
		}
	}
	return nil
}

func (s *Store) AdoptRecovery(sid, rid string, generation uint64, epoch string) error {
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	r, ok, e := recoveryLookup(tx, sid, rid)
	if e != nil {
		return e
	}
	if !ok || r.Generation != generation || r.State != "retry_wait" {
		return recovery.ErrGuard
	}
	r.OwnerEpoch = epoch
	if e = saveRecovery(tx, &r); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) RecoveryNoNewInput(sid, rid string) error {
	var n int
	e := s.db.QueryRow(`SELECT COUNT(*) FROM queue_commands newer JOIN queue_commands original ON original.session_id=newer.session_id WHERE original.session_id=? AND original.request_id=? AND newer.seq>original.seq`, sid, rid).Scan(&n)
	if e != nil {
		return e
	}
	if n != 0 {
		return recovery.ErrGuard
	}
	return nil
}

func (s *Store) RecoveryNativeOwned(sid, rid, thread, turn string) (bool, error) {
	r, ok, e := s.Recovery(sid, rid)
	if e != nil || !ok {
		return false, e
	}
	if r.State == "disabled" && r.LastAttempt == "" {
		return false, nil
	}
	var n int
	e = s.db.QueryRow(`SELECT count(*) FROM recovery_attempts WHERE session_id=? AND request_id=? AND json_extract(payload,'$.thread_id')=? AND json_extract(payload,'$.turn_id')=? AND json_extract(payload,'$.acceptance')='accepted'`, sid, rid, thread, turn).Scan(&n)
	if e != nil {
		return false, e
	}
	if n != 1 {
		return false, recovery.ErrGuard
	}
	_ = r
	return true, nil
}

func (s *Store) MarkRecoveryReconciling(sid, rid, reason string) error {
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	r, ok, e := recoveryLookup(tx, sid, rid)
	if e != nil {
		return e
	}
	if !ok {
		return nil
	}
	if r.State == "cancelled" || r.State == "disabled" {
		return nil
	}
	r.State = "reconciling"
	r.Reason = reason
	r.NextAt = 0
	if e = saveRecovery(tx, &r); e != nil {
		return e
	}
	return tx.Commit()
}

func (s *Store) BindRecoveryThread(sid, rid, thread string, generation uint64, epoch string, sourceRevision ...uint64) error {
	if thread == "" {
		return recovery.ErrGuard
	}
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	r, ok, e := recoveryLookup(tx, sid, rid)
	if e != nil {
		return e
	}
	if !ok || r.Generation != generation || r.OwnerEpoch != epoch {
		return recovery.ErrGuard
	}
	if r.Source.Thread == thread {
		return nil
	}
	if r.Source.Thread != "" || r.State != "ready" || r.LastAttempt != "" {
		return recovery.ErrGuard
	}
	if len(sourceRevision) != 1 || sourceRevision[0] != r.Source.SourceRevision+1 || sourceRevision[0] == 0 {
		return recovery.ErrGuard
	}
	r.Source.Thread = thread
	r.Source.SourceRevision = sourceRevision[0]
	if e = saveRecovery(tx, &r); e != nil {
		return e
	}
	return tx.Commit()
}

func (s *Store) RecoveryFileLeases(sid, rid string) ([]recovery.FileLease, error) {
	var raw []byte
	e := s.db.QueryRow(`SELECT payload FROM recovery_file_leases WHERE session_id=? AND request_id=?`, sid, rid).Scan(&raw)
	if e != nil {
		return nil, e
	}
	var files []recovery.FileLease
	e = json.Unmarshal(raw, &files)
	return files, e
}
func (s *Store) BindRecoveryProvider(sid, rid, provider string, generation uint64, epoch string) error {
	if provider == "" {
		return recovery.ErrGuard
	}
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	r, ok, e := recoveryLookup(tx, sid, rid)
	if e != nil {
		return e
	}
	if !ok || r.Generation != generation || r.OwnerEpoch != epoch {
		return recovery.ErrGuard
	}
	if r.Source.Provider == provider {
		return nil
	}
	if r.Source.Provider != "" || r.State != "ready" || r.LastAttempt != "" {
		return recovery.ErrGuard
	}
	r.Source.Provider = provider
	r.Source.Scope = recovery.Hash([]byte(r.Source.Authority + ":" + provider + ":" + r.Source.Scope))
	if e = saveRecovery(tx, &r); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) RecoveryStorageReady() error {
	var n int
	e := s.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('recovery_runs','recovery_inputs','recovery_attempts','recovery_scopes','recovery_file_leases')`).Scan(&n)
	if e != nil {
		return e
	}
	if n != 5 {
		return recovery.ErrGuard
	}
	return nil
}

func (s *Store) ObserveRecoveryFailure(sid, rid string, o recovery.AutomaticOutcome, now int64) error {
	if o.Failure.Source != "codex_error_info" || (o.Failure.Category != recovery.ModelCapacity && o.Failure.Category != recovery.TransportUncertain && o.Failure.Category != recovery.TemporaryRateLimit) {
		return nil
	}
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	r, ok, e := recoveryLookup(tx, sid, rid)
	if e != nil {
		return e
	}
	if !ok || r.FirstFailureAt != 0 || r.State == "cancelled" || r.State == "disabled" {
		return nil
	}
	r.ClockUnsafe = r.ClockUnsafe || now < r.ClockLast
	r.ClockLast = max(now, r.ClockLast)
	r.FirstFailureAt = now
	r.Deadline = now + 1800000
	if r.Deadline > r.LeaseUntil {
		r.Deadline = r.LeaseUntil
	}
	if e = saveRecovery(tx, &r); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *Store) RecoveryWaitingCancellable(sid, rid string) (bool, error) {
	r, ok, e := s.Recovery(sid, rid)
	if e != nil || !ok {
		return false, e
	}
	var raw []byte
	e = s.db.QueryRow(`SELECT payload FROM recovery_attempts WHERE session_id=? AND request_id=? AND attempt_id=?`, sid, rid, r.LastAttempt).Scan(&raw)
	if e != nil {
		return false, e
	}
	var a recovery.Attempt
	if e = json.Unmarshal(raw, &a); e != nil {
		return false, e
	}
	return a.Acceptance == recovery.Rejected || a.Acceptance == recovery.Accepted && a.Terminal, nil
}

// PauseRecoveryClock only retires NEW dispatch authority. Existing accepted or
// unknown native ownership and its scope lease remain untouched.
func (s *Store) PauseRecoveryClock(sid, rid string) error {
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	r, ok, e := recoveryLookup(tx, sid, rid)
	if e != nil {
		return e
	}
	if !ok {
		return recovery.ErrGuard
	}
	r.ClockUnsafe = true
	if r.State == "ready" || r.State == "retry_wait" {
		r.State = "paused_uncertain"
		r.Reason = "clock_provenance_unavailable"
		r.NextAt = 0
	}
	if e = saveRecovery(tx, &r); e != nil {
		return e
	}
	return tx.Commit()
}
