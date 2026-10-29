package taskpipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

const missionRecoveryPrefix = "__mission_resume:"

func ProgressHash(r Record) string {
	proofs := []struct{ ID, State, Evidence string }{}
	required := map[string]bool{}
	for _, st := range Steps(r) {
		required[st.ID] = st.Required
	}
	for _, s := range r.Steps {
		if !required[s.ID] {
			continue
		}
		state := s.State
		if state != "done" && state != "satisfied" && state != "failed" {
			state = "pending"
		}
		proofs = append(proofs, struct{ ID, State, Evidence string }{s.ID, state, s.EvidenceHash})
	}
	return digest(proofs)
}
func RequiredSatisfied(r Record) bool {
	for _, st := range Steps(r) {
		if !st.Required {
			continue
		}
		found := false
		for _, v := range r.Steps {
			if v.ID == st.ID && (v.State == "done" || (st.Action == nil && v.State == "satisfied")) {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// EnsureRecovery coalesces any number of received facts into ONE authorized
// original-actor attempt. Only a known sealed predecessor may advance a new
// generation; unknown receipts never create a successor key.
func (s *Store) EnsureRecovery(ctx context.Context, id string, expected uint64, facts []Fact, check func(context.Context) error) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var b, ir []byte
	e = tx.QueryRowContext(ctx, `SELECT record,issuer FROM task_pipeline_records WHERE id=?`, id).Scan(&b, &ir)
	if e != nil {
		return e
	}
	var r Record
	var issuer Issuer
	if e = json.Unmarshal(b, &r); e != nil {
		return e
	}
	if e = json.Unmarshal(ir, &issuer); e != nil {
		return e
	}
	if r.Revision != expected {
		return ErrConflict
	}
	if r.State != "active" || r.Mission == nil || r.Mission.RecoveryAction == nil {
		return nil
	}
	if check == nil {
		return ErrScope
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return e
	}
	if e = triggersAllowed(ctx, tx, issuer.Scope); e != nil {
		return e
	}
	var pending int
	if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND state NOT IN ('checkpointed','retired','rejected','no_progress')`, id).Scan(&pending); e != nil {
		return e
	}
	if pending != 0 {
		return nil
	}
	// A satisfied whole goal still receives its one processing attempt; its final
	// can finish the mission only with these same immutable prerequisite proofs.
	auth := Auth(r)
	if !auth.Confirmed || auth.Budget.DeadlineMS <= now() {
		r.State = "expired"
		r.BlockedReason = "authorization_expired"
	} else if r.NoProgressCount >= uint64(auth.Budget.MaxNoProgress) {
		r.State = "blocked"
		r.BlockedReason = "blocked_no_progress"
	} else {
		var used int
		if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND state NOT IN ('retired','rejected')`, id).Scan(&used); e != nil {
			return e
		}
		if used >= auth.Budget.MaxEffects || used >= auth.Budget.MaxModelTurns {
			r.State = "blocked"
			r.BlockedReason = "budget_exhausted"
		} else {
			if r.RecoveryGeneration > 0 && ExternalWaiting(r) {
				var lastRaw []byte
				if err := tx.QueryRowContext(ctx, `SELECT continuation FROM task_pipeline_outbox WHERE record_id=? AND node_id LIKE '__mission_resume:%' ORDER BY rowid DESC LIMIT 1`, r.ID).Scan(&lastRaw); err == nil {
					var last Continuation
					if err = json.Unmarshal(lastRaw, &last); err != nil {
						return err
					}
					if last.StartProgressHash == ProgressHash(r) {
						return nil
					}
				}
			}
			r.RecoveryGeneration++
			node := fmt.Sprintf("%s%d", missionRecoveryPrefix, r.RecoveryGeneration)
			cid := "pc_" + digest(struct {
				ID                  string
				Version, Generation uint64
			}{id, r.DefinitionRevision, r.RecoveryGeneration})[:32]
			c := Continuation{NativeOperations: []NativeOperationRef{}, ID: cid, RecordID: id, DefinitionRevision: r.DefinitionRevision, NodeID: node, Generation: r.RecoveryGeneration, StartProgressHash: ProgressHash(r), State: "ready", Action: *r.Mission.RecoveryAction, RequestID: "pauto_" + cid, Evidence: facts, Fence: 1}
			if c.Evidence == nil {
				c.Evidence = []Fact{}
			}
			if _, e = tx.ExecContext(ctx, `INSERT INTO task_pipeline_outbox VALUES(?,?,?,?,?,?)`, c.ID, id, c.DefinitionRevision, node, c.State, encode(c)); e != nil {
				return e
			}
		}
	}
	r.Revision++
	r.UpdatedAt = now()
	if e = save(ctx, tx, issuer.Scope, r, issuer); e != nil {
		return e
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return e
	}
	return tx.Commit()
}

// FinishRecovery records actual original native proof before deciding whether a
// new finite generation is needed. A final is processing evidence, not a vote
// that the goal/QA/remaining required steps are complete.
func (s *Store) FinishRecovery(ctx context.Context, c Continuation, f Fact, check func(context.Context) error) error {
	if !PredicateMet(Predicate{Kind: "success_sealed"}, f) {
		return errors.New("recovery_native_final_unverified")
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var raw, ir []byte
	e = tx.QueryRowContext(ctx, `SELECT record,issuer FROM task_pipeline_records WHERE id=?`, c.RecordID).Scan(&raw, &ir)
	if e != nil {
		return e
	}
	var r Record
	var i Issuer
	if e = json.Unmarshal(raw, &r); e != nil {
		return e
	}
	if e = json.Unmarshal(ir, &i); e != nil {
		return e
	}
	var original []byte
	var state string
	if e = tx.QueryRowContext(ctx, `SELECT state,continuation FROM task_pipeline_outbox WHERE id=?`, c.ID).Scan(&state, &original); e != nil {
		return e
	}
	if state == "checkpointed" || state == "no_progress" {
		return nil
	}
	if e = json.Unmarshal(original, &c); e != nil {
		return e
	}
	if c.OriginalReceipt == nil || c.OriginalReceipt.Session != f.Source.Session || c.OriginalReceipt.Request != f.Source.Request || f.Source.Authority != c.Action.Target.Authority || f.Source.Thread != c.Action.Target.Thread || f.Source.Revision != c.Action.Target.Revision {
		return ErrScope
	}
	if check == nil {
		return ErrScope
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return e
	}
	sameDefinition := r.DefinitionRevision == c.DefinitionRevision
	c.Terminal = f.Terminal
	c.NativeStart = f.NativeStart
	c.State = "checkpointed"
	applied := uint64(0)
	if sameDefinition {
		r.Revision++
		applied = r.Revision
		progress := ProgressHash(r)
		if r.State == "active" {
			if RequiredSatisfied(r) && progress == c.StartProgressHash {
				r.State = "done"
				r.BlockedReason = ""
				r.NoProgressCount = 0
			} else {
				c.State = "no_progress"
				if progress == c.StartProgressHash && !ExternalWaiting(r) {
					r.NoProgressCount++
				} else if progress != c.StartProgressHash {
					r.NoProgressCount = 0
				}
				if r.NoProgressCount >= uint64(Auth(r).Budget.MaxNoProgress) {
					r.State = "blocked"
					r.BlockedReason = "blocked_no_progress"
				}
			}
		}
		c.CheckpointRevision = r.Revision
		r.Checkpoint = &Checkpoint{Revision: r.Revision, StepID: c.NodeID, Processed: []string{}, ProgressHash: progress, Outcome: "progress", Blockers: []string{}, RequestID: f.Source.Request}
		if r.State == "done" {
			r.Checkpoint.Outcome = "done"
		}
		if r.State == "blocked" {
			r.Checkpoint.Outcome = "blocked"
			r.Checkpoint.Blockers = []string{r.BlockedReason}
		}
		r.UpdatedAt = now()
		if e = save(ctx, tx, i.Scope, r, i); e != nil {
			return e
		}
	}
	event := Event{ID: "pe_" + digest(struct{ Continuation, Proof string }{c.ID, sourceKey(f)}), RecordID: r.ID, NodeID: c.NodeID, State: "checkpointed", Fact: f, AppliedRevision: applied}
	if _, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO task_pipeline_events(event_id,record_id,node_id,source_key,event) VALUES(?,?,?,?,?)`, event.ID, r.ID, c.NodeID, sourceKey(f), encode(event)); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `UPDATE task_pipeline_outbox SET state=?,continuation=? WHERE id=?`, c.State, encode(c), c.ID); e != nil {
		return e
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return e
	}
	return tx.Commit()
}

func ExternalWaiting(r Record) bool {
	for _, st := range Steps(r) {
		if st.Source == nil && !(st.WorkItem != nil && st.Predicate.Kind == "human_decision") {
			continue
		}
		satisfied := false
		for _, state := range r.Steps {
			if state.ID == st.ID && (state.State == "satisfied" || state.State == "done" || state.State == "failed") {
				satisfied = true
			}
		}
		if !satisfied {
			return true
		}
	}
	return false
}

// HasRecoveryOwner reserves wake ownership for the original mission even while paused.
func (s *Store) HasRecoveryOwner(ctx context.Context, manager string) (bool, error) {
	var found bool
	e := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM task_pipeline_records WHERE json_extract(record,'$.manager.session_id')=? AND json_extract(record,'$.mission.recovery_action') IS NOT NULL AND state NOT IN ('done','cancelled','expired'))`, manager).Scan(&found)
	return found, e
}
