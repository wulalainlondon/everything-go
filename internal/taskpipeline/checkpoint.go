package taskpipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// ExecutionContext requires the exact original admitted request. It is not a
// session-wide watch or a way for a child to adopt its parent's mission catalog.
func (s *Store) ExecutionContext(ctx context.Context, session, request string) (Continuation, Issuer, Record, bool, error) {
	var b []byte
	e := s.db.QueryRowContext(ctx, `SELECT continuation FROM task_pipeline_outbox WHERE state IN ('queued','native_started','checkpoint_applied','checkpointed') AND ((json_extract(continuation,'$.request_id')=? AND json_extract(continuation,'$.action.target.session_id')=?) OR (json_extract(continuation,'$.original_receipt.session_id')=? AND json_extract(continuation,'$.original_receipt.request_id')=?)) LIMIT 1`, request, session, session, request).Scan(&b)
	if e == sql.ErrNoRows {
		return Continuation{}, Issuer{}, Record{}, false, nil
	}
	if e != nil {
		return Continuation{}, Issuer{}, Record{}, false, e
	}
	var c Continuation
	if e = json.Unmarshal(b, &c); e != nil {
		return c, Issuer{}, Record{}, false, e
	}
	i, r, e := s.Issuer(ctx, c.RecordID)
	return c, i, r, e == nil, e
}

// A native checkpoint ACK has its own durable receipt and updates the mission
// and outbox atomically. It proves processing, never native final or task PASS.
func (s *Store) Ack(ctx context.Context, scope Scope, intent, storageKey, hash, id string, expected uint64, cp Checkpoint, session, request string, check func(context.Context) error) (Receipt, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return Receipt{}, e
	}
	defer tx.Rollback()
	var oldOp, oldHash string
	var raw []byte
	e = tx.QueryRowContext(ctx, `SELECT operation,input_hash,receipt FROM task_pipeline_operations WHERE scope=? AND manager=? AND intent=?`, ScopeKey(scope), ManagerKey(scope), storageKey).Scan(&oldOp, &oldHash, &raw)
	if e == nil {
		if oldOp != "continuation_ack" || oldHash != hash {
			return Receipt{}, ErrConflict
		}
		var old Receipt
		e = json.Unmarshal(raw, &old)
		return old, e
	}
	if e != sql.ErrNoRows {
		return Receipt{}, e
	}
	if check == nil {
		return Receipt{}, ErrScope
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return Receipt{}, e
	}
	var c Continuation
	var state string
	e = tx.QueryRowContext(ctx, `SELECT continuation,state FROM task_pipeline_outbox WHERE id=?`, id).Scan(&raw, &state)
	if e != nil {
		return Receipt{}, e
	}
	if e = json.Unmarshal(raw, &c); e != nil {
		return Receipt{}, e
	}
	if c.Action.Target.Session != session || (c.RequestID != request && (c.OriginalReceipt == nil || c.OriginalReceipt.Request != request || c.OriginalReceipt.Session != session)) || cp.RequestID != request {
		return Receipt{}, ErrScope
	}
	r, i, found, e := get(ctx, tx, scope, c.RecordID)
	if e != nil {
		return Receipt{}, e
	}
	if !found || r.Revision != expected || cp.Revision != expected {
		return Receipt{}, ErrConflict
	}
	if r.DefinitionRevision != c.DefinitionRevision {
		return Receipt{}, errors.New("pipeline_definition_changed")
	}
	if state != "queued" && state != "native_started" && state != "checkpoint_applied" {
		return Receipt{}, errors.New("continuation_not_admitted")
	}
	for _, event := range cp.Processed {
		var n int
		if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_events WHERE record_id=? AND event_id=? AND json_extract(event,'$.state') IN ('evidence_verified','mission_applied')`, r.ID, event).Scan(&n); e != nil {
			return Receipt{}, e
		}
		if n != 1 {
			return Receipt{}, errors.New("unverified_checkpoint_event")
		}
	}
	if cp.Outcome == "done" && r.State != "done" {
		return Receipt{}, errors.New("required_steps_unsatisfied")
	}
	r.Checkpoint = &cp
	r.Revision++
	r.UpdatedAt = now()
	if e = appendNativeOperation(&c, intent, "continuation_ack", hash); e != nil {
		return Receipt{}, e
	}
	c.State = "checkpoint_applied"
	c.CheckpointRevision = r.Revision
	if e = save(ctx, tx, scope, r, i); e != nil {
		return Receipt{}, e
	}
	if _, e = tx.ExecContext(ctx, `UPDATE task_pipeline_outbox SET state=?,continuation=? WHERE id=?`, c.State, encode(c), c.ID); e != nil {
		return Receipt{}, e
	}
	receipt := Receipt{Provenance: s.proof, Kind: "operation_receipt", IntentID: intent, InputHash: hash, Operation: "continuation_ack", State: "applied", Record: &r, Continuation: &c, NoEffect: true}
	if _, e = tx.ExecContext(ctx, `INSERT INTO task_pipeline_operations VALUES(?,?,?,?,?,?)`, ScopeKey(scope), ManagerKey(scope), storageKey, "continuation_ack", hash, encode(receipt)); e != nil {
		return Receipt{}, e
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return Receipt{}, e
	}
	return receipt, tx.Commit()
}

// CompleteFromOriginal atomically checkpoints a predicate-proven exact native
// result. It never treats the chat's final text as mission completion by itself.
func (s *Store) CompleteFromOriginal(ctx context.Context, c Continuation, f Fact, check func(context.Context) error) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var b, ir []byte
	e = tx.QueryRowContext(ctx, `SELECT record,issuer FROM task_pipeline_records WHERE id=?`, c.RecordID).Scan(&b, &ir)
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
	var original []byte
	var state string
	e = tx.QueryRowContext(ctx, `SELECT continuation,state FROM task_pipeline_outbox WHERE id=?`, c.ID).Scan(&original, &state)
	if e != nil {
		return e
	}
	if state == "checkpointed" {
		var exists int
		if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_events WHERE record_id=? AND node_id=? AND source_key=?`, c.RecordID, c.NodeID, sourceKey(f)).Scan(&exists); e != nil {
			return e
		}
		if exists > 0 {
			return nil
		}
	}
	if check == nil {
		return ErrScope
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return e
	}
	if e = json.Unmarshal(original, &c); e != nil {
		return e
	}
	if c.OriginalReceipt == nil || c.OriginalReceipt.Session != f.Source.Session || c.OriginalReceipt.Request != f.Source.Request || f.Source.Authority != c.Action.Target.Authority || f.Source.Thread != c.Action.Target.Thread || f.Source.Revision != c.Action.Target.Revision || (c.OriginalReceipt.Kind == "delegation_round" && c.OriginalReceipt.NativeID != f.Source.RoundID) {
		return ErrScope
	}
	var frozen Record
	var frozenRaw []byte
	if e = tx.QueryRowContext(ctx, `SELECT record FROM task_pipeline_revisions WHERE record_id=? AND json_extract(record,'$.definition_revision')=? ORDER BY revision LIMIT 1`, c.RecordID, c.DefinitionRevision).Scan(&frozenRaw); e != nil {
		return e
	}
	if e = json.Unmarshal(frozenRaw, &frozen); e != nil {
		return e
	}
	sameDefinition := r.DefinitionRevision == c.DefinitionRevision
	var step *Step
	for _, st := range Steps(frozen) {
		if st.ID == c.NodeID {
			copy := st
			step = &copy
			break
		}
	}
	if step == nil {
		return ErrScope
	}
	// Step.Predicate guards the predecessor source. The newly issued action has
	// its own immutable input/native-final proof, never the predecessor's hash.
	predicate := Predicate{Kind: "success_sealed"}
	failed := (f.Terminal == "failed" || f.Terminal == "cancelled" || f.Terminal == "rejected") && PredicateMet(Predicate{Kind: "terminal"}, f)
	if !PredicateMet(predicate, f) && !failed {
		return errors.New("continuation_required_evidence_unverified")
	}
	c.State = "checkpointed"
	c.Terminal = f.Terminal
	c.NativeStart = f.NativeStart
	if sameDefinition {
		r.Revision++
		c.CheckpointRevision = r.Revision
	}
	appliedRevision := uint64(0)
	if sameDefinition {
		appliedRevision = r.Revision
	}
	event := Event{ID: "pe_" + digest(struct{ Continuation, Proof string }{c.ID, sourceKey(f)}), RecordID: r.ID, NodeID: c.NodeID, State: "checkpointed", Fact: f, AppliedRevision: appliedRevision}
	if _, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO task_pipeline_events(event_id,record_id,node_id,source_key,event) VALUES(?,?,?,?,?)`, event.ID, r.ID, c.NodeID, sourceKey(f), encode(event)); e != nil {
		return e
	}
	if !sameDefinition {
		if _, e = tx.ExecContext(ctx, `UPDATE task_pipeline_outbox SET state=?,continuation=? WHERE id=?`, c.State, encode(c), c.ID); e != nil {
			return e
		}
		if e = check(s.context(ctx, tx)); e != nil {
			return e
		}
		return tx.Commit()
	}
	found := false
	for index, st := range r.Steps {
		if st.ID == c.NodeID {
			r.Steps[index].State = "done"
			if failed {
				r.Steps[index].State = "failed"
				r.Steps[index].Reason = f.Terminal
			}
			r.Steps[index].EvidenceHash = sourceKey(f)
			found = true
		}
	}
	if !found {
		r.Steps = append(r.Steps, StepState{ID: c.NodeID, State: func() string {
			if failed {
				return "failed"
			}
			return "done"
		}(), EvidenceHash: sourceKey(f), ContinuationID: c.ID})
	}
	r.Checkpoint = &Checkpoint{Revision: r.Revision, StepID: c.NodeID, Processed: []string{event.ID}, ProgressHash: sourceKey(f), Outcome: "progress", Blockers: []string{}, RequestID: f.Source.Request}
	if _, e = tx.ExecContext(ctx, `UPDATE task_pipeline_outbox SET state=?,continuation=? WHERE id=?`, c.State, encode(c), c.ID); e != nil {
		return e
	}
	done := true
	for _, st := range Steps(r) {
		if !st.Required {
			continue
		}
		satisfied := false
		for _, v := range r.Steps {
			if v.ID == st.ID && (v.State == "done" || (st.Action == nil && v.State == "satisfied")) {
				satisfied = true
			}
		}
		done = done && satisfied
	}
	var unsettled int
	if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND state NOT IN ('checkpointed','retired','rejected','no_progress')`, r.ID).Scan(&unsettled); e != nil {
		return e
	}
	if done && unsettled == 0 && r.State == "active" && (r.Mission == nil || r.Mission.RecoveryAction == nil) {
		r.State = "done"
		r.Checkpoint.Outcome = "done"
	}
	r.UpdatedAt = now()
	if e = save(ctx, tx, issuer.Scope, r, issuer); e != nil {
		return e
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return e
	}
	return tx.Commit()
}
