package taskpipeline

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/workitems"
	"strings"
)

func PredicateMet(p Predicate, f Fact) bool {
	if p.Kind == "human_decision" {
		return p.DecisionRef != "" && p.DecisionRef == f.DecisionRef && f.Store == "jarvis_worklist_human_decision" && f.Sequence > 0 && f.EvidenceStatus == "paired_human_decision" && (p.InputRevision == 0 || p.InputRevision == f.InputRevision) && (p.InputHash == "" || p.InputHash == f.InputHash) && (p.ArtifactHash == "" || p.ArtifactHash == f.ArtifactHash)
	}
	if f.Source.Request == "" || f.Source.RoundID == "" || f.Store == "" || f.Sequence == 0 {
		return false
	}
	if p.InputRevision != 0 && p.InputRevision != f.InputRevision {
		return false
	}
	if p.InputHash != "" && p.InputHash != f.InputHash {
		return false
	}
	if p.Kind == "terminal" && f.EvidenceStatus == "verified_preexecution_terminal" && (f.Terminal == "cancelled" || f.Terminal == "failed" || f.Terminal == "rejected") {
		return true
	}
	if p.Kind != "human_decision" && (f.NativeStart != "observed" || f.ConsumedRevision == 0 || f.ConsumedHash == "" || f.ConsumedRevision != f.InputRevision || f.ConsumedHash != f.InputHash) {
		return false
	}
	sealed := f.Final == "sealed" && f.SealID != "" && len(f.FullHash) == 64
	switch p.Kind {
	case "success_sealed":
		return (f.Terminal == "succeeded" || f.Terminal == "completed") && sealed
	case "terminal":
		return f.Terminal == "failed" || f.Terminal == "cancelled" || ((f.Terminal == "succeeded" || f.Terminal == "completed") && sealed)
	case "qa_pass":
		return sealed && f.QA == "PASS" && f.QAOwnerRound == f.Source.RoundID && f.QARound != "" && (p.ArtifactHash == "" || p.ArtifactHash == f.ArtifactHash)
	case "artifact_available":
		return sealed && p.ArtifactHash != "" && f.ArtifactHash == p.ArtifactHash
	case "human_decision":
		return p.DecisionRef != "" && p.DecisionRef == f.DecisionRef
	}
	return false
}
func Steps(r Record) []Step {
	switch r.Kind {
	case "mission":
		if r.Mission != nil {
			return r.Mission.Steps
		}
	case "pipeline":
		if r.Pipeline != nil {
			return r.Pipeline.Nodes
		}
	case "watch":
		if r.Watch != nil {
			return []Step{{ID: "source", Title: "Exact source", Required: true, Source: &r.Watch.Source, Predicate: r.Watch.Predicate}}
		}
	}
	return nil
}
func Auth(r Record) Authorization {
	if r.Mission != nil {
		return r.Mission.Authorization
	}
	if r.Pipeline != nil {
		return r.Pipeline.Authorization
	}
	if r.Watch != nil {
		return r.Watch.Authorization
	}
	return Authorization{}
}
func Validate(r Record) error {
	if e := validateRecord(r); e != nil {
		return e
	}
	steps := Steps(r)
	if len(steps) > 32 {
		return errors.New("pipeline_node_limit")
	}
	seen := map[string]bool{}
	for _, st := range steps {
		if st.ID == "" || strings.HasPrefix(st.ID, missionRecoveryPrefix) || seen[st.ID] {
			return errors.New("duplicate_step")
		}
		seen[st.ID] = true
		if st.Source == nil && st.Action == nil && !(st.Predicate.Kind == "human_decision" && st.WorkItem != nil) && r.Kind != "mission" {
			return errors.New("step_source_or_action_required")
		}
		if st.Source != nil && (st.Source.Request == "" || st.Source.RoundID == "" || st.Source.Authority == "") {
			return errors.New("exact_source_round_required")
		}
		if st.Action != nil {
			a := st.Action
			if a.Instruction == "" || a.InstructionHash != digestText(a.Instruction) || a.Target.Thread == "" {
				return errors.New("frozen_action_hash_required")
			}
			if a.Kind == "delegation_continue" && (a.OriginalTask == nil || a.OriginalTask.RoundID == "") {
				return errors.New("original_parent_round_required")
			}
		}
	}
	if r.Pipeline != nil {
		d := workitems.WorkflowDefinition{}
		for _, s := range steps {
			d.Nodes = append(d.Nodes, workitems.WorkflowNode{ID: s.ID, Name: s.Title, Kind: "task_pipeline"})
		}
		if len(r.Pipeline.Edges) > 64 {
			return errors.New("pipeline_edge_limit")
		}
		edges := map[string]bool{}
		for _, e := range r.Pipeline.Edges {
			if e.ID == "" || edges[e.ID] || len(e.From) == 0 || !seen[e.To] {
				return errors.New("invalid_edge")
			}
			edges[e.ID] = true
			for _, from := range e.From {
				if !seen[from] {
					return errors.New("unknown_edge_source")
				}
				d.Edges = append(d.Edges, workitems.WorkflowEdge{From: from, To: e.To})
			}
		}
		if e := workitems.ValidateWorkflow(d); e != nil {
			return e
		}
	}
	auth := Auth(r)
	if auth.Budget.DeadlineMS <= now() {
		return errors.New("rule_deadline_required")
	}
	hasAction := r.Watch != nil && r.Watch.Mode == "resume_authorized"
	if r.Mission != nil && r.Mission.RecoveryAction != nil {
		hasAction = true
		if r.Mission.RecoveryAction.Kind != "session_resume" || r.Mission.RecoveryAction.Target != r.Manager || r.Mission.RecoveryAction.InstructionHash != digestText(r.Mission.RecoveryAction.Instruction) {
			return errors.New("mission_recovery_frozen_self_action_required")
		}
	}
	for _, s := range steps {
		hasAction = hasAction || s.Action != nil
	}
	if hasAction && (!auth.Confirmed || auth.SourceRef == "" || auth.Budget.DeadlineMS <= now() || auth.Budget.MaxEffects < 1 || auth.Budget.MaxModelTurns < 1 || auth.Budget.MaxNoProgress < 1) {
		return errors.New("explicit_automation_authorization_required")
	}
	if r.Watch != nil && r.Watch.Mode == "resume_authorized" && r.Watch.Action == nil {
		return errors.New("watch_action_required")
	}
	return nil
}
func digestText(s string) string { h := shaText([]byte(s)); return h }
func sourceKey(f Fact) string    { f.ObservedAt = 0; f.Sequence = 0; return digest(f) }

// Baseline facts are committed with the definition, never inferred from a later
// polling time. Catch-up is explicit; no historical terminal auto-runs an edge.
func seedTx(ctx context.Context, tx *sql.Tx, r Record, facts []Fact) error {
	if r.Kind != "watch" {
		return nil
	}
	for _, f := range facts {
		ev := Event{ID: "pe_" + digest(struct{ Record, Source string }{r.ID, sourceKey(f)}), RecordID: r.ID, NodeID: "", State: "baseline", Fact: f, AppliedRevision: r.Revision}
		if r.Watch != nil && r.Watch.CatchUp {
			continue
		}
		if _, e := tx.ExecContext(ctx, `INSERT OR IGNORE INTO task_pipeline_events(event_id,record_id,node_id,source_key,event) VALUES(?,?,?,?,?)`, ev.ID, r.ID, "", sourceKey(f), encode(ev)); e != nil {
			return e
		}
	}
	return nil
}

// Observe commits inbox, predicate application, chosen input set and outbox claim
// in one local transaction. Native start/final remain original-store facts.
func (s *Store) Observe(ctx context.Context, id string, expected uint64, facts []Fact, check func(context.Context) error) error {
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
	if e = jsonUnmarshal(b, &r); e != nil {
		return e
	}
	if e = jsonUnmarshal(ir, &issuer); e != nil {
		return e
	}
	if r.Revision != expected {
		return ErrConflict
	}
	if r.State != "active" {
		return nil
	}
	if check == nil {
		return ErrScope
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return e
	}
	if Auth(r).Budget.DeadlineMS <= now() {
		r.State = "expired"
		r.BlockedReason = "authorization_expired"
		r.Revision++
		r.UpdatedAt = now()
		if _, e = tx.ExecContext(ctx, `UPDATE task_pipeline_outbox SET state='retired',continuation=json_set(continuation,'$.state','retired','$.reason','authorization_expired') WHERE record_id=? AND state='ready'`, r.ID); e != nil {
			return e
		}
		if e = save(ctx, tx, issuer.Scope, r, issuer); e != nil {
			return e
		}
		return tx.Commit()
	}
	changed := false
	states := map[string]StepState{}
	for _, v := range r.Steps {
		states[v.ID] = v
	}
	for _, st := range Steps(r) {
		for _, f := range facts {
			if st.Predicate.Kind == "human_decision" && st.WorkItem != nil {
				if st.WorkItem.ItemID != f.Source.NativeID || st.WorkItem.Revision != f.Sequence {
					continue
				}
			} else if st.Source != nil {
				if *st.Source != f.Source {
					continue
				}
			} else if !(st.Predicate.Kind == "human_decision" && st.WorkItem != nil && st.WorkItem.ItemID == f.Source.NativeID && st.WorkItem.Revision == f.Sequence) {
				continue
			}
			key := sourceKey(f)
			eventID := "pe_" + digest(struct{ Record, Node, Source string }{r.ID, st.ID, key})
			var count int
			if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_events WHERE record_id=? AND source_key=? AND (node_id='' OR node_id=?)`, r.ID, key, st.ID).Scan(&count); e != nil {
				return e
			}
			if count != 0 {
				continue
			}
			state := "received"
			if f.Store != "" && f.Sequence > 0 {
				state = "evidence_verified"
			}
			v := states[st.ID]
			if v.State == "done" || (st.Action != nil && v.State == "failed") {
				continue
			}
			if v.ID == "" {
				v.ID = st.ID
				v.State = "waiting_dependency"
			}
			if PredicateMet(st.Predicate, f) {
				v.State = "satisfied"
				v.EvidenceHash = key
				state = "mission_applied"
			} else if (f.Terminal == "failed" || f.Terminal == "cancelled" || f.Terminal == "rejected") && PredicateMet(Predicate{Kind: "terminal"}, f) {
				v.State = "failed"
				v.Reason = f.Terminal
				v.EvidenceHash = key
			} else if f.Final == "conflicted" {
				v.State = "blocked"
				v.Reason = "conflicted"
			}
			states[st.ID] = v
			ev := Event{ID: eventID, RecordID: id, NodeID: st.ID, State: state, Fact: f, AppliedRevision: r.Revision + 1}
			if _, e = tx.ExecContext(ctx, `INSERT INTO task_pipeline_events(event_id,record_id,node_id,source_key,event) VALUES(?,?,?,?,?)`, ev.ID, id, st.ID, key, encode(ev)); e != nil {
				return e
			}
			changed = true
		}
	}
	// Iterate topologically until no additional zero-effect eligibility exists.
	for _, st := range Steps(r) {
		v := states[st.ID]
		if v.ID == "" {
			v = StepState{ID: st.ID, State: "waiting_dependency"}
		}
		if (v.State == "satisfied" && r.Watch == nil && st.Action == nil) || v.State == "queued" || v.State == "done" {
			states[st.ID] = v
			continue
		}
		eligible := st.Source == nil || v.State == "satisfied"
		if st.Predicate.Kind == "human_decision" {
			eligible = v.State == "satisfied"
		}
		chosen := []Fact{}
		if r.Mission != nil {
			for _, previous := range r.Mission.Steps {
				if previous.ID == st.ID {
					break
				}
				if previous.Required {
					state := states[previous.ID].State
					if (previous.Action != nil && state != "done") || (previous.Action == nil && state != "satisfied" && state != "done") {
						eligible = false
					}
				}
			}
		}
		if r.Pipeline != nil {
			for _, edge := range r.Pipeline.Edges {
				if edge.To != st.ID {
					continue
				}
				met := 0
				for _, from := range edge.From {
					x := states[from]
					predecessor := Step{}
					for _, candidate := range Steps(r) {
						if candidate.ID == from {
							predecessor = candidate
							break
						}
					}
					good := false
					var proof Fact
					if x.EvidenceHash != "" {
						var encoded []byte
						err := tx.QueryRowContext(ctx, `SELECT event FROM task_pipeline_events WHERE record_id=? AND node_id=? AND source_key=? ORDER BY seq DESC LIMIT 1`, r.ID, from, x.EvidenceHash).Scan(&encoded)
						if err != nil && err != sql.ErrNoRows {
							return err
						}
						if err == nil {
							var original Event
							if err = json.Unmarshal(encoded, &original); err != nil {
								return err
							}
							if predecessor.Action != nil {
								matched, err := actionEdgeCheckpoint(ctx, tx, r, predecessor, x, original, edge.Condition)
								if err != nil {
									return err
								}
								if !matched {
									continue
								}
							}
							proof = original.Fact
							kind := "success_sealed"
							switch edge.Condition {
							case "on_terminal":
								kind = "terminal"
							case "on_QA_PASS":
								kind = "qa_pass"
							case "on_human_decision":
								kind = "human_decision"
							}
							predicate := Predicate{Kind: kind, DecisionRef: proof.DecisionRef}
							good = PredicateMet(predicate, proof)
						}
					}
					if good {
						met++
						if edge.Join != "any_success" || met == 1 {
							chosen = append(chosen, proof)
						}
					}
				}
				if edge.Join == "any_success" {
					eligible = eligible && met > 0
				} else {
					eligible = eligible && met == len(edge.From)
				}
			}
		}
		action := st.Action
		if r.Watch != nil && r.Watch.Mode == "resume_authorized" {
			action = r.Watch.Action
			eligible = states["source"].State == "satisfied"
		}
		if action != nil && eligible {
			var oldState, oldID, oldTerminal string
			err := tx.QueryRowContext(ctx, `SELECT state,id,COALESCE(json_extract(continuation,'$.terminal'),'') FROM task_pipeline_outbox WHERE record_id=? AND definition_revision=? AND node_id=?`, r.ID, r.DefinitionRevision, st.ID).Scan(&oldState, &oldID, &oldTerminal)
			if err == nil {
				v.State = oldState
				if oldState == "checkpointed" {
					v.State = "done"
					if oldTerminal == "failed" || oldTerminal == "cancelled" || oldTerminal == "rejected" {
						v.State = "failed"
						v.Reason = oldTerminal
					}
				}
				v.ContinuationID = oldID
				states[st.ID] = v
				continue
			}
			if err != sql.ErrNoRows {
				return err
			}
			auth := Auth(r)
			if !auth.Confirmed || auth.Budget.DeadlineMS <= now() {
				r.State = "blocked"
				r.BlockedReason = "authorization_expired"
				changed = true
				continue
			}
			var used int
			if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND state NOT IN ('retired','rejected')`, id).Scan(&used); e != nil {
				return e
			}
			if used >= auth.Budget.MaxEffects || used >= auth.Budget.MaxModelTurns {
				r.State = "blocked"
				r.BlockedReason = "budget_exhausted"
				changed = true
				continue
			}
			for _, f := range facts {
				own := st.Source != nil && f.Source == *st.Source
				if st.WorkItem != nil && st.Predicate.Kind == "human_decision" {
					own = f.DecisionRef == st.Predicate.DecisionRef && PredicateMet(st.Predicate, f)
				}
				if own && PredicateMet(st.Predicate, f) {
					chosen = append(chosen, f)
				}
			}
			cid := "pc_" + digest(struct {
				ID      string
				Version uint64
				Node    string
			}{id, r.DefinitionRevision, st.ID})[:32]
			c := Continuation{NativeOperations: []NativeOperationRef{}, ID: cid, RecordID: id, DefinitionRevision: r.DefinitionRevision, NodeID: st.ID, State: "ready", Action: *action, RequestID: "pauto_" + cid, Evidence: chosen, Fence: 1}
			res, x := tx.ExecContext(ctx, `INSERT OR IGNORE INTO task_pipeline_outbox VALUES(?,?,?,?,?,?)`, cid, id, r.DefinitionRevision, st.ID, "ready", encode(c))
			if x != nil {
				return x
			}
			n, _ := res.RowsAffected()
			if n > 0 {
				changed = true
			}
			v.State = "ready"
			v.ContinuationID = cid
		}
		states[st.ID] = v
	}
	if !changed {
		return nil
	}
	r.Steps = []StepState{}
	done := true
	for _, st := range Steps(r) {
		v := states[st.ID]
		r.Steps = append(r.Steps, v)
		if st.Required && v.State != "satisfied" && v.State != "done" {
			done = false
		}
	}
	var pending int
	if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND state NOT IN ('checkpointed','retired','rejected','no_progress')`, id).Scan(&pending); e != nil {
		return e
	}
	if done && pending == 0 && (r.Mission == nil || r.Mission.RecoveryAction == nil) {
		r.State = "done"
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
func (s *Store) Pending(ctx context.Context, limit int) ([]Continuation, error) {
	cursor, e := s.scanCursor(ctx, "outbox")
	if e != nil {
		return nil, e
	}
	var count int
	if e = s.db.QueryRowContext(ctx, `SELECT count(*) FROM task_pipeline_outbox WHERE (state IN ('ready','issued','unknown','queued','native_started','checkpoint_applied','waiting_evidence') OR (state='checkpointed' AND EXISTS(SELECT 1 FROM task_pipeline_records r WHERE r.id=task_pipeline_outbox.record_id AND r.state='active' AND json_extract(r.record,'$.kind')='pipeline'))) AND id>?`, cursor).Scan(&count); e != nil {
		return nil, e
	}
	if count == 0 {
		cursor = ""
	}
	rows, e := s.db.QueryContext(ctx, `SELECT state,continuation FROM task_pipeline_outbox WHERE (state IN ('ready','issued','unknown','queued','native_started','checkpoint_applied','waiting_evidence') OR (state='checkpointed' AND EXISTS(SELECT 1 FROM task_pipeline_records r WHERE r.id=task_pipeline_outbox.record_id AND r.state='active' AND json_extract(r.record,'$.kind')='pipeline'))) AND id>? ORDER BY id LIMIT ?`, cursor, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Continuation{}
	for rows.Next() {
		var state string
		var b []byte
		if e = rows.Scan(&state, &b); e != nil {
			return nil, e
		}
		var c Continuation
		if e = jsonUnmarshal(b, &c); e != nil {
			return nil, e
		}
		c.State = state
		out = append(out, c)
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}
	rows.Close()
	if len(out) > 0 {
		if e = s.saveScanCursor(ctx, "outbox", out[len(out)-1].ID); e != nil {
			return nil, e
		}
	}
	return out, nil
}
func (s *Store) Issuer(ctx context.Context, id string) (Issuer, Record, error) {
	var b, ir []byte
	e := s.db.QueryRowContext(ctx, `SELECT record,issuer FROM task_pipeline_records WHERE id=?`, id).Scan(&b, &ir)
	var i Issuer
	var r Record
	if e != nil {
		return i, r, e
	}
	if e = jsonUnmarshal(b, &r); e != nil {
		return i, r, e
	}
	e = jsonUnmarshal(ir, &i)
	return i, r, e
}
func (s *Store) MarkIssued(ctx context.Context, c Continuation, check func(context.Context) error) (Continuation, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return c, e
	}
	defer tx.Rollback()
	if check == nil {
		return c, ErrScope
	}
	if e = check(s.context(ctx, tx)); e != nil {
		return c, e
	}
	var state string
	var version uint64
	e = tx.QueryRowContext(ctx, `SELECT o.state,r.definition_revision FROM task_pipeline_outbox o JOIN task_pipeline_records r ON r.id=o.record_id WHERE o.id=? AND r.state='active'`, c.ID).Scan(&state, &version)
	if e == sql.ErrNoRows {
		return c, ErrConflict
	}
	if e != nil {
		return c, e
	}
	if state != "ready" || version != c.DefinitionRevision {
		return c, ErrConflict
	}
	c.State = "issued"
	c.Attempt++
	c.Fence++
	if _, e = tx.ExecContext(ctx, `UPDATE task_pipeline_outbox SET state=?,continuation=? WHERE id=? AND state='ready'`, c.State, encode(c), c.ID); e != nil {
		return c, e
	}
	return c, tx.Commit()
}

// Admission is called by the ORIGINAL message writer in its own transaction.
// A cancelled/revised definition cannot win between claim and native enqueue.
type Admission struct {
	ID    string
	Fence uint64
	Check func(context.Context) error
}

func AdmitTx(ctx context.Context, tx *sql.Tx, a *Admission, session, request string, payloadHash string) error {
	if a == nil {
		return nil
	}
	if a.Check == nil {
		return ErrScope
	}
	if e := a.Check(ctx); e != nil {
		return e
	}
	var raw []byte
	var state, rs string
	var version uint64
	e := tx.QueryRowContext(ctx, `SELECT o.continuation,o.state,r.state,r.definition_revision FROM task_pipeline_outbox o JOIN task_pipeline_records r ON r.id=o.record_id WHERE o.id=?`, a.ID).Scan(&raw, &state, &rs, &version)
	if e != nil {
		return e
	}
	var c Continuation
	if e = jsonUnmarshal(raw, &c); e != nil {
		return e
	}
	if state != "issued" || rs != "active" || version != c.DefinitionRevision || c.Fence != a.Fence || c.RequestID != request || c.Action.Target.Session != session {
		return ErrScope
	}
	c.State = "queued"
	c.OriginalReceipt = &EffectReceipt{Kind: "session_input", Authority: c.Action.Target.Authority, Session: session, Request: request, Key: c.ID, InputHash: payloadHash}
	_, e = tx.ExecContext(ctx, `UPDATE task_pipeline_outbox SET state='queued',continuation=? WHERE id=? AND state='issued'`, encode(c), c.ID)
	return e
}
func (s *Store) SetOutcome(ctx context.Context, c Continuation, state, reason string, receipt *EffectReceipt) error {
	if !strings.Contains("|unknown|queued|native_started|checkpoint_applied|waiting_evidence|checkpointed|blocked|rejected|", "|"+state+"|") {
		return errors.New("invalid_continuation_state")
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var raw []byte
	var phase string
	if e = tx.QueryRowContext(ctx, `SELECT state,continuation FROM task_pipeline_outbox WHERE id=?`, c.ID).Scan(&phase, &raw); e != nil {
		return e
	}
	if phase == "checkpointed" || phase == "retired" || phase == "rejected" || phase == "no_progress" {
		return nil
	}
	var current Continuation
	if e = json.Unmarshal(raw, &current); e != nil {
		return e
	}
	if current.Fence != c.Fence {
		return ErrConflict
	}
	if receipt != nil {
		if current.OriginalReceipt != nil && *current.OriginalReceipt != *receipt {
			return errors.New("pipeline_original_receipt_conflict")
		}
		current.OriginalReceipt = receipt
	}
	if c.CheckpointRevision > current.CheckpointRevision {
		current.CheckpointRevision = c.CheckpointRevision
	}
	if c.NativeStart == "observed" {
		current.NativeStart = "observed"
	}
	if c.Terminal != "" {
		current.Terminal = c.Terminal
	}
	if phase == "checkpoint_applied" && (state == "queued" || state == "native_started") {
		state = phase
	}
	if phase == "native_started" && state == "queued" {
		state = phase
	}
	current.State = state
	current.Reason = reason
	if _, e = tx.ExecContext(ctx, `UPDATE task_pipeline_outbox SET state=?,continuation=? WHERE id=?`, state, encode(current), c.ID); e != nil {
		return e
	}
	return tx.Commit()
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
func shaText(b []byte) string             { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// A node's Source is the prerequisite for its action, not the result of that
// action. Downstream edges may consume only this definition's original action
// checkpoint, never a sealed readiness/source event from an earlier round.
func actionEdgeCheckpoint(ctx context.Context, tx *sql.Tx, r Record, st Step, x StepState, event Event, condition string) (bool, error) {
	if x.ContinuationID == "" || event.State != "checkpointed" || event.RecordID != r.ID || event.NodeID != st.ID {
		return false, nil
	}
	if x.State != "done" && (condition != "on_terminal" || x.State != "failed") {
		return false, nil
	}
	var raw []byte
	var state string
	err := tx.QueryRowContext(ctx, `SELECT continuation,state FROM task_pipeline_outbox WHERE id=? AND record_id=? AND definition_revision=? AND node_id=?`, x.ContinuationID, r.ID, r.DefinitionRevision, st.ID).Scan(&raw, &state)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var c Continuation
	if err = json.Unmarshal(raw, &c); err != nil {
		return false, err
	}
	if state != "checkpointed" || c.State != "checkpointed" || c.ID != x.ContinuationID || c.RecordID != r.ID || c.DefinitionRevision != r.DefinitionRevision || c.NodeID != st.ID || st.Action == nil || digest(c.Action) != digest(*st.Action) || c.OriginalReceipt == nil {
		return false, nil
	}
	f := event.Fact
	receipt := c.OriginalReceipt
	if receipt.Key != c.ID || (receipt.Kind != "session_input" && receipt.Kind != "delegation_round") || receipt.Session != f.Source.Session || receipt.Request != f.Source.Request || c.Action.Target.Session != f.Source.Session || receipt.Authority != f.Source.Authority || c.Action.Target.Authority != f.Source.Authority || c.Action.Target.Thread != f.Source.Thread || c.Action.Target.Revision != f.Source.Revision || c.Terminal != f.Terminal {
		return false, nil
	}
	if receipt.Kind == "delegation_round" && receipt.NativeID != f.Source.RoundID {
		return false, nil
	}
	if event.ID != "pe_"+digest(struct{ Continuation, Proof string }{c.ID, sourceKey(f)}) || x.EvidenceHash != sourceKey(f) {
		return false, nil
	}
	if x.State == "failed" {
		return PredicateMet(Predicate{Kind: "terminal"}, f) && (f.Terminal == "failed" || f.Terminal == "cancelled" || f.Terminal == "rejected"), nil
	}
	return PredicateMet(Predicate{Kind: "success_sealed"}, f), nil
}
