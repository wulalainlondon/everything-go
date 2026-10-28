package taskpipeline

import (
	"context"
	"encoding/json"
	"testing"
)

func admitted(t *testing.T, s *Store, c Continuation) Continuation {
	t.Helper()
	var e error
	c, e = s.MarkIssued(context.Background(), c, allow)
	if e != nil {
		t.Fatal(e)
	}
	tx, e := s.db.BeginTx(context.Background(), nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = AdmitTx(context.Background(), tx, &Admission{ID: c.ID, Fence: c.Fence, Check: allow}, c.Action.Target.Session, c.RequestID, "native-input"); e != nil {
		tx.Rollback()
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	c, _, e = s.ExactContinuation(context.Background(), c.ID)
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func completion(c Continuation) Fact {
	f := fact()
	f.Source = Locator{Kind: "request", Authority: c.Action.Target.Authority, Session: c.Action.Target.Session, Thread: c.Action.Target.Thread, Revision: c.Action.Target.Revision, Request: c.RequestID, RoundID: c.RequestID}
	f.InputHash = "native-input"
	f.ConsumedHash = "native-input"
	return f
}
func recoveryMission() Record {
	return Record{Manager: action().Target, Kind: "mission", Mission: &Mission{Goal: "Do not treat early final as completion", Completion: "server required proof", Steps: []Step{{ID: "required", Title: "missing real source", Required: true, Predicate: Predicate{Kind: "success_sealed"}}}, RecoveryAction: action(), Authorization: auth()}}
}
func TestRecoveryEarlyFinalFiniteNoProgressAndNoOpaqueCheckpointReset(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	r := create(t, s, scope, i, recoveryMission(), nil)
	for generation := uint64(1); generation <= 2; generation++ {
		r, _, _, _ = s.Get(ctx, scope, r.ID)
		if e := s.EnsureRecovery(ctx, r.ID, r.Revision, nil, allow); e != nil {
			t.Fatal(e)
		}
		pending, e := s.Pending(ctx, 10)
		if e != nil || len(pending) != 1 {
			t.Fatal(pending, e)
		}
		c := admitted(t, s, pending[0])
		if c.Generation != generation {
			t.Fatal(c)
		}
		// An ACK's arbitrary progress hash is metadata, never a server progress fact.
		r, _, _, _ = s.Get(ctx, scope, r.ID)
		proof := &NativeProvenance{Source: c.Action.Target, RequestID: c.RequestID, NativeTurnID: "native-turn", ToolCallID: "tool", ProcessGeneration: "process", Tool: "task_pipeline"}
		cp := Checkpoint{Revision: r.Revision, StepID: "required", Processed: []string{}, ProgressHash: "model-opaque-new-value", Outcome: "progress", Blockers: []string{}, RequestID: c.RequestID}
		if _, e = s.WithProof(proof).Ack(ctx, scope, "ack", "native:"+c.ID+":ack", "hash", c.ID, r.Revision, cp, c.Action.Target.Session, c.RequestID, allow); e != nil {
			t.Fatal(e)
		}
		if e = s.FinishRecovery(ctx, c, completion(c), allow); e != nil {
			t.Fatal(e)
		}
		r, _, _, _ = s.Get(ctx, scope, r.ID)
		if r.NoProgressCount != generation {
			t.Fatal("opaque checkpoint reset true no-progress", r)
		}
		if e = s.FinishRecovery(ctx, c, completion(c), allow); e != nil {
			t.Fatal(e)
		}
		r, _, _, _ = s.Get(ctx, scope, r.ID)
		if r.NoProgressCount != generation {
			t.Fatal("duplicate completion changed count", r)
		}
	}
	if r.State != "blocked" || r.BlockedReason != "blocked_no_progress" {
		t.Fatal(r)
	}
	if e := s.EnsureRecovery(ctx, r.ID, r.Revision, nil, allow); e != nil {
		t.Fatal(e)
	}
	pending, _ := s.Pending(ctx, 10)
	if len(pending) != 0 {
		t.Fatal("unbounded recovery", pending)
	}
}
func TestRecoveryUnknownAndExternalQAWaitNeverCreateSuccessor(t *testing.T) {
	for _, waiting := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown", true: "qa"}[waiting], func(t *testing.T) {
			s, scope, i := fixture(t)
			ctx := context.Background()
			m := recoveryMission()
			if waiting {
				m.Mission.Steps[0].Source = ptr(identity())
				m.Mission.Steps[0].Predicate.Kind = "qa_pass"
			}
			r := create(t, s, scope, i, m, nil)
			if e := s.EnsureRecovery(ctx, r.ID, r.Revision, nil, allow); e != nil {
				t.Fatal(e)
			}
			pending, _ := s.Pending(ctx, 10)
			if !waiting {
				c, e := s.MarkIssued(ctx, pending[0], allow)
				if e != nil {
					t.Fatal(e)
				}
				if e = s.SetOutcome(ctx, c, "unknown", "lost ACK", nil); e != nil {
					t.Fatal(e)
				}
			} else {
				c := admitted(t, s, pending[0])
				if e := s.FinishRecovery(ctx, c, completion(c), allow); e != nil {
					t.Fatal(e)
				}
			}
			r, _, _, _ = s.Get(ctx, scope, r.ID)
			if e := s.EnsureRecovery(ctx, r.ID, r.Revision, nil, allow); e != nil {
				t.Fatal(e)
			}
			var count int
			s.db.QueryRow(`SELECT count(*) FROM task_pipeline_outbox`).Scan(&count)
			if count != 1 {
				t.Fatal("unknown or waiting QA reissued", count)
			}
		})
	}
}
func TestOriginalCompletionCannotOverwritePausedCancelledOrChangedDefinition(t *testing.T) {
	for _, op := range []string{"mission_pause", "mission_cancel", "mission_update"} {
		t.Run(op, func(t *testing.T) {
			s, scope, i := fixture(t)
			ctx := context.Background()
			m := recoveryMission()
			m.Mission.RecoveryAction = nil
			m.Mission.Steps[0].Action = action()
			r := create(t, s, scope, i, m, nil)
			if e := s.Observe(ctx, r.ID, r.Revision, nil, allow); e != nil {
				t.Fatal(e)
			}
			pending, _ := s.Pending(ctx, 10)
			c := admitted(t, s, pending[0])
			r, _, _, _ = s.Get(ctx, scope, r.ID)
			var definition *Record
			if op == "mission_update" {
				raw, _ := json.Marshal(m)
				var changed Record
				json.Unmarshal(raw, &changed)
				changed.Mission.Steps[0].ID = "new-required"
				definition = &changed
			}
			receipt, e := s.Mutate(ctx, scope, i, op, op, "hash", r.ID, r.Revision, definition, nil, nil, allow)
			if e != nil {
				t.Fatal(e)
			}
			if e = s.CompleteFromOriginal(ctx, c, completion(c), allow); e != nil {
				t.Fatal(e)
			}
			current, _, _, _ := s.Get(ctx, scope, r.ID)
			if current.State != receipt.Record.State {
				t.Fatal("completion overwrote state", current)
			}
			if op == "mission_update" && len(current.Steps) != 0 {
				t.Fatal("old node adopted by new definition", current)
			}
		})
	}
}
func TestGuardRejectsUnknownOutboxEvenPausedRecord(t *testing.T) {
	s, scope, i := fixture(t)
	r := create(t, s, scope, i, recoveryMission(), nil)
	ctx := context.Background()
	if e := s.EnsureRecovery(ctx, r.ID, r.Revision, nil, allow); e != nil {
		t.Fatal(e)
	}
	if _, e := s.db.Exec(`UPDATE task_pipeline_records SET state='paused'`); e != nil {
		t.Fatal(e)
	}
	if _, e := s.db.Exec(`UPDATE task_pipeline_outbox SET state='future_unrecognized'`); e != nil {
		t.Fatal(e)
	}
	g, e := s.Guard(ctx, "owner")
	if e != nil || g.Ready || g.Unknown != 1 {
		t.Fatal(g, e)
	}
	tx, _ := s.db.BeginTx(ctx, nil)
	e = GuardTx(ctx, tx, "owner")
	tx.Rollback()
	if e == nil {
		t.Fatal("context reset ignored unknown phase")
	}
}

func TestMissionPreviousActionMustFinishAndIncomingDoesNotEraseOwnPredicate(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	a := action()
	r := create(t, s, scope, i, Record{Kind: "mission", Mission: &Mission{Goal: "ordered", Completion: "original receipts", Authorization: auth(), Steps: []Step{{ID: "first", Title: "first", Required: true, Action: a}, {ID: "second", Title: "second", Required: true, Action: a}}}}, nil)
	if e := s.Observe(ctx, r.ID, r.Revision, nil, allow); e != nil {
		t.Fatal(e)
	}
	pending, _ := s.Pending(ctx, 10)
	if len(pending) != 1 || pending[0].NodeID != "first" {
		t.Fatal("later action escaped original predecessor", pending)
	}
	c := admitted(t, s, pending[0])
	if e := s.CompleteFromOriginal(ctx, c, completion(c), allow); e != nil {
		t.Fatal(e)
	}
	r, _, _, _ = s.Get(ctx, scope, r.ID)
	if e := s.Observe(ctx, r.ID, r.Revision, nil, allow); e != nil {
		t.Fatal(e)
	}
	pending, _ = s.Pending(ctx, 10)
	if len(pending) != 1 || pending[0].NodeID != "second" {
		t.Fatal(pending)
	}
	s, scope, i = fixture(t)
	source := identity()
	different := source
	different.Request = "still-pending"
	different.RoundID = "pending-round"
	dag := Record{Kind: "pipeline", Pipeline: &Pipeline{Name: "own source not bypassed", Authorization: auth(), Nodes: []Step{{ID: "a", Title: "a", Required: true, Source: &source, Predicate: Predicate{Kind: "success_sealed"}}, {ID: "b", Title: "b", Required: true, Source: &different, Predicate: Predicate{Kind: "qa_pass"}, Action: action()}}, Edges: []Edge{{ID: "ab", From: []string{"a"}, To: "b", Condition: "on_success", Join: "all_success", Output: "references_only"}}}}
	r = create(t, s, scope, i, dag, nil)
	enabled, e := s.Mutate(ctx, scope, i, "enable", "pipeline_enable", "hash", r.ID, r.Revision, nil, nil, nil, allow)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Observe(ctx, r.ID, enabled.Record.Revision, []Fact{fact()}, allow); e != nil {
		t.Fatal(e)
	}
	pending, _ = s.Pending(ctx, 10)
	if len(pending) != 0 {
		t.Fatal("incoming source erased own QA predicate", pending)
	}
}
func TestActualActionFailureFeedsOnlyTerminalBranch(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	dag := Record{Kind: "pipeline", Pipeline: &Pipeline{Name: "terminal branch", Authorization: auth(), Nodes: []Step{{ID: "a", Title: "a", Required: true, Predicate: Predicate{Kind: "success_sealed"}, Action: action()}, {ID: "b", Title: "b", Required: true, Action: action()}}, Edges: []Edge{{ID: "ab", From: []string{"a"}, To: "b", Condition: "on_terminal", Join: "all_terminal", Output: "references_only"}}}}
	r := create(t, s, scope, i, dag, nil)
	enabled, e := s.Mutate(ctx, scope, i, "enable", "pipeline_enable", "hash", r.ID, r.Revision, nil, nil, nil, allow)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Observe(ctx, r.ID, enabled.Record.Revision, nil, allow); e != nil {
		t.Fatal(e)
	}
	pending, _ := s.Pending(ctx, 10)
	c := admitted(t, s, pending[0])
	f := completion(c)
	f.Terminal = "failed"
	f.Final = "pending"
	f.SealID = ""
	f.FullHash = ""
	if e = s.CompleteFromOriginal(ctx, c, f, allow); e != nil {
		t.Fatal(e)
	}
	r, _, _, _ = s.Get(ctx, scope, r.ID)
	if e = s.Observe(ctx, r.ID, r.Revision, nil, allow); e != nil {
		t.Fatal(e)
	}
	pending, _ = s.Pending(ctx, 10)
	if len(pending) != 1 || pending[0].NodeID != "b" {
		t.Fatal("truthful failed branch not available", pending)
	}
}

func TestConcurrentDuplicateObservationsClaimOneAndExpiredReadyIsRetired(t *testing.T) {
	s, scope, i := fixture(t)
	ctx := context.Background()
	r := create(t, s, scope, i, Record{Kind: "watch", Watch: &Watch{Source: identity(), Destination: scope.Manager, Predicate: Predicate{Kind: "success_sealed"}, Mode: "resume_authorized", CatchUp: true, Action: action(), Authorization: auth()}}, nil)
	results := make(chan error, 8)
	for j := 0; j < 8; j++ {
		go func() { results <- s.Observe(ctx, r.ID, r.Revision, []Fact{fact()}, allow) }()
	}
	success := 0
	for j := 0; j < 8; j++ {
		if e := <-results; e == nil {
			success++
		} else if e != ErrConflict {
			t.Fatal(e)
		}
	}
	if success != 1 {
		t.Fatal(success)
	}
	pending, _ := s.Pending(ctx, 10)
	if len(pending) != 1 {
		t.Fatal(pending)
	}
	current, _, _, _ := s.Get(ctx, scope, r.ID)
	current.Watch.Authorization.Budget.DeadlineMS = 1
	raw, _ := json.Marshal(current)
	if _, e := s.db.Exec(`UPDATE task_pipeline_records SET record=? WHERE id=?`, raw, r.ID); e != nil {
		t.Fatal(e)
	}
	if e := s.Observe(ctx, r.ID, current.Revision, nil, allow); e != nil {
		t.Fatal(e)
	}
	current, _, _, _ = s.Get(ctx, scope, r.ID)
	if current.State != "expired" {
		t.Fatal(current)
	}
	c, found, e := s.Continuation(ctx, scope, pending[0].ID)
	if e != nil || !found || c.State != "retired" {
		t.Fatal(c, found, e)
	}
	if _, e = s.MarkIssued(ctx, pending[0], allow); e != ErrScope && e != ErrConflict {
		t.Fatal(e)
	}
}
func TestHumanApprovalCannotIgnoreRequestedInputAndPlanHash(t *testing.T) {
	p := Predicate{Kind: "human_decision", DecisionRef: "real-user", InputRevision: 2, InputHash: "newinput", ArtifactHash: digest("newplan")}
	f := Fact{Store: "jarvis_worklist_human_decision", Sequence: 4, EvidenceStatus: "paired_human_decision", DecisionRef: "real-user", InputRevision: 1, InputHash: "oldinput", ArtifactHash: digest("oldplan")}
	if PredicateMet(p, f) {
		t.Fatal("approval bypassed exact plan/input")
	}
	f.InputRevision = 2
	f.InputHash = p.InputHash
	f.ArtifactHash = p.ArtifactHash
	if !PredicateMet(p, f) {
		t.Fatal("matching explicit human proof refused")
	}
}
