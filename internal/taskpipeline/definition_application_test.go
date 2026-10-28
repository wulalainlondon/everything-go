package taskpipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func definitionWithExecutionSource(t *testing.T, s *Store, scope Scope, i Issuer, r Record, original Continuation) Record {
	t.Helper()
	current, _, _, e := s.Get(context.Background(), scope, r.ID)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(current)
	var next Record
	if e = json.Unmarshal(raw, &next); e != nil {
		t.Fatal(e)
	}
	source := completion(original).Source
	next.Pipeline.Nodes[0].Source = &source
	next.Pipeline.Nodes[0].Action = nil
	next.Pipeline.Nodes[len(next.Pipeline.Nodes)-1].Title = "explicit new unissued frontier"
	receipt, e := s.Mutate(context.Background(), scope, i, "new-definition", "pipeline_define", "new-definition-hash", r.ID, current.Revision, &next, nil, nil, allow)
	if e != nil {
		t.Fatal(e)
	}
	return *receipt.Record
}
func TestDefinitionReappliesExactCanonicalSourceWithoutDuplicateIngressOrOldAction(t *testing.T) {
	s, scope, i := fixture(t)
	r, facts := edgePipeline(t, s, scope, i, "on_success", "all_success", 1)
	old := admitted(t, s, actionRows(t, s, r.ID)["A"])
	actual := completion(old)
	if e := s.CompleteFromOriginal(context.Background(), old, actual, allow); e != nil {
		t.Fatal(e)
	}
	original, _, _ := s.ExactContinuation(context.Background(), old.ID)
	var count int
	if e := s.db.QueryRow(`SELECT count(*) FROM task_pipeline_events WHERE record_id=?`, r.ID).Scan(&count); e != nil {
		t.Fatal(e)
	}
	r = definitionWithExecutionSource(t, s, scope, i, r, old)
	current := observeCurrent(t, s, scope, r.ID, []Fact{actual, facts[2]})
	next := actionRows(t, s, r.ID)["C"]
	if next.ID == "" || next.DefinitionRevision != r.DefinitionRevision {
		t.Fatal("new explicitly authorized frontier stalled", current.Steps, next)
	}
	var events, newA int
	s.db.QueryRow(`SELECT count(*) FROM task_pipeline_events WHERE record_id=?`, r.ID).Scan(&events)
	s.db.QueryRow(`SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND definition_revision=? AND node_id='A'`, r.ID, r.DefinitionRevision).Scan(&newA)
	if events != count || newA != 0 {
		t.Fatal("source reapplied by duplicate ingress or old action", events, count, newA)
	}
	oldAfter, _, _ := s.ExactContinuation(context.Background(), old.ID)
	if digest(oldAfter) != digest(original) {
		t.Fatal("original changed")
	}
	before := digest(next)
	observeCurrent(t, s, scope, r.ID, []Fact{facts[2], actual})
	if digest(actionRows(t, s, r.ID)["C"]) != before {
		t.Fatal("duplicate reapplication replaced frozen frontier")
	}
	reopened, _, found, e := New(s.db).Get(context.Background(), scope, r.ID)
	if e != nil || !found || len(reopened.Steps) != len(current.Steps) {
		t.Fatal("definition application was not durable", e)
	}
}
func TestDefinitionCannotRepeatPriorIssuedActionOrUseItAsCurrentActionProof(t *testing.T) {
	for _, state := range []string{"issued", "unknown", "queued", "completed", "failed", "rejected"} {
		t.Run(state, func(t *testing.T) {
			s, scope, i := fixture(t)
			r, facts := edgePipeline(t, s, scope, i, "on_success", "all_success", 1)
			old := actionRows(t, s, r.ID)["A"]
			if state == "completed" || state == "failed" || state == "queued" {
				old = admitted(t, s, old)
			} else {
				var e error
				old, e = s.MarkIssued(context.Background(), old, allow)
				if e != nil {
					t.Fatal(e)
				}
			}
			if state == "completed" || state == "failed" {
				f := completion(old)
				if state == "failed" {
					f.Terminal = "failed"
					f.Final = "pending"
					f.SealID = ""
					f.FullHash = ""
				}
				if e := s.CompleteFromOriginal(context.Background(), old, f, allow); e != nil {
					t.Fatal(e)
				}
			} else if state == "unknown" || state == "rejected" {
				if e := s.SetOutcome(context.Background(), old, state, "original frozen outcome", nil); e != nil {
					t.Fatal(e)
				}
			}
			original, _, _ := s.ExactContinuation(context.Background(), old.ID)
			current, _, _, _ := s.Get(context.Background(), scope, r.ID)
			raw, _ := json.Marshal(current)
			var updated Record
			json.Unmarshal(raw, &updated)
			updated.Pipeline.Name = "title/downstream edit only"
			receipt, e := s.Mutate(context.Background(), scope, i, "update", "pipeline_define", "hash", r.ID, current.Revision, &updated, nil, nil, allow)
			if e != nil {
				t.Fatal(e)
			}
			observeCurrent(t, s, scope, r.ID, facts)
			var a, c int
			s.db.QueryRow(`SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND definition_revision=? AND node_id='A'`, r.ID, receipt.Record.DefinitionRevision).Scan(&a)
			s.db.QueryRow(`SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND definition_revision=? AND node_id='C'`, r.ID, receipt.Record.DefinitionRevision).Scan(&c)
			if c != 0 || (state != "rejected" && a != 0) {
				t.Fatal("new definition repeated issued action or consumed readiness instead of action", state, a, c)
			}
			after, _, _ := s.ExactContinuation(context.Background(), old.ID)
			if digest(original) != digest(after) {
				t.Fatal("old frozen effect was rewritten")
			}
		})
	}
}
func TestDefinitionNewApplicationRejectsWrongSourceAndFinalGuardRollsBack(t *testing.T) {
	for _, kind := range []string{"round", "hash", "authority", "revoked_before_commit"} {
		t.Run(kind, func(t *testing.T) {
			s, scope, i := fixture(t)
			r, facts := edgePipeline(t, s, scope, i, "on_success", "all_success", 1)
			old := admitted(t, s, actionRows(t, s, r.ID)["A"])
			actual := completion(old)
			if e := s.CompleteFromOriginal(context.Background(), old, actual, allow); e != nil {
				t.Fatal(e)
			}
			r = definitionWithExecutionSource(t, s, scope, i, r, old)
			wrong := actual
			switch kind {
			case "round":
				wrong.Source.RoundID = "foreign-round"
			case "hash":
				wrong.InputHash = "other-input"
			case "authority":
				wrong.Source.Authority = "other-authority"
			}
			calls := 0
			check := allow
			if kind == "revoked_before_commit" {
				check = func(context.Context) error {
					calls++
					if calls == 2 {
						return ErrScope
					}
					return nil
				}
			}
			e := s.Observe(context.Background(), r.ID, r.Revision, []Fact{wrong, facts[2]}, check)
			if kind == "revoked_before_commit" {
				if !errors.Is(e, ErrScope) || calls != 2 {
					t.Fatal("final disclosure guard not checked", e, calls)
				}
			} else if e != nil {
				t.Fatal(e)
			}
			var n int
			s.db.QueryRow(`SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND definition_revision=? AND node_id='C'`, r.ID, r.DefinitionRevision).Scan(&n)
			if n != 0 {
				t.Fatal("invalid/changed authorization released new frontier", kind)
			}
			observeCurrent(t, s, scope, r.ID, []Fact{actual, facts[2]})
			if actionRows(t, s, r.ID)["C"].ID == "" {
				t.Fatal("invalid observation poisoned later canonical source")
			}
		})
	}
}
func TestDefinitionLateCompletionAloneCannotAdvancePausedCancelledOrChangedAction(t *testing.T) {
	for _, op := range []string{"pipeline_pause", "pipeline_cancel", "none"} {
		t.Run(op, func(t *testing.T) {
			s, scope, i := fixture(t)
			r, facts := edgePipeline(t, s, scope, i, "on_success", "all_success", 1)
			old := admitted(t, s, actionRows(t, s, r.ID)["A"])
			current, _, _, _ := s.Get(context.Background(), scope, r.ID)
			raw, _ := json.Marshal(current)
			var updated Record
			json.Unmarshal(raw, &updated)
			updated.Pipeline.Name = "new current version"
			updated.Pipeline.Nodes[0].Action.Instruction += " changed"
			updated.Pipeline.Nodes[0].Action.InstructionHash = digestText(updated.Pipeline.Nodes[0].Action.Instruction)
			receipt, e := s.Mutate(context.Background(), scope, i, "update", "pipeline_define", "hash", r.ID, current.Revision, &updated, nil, nil, allow)
			if e != nil {
				t.Fatal(e)
			}
			r = *receipt.Record
			if op != "none" {
				receipt, e = s.Mutate(context.Background(), scope, i, "stop", op, "stophash", r.ID, r.Revision, nil, nil, nil, allow)
				if e != nil {
					t.Fatal(e)
				}
				r = *receipt.Record
			}
			if e = s.CompleteFromOriginal(context.Background(), old, completion(old), allow); e != nil {
				t.Fatal(e)
			}
			observeCurrent(t, s, scope, r.ID, append(facts, completion(old)))
			after, _, _, _ := s.Get(context.Background(), scope, r.ID)
			if op != "none" && after.State != r.State {
				t.Fatal("late completion overwrote pause/cancel", after.State, r.State)
			}
			var n int
			s.db.QueryRow(`SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND definition_revision=?`, r.ID, r.DefinitionRevision).Scan(&n)
			if n != 0 {
				t.Fatal("late old completion implicitly adopted into new action", op, n)
			}
		})
	}
}

// An unchanged admitted A is not repeated. Only a fresh canonical observation
// of its exact original checkpoint may carry it into the new unissued frontier.
func TestDefinitionUnchangedAdmittedActionCarriesOnlyExactFreshCheckpoint(t *testing.T) {
	for _, completeBefore := range []bool{false, true} {
		for _, terminal := range []string{"completed", "failed"} {
			t.Run(fmt.Sprintf("before_%t_%s", completeBefore, terminal), func(t *testing.T) {
				s, scope, i := fixture(t)
				condition, join := "on_success", "all_success"
				if terminal == "failed" {
					condition, join = "on_terminal", "all_terminal"
				}
				r, facts := edgePipeline(t, s, scope, i, condition, join, 1)
				old := admitted(t, s, actionRows(t, s, r.ID)["A"])
				actual := completion(old)
				if terminal == "failed" {
					actual.Terminal = "failed"
					actual.Final = "pending"
					actual.SealID = ""
					actual.FullHash = ""
				}
				if completeBefore {
					if e := s.CompleteFromOriginal(context.Background(), old, actual, allow); e != nil {
						t.Fatal(e)
					}
				}
				current, _, _, _ := s.Get(context.Background(), scope, r.ID)
				raw, _ := json.Marshal(current)
				var next Record
				json.Unmarshal(raw, &next)
				next.Pipeline.Nodes[1].Action.Instruction += " new C"
				next.Pipeline.Nodes[1].Action.InstructionHash = digestText(next.Pipeline.Nodes[1].Action.Instruction)
				receipt, e := s.Mutate(context.Background(), scope, i, "frontier", "pipeline_define", "frontierhash", r.ID, current.Revision, &next, nil, nil, allow)
				if e != nil {
					t.Fatal(e)
				}
				r = *receipt.Record
				observeCurrent(t, s, scope, r.ID, facts)
				if actionRows(t, s, r.ID)["C"].ID != "" {
					t.Fatal("new C used old readiness without exact fresh action result")
				}
				if !completeBefore {
					if e = s.CompleteFromOriginal(context.Background(), old, actual, allow); e != nil {
						t.Fatal(e)
					}
				}
				after, _, _, _ := s.Get(context.Background(), scope, r.ID)
				if len(after.Steps) > 0 && after.Steps[0].State == "done" {
					t.Fatal("old completion directly overwrote new definition")
				}
				original, _, _ := s.ExactContinuation(context.Background(), old.ID)
				current = observeCurrent(t, s, scope, r.ID, append(facts, actual))
				rows := actionRows(t, s, r.ID)
				if rows["C"].ID == "" || rows["C"].DefinitionRevision != r.DefinitionRevision || rows["A"].ID != old.ID {
					t.Fatal("exact unchanged admitted carry did not advance only C", current.Steps, rows)
				}
				var count int
				s.db.QueryRow(`SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND node_id='A'`, r.ID).Scan(&count)
				if count != 1 {
					t.Fatal("A repeated", count)
				}
				found := false
				for _, v := range current.Steps {
					if v.ID == "A" && v.ContinuationID == old.ID && v.Reason == "carried_original_checkpoint" {
						found = true
					}
				}
				if !found {
					t.Fatal("missing explicit carry lineage", current.Steps)
				}
				before := digest(rows["C"])
				observeCurrent(t, s, scope, r.ID, append(facts, actual))
				if digest(actionRows(t, s, r.ID)["C"]) != before {
					t.Fatal("duplicate carry changed C")
				}
				preserved, _, _ := s.ExactContinuation(context.Background(), old.ID)
				if digest(preserved) != digest(original) {
					t.Fatal("carry rewrote original outbox")
				}
			})
		}
	}
}

func TestDefinitionCarryRejectsChangedFrozenExecutionAndInputs(t *testing.T) {
	for _, kind := range []string{"action", "source", "predicate", "incoming", "manager", "source_generation", "grant_revision", "target_ABA", "wrong_round", "consumed_hash", "not_checkpointed", "missing_input", "output_sequence", "input_sequence", "final_revoke"} {
		t.Run(kind, func(t *testing.T) {
			s, scope, i := fixture(t)
			r, facts := edgePipeline(t, s, scope, i, "on_success", "all_success", 1)
			old := admitted(t, s, actionRows(t, s, r.ID)["A"])
			actual := completion(old)
			if kind != "not_checkpointed" {
				if e := s.CompleteFromOriginal(context.Background(), old, actual, allow); e != nil {
					t.Fatal(e)
				}
			}
			current, _, _, _ := s.Get(context.Background(), scope, r.ID)
			raw, _ := json.Marshal(current)
			var next Record
			json.Unmarshal(raw, &next)
			next.Pipeline.Name = "new frontier"
			ni := i
			switch kind {
			case "action":
				next.Pipeline.Nodes[0].Action.Instruction += " changed"
				next.Pipeline.Nodes[0].Action.InstructionHash = digestText(next.Pipeline.Nodes[0].Action.Instruction)
			case "source":
				next.Pipeline.Nodes[0].Source.Request = "changed-request"
			case "predicate":
				next.Pipeline.Nodes[0].Predicate.InputHash = "changed"
			case "incoming":
				next.Pipeline.Nodes = append(next.Pipeline.Nodes, Step{ID: "B", Title: "new input", Source: &facts[1].Source, Predicate: Predicate{Kind: "success_sealed"}})
				next.Pipeline.Edges = append(next.Pipeline.Edges, Edge{ID: "newIncoming", From: []string{"B"}, To: "A", Condition: "on_success", Join: "all_success", Output: "references_only"})
			case "manager":
				next.Manager.Thread = "ABA-manager"
			case "source_generation":
				ni.SourceGeneration++
			case "grant_revision":
				ni.GrantRevision++
			case "target_ABA":
				ni.Targets = map[string]uint64{"A": 999}
			case "wrong_round":
				actual.Source.RoundID = "other-round"
			case "consumed_hash":
				actual.ConsumedHash = "other-hash"
			case "output_sequence":
				actual.Sequence = 0
			case "input_sequence":
				facts[0].Sequence = 0
			}
			// The original definition writer retains its own validation/manager rules.
			receipt, e := s.Mutate(context.Background(), scope, ni, "new-frontier", "pipeline_define", "newhash", r.ID, current.Revision, &next, nil, nil, allow)
			if kind == "incoming" || kind == "manager" {
				if e == nil && receipt.Record.Manager != scope.Manager {
					t.Fatal("manager was changed")
				}
				if e != nil {
					return
				}
			} else if e != nil {
				t.Fatal(e)
			}
			r = *receipt.Record
			if kind == "manager" {
				// Mutate correctly owns/normalizes the manager; a foreign manager
				// cannot read a compatible carry even if presented in memory.
				if r.Manager != current.Manager {
					t.Fatal("manager mutation escaped normalization")
				}
				r.Manager.Thread = "foreign-manager"
				candidates, e := s.CarryCandidates(context.Background(), r, ni)
				if e != nil || len(candidates) != 0 {
					t.Fatal("foreign manager carry", candidates, e)
				}
				return
			}
			input := append(append([]Fact{}, facts...), actual)
			if kind == "missing_input" {
				input = []Fact{facts[2], actual}
			}
			check := allow
			if kind == "final_revoke" {
				calls := 0
				check = func(context.Context) error {
					calls++
					if calls == 2 {
						return ErrScope
					}
					return nil
				}
			}
			e = s.Observe(context.Background(), r.ID, r.Revision, input, check)
			if kind == "final_revoke" && !errors.Is(e, ErrScope) {
				t.Fatal("final guard not retained", e)
			} else if kind != "final_revoke" && e != nil {
				t.Fatal(e)
			}
			var count int
			s.db.QueryRow(`SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND definition_revision=?`, r.ID, r.DefinitionRevision).Scan(&count)
			if count != 0 {
				t.Fatal("changed lineage released a current effect", kind, count)
			}
		})
	}
}
