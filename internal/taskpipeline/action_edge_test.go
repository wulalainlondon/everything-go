package taskpipeline

import (
	"context"
	"encoding/json"
	"testing"
)

func readinessFact(name string) Fact {
	f := fact()
	f.Source.Session = name
	f.Source.NativeID = name + "-r1"
	f.Source.RoundID = name + "-r1"
	f.Source.Request = name + "-request-r1"
	f.Source.Thread = name + "-thread"
	f.SealID = name + "-seal-r1"
	f.FullHash = digest(name + "-sealed-readiness")
	return f
}
func actionOn(f Fact) *Action {
	a := action()
	a.Target = Identity{Authority: f.Source.Authority, Session: f.Source.Session, Thread: f.Source.Thread, Revision: f.Source.Revision}
	return a
}
func actionRows(t *testing.T, s *Store, id string) map[string]Continuation {
	t.Helper()
	rows, e := s.db.Query(`SELECT continuation FROM task_pipeline_outbox WHERE record_id=? ORDER BY rowid`, id)
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	out := map[string]Continuation{}
	for rows.Next() {
		var raw []byte
		if e = rows.Scan(&raw); e != nil {
			t.Fatal(e)
		}
		var c Continuation
		if e = json.Unmarshal(raw, &c); e != nil {
			t.Fatal(e)
		}
		out[c.NodeID] = c
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	return out
}
func observeCurrent(t *testing.T, s *Store, scope Scope, id string, facts []Fact) Record {
	t.Helper()
	r, _, _, e := s.Get(context.Background(), scope, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Observe(context.Background(), r.ID, r.Revision, facts, allow); e != nil {
		t.Fatal(e)
	}
	r, _, _, e = s.Get(context.Background(), scope, id)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func edgePipeline(t *testing.T, s *Store, scope Scope, issuer Issuer, condition, join string, predecessors int) (Record, []Fact) {
	t.Helper()
	facts := []Fact{readinessFact("A"), readinessFact("D"), readinessFact("C")}
	nodes := []Step{}
	from := []string{}
	for n := 0; n < predecessors; n++ {
		f := facts[n]
		nodes = append(nodes, Step{ID: f.Source.Session, Title: f.Source.Session, Required: true, Source: &f.Source, Predicate: Predicate{Kind: "success_sealed"}, Action: actionOn(f)})
		from = append(from, f.Source.Session)
	}
	c := facts[2]
	nodes = append(nodes, Step{ID: "C", Title: "downstream", Required: true, Source: &c.Source, Predicate: Predicate{Kind: "success_sealed"}, Action: actionOn(c)})
	r := create(t, s, scope, issuer, Record{Kind: "pipeline", Pipeline: &Pipeline{Name: "original action boundaries", Authorization: auth(), Nodes: nodes, Edges: []Edge{{ID: "toC", From: from, To: "C", Condition: condition, Join: join, Output: "references_only"}}}}, nil)
	enabled, e := s.Mutate(context.Background(), scope, issuer, "enable", "pipeline_enable", "hash", r.ID, r.Revision, nil, nil, nil, allow)
	if e != nil {
		t.Fatal(e)
	}
	r = observeCurrent(t, s, scope, enabled.Record.ID, facts)
	return r, facts
}
func TestActionEdgeReadinessDoesNotReleasePendingOrUnknownAction(t *testing.T) {
	for _, phase := range []string{"ready", "issued", "queued", "unknown", "checkpoint_applied"} {
		t.Run(phase, func(t *testing.T) {
			s, scope, issuer := fixture(t)
			r, facts := edgePipeline(t, s, scope, issuer, "on_success", "all_success", 1)
			rows := actionRows(t, s, r.ID)
			if len(rows) != 1 || rows["A"].ID == "" {
				t.Fatal("R1 readiness released C before action", rows)
			}
			c := rows["A"]
			if phase == "issued" {
				var e error
				c, e = s.MarkIssued(context.Background(), c, allow)
				if e != nil {
					t.Fatal(e)
				}
			} else if phase != "ready" {
				c = admitted(t, s, c)
				if phase != "queued" {
					if e := s.SetOutcome(context.Background(), c, phase, "original receipt pending", c.OriginalReceipt); e != nil {
						t.Fatal(e)
					}
				}
			}
			observeCurrent(t, s, scope, r.ID, facts)
			rows = actionRows(t, s, r.ID)
			if rows["C"].ID != "" {
				t.Fatal("unfinished action released C", phase, rows)
			}
		})
	}
}
func TestActionEdgeSuccessRequiresMatchingActionProofAndForwardsR2(t *testing.T) {
	s, scope, issuer := fixture(t)
	r, facts := edgePipeline(t, s, scope, issuer, "on_success", "all_success", 1)
	c := admitted(t, s, actionRows(t, s, r.ID)["A"])
	f := completion(c)
	wrong := f
	wrong.Source.Request = "foreign-request"
	if e := s.CompleteFromOriginal(context.Background(), c, wrong, allow); e == nil {
		t.Fatal("foreign original request completed action")
	}
	observeCurrent(t, s, scope, r.ID, facts)
	if actionRows(t, s, r.ID)["C"].ID != "" {
		t.Fatal("wrong request released C")
	}
	if e := s.CompleteFromOriginal(context.Background(), c, f, allow); e != nil {
		t.Fatal(e)
	}
	observeCurrent(t, s, scope, r.ID, facts)
	out := actionRows(t, s, r.ID)["C"]
	if out.ID == "" {
		t.Fatal("matching R2 did not release C")
	}
	hasNew := false
	for _, ev := range out.Evidence {
		if ev.Source.Session == "A" {
			if ev.Source.Request != c.RequestID {
				t.Fatal("old readiness forwarded instead of action output", ev)
			}
			hasNew = true
		}
	}
	if !hasNew {
		t.Fatal("R2 output not chosen", out)
	}
	if e := s.CompleteFromOriginal(context.Background(), c, f, allow); e != nil {
		t.Fatal(e)
	}
	observeCurrent(t, s, scope, r.ID, facts)
	if actionRows(t, s, r.ID)["C"].ID != out.ID {
		t.Fatal("repeat final replaced downstream original claim")
	}
}
func TestActionEdgeSourceFailureIsNotActionFailure(t *testing.T) {
	s, scope, issuer := fixture(t)
	r, facts := edgePipeline(t, s, scope, issuer, "on_terminal", "all_terminal", 1)
	// This source-readiness failure belongs to R1 and cannot complete queued A.
	facts[0].Terminal = "failed"
	facts[0].Final = "pending"
	facts[0].SealID = ""
	facts[0].FullHash = ""
	observeCurrent(t, s, scope, r.ID, facts)
	if actionRows(t, s, r.ID)["C"].ID != "" {
		t.Fatal("old source failure became new action terminal")
	}
	c := admitted(t, s, actionRows(t, s, r.ID)["A"])
	f := completion(c)
	f.Terminal = "failed"
	f.Final = "pending"
	f.SealID = ""
	f.FullHash = ""
	if e := s.CompleteFromOriginal(context.Background(), c, f, allow); e != nil {
		t.Fatal(e)
	}
	r = observeCurrent(t, s, scope, r.ID, facts)
	if actionRows(t, s, r.ID)["C"].ID == "" {
		t.Fatal("true action terminal failure not forwarded")
	}
	for _, st := range r.Steps {
		if st.ID == "A" && st.State != "failed" {
			t.Fatal("action failure reclassified done", st)
		}
	}
}
func TestActionEdgeJoinsWaitForActualTerminalInputsAndFreezeAnyWinner(t *testing.T) {
	for _, tc := range []struct {
		join, condition, first string
		wantFirst, wantLast    bool
	}{{"all_success", "on_success", "completed", false, true}, {"any_success", "on_success", "completed", true, true}, {"all_terminal", "on_terminal", "failed", false, true}, {"all_success", "on_success", "failed", false, false}, {"any_success", "on_success", "failed", false, true}} {
		t.Run(tc.join+tc.condition+tc.first, func(t *testing.T) {
			s, scope, issuer := fixture(t)
			r, facts := edgePipeline(t, s, scope, issuer, tc.condition, tc.join, 2)
			rows := actionRows(t, s, r.ID)
			if rows["C"].ID != "" {
				t.Fatal("readiness satisfied join")
			}
			a := admitted(t, s, rows["A"])
			d := admitted(t, s, rows["D"])
			if e := s.SetOutcome(context.Background(), d, "unknown", "lost ACK", d.OriginalReceipt); e != nil {
				t.Fatal(e)
			}
			f := completion(a)
			f.Terminal = tc.first
			if tc.first == "failed" {
				f.Final = "pending"
				f.SealID = ""
				f.FullHash = ""
			}
			if e := s.CompleteFromOriginal(context.Background(), a, f, allow); e != nil {
				t.Fatal(e)
			}
			observeCurrent(t, s, scope, r.ID, facts)
			first := actionRows(t, s, r.ID)["C"]
			if (first.ID != "") != tc.wantFirst {
				t.Fatal("unknown join treated as success", tc, first)
			}
			if e := s.CompleteFromOriginal(context.Background(), d, completion(d), allow); e != nil {
				t.Fatal(e)
			}
			observeCurrent(t, s, scope, r.ID, facts)
			last := actionRows(t, s, r.ID)["C"]
			if (last.ID != "") != tc.wantLast {
				t.Fatal("matching terminals not respected", tc, last)
			}
			if first.ID != "" && (first.ID != last.ID || digest(first.Evidence) != digest(last.Evidence)) {
				t.Fatal("late terminal replaced any-success chosen set")
			}
		})
	}
}
func TestActionEdgeQAAndDefinitionAreExact(t *testing.T) {
	s, scope, issuer := fixture(t)
	r, facts := edgePipeline(t, s, scope, issuer, "on_QA_PASS", "all_success", 1)
	c := admitted(t, s, actionRows(t, s, r.ID)["A"])
	f := completion(c)
	f.QA = "PASS"
	f.QARound = "qa"
	f.QAOwnerRound = facts[0].Source.RoundID
	if e := s.CompleteFromOriginal(context.Background(), c, f, allow); e != nil {
		t.Fatal(e)
	}
	observeCurrent(t, s, scope, r.ID, facts)
	if actionRows(t, s, r.ID)["C"].ID != "" {
		t.Fatal("R1 owner QA adopted for R2 action")
	}
	f.QAOwnerRound = f.Source.RoundID
	if e := s.CompleteFromOriginal(context.Background(), c, f, allow); e != nil {
		t.Fatal(e)
	}
	observeCurrent(t, s, scope, r.ID, facts)
	if actionRows(t, s, r.ID)["C"].ID == "" {
		t.Fatal("exact R2 QA did not release edge")
	}
	s, scope, issuer = fixture(t)
	r, facts = edgePipeline(t, s, scope, issuer, "on_success", "all_success", 1)
	old := admitted(t, s, actionRows(t, s, r.ID)["A"])
	current, _, _, _ := s.Get(context.Background(), scope, r.ID)
	updated := current
	copyPipeline := *current.Pipeline
	updated.Pipeline = &copyPipeline
	updated.Pipeline.Name = "new definition"
	receipt, e := s.Mutate(context.Background(), scope, issuer, "update", "pipeline_define", "new-hash", r.ID, current.Revision, &updated, nil, nil, allow)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.CompleteFromOriginal(context.Background(), old, completion(old), allow); e != nil {
		t.Fatal(e)
	}
	observeCurrent(t, s, scope, r.ID, facts)
	var downstream int
	if e = s.db.QueryRow(`SELECT count(*) FROM task_pipeline_outbox WHERE record_id=? AND definition_revision=? AND node_id='C'`, r.ID, receipt.Record.DefinitionRevision).Scan(&downstream); e != nil || downstream != 0 {
		t.Fatal("old definition completion released new C", downstream, e)
	}
}

func TestActionEdgeDelegationRequiresExactRetainedActionRound(t *testing.T) {
	s, scope, issuer := fixture(t)
	a, c := readinessFact("A"), readinessFact("C")
	action := actionOn(a)
	action.Kind = "delegation_continue"
	action.OriginalTask = &a.Source
	r := create(t, s, scope, issuer, Record{Kind: "pipeline", Pipeline: &Pipeline{Name: "retained R2 output", Authorization: auth(), Nodes: []Step{{ID: "A", Title: "readiness", Required: true, Source: &a.Source, Predicate: Predicate{Kind: "success_sealed"}, Action: action}, {ID: "C", Title: "next", Required: true, Source: &c.Source, Predicate: Predicate{Kind: "success_sealed"}, Action: actionOn(c)}}, Edges: []Edge{{ID: "AC", From: []string{"A"}, To: "C", Condition: "on_success", Join: "all_success", Output: "references_only"}}}}, nil)
	enabled, e := s.Mutate(context.Background(), scope, issuer, "enable", "pipeline_enable", "hash", r.ID, r.Revision, nil, nil, nil, allow)
	if e != nil {
		t.Fatal(e)
	}
	r = observeCurrent(t, s, scope, enabled.Record.ID, []Fact{a, c})
	original := actionRows(t, s, r.ID)["A"]
	original, e = s.MarkIssued(context.Background(), original, allow)
	if e != nil {
		t.Fatal(e)
	}
	receipt := &EffectReceipt{Kind: "delegation_round", Authority: a.Source.Authority, Session: a.Source.Session, Request: "actual-child-request-r2", NativeID: "actual-retained-round-r2", Key: original.ID, InputHash: "native-input"}
	if e = s.SetOutcome(context.Background(), original, "queued", "", receipt); e != nil {
		t.Fatal(e)
	}
	original, _, e = s.ExactContinuation(context.Background(), original.ID)
	if e != nil {
		t.Fatal(e)
	}
	f := completion(original)
	f.Source.Kind = "delegation"
	f.Source.Request = receipt.Request
	f.Source.NativeID = receipt.NativeID
	f.Source.RoundID = receipt.NativeID
	wrong := f
	wrong.Source.RoundID = a.Source.RoundID
	if e = s.CompleteFromOriginal(context.Background(), original, wrong, allow); e == nil {
		t.Fatal("readiness round substituted for retained action round")
	}
	observeCurrent(t, s, scope, r.ID, []Fact{a, c})
	if actionRows(t, s, r.ID)["C"].ID != "" {
		t.Fatal("wrong round released C")
	}
	if e = s.CompleteFromOriginal(context.Background(), original, f, allow); e != nil {
		t.Fatal(e)
	}
	observeCurrent(t, s, scope, r.ID, []Fact{a, c})
	next := actionRows(t, s, r.ID)["C"]
	if next.ID == "" {
		t.Fatal("exact retained action output did not release C")
	}
	for _, proof := range next.Evidence {
		if proof.Source.Session == a.Source.Session && (proof.Source.Request != receipt.Request || proof.Source.RoundID != receipt.NativeID) {
			t.Fatal("old R1 forwarded", proof)
		}
	}
}
