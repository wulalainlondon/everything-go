package taskpipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"everything-go/internal/taskapi"
	"sort"
)

// CarryProof is an internal reader, not a wire field or new execution truth.
// Its lineage is persisted by RecordRevision -> StepState.ContinuationID and
// EvidenceHash -> original frozen outbox/definition and checkpoint event.
type CarryProof struct {
	Continuation Continuation
	Fact         Fact
}

func stepExecutionHash(s Step) string { s.Title = ""; return digest(s) }
func incomingEdges(r Record, node string) []Edge {
	out := []Edge{}
	if r.Pipeline != nil {
		for _, e := range r.Pipeline.Edges {
			if e.To == node {
				out = append(out, e)
			}
		}
	}
	return out
}
func originalDefinitionRead(ctx context.Context, q reader, id string, version uint64) (Record, Issuer, error) {
	var r Record
	var i Issuer
	var raw, ir []byte
	e := q.QueryRowContext(ctx, `SELECT r.record,a.issuer FROM task_pipeline_revisions r JOIN task_pipeline_authorizations a ON a.record_id=r.record_id AND a.definition_revision=? WHERE r.record_id=? AND json_extract(r.record,'$.definition_revision')=? ORDER BY r.revision LIMIT 1`, version, id, version).Scan(&raw, &ir)
	if e == nil {
		e = json.Unmarshal(raw, &r)
	}
	if e == nil {
		e = json.Unmarshal(ir, &i)
	}
	return r, i, e
}
func carryCompatible(ctx context.Context, q reader, r Record, i Issuer, st Step, c Continuation) (bool, error) {
	if r.Pipeline == nil || st.Action == nil || c.DefinitionRevision >= r.DefinitionRevision || c.RecordID != r.ID || c.NodeID != st.ID {
		return false, nil
	}
	old, oi, e := originalDefinitionRead(ctx, q, r.ID, c.DefinitionRevision)
	if e != nil {
		return false, e
	}
	if old.Manager != r.Manager || oi.Scope != i.Scope || oi.Kind != i.Kind || oi.SourceGeneration != i.SourceGeneration || oi.GrantRevision != i.GrantRevision || digest(incomingEdges(old, st.ID)) != digest(incomingEdges(r, st.ID)) {
		return false, nil
	}
	matched := false
	for _, previous := range Steps(old) {
		if previous.ID == st.ID && stepExecutionHash(previous) == stepExecutionHash(st) {
			matched = true
			break
		}
	}
	if !matched {
		return false, nil
	}
	targets := []string{c.Action.Target.Session}
	for _, f := range c.Evidence {
		targets = append(targets, f.Source.Session)
	}
	for _, id := range targets {
		a, ak := oi.Targets[id]
		b, bk := i.Targets[id]
		if ak != bk || a != b {
			return false, nil
		}
	}
	return digest(*st.Action) == digest(c.Action), nil
}
func checkpointFactMatches(c Continuation, event Event) bool {
	if c.State != "checkpointed" || c.OriginalReceipt == nil || event.State != "checkpointed" || event.RecordID != c.RecordID || event.NodeID != c.NodeID {
		return false
	}
	f := event.Fact
	receipt := c.OriginalReceipt
	if receipt.Key != c.ID || (receipt.Kind != "session_input" && receipt.Kind != "delegation_round") || receipt.Session != f.Source.Session || receipt.Request != f.Source.Request || c.Action.Target.Session != f.Source.Session || receipt.Authority != f.Source.Authority || c.Action.Target.Authority != f.Source.Authority || c.Action.Target.Thread != f.Source.Thread || c.Action.Target.Revision != f.Source.Revision || c.Terminal != f.Terminal {
		return false
	}
	if receipt.Kind == "delegation_round" && receipt.NativeID != f.Source.RoundID {
		return false
	}
	if event.ID != "pe_"+digest(struct{ Continuation, Proof string }{c.ID, sourceKey(f)}) {
		return false
	}
	return PredicateMet(Predicate{Kind: "success_sealed"}, f) || ((f.Terminal == "failed" || f.Terminal == "cancelled" || f.Terminal == "rejected") && PredicateMet(Predicate{Kind: "terminal"}, f))
}
func checkpointEventRead(ctx context.Context, q reader, c Continuation) (Event, bool, error) {
	var event Event
	if c.OriginalReceipt == nil {
		return event, false, nil
	}
	var raw []byte
	e := q.QueryRowContext(ctx, `SELECT event FROM task_pipeline_events WHERE record_id=? AND node_id=? AND json_extract(event,'$.state')='checkpointed' AND json_extract(event,'$.fact.source.request_id')=? ORDER BY seq DESC LIMIT 1`, c.RecordID, c.NodeID, c.OriginalReceipt.Request).Scan(&raw)
	if e == sql.ErrNoRows {
		return event, false, nil
	}
	if e != nil {
		return event, false, e
	}
	if e = json.Unmarshal(raw, &event); e != nil {
		return event, false, e
	}
	return event, checkpointFactMatches(c, event), nil
}

// MatchCarryInputs requires every frozen selected input, not newly chosen
// any-success inputs or metadata hashes, to have current canonical proof.
func MatchCarryInputs(c Continuation, facts []Fact) bool {
	keys := []string{}
	for _, f := range facts {
		if f.Sequence > 0 {
			keys = append(keys, sourceKey(f))
		}
	}
	sort.Strings(keys)
	for _, old := range c.Evidence {
		key := sourceKey(old)
		pos := sort.SearchStrings(keys, key)
		if pos == len(keys) || keys[pos] != key {
			return false
		}
	}
	return true
}
func SameFactProof(a, b Fact) bool {
	return a.Sequence > 0 && b.Sequence > 0 && sourceKey(a) == sourceKey(b)
}

// Each lookup is bounded by the current DAG's <=32 nodes. No history/body
// catalog is exported and SQL readers use the original writer transaction.
func (s *Store) CarryCandidates(ctx context.Context, r Record, i Issuer) ([]CarryProof, error) {
	return carryCandidates(ctx, taskapi.ReadSQL(ctx, s.db), r, i)
}
func carryCandidates(ctx context.Context, q reader, r Record, i Issuer) ([]CarryProof, error) {
	out := []CarryProof{}
	if r.Pipeline == nil || r.DefinitionRevision <= 1 {
		return out, nil
	}
	for _, st := range Steps(r) {
		if st.Action == nil {
			continue
		}
		var raw []byte
		e := q.QueryRowContext(ctx, `SELECT continuation FROM task_pipeline_outbox WHERE record_id=? AND definition_revision<? AND node_id=? ORDER BY definition_revision DESC LIMIT 1`, r.ID, r.DefinitionRevision, st.ID).Scan(&raw)
		if e == sql.ErrNoRows {
			continue
		}
		if e != nil {
			return nil, e
		}
		var c Continuation
		if e = json.Unmarshal(raw, &c); e != nil {
			return nil, e
		}
		compatible, e := carryCompatible(ctx, q, r, i, st, c)
		if e != nil {
			return nil, e
		}
		if !compatible {
			continue
		}
		event, proved, e := checkpointEventRead(ctx, q, c)
		if e != nil {
			return nil, e
		}
		if proved {
			out = append(out, CarryProof{Continuation: c, Fact: event.Fact})
		}
	}
	return out, nil
}
