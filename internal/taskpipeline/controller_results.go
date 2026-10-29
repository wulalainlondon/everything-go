package taskpipeline

import (
	"context"
	"encoding/json"
	"errors"
)

// ControllerApplication is a read projection of ORIGINAL checkpoint receipts or
// checkpointed continuation evidence. It never changes mission state or grants.
type ControllerApplication struct {
	RecordID                                     string
	DefinitionRevision, CheckpointRevision       uint64
	EventID, ContinuationID, IntentID, InputHash string
	Fact                                         Fact
	Processing                                   Identity
	ProcessingRequest, ProcessingTurn            string
	Final                                        *Fact
	Issuer                                       Issuer
	Definition                                   Record
}

func (s *Store) ControllerApplications(ctx context.Context, manager Identity, generation uint64) ([]ControllerApplication, error) {
	out := []ControllerApplication{}
	managerKey := ManagerKey(Scope{Manager: manager})
	// Materialize bounded rows before looking up the original events/definitions;
	// single-connection stores must never borrow a held rows cursor recursively.
	rows, e := s.db.QueryContext(ctx, `SELECT receipt FROM task_pipeline_operations WHERE manager=? AND operation IN ('mission_checkpoint','continuation_ack') ORDER BY rowid DESC LIMIT 4097`, managerKey)
	if e != nil {
		return nil, e
	}
	receipts := []Receipt{}
	for rows.Next() {
		var b []byte
		var r Receipt
		if e = rows.Scan(&b); e != nil {
			break
		}
		if e = json.Unmarshal(b, &r); e != nil {
			break
		}
		receipts = append(receipts, r)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return nil, e
	}
	if len(receipts) > 4096 {
		return nil, errors.New("controller_application_capacity")
	}
	for _, receipt := range receipts {
		r := receipt.Record
		if r == nil || r.Kind != "mission" || r.Manager != manager || r.Checkpoint == nil || receipt.Provenance == nil || receipt.State != "applied" || receipt.Provenance.RequestID != r.Checkpoint.RequestID {
			continue
		}
		definition, issuer, e := s.OriginalDefinition(ctx, r.ID, r.DefinitionRevision)
		if e != nil {
			return nil, e
		}
		if issuer.SourceGeneration != generation || issuer.Scope.Manager != manager {
			continue
		}
		for _, id := range r.Checkpoint.Processed {
			var b []byte
			e = s.db.QueryRowContext(ctx, `SELECT event FROM task_pipeline_events WHERE record_id=? AND event_id=?`, r.ID, id).Scan(&b)
			if e != nil {
				return nil, e
			}
			var event Event
			if e = json.Unmarshal(b, &event); e != nil {
				return nil, e
			}
			if event.State != "mission_applied" && event.State != "evidence_verified" {
				continue
			}
			out = append(out, ControllerApplication{RecordID: r.ID, DefinitionRevision: r.DefinitionRevision, CheckpointRevision: r.Revision, EventID: id, IntentID: receipt.IntentID, InputHash: receipt.InputHash, Fact: event.Fact, Processing: receipt.Provenance.Source, ProcessingRequest: receipt.Provenance.RequestID, ProcessingTurn: receipt.Provenance.NativeTurnID, Issuer: issuer, Definition: definition})
		}
	}
	rows, e = s.db.QueryContext(ctx, `SELECT continuation FROM task_pipeline_outbox WHERE state='checkpointed' ORDER BY rowid DESC LIMIT 4097`)
	if e != nil {
		return nil, e
	}
	continuations := []Continuation{}
	for rows.Next() {
		var b []byte
		var c Continuation
		if e = rows.Scan(&b); e != nil {
			break
		}
		if e = json.Unmarshal(b, &c); e != nil {
			break
		}
		continuations = append(continuations, c)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return nil, e
	}
	if len(continuations) > 4096 {
		return nil, errors.New("controller_application_capacity")
	}
	for _, c := range continuations {
		if c.CheckpointRevision == 0 || c.OriginalReceipt == nil || c.NativeStart != "observed" {
			continue
		}
		definition, issuer, e := s.OriginalDefinition(ctx, c.RecordID, c.DefinitionRevision)
		if e != nil {
			return nil, e
		}
		if definition.Kind != "mission" || definition.Manager != manager || issuer.SourceGeneration != generation {
			continue
		}
		// The checkpoint's actual terminal fact must match the ORIGINAL effect; a
		// merely queued/admitted or native-ACK-only continuation is not completion.
		var b []byte
		e = s.db.QueryRowContext(ctx, `SELECT event FROM task_pipeline_events WHERE record_id=? AND node_id=? AND json_extract(event,'$.state')='checkpointed' AND json_extract(event,'$.applied_revision')=? ORDER BY seq DESC LIMIT 1`, c.RecordID, c.NodeID, c.CheckpointRevision).Scan(&b)
		if e != nil {
			return nil, e
		}
		var terminal Event
		if e = json.Unmarshal(b, &terminal); e != nil {
			return nil, e
		}
		f := terminal.Fact
		if f.Source.Session != c.OriginalReceipt.Session || f.Source.Request != c.OriginalReceipt.Request || (!PredicateMet(Predicate{Kind: "success_sealed"}, f) && !PredicateMet(Predicate{Kind: "terminal"}, f)) {
			continue
		}
		for _, fact := range c.Evidence {
			copy := f
			out = append(out, ControllerApplication{RecordID: c.RecordID, DefinitionRevision: c.DefinitionRevision, CheckpointRevision: c.CheckpointRevision, EventID: terminal.ID, ContinuationID: c.ID, Fact: fact, Processing: c.Action.Target, ProcessingRequest: c.OriginalReceipt.Request, Final: &copy, Issuer: issuer, Definition: definition})
		}
	}
	return out, nil
}
