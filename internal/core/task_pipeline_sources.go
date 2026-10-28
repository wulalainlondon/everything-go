package core

import (
	"context"
	"errors"
	"everything-go/internal/taskapi"
	p "everything-go/internal/taskpipeline"
)

func pipelineSourceDomains(loc p.Locator) []string {
	path := loc.Kind
	if path == "request" {
		path = "ordinary"
	}
	if path == "ordinary" {
		return []string{path}
	}
	return []string{path, "ordinary"}
}

// Each exact source has separate original journal/native-proof store cursors.
// The vector is not advertised as an atomic snapshot of independent stores.
// Observe alone commits cursor advancement with imported facts in the local DB.
func (h *Hub) pipelineSourceBatch(ctx context.Context, i p.Issuer, r p.Record) ([]p.Fact, []p.SourceCursor, error) {
	store := h.messageQueue.PipelineStore()
	saved, e := store.SourceCursors(ctx, i.Scope, r.ID)
	if e != nil {
		return nil, nil, e
	}
	old := map[string]p.SourceCursor{}
	for _, c := range saved {
		old[c.Key] = c
	}
	facts := []p.Fact{}
	cursors := []p.SourceCursor{}
	for _, step := range p.Steps(r) {
		if step.Source == nil {
			if f, e := h.pipelineWorkRef(ctx, pipelineObserver(i, r), r.Manager.Session, step); e == nil && f != nil {
				facts = append(facts, *f)
			}
			continue
		}
		ready := true
		var selected *p.Fact
		for _, path := range pipelineSourceDomains(*step.Source) {
			c, f := h.samplePipelineSourceCursor(ctx, i, r, step, path, old)
			cursors = append(cursors, c)
			if f == nil {
				ready = false
			} else {
				selected = f
			}
		}
		if ready && selected != nil {
			facts = append(facts, *selected)
		}
	}
	// Old carried effects retain the stronger exact original receipt/source chain.
	carry, e := store.CarryCandidates(ctx, r, i)
	if e != nil {
		return nil, nil, e
	}
	for _, proof := range carry {
		f, e := h.pipelineAdmittedFact(ctx, proof.Continuation, true)
		if e != nil || !p.SameFactProof(f, proof.Fact) {
			continue
		}
		if p.MatchCarryInputs(proof.Continuation, facts) {
			facts = append(facts, f)
		}
	}
	return facts, cursors, nil
}
func (h *Hub) samplePipelineSourceCursor(ctx context.Context, i p.Issuer, r p.Record, step p.Step, path string, old map[string]p.SourceCursor) (p.SourceCursor, *p.Fact) {
	loc := *step.Source
	journal := h.apiJournals()[path]
	identity := r.Manager.Authority + ":" + path
	identityKnown := false
	// Failure to read a store epoch does not invent a replacement epoch or skip it.
	if journal != nil {
		epoch, e := journal.Identity(ctx)
		if e == nil {
			identity += ":" + epoch
			identityKnown = true
		} else {
			for _, c := range old {
				if c.DefinitionRevision == r.DefinitionRevision && c.NodeID == step.ID && c.Source == loc && c.Store == path {
					c.State = "offline"
					c.Reason = "source_identity_unavailable"
					return c, nil
				}
			}
		}
	}
	key := p.SourceCursorKey(i.Scope, r.ID, r.DefinitionRevision, step.ID, loc, path, identity)
	c := old[key]
	if c.Key == "" {
		c = p.SourceCursor{Key: key, RecordID: r.ID, DefinitionRevision: r.DefinitionRevision, NodeID: step.ID, Source: loc, Store: path, StoreIdentity: identity, ScopeHash: p.ScopeKey(i.Scope)}
	}
	c.State = "offline"
	c.Reason = "source_read_unavailable"
	for _, previous := range old {
		if previous.DefinitionRevision == r.DefinitionRevision && previous.NodeID == step.ID && previous.Source == loc && previous.Store == path && previous.StoreIdentity != identity {
			c.State = "evidence_gap"
			c.Reason = "source_store_identity_changed"
			return c, nil
		}
	}
	if journal == nil || !identityKnown {
		c.Reason = "source_adapter_unavailable"
		return c, nil
	}
	wm, e := journal.Watermark(ctx)
	if e != nil {
		return c, nil
	}
	c.Watermark = wm
	if wm < c.After {
		c.State = "evidence_gap"
		c.Reason = "source_sequence_regressed"
		return c, nil
	}
	changes, floor, e := journal.Changes(ctx, c.After, 100)
	c.Floor = floor
	gap := false
	if e != nil {
		var api *taskapi.APIError
		if !errors.As(e, &api) || api.Code != "cursor_expired" {
			return c, nil
		}
		gap = true
	}
	f, e := h.pipelineFact(ctx, pipelineObserver(i, r), r.Manager.Session, loc)
	if e != nil {
		return c, nil
	}
	if e = h.pipelineArtifactFact(ctx, step.Predicate, &f); e != nil {
		c.Reason = "artifact_read_unavailable"
		return c, nil
	}
	// Gap recovery proves only the named current snapshot, never discarded starts.
	if gap && floor > wm {
		c.State = "partial"
		c.Reason = "source_retention_advanced_during_snapshot"
		return c, nil
	}
	if gap {
		c.After = wm
		c.State = "snapshot_recovered"
		c.Reason = "retention_gap_exact_snapshot_only"
		if f.Sequence == 0 {
			c.State = "evidence_gap"
			c.Reason = "retained_snapshot_has_no_original_sequence"
		}
	} else {
		next := c.After
		for _, v := range changes {
			if v.Sequence <= wm && v.Sequence > next {
				next = v.Sequence
			}
		}
		c.After = next
		c.State = "current"
		c.Reason = ""
		if len(changes) == 100 && next < wm {
			c.State = "partial"
			c.Reason = "more_original_source_pages"
		} else if next < wm {
			c.After = wm
		}
	}
	c.FactHash = taskapi.Digest(f)
	if c.State == "partial" || c.State == "evidence_gap" {
		return c, nil
	}
	return c, &f
}
func (h *Hub) reconcilePipelineNotifications(ctx context.Context) {
	store := h.messageQueue.PipelineStore()
	pending, e := store.PendingNotifications(ctx, 32)
	if e != nil {
		return
	}
	for _, n := range pending {
		old, i, e := store.OriginalDefinition(ctx, n.RecordID, n.DefinitionRevision)
		if e != nil {
			continue
		}
		check := func(ctx context.Context) error {
			if n.Destination != old.Manager {
				return errors.New("visual_destination_requires_owner_scope")
			}
			if e := h.pipelineIssuerCurrent(ctx, i, old); e != nil {
				return e
			}
			return h.pipelineDisclosure(ctx, pipelineObserver(i, old), old.Manager.Session, old)
		}
		if e = store.DeliverNotification(ctx, n, check); e != nil {
			reason := "visual_delivery_unavailable"
			if n.Destination != old.Manager {
				reason = "visual_destination_requires_owner_scope"
			}
			_ = store.NotificationFailed(ctx, n, reason)
		}
	}
}
