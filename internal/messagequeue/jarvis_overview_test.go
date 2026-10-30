package messagequeue

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestOverviewDurableCASIntentsAssetScopeAndDecisionAtomicity(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	ctx := t.Context()
	scope := "phone\x001"
	asset := &OverviewAsset{ID: "asset", SHA256: strings.Repeat("a", 64), MIME: "image/png", Width: 2, Height: 2, Bytes: []byte("original bytes"), Thumbnail: []byte("thumb bytes"), SourceAttachment: "original_attachment", SourceJSON: json.RawMessage(`{"attachment_id":"original_attachment"}`)}
	out, e := s.MutateOverview(ctx, scope, "collection", "c", "r_overview_original", "overview/collections/mutate", strings.Repeat("b", 64), 0, json.RawMessage(`{"collection_id":"c","pins":[]}`), asset)
	if e != nil || out.Revision != 1 {
		t.Fatal(out, e)
	}
	_, e = s.MutateOverview(ctx, scope, "collection", "c", "r_overview_original", "overview/collections/mutate", strings.Repeat("c", 64), 0, json.RawMessage(`{}`), nil)
	if !errors.Is(e, ErrWorkConflict) {
		t.Fatal(e)
	}
	_, e = s.MutateOverview(ctx, scope, "collection", "c", "r_overview_conflict", "overview/collections/mutate", strings.Repeat("c", 64), 0, json.RawMessage(`{}`), nil)
	if !errors.Is(e, ErrWorkConflict) {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	receipt, e := s.OverviewOperation(ctx, scope, "r_overview_original")
	if e != nil || receipt.InputHash != out.InputHash || receipt.Revision != 1 {
		t.Fatal(receipt, e)
	}
	var c map[string]any
	if e = json.Unmarshal(receipt.Record.Payload, &c); e != nil || c["revision"] != float64(1) {
		t.Fatal(c, e)
	}
	a, e := s.OverviewAsset(ctx, scope, "asset")
	if e != nil || !bytes.Equal(a.Bytes, asset.Bytes) {
		t.Fatal(a, e)
	}
	if _, e = s.OverviewAsset(ctx, "phone\x002", "asset"); e == nil {
		t.Fatal("credential epoch leaked")
	}
	if _, e = s.OverviewOperation(ctx, "phone\x002", "r_overview_original"); e == nil {
		t.Fatal("intent scope leaked")
	}
	item, e := s.MutateWork(ctx, scope, "manager", WorkMutation{IntentID: "r_metadata_original", WorkItem: WorkItem{ID: "note", Kind: "discussion_note", Title: "決定", RecordingState: "recorded"}})
	if e != nil {
		t.Fatal(e)
	}
	decision := WorkDecision{State: "approved", Instruction: "人的原文", SourceRef: "r_human_decision"}
	d, e := s.RecordOverviewDecision(ctx, scope, scope, "manager", item.ID, "r_human_decision", "overview/decision/record", strings.Repeat("d", 64), item.Revision, decision, json.RawMessage(`{}`))
	if e != nil || d.Revision != 2 {
		t.Fatal(d, e)
	}
	same, e := s.RecordOverviewDecision(ctx, scope, scope, "manager", item.ID, "r_human_decision", "overview/decision/record", strings.Repeat("d", 64), 1, decision, json.RawMessage(`{}`))
	if e != nil || same.Revision != 2 {
		t.Fatal(same, e)
	}
	work, e := s.WorkItem(ctx, scope, "manager", item.ID)
	if e != nil || work.Decision.Instruction != "人的原文" || work.Revision != 2 {
		t.Fatal(work, e)
	}
	queue, e := s.Snapshot("manager")
	if e != nil || len(queue.Items) != 0 {
		t.Fatal("decision dispatched", queue, e)
	}
}
