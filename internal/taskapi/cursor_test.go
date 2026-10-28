package taskapi

import (
	"context"
	"encoding/json"
	taskcontract "everything-go/contracts/task-api/v1"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// In-memory frozen rows here are a test double only. Production owner must
// implement Capture/Page through its original canonical store API.
type fixtureSnapshot struct {
	latest  map[string]string
	frozen  map[string][]SnapshotRow
	seq     uint64
	lost    bool
	changes []uint64
}

func (f *fixtureSnapshot) Capture(_ context.Context, _ ReadScope) (Freeze, error) {
	keys := []string{}
	for k := range f.latest {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rows := []SnapshotRow{}
	for _, k := range keys {
		rows = append(rows, SnapshotRow{k, f.latest[k]})
	}
	if f.frozen == nil {
		f.frozen = map[string][]SnapshotRow{}
	}
	f.frozen["fixture-freeze"] = rows
	return Freeze{"fixture-freeze", 10000, []Watermark{{"fixture-store", f.seq}}}, nil
}
func (f *fixtureSnapshot) Page(_ context.Context, _ ReadScope, freeze Freeze, after string, limit int) ([]SnapshotRow, bool, error) {
	if f.lost {
		return nil, false, Failure("cursor_expired", "known_none", "refresh_snapshot")
	}
	rows, ok := f.frozen[freeze.ID]
	if !ok {
		return nil, false, Failure("cursor_expired", "known_none", "refresh_snapshot")
	}
	out := []SnapshotRow{}
	for _, r := range rows {
		if r.Key > after {
			out = append(out, r)
		}
	}
	more := len(out) > limit
	if more {
		out = out[:limit]
	}
	return out, more, nil
}
func (f *fixtureSnapshot) change(key, value string) {
	if value == "" {
		delete(f.latest, key)
	} else {
		f.latest[key] = value
	}
	f.seq++
	f.changes = append(f.changes, f.seq)
}
func TestSnapshotPagingFrozenMembershipAndWatermark(t *testing.T) {
	clock := time.UnixMilli(1)
	codec, _ := NewCursorCodec(make([]byte, 32), func() time.Time { return clock })
	source := &fixtureSnapshot{latest: map[string]string{"b": "b-v1", "c": "c-v1", "d": "d-v1"}, seq: 10}
	engine := SnapshotEngine{codec, source}
	scope := ReadScope{"authority", "scope", "filter", 7}
	first, err := engine.Page(context.Background(), scope, []string{"self"}, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	source.change("a", "new-before-first")
	source.change("c", "c-v2")
	source.change("d", "")
	second, err := engine.Page(context.Background(), scope, []string{"self"}, first.NextPageCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	again, err := engine.Page(context.Background(), scope, []string{"self"}, first.NextPageCursor, 1)
	if err != nil || !reflect.DeepEqual(second, again) {
		t.Fatal("page replay drift", err)
	}
	third, err := engine.Page(context.Background(), scope, []string{"self"}, second.NextPageCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.Items[0] != "b-v1" || second.Items[0] != "c-v1" || third.Items[0] != "d-v1" || third.HasMore {
		t.Fatal(first, second, third)
	}
	for _, page := range []SnapshotPage{first, second, third} {
		marks, err := codec.EventWatermarks(page.EventsCursor, scope)
		if err != nil || marks[0].Sequence != 10 {
			t.Fatal("watermark advanced across pages", marks, err)
		}
		events := []uint64{}
		for _, seq := range source.changes {
			if seq > marks[0].Sequence {
				events = append(events, seq)
			}
		}
		if !reflect.DeepEqual(events, []uint64{11, 12, 13}) {
			t.Fatal("mutation lost", events)
		}
	}
}
func TestCursorScopeTamperExpiryRestart(t *testing.T) {
	clock := time.UnixMilli(1)
	codec, _ := NewCursorCodec(make([]byte, 32), func() time.Time { return clock })
	source := &fixtureSnapshot{latest: map[string]string{"a": "a", "b": "b"}}
	e := SnapshotEngine{codec, source}
	scope := ReadScope{"authority", "scope", "filter", 7}
	first, _ := e.Page(context.Background(), scope, []string{"self"}, "", 1)
	if _, err := e.Page(context.Background(), scope, []string{"results"}, first.NextPageCursor, 1); err == nil {
		t.Fatal("changed views accepted under same scope")
	}
	for _, wrong := range []ReadScope{{"other", "scope", "filter", 7}, {"authority", "other", "filter", 7}, {"authority", "scope", "changed", 7}, {"authority", "scope", "filter", 8}} {
		if _, err := e.Page(context.Background(), wrong, []string{"self"}, first.NextPageCursor, 1); err == nil {
			t.Fatal("accepted changed scope")
		}
	}
	if _, err := e.Page(context.Background(), scope, []string{"self"}, "x"+first.NextPageCursor, 1); err == nil {
		t.Fatal("accepted tamper")
	}
	source.lost = true
	if _, err := e.Page(context.Background(), scope, []string{"self"}, first.NextPageCursor, 1); err == nil {
		t.Fatal("lost freeze silently replaced")
	}
	source.lost = false
	clock = time.UnixMilli(10001)
	if _, err := e.Page(context.Background(), scope, []string{"self"}, first.NextPageCursor, 1); err == nil {
		t.Fatal("accepted expired cursor")
	}
}
func TestRealSizeCursorFullContractRoundtrip(t *testing.T) {
	codec, _ := NewCursorCodec(make([]byte, 32), func() time.Time { return time.UnixMilli(1) })
	scope := ReadScope{strings.Repeat("a", 128), strings.Repeat("s", 128), strings.Repeat("f", 64), 7}
	marks := []Watermark{}
	for _, store := range []string{"ordinary-store", "controller-store", "delegation-store", "pm-v2-store"} {
		marks = append(marks, Watermark{store, 123456})
	}
	p := cursorPayload{Kind: "snapshot", Schema: taskcontract.Hash(), Scope: scope, Freeze: Freeze{"freeze-fixture", 10000, marks}, Views: []string{"self"}, After: "task-a"}
	token, err := codec.encode(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(token) <= 512 {
		t.Fatal("fixture not realistic-sized", len(token))
	}
	contract, _ := taskcontract.New()
	raw, _ := json.Marshal(token)
	if err = contract.Validate(raw, "Cursor"); err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.decode(token, scope, "snapshot")
	if err != nil || !reflect.DeepEqual(decoded, p) {
		t.Fatal(decoded, err)
	}
	request := map[string]any{"type": "task_api_request", "api_version": taskcontract.Version, "correlation_id": "fixture", "operation": "snapshot", "input": map[string]any{"session_id": "session-fixture", "views": []string{"self"}, "cursor": token, "limit": 1}}
	raw, _ = json.Marshal(request)
	if err = contract.Validate(raw, "Request"); err != nil {
		t.Fatal(err)
	}
	page := goldenResult("snapshot")
	page["next_page_cursor"] = token
	page["events_cursor"] = token
	page["watermarks"] = marks
	raw, _ = json.Marshal(page)
	if err = contract.Validate(raw, "Snapshot"); err != nil {
		t.Fatal(err)
	}
	p.After = strings.Repeat("x", 16000)
	if _, err = codec.encode(p); err == nil {
		t.Fatal("oversized cursor encoded")
	}
}

func TestNF01SourceChildrenCoverageIsFrozenKnownSubsetNotGlobalZero(t *testing.T) {
	codec, _ := NewCursorCodec(make([]byte, 32), func() time.Time { return time.UnixMilli(1) })
	source := &fixtureSnapshot{latest: map[string]string{}, seq: 10}
	engine := SnapshotEngine{codec, source}
	scope := ReadScope{"authority", "native-worker-scope", "filter", 7}
	page, e := engine.Page(context.Background(), scope, []string{"source_children"}, "", 10)
	if e != nil {
		t.Fatal(e)
	}
	if len(page.Items) != 0 || !page.Partial || page.SourceRelationCoverage != "trusted_relations_only" {
		t.Fatal("scoped empty became global none", page)
	}
	self, e := engine.Page(context.Background(), scope, []string{"self"}, "", 10)
	if e != nil || self.SourceRelationCoverage != "not_requested" {
		t.Fatal(self, e)
	}
}

func TestSnapshotByteBoundKeepsOriginalRowsCursorAndFrozenVector(t *testing.T) {
	codec, _ := NewCursorCodec(make([]byte, 32), func() time.Time { return time.UnixMilli(1) })
	source := &fixtureSnapshot{latest: map[string]string{}, seq: 10}
	for i := 0; i < 80; i++ {
		source.latest[fmt.Sprintf("task-%03d", i)] = fmt.Sprintf("original-%03d:", i) + strings.Repeat("繁<", 2000)
	}
	engine := SnapshotEngine{codec, source}
	scope := ReadScope{"authority", "paired-scope", "filter", 7}
	page, err := engine.Page(context.Background(), scope, []string{"self", "source_children"}, "", 100)
	if err != nil || len(page.Items) == 0 || len(page.Items) >= 80 || !page.HasMore {
		t.Fatal("row count was treated as transport byte budget", len(page.Items), err)
	}
	firstEvents := page.EventsCursor
	// Later source mutations must not replace a trimmed frozen suffix.
	source.change("task-079", "changed-latest")
	source.change("task-new", "new-latest")
	seen := map[string]bool{}
	for pages := 0; ; pages++ {
		if pages > 20 {
			t.Fatal("frozen byte paging did not terminate")
		}
		for _, item := range page.Items {
			value := item.(string)
			if !strings.Contains(value, "繁<") || seen[value] {
				t.Fatal("original value truncated, replaced or duplicated")
			}
			seen[value] = true
		}
		op := "snapshot"
		raw, _ := json.Marshal(Response{Type: "task_api_response", Version: taskcontract.Version, CorrelationID: strings.Repeat("&", 512), Operation: &op, OK: true, Result: page})
		if len(raw) > taskcontract.MaxBytes || page.EventsCursor != firstEvents || !page.Partial {
			t.Fatal("wire bound, coverage or original event vector drift", len(raw))
		}
		if !page.HasMore {
			break
		}
		next, err := engine.Page(context.Background(), scope, []string{"self", "source_children"}, page.NextPageCursor, 100)
		again, replayErr := engine.Page(context.Background(), scope, []string{"self", "source_children"}, page.NextPageCursor, 100)
		if err != nil || replayErr != nil || !reflect.DeepEqual(next, again) {
			t.Fatal("trimmed cursor replay changed", err, replayErr)
		}
		wrong := scope
		wrong.NamespaceGeneration++
		if _, err := engine.Page(context.Background(), wrong, []string{"self", "source_children"}, page.NextPageCursor, 100); err == nil {
			t.Fatal("trimmed cursor bypassed original scope")
		}
		page = next
	}
	if len(seen) != 80 {
		t.Fatal("frozen membership silently lost", len(seen))
	}
}

func TestSnapshotOversizedSingleRowCannotBecomeEmptyOrSkip(t *testing.T) {
	for _, later := range []bool{false, true} {
		t.Run(fmt.Sprint(later), func(t *testing.T) {
			codec, _ := NewCursorCodec(make([]byte, 32), func() time.Time { return time.UnixMilli(1) })
			source := &fixtureSnapshot{latest: map[string]string{"z-large": strings.Repeat("x", taskcontract.MaxBytes)}, seq: 1}
			if later {
				source.latest["a-small"] = "original-small"
			}
			engine := SnapshotEngine{codec, source}
			scope := ReadScope{"authority", "paired-scope", "filter", 7}
			page, err := engine.Page(context.Background(), scope, []string{"self"}, "", 100)
			if later {
				if err != nil || len(page.Items) != 1 || page.Items[0] != "original-small" || !page.HasMore {
					t.Fatal("oversized suffix skipped or poisoned first bounded row", page, err)
				}
				page, err = engine.Page(context.Background(), scope, []string{"self"}, page.NextPageCursor, 100)
			}
			failure, ok := err.(*APIError)
			if !ok || failure.Code != "unsupported" || failure.Acceptance != "known_none" || len(page.Items) != 0 || page.HasMore {
				t.Fatal("single oversized row became complete empty result", page, err)
			}
			if len(source.frozen["fixture-freeze"]) != len(source.latest) {
				t.Fatal("oversized original was removed")
			}
		})
	}
}
