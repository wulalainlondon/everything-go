package core

import (
	"encoding/json"
	activitycontract "everything-go/contracts/agent-activity/v2"
	taskcontract "everything-go/contracts/task-api/v1"
	"everything-go/internal/activitywire"
	"everything-go/internal/backend"
	"fmt"
	"os"
	"testing"
)

func validateActivityRuntime(t *testing.T, v map[string]any) {
	t.Helper()
	raw, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	if len(raw) > 128<<10 {
		t.Fatalf("unreadable runtime envelope %d", len(raw))
	}
	if e = taskcontract.Validate2020(activitycontract.Schema, raw); e != nil {
		t.Fatalf("actual runtime %s schema: %v", v["type"], e)
	}
}
func TestActivityRuntime1000EventsMetadataTimelineAndReport(t *testing.T) {
	h, _ := newTestHub(t)
	p := &activityFixtureProvider{epoch: "boot:1:1", events: 1000}
	h.exec = &activityFixtureExec{p: p}
	s := h.registry.Create("activity-root", "fixture", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("original-root")
	c := sharedReadClient(t, h, "reader")
	route(h, c, `{"type":"request_agent_activity","session_id":"activity-root","request_id":"act_runtime","agent_activity":{"view":"snapshot"}}`)
	first := waitForType(t, c, "agent_activity_snapshot")
	token := first["snapshot_token"].(string)
	pages := []map[string]any{}
	page := first
	events := 0
	for {
		validateActivityRuntime(t, page)
		pages = append(pages, page)
		for _, a := range page["actors"].([]any) {
			events += len(a.(map[string]any)["activities"].([]any))
		}
		if page["next_offset"] == nil {
			break
		}
		next := int(page["next_offset"].(float64))
		route(h, c, fmt.Sprintf(`{"type":"request_agent_activity","session_id":"activity-root","request_id":"act_page%d","agent_activity":{"view":"page","snapshot_token":%q,"offset":%d}}`, next, token, next))
		page = waitForType(t, c, "agent_activity_snapshot")
		if page["revision"] != first["revision"] || page["native_source_revision"] != first["native_source_revision"] || page["snapshot_token"] != token {
			t.Fatal("mixed immutable cut")
		}
	}
	if events != 1000 || len(pages) < 10 || p.calls != 1 {
		t.Fatal(events, len(pages), p.calls)
	}
	timelines := []map[string]any{}
	offset, previous := 0, -1
	seen := map[string]bool{}
	for {
		route(h, c, fmt.Sprintf(`{"type":"request_agent_activity","session_id":"activity-root","request_id":"act_timeline%d","agent_activity":{"view":"timeline","snapshot_token":%q,"actor_id":"0","activity_offset":%d}}`, offset, token, offset))
		v := waitForType(t, c, "agent_activity_timeline")
		validateActivityRuntime(t, v)
		timelines = append(timelines, v)
		es := v["events"].([]any)
		if len(es) > 100 {
			t.Fatal("DOM page bound")
		}
		for _, e := range es {
			id := e.(map[string]any)["id"].(string)
			if seen[id] {
				t.Fatal("replayed cursor")
			}
			seen[id] = true
		}
		if previous < 0 {
			if v["previous_offset"] != nil {
				t.Fatal(v)
			}
		} else if int(v["previous_offset"].(float64)) != previous {
			t.Fatal("wrong backward cursor")
		}
		if v["next_offset"] == nil {
			break
		}
		previous, offset = offset, int(v["next_offset"].(float64))
	}
	if len(seen) != 1000 {
		t.Fatal("incomplete timeline", len(seen))
	}
	route(h, c, fmt.Sprintf(`{"type":"request_agent_activity","session_id":"activity-root","request_id":"act_report","agent_activity":{"view":"report","snapshot_token":%q,"actor_id":"0","turn_id":"native-round"}}`, token))
	report := waitForType(t, c, "agent_activity_report")
	validateActivityRuntime(t, report)
	route(h, c, `{"type":"request_agent_activity","session_id":"activity-root","request_id":"act_failure","agent_activity":{"view":"invalid"}}`)
	failure := waitForType(t, c, "error")
	validateActivityRuntime(t, failure)
	if failure["request_id"] != "act_failure" || failure["command_type"] != "request_agent_activity" || failure["code"] != "activity_invalid_query" {
		t.Fatal(failure)
	}
	// Publish only deterministic engineering fixture data, never provider/user data.
	if path := os.Getenv("ACTIVITY_RUNTIME_FIXTURE"); path != "" {
		raw, _ := json.MarshalIndent(map[string]any{"engineeringFixture": true, "pages": pages, "timelines": timelines, "report": report, "error": failure}, "", "  ")
		if e := os.WriteFile(path, raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
}
func TestActivityTimelineByteBoundAndOriginalCursors(t *testing.T) {
	a := activitywire.Actor{ID: "a", Activities: []activitywire.Activity{}}
	x := ""
	for i := 0; i < 256; i++ {
		x += "<"
	}
	for i := 0; i < 1000; i++ {
		a.Activities = append(a.Activities, activitywire.Activity{ID: fmt.Sprint(i) + x, ParentTurnID: x, Kind: "interacted", CreationModel: &x})
	}
	p := activityPin{snapshot: activitywire.Snapshot{SnapshotToken: "fixture"}}
	out, ok := activityTimeline(p, a, 0, "act_t")
	if !ok || len(out.Events) >= 100 || out.NextOffset == nil {
		t.Fatal("byte paging absent")
	}
	next, ok := activityTimeline(p, a, *out.NextOffset, "act_t")
	if !ok || next.PreviousOffset == nil || *next.PreviousOffset != 0 {
		t.Fatal("backward cursor missing")
	}
	raw, _ := json.Marshal(next)
	if len(raw) > activityPageBytes {
		t.Fatal(len(raw))
	}
	if _, ok = activityTimeline(p, a, 1, "act_t"); ok {
		t.Fatal("non-boundary cursor admitted")
	}
}
