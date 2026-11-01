package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/activitywire"
	"everything-go/internal/agentactivity"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"fmt"
	"testing"
)

type activityFixtureProvider struct {
	plainProv
	calls  int
	epoch  string
	events int
	mutate func(*session.Session)
}

func (p *activityFixtureProvider) HostedActivityScope(s *session.Session, a string) agentactivity.NativeScope {
	return agentactivity.NativeScope{Authority: a, Session: s.ID, RootThread: s.ResumeID(), Epoch: p.epoch}
}
func (p *activityFixtureProvider) HostedActivitySnapshot(_ context.Context, s *session.Session, a string) (activitywire.Snapshot, error) {
	p.calls++
	out := activitywire.Snapshot{Type: "agent_activity_snapshot", Schema: "agent-activity.v2", Authority: a, SessionID: s.ID, RootThreadID: s.ResumeID(), SourceEpoch: p.epoch, ConfigRevision: int64(s.Snapshot().ConfigRevision), Availability: "live", Coverage: "partial_selected_root_native_activity", ObservedAt: "2026-11-02T00:00:00Z", Actors: []activitywire.Actor{}, Origins: []activitywire.Origin{}, Waits: []activitywire.Wait{}, Recovery: "captured_prefix_partial_inventory", Capabilities: []string{}}
	for i := 0; i < 5; i++ {
		out.Actors = append(out.Actors, activitywire.Actor{ID: fmt.Sprint(i), ThreadID: fmt.Sprint("child", i), Path: fmt.Sprint("/root/child", i), Activities: []activitywire.Activity{}, Rounds: []activitywire.Round{{ID: "native-round", Status: "completed", ParentAssociation: "unresolved"}}})
	}
	for i := 0; i < p.events; i++ {
		out.Actors[0].Activities = append(out.Actors[0].Activities, activitywire.Activity{ID: fmt.Sprint("event", i), ParentTurnID: "parent", Kind: "interacted", AtMS: 1000})
	}
	out.AggregateObservedActors = 5
	if p.mutate != nil {
		p.mutate(s)
	}
	return out, nil
}
func (p *activityFixtureProvider) HostedActivityReport(_ context.Context, s *session.Session, a string, scope agentactivity.NativeScope, child, turn string) (agentactivity.NativePublicReturn, string, error) {
	return agentactivity.NativePublicReturn{Scope: scope, ChildThread: child, NativeTurn: turn, ItemID: "public-item"}, "untrusted <script>not executed</script> PASS is text", nil
}

type activityFixtureExec struct {
	fakeExec
	p *activityFixtureProvider
}

func (e *activityFixtureExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.p, true
}
func (e *activityFixtureExec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.p}
}

func TestActivityQueryStrictBoundedReadOnly(t *testing.T) {
	for _, raw := range []string{`{}`, `{"view":"snapshot","authority":"fake"}`, `{"view":"snapshot","view":"report"}`, `{"view":"snapshot","offset":1}`, `{"view":"page","snapshot_token":"x","offset":64}`, `{"view":"report","snapshot_token":"x","actor_id":"x"}`} {
		if _, ok := decodeActivityQuery([]byte(raw)); ok {
			t.Fatal("accepted", raw)
		}
	}
	for _, raw := range []string{`{"view":"snapshot"}`, `{"view":"page","snapshot_token":"x","offset":1}`, `{"view":"report","snapshot_token":"x","actor_id":"x","turn_id":"real"}`} {
		if _, ok := decodeActivityQuery([]byte(raw)); !ok {
			t.Fatal("rejected", raw)
		}
	}
}
func TestActivityOriginalHubPinnedPagesAndReport(t *testing.T) {
	h, _ := newTestHub(t)
	p := &activityFixtureProvider{epoch: "boot:1:1", events: 100}
	h.exec = &activityFixtureExec{p: p}
	s := h.registry.Create("activity-root", "fixture", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("original-root")
	c := sharedReadClient(t, h, "activity-reader")
	route(h, c, `{"type":"request_agent_activity","session_id":"activity-root","request_id":"act_snapshot","authority":"forged","agent_activity":{"view":"snapshot"}}`)
	first := waitForType(t, c, "agent_activity_snapshot")
	if first["authority"] == "forged" || first["root_thread_id"] != "original-root" || len(first["actors"].([]any)) != 1 || first["inventory_complete"] != false {
		t.Fatal("wrong original projection", first)
	}
	token := first["snapshot_token"].(string)
	route(h, c, fmt.Sprintf(`{"type":"request_agent_activity","session_id":"activity-root","request_id":"act_page","agent_activity":{"view":"page","snapshot_token":%q,"offset":1}}`, token))
	page := waitForType(t, c, "agent_activity_snapshot")
	if page["snapshot_token"] != token || page["revision"] != first["revision"] || len(page["actors"].([]any)) != 4 || p.calls != 1 {
		t.Fatal("page recollected or mixed snapshot", page)
	}
	route(h, c, fmt.Sprintf(`{"type":"request_agent_activity","session_id":"activity-root","request_id":"act_report","agent_activity":{"view":"report","snapshot_token":%q,"actor_id":"0","turn_id":"native-round"}}`, token))
	report := waitForType(t, c, "agent_activity_report")
	if report["availability"] != "available" || report["canonical_qa"] != "unverified" || report["item_id"] != "public-item" {
		t.Fatal("wrong reference", report)
	}
	// Another original client cannot borrow this client's pinned snapshot.
	other := sharedReadClient(t, h, "other-reader")
	route(h, other, fmt.Sprintf(`{"type":"request_agent_activity","session_id":"activity-root","request_id":"act_foreign","agent_activity":{"view":"page","snapshot_token":%q,"offset":1}}`, token))
	waitForType(t, other, "error")
	p.epoch = "boot:2:1"
	route(h, c, fmt.Sprintf(`{"type":"request_agent_activity","session_id":"activity-root","request_id":"act_stale","agent_activity":{"view":"page","snapshot_token":%q,"offset":1}}`, token))
	waitForType(t, c, "error")
}
func TestActivityOriginalHubDropsChangedOwner(t *testing.T) {
	h, _ := newTestHub(t)
	p := &activityFixtureProvider{epoch: "boot:1:1", mutate: func(s *session.Session) { s.SetResumeID("different-root") }}
	h.exec = &activityFixtureExec{p: p}
	s := h.registry.Create("activity-root", "fixture", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("original-root")
	c := sharedReadClient(t, h, "reader")
	route(h, c, `{"type":"request_agent_activity","session_id":"activity-root","request_id":"act_drift","agent_activity":{"view":"snapshot"}}`)
	e := waitForType(t, c, "error")
	b, _ := json.Marshal(e)
	if len(b) == 0 || p.calls != 1 {
		t.Fatal("missing failure")
	}
}
