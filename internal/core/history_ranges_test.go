package core

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"fmt"
	"testing"
)

type historyRangeFixtureProvider struct {
	byID       map[string][]map[string]any
	generation map[string]string
	afterLoad  func()
}

func (p *historyRangeFixtureProvider) LoadHistory(id string, opts history.Opts) (*history.Result, error) {
	r := history.Slice(p.byID[id], opts)
	r.SourceGeneration = p.generation[id]
	if p.afterLoad != nil {
		p.afterLoad()
	}
	return r, nil
}
func (p *historyRangeFixtureProvider) ResumableSessions(int) ([]history.ResumableSession, error) {
	return nil, nil
}
func (p *historyRangeFixtureProvider) HistorySourceGeneration(id string) (string, error) {
	return p.generation[id], nil
}

type historyRangeFixtureExec struct {
	fakeExec
	provider *historyRangeFixtureProvider
}

func (e *historyRangeFixtureExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.provider, true
}
func (e *historyRangeFixtureExec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.provider}
}
func scopedHistoryRow(thread string, line int) map[string]any {
	return map[string]any{"role": "user", "content": fmt.Sprint("same text ", line), "source": "codex", "source_session_id": thread, "source_message_id": fmt.Sprintf("codex:%s:line:%d", thread, line)}
}
func TestHistoryRangesWalkArchivedPhysicalThreadsWithoutGaps(t *testing.T) {
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"old": {scopedHistoryRow("old", 1), scopedHistoryRow("old", 2)}, "new": {scopedHistoryRow("new", 1), scopedHistoryRow("new", 2)}}, generation: map[string]string{"old": "g-old", "new": "g-new"}}
	initial, e := loadScopedLogicalHistory(p, []string{"old", "new"}, history.Opts{Scoped: true, Mode: "snapshot", Limit: 3})
	if e != nil {
		t.Fatal(e)
	}
	if len(initial.Messages) != 3 || initial.Messages[0]["source_message_id"] != "codex:old:line:2" || !initial.HasMoreBefore {
		t.Fatalf("initial scope %+v", initial)
	}
	before, e := loadScopedLogicalHistory(p, []string{"old", "new"}, history.Opts{Scoped: true, Before: "codex:new:line:1", Limit: 3})
	if e != nil {
		t.Fatal(e)
	}
	if len(before.Messages) != 2 || before.HasMoreBefore || !before.KnownIDFound {
		t.Fatalf("lost old physical thread %+v", before)
	}
	first, e := loadScopedLogicalHistory(p, []string{"old", "new"}, history.Opts{Scoped: true, Mode: "continuation", KnownLast: "codex:old:line:2", Limit: 2})
	if e != nil {
		t.Fatal(e)
	}
	if len(first.Messages) != 2 || first.Messages[1]["source_message_id"] != "codex:new:line:1" || !first.HasMoreAfter {
		t.Fatalf("cross-thread continuation %+v", first)
	}
	second, e := loadScopedLogicalHistory(p, []string{"old", "new"}, history.Opts{Scoped: true, Mode: "continuation", KnownLast: "codex:new:line:1", Limit: 2})
	if e != nil {
		t.Fatal(e)
	}
	if second.HasMoreAfter || len(second.Messages) != 2 {
		t.Fatalf("did not end at actual tail %+v", second)
	}
	missing, e := loadScopedLogicalHistory(p, []string{"old", "new"}, history.Opts{Scoped: true, Before: "codex:foreign:line:1", Limit: 3})
	if e != nil {
		t.Fatal(e)
	}
	if missing.KnownIDFound || len(missing.Messages) != 0 {
		t.Fatal("unknown cursor must not return newest tail")
	}
}
func TestHistoryRangeWireCorrelatesOriginalReadAndRejectsChangedSource(t *testing.T) {
	h, _ := newTestHub(t)
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"physical": {scopedHistoryRow("physical", 1), scopedHistoryRow("physical", 2), scopedHistoryRow("physical", 3)}}, generation: map[string]string{"physical": "file-original"}}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	s := h.registry.Create("range-session", "Synthetic", "/tmp", "codex", "", "read-only", "")
	s.SetResumeID("physical")
	c := newDeviceClient(h, "range-fixture", 16)
	h.registerLatest(c)
	cmd := clientproto.NewAppV1().ParseCommand(protocol.Inbound{Type: "request_history", SessionID: s.ID, Mode: "snapshot", Limit: 2, HistoryContinuity: true, HistoryRequestID: "fixture-read-1"})
	h.sendHistory(c, s, cmd)
	frame := accessWire(t, c)
	proof, ok := frame["history_continuity"].(map[string]any)
	if !ok || proof["request_id"] != "fixture-read-1" || proof["native_thread_id"] != "physical" || proof["first_source_message_id"] != "codex:physical:line:2" {
		t.Fatalf("wire proof missing exact read: %v", frame)
	}
	raw, e := marshalEvent(frame, h.cfg.InstanceID)
	if e != nil {
		t.Fatal(e)
	}
	var projected map[string]any
	if e = json.Unmarshal(raw, &projected); e != nil {
		t.Fatal(e)
	}
	first := projected["messages"].([]any)[0].(map[string]any)
	if first["native_source_thread_id"] != "physical" || first["source_session_id"] == "physical" {
		t.Fatalf("native physical ID must survive negotiated SessionKey projection: %v", first)
	}
	cmd.HistoryContinuity = false
	cmd.HistoryRequestID = ""
	h.sendHistory(c, s, cmd)
	legacy := accessWire(t, c)
	if legacy["history_continuity"] != nil {
		t.Fatal("old clients must not receive opt-in proof")
	}
	// Rebinding during the actual source read cannot turn old rows into new scope.
	cmd.HistoryContinuity = true
	cmd.HistoryRequestID = "fixture-read-rebind"
	cmd.Before = "codex:physical:line:3"
	p.afterLoad = func() { s.SetResumeID("other") }
	h.sendHistory(c, s, cmd)
	rejected := accessWire(t, c)
	if rejected["type"] != "error" || rejected["request_id"] != cmd.HistoryRequestID {
		t.Fatal("scope refusal must correlate without fake history", rejected)
	}
}
