package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/sessiondispatch"
	p "everything-go/internal/taskpipeline"
	"testing"
	"time"
)

func pipelineProjectionFixture(t *testing.T, h *Hub, manager p.Identity, principal, kind, key string, grant uint64) p.Record {
	t.Helper()
	scope := p.Scope{Principal: principal, Manager: manager}
	if kind == "paired_human" {
		scope.Generation = jarvisTokenGeneration("jarvis-token")
	}
	source, _ := h.registry.Get(manager.Session)
	issuer := p.Issuer{Scope: scope, Kind: kind, SourceRequest: "synthetic-original-native-request", SourceGeneration: source.Snapshot().SourceRevision, GrantRevision: grant, Targets: map[string]uint64{}}
	r := p.Record{Manager: manager, Kind: "mission", Mission: &p.Mission{Goal: key, Completion: "no missing source inferred", Steps: []p.Step{{ID: "pending", Title: "pending exact source", Required: true, Predicate: p.Predicate{Kind: "success_sealed"}}}, Authorization: p.Authorization{SourceRef: "synthetic original owner instruction", Budget: p.Budget{DeadlineMS: time.Now().Add(time.Hour).UnixMilli(), MaxNoProgress: 1}}}}
	result, e := h.messageQueue.PipelineStore().Mutate(context.Background(), scope, issuer, key, "mission_create", key, "", 0, &r, nil, nil, func(context.Context) error { return nil })
	if e != nil {
		t.Fatal(e)
	}
	return *result.Record
}
func TestPipelineNativeProjectionMixedCursorOriginalScopeAndRevocation(t *testing.T) {
	h, call := jarvisFixture(t)
	var e error
	h.dispatches, e = sessiondispatch.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer h.dispatches.Close()
	s := h.registry.Create("projection-manager", "synthetic", h.cfg.RootDir, "codex", "m", "read-only", "projection-thread")
	defer s.CloseAndWait()
	manager := pipelineIdentity(h.unifiedIdentity(s.ID))
	grant, e := h.dispatches.SetGrant(context.Background(), s.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	if e != nil {
		t.Fatal(e)
	}
	native := pipelineProjectionFixture(t, h, manager, "session:"+s.ID, "native_tool", "native rule", grant.Revision)
	own := pipelineProjectionFixture(t, h, manager, "device:phone", "paired_human", "paired rule", 0)
	foreign := pipelineProjectionFixture(t, h, manager, "device:another-private-device", "paired_human", "not disclosed", 0)
	cursor := ""
	seen := map[string]string{}
	for page := 0; page < 3; page++ {
		r := decodePipeline(t, call("POST", "task-api", pipelineRequest(t, "mission_list", "", map[string]any{"manager": manager, "limit": 1, "cursor": cursor}), "phone").Body.Bytes())
		if !r.OK {
			t.Fatal(r.Error)
		}
		raw, _ := json.Marshal(r.Result)
		var result struct {
			Items    []p.Record `json:"items"`
			Next     string     `json:"next_cursor"`
			Complete bool       `json:"complete"`
		}
		json.Unmarshal(raw, &result)
		for _, row := range result.Items {
			if _, ok := seen[row.ID]; ok {
				t.Fatal("duplicate page", row)
			}
			seen[row.ID] = row.AccessMode
		}
		cursor = result.Next
		if result.Complete {
			break
		}
	}
	if len(seen) != 2 || seen[native.ID] != "read_only_projection" || seen[own.ID] != "owner" || seen[foreign.ID] != "" {
		t.Fatal(seen)
	}
	get := pipelineRequest(t, "mission_get", "", map[string]any{"manager": manager, "id": native.ID})
	response := decodePipeline(t, call("POST", "task-api", get, "phone").Body.Bytes())
	if !response.OK {
		t.Fatal(response.Error)
	}
	raw, _ := json.Marshal(response.Result)
	var projection p.Record
	json.Unmarshal(raw, &projection)
	if projection.AccessMode != "read_only_projection" {
		t.Fatal(projection)
	}
	mutation := pipelineRequest(t, "mission_pause", "r_55555555555555555555555555555555", map[string]any{"manager": manager, "id": native.ID, "expected_revision": native.Revision})
	denied := decodePipeline(t, call("POST", "task-api", mutation, "phone").Body.Bytes())
	if denied.OK || denied.Error.Code != "read_only_projection" || denied.Error.Acceptance != "known_none" {
		t.Fatal(denied)
	}
	wrong := manager
	wrong.Thread += "foreign"
	denied = decodePipeline(t, call("POST", "task-api", pipelineRequest(t, "mission_get", "", map[string]any{"manager": wrong, "id": native.ID}), "phone").Body.Bytes())
	if denied.OK {
		t.Fatal("different thread adopted native rule")
	}
	if _, e = h.dispatches.SetGrant(context.Background(), s.ID, sessiondispatch.Grant{Enabled: false}, grant.Revision); e != nil {
		t.Fatal(e)
	}
	denied = decodePipeline(t, call("POST", "task-api", pipelineRequest(t, "mission_list", "", map[string]any{"manager": manager, "limit": 50, "cursor": ""}), "phone").Body.Bytes())
	if denied.OK || denied.Error.Code != "permission" {
		t.Fatal("revoked native source skipped and complete asserted", denied)
	}
}
