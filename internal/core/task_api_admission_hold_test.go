//go:build taskapihold

package core

import (
	"context"
	"everything-go/internal/taskapi"
	"testing"
)

func TestTaskAPICompatibleRollbackHoldsOnlyOwnedNewAdmission(t *testing.T) {
	h, _ := newTestHub(t)
	reader := sharedReadClient(t, h, "rollback-fixture")
	bound, err := h.taskHuman(reader)
	if err != nil {
		t.Fatal(err)
	}
	caller, err := bound.Check(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"create_dispatch", "append"} {
		_, err = h.Authorize(context.Background(), caller, taskapi.Request{Operation: op, Input: []byte(`{}`)})
		failure, ok := err.(*taskapi.APIError)
		if !ok || failure.Code != "busy" || failure.Acceptance != "known_none" {
			t.Fatal("held admission did not refuse before effects", op, err)
		}
	}
	caps, err := h.Read(context.Background(), taskapi.AuthorizedCommand{Caller: caller, Request: taskapi.Request{Operation: "capabilities"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range caps.(map[string]any)["allowed_operations"].([]string) {
		if op == "create_dispatch" || op == "append" {
			t.Fatal("hold advertises new admission")
		}
	}
	if len(caps.(map[string]any)["create_routes"].(map[string]string)) != 0 {
		t.Fatal("hold exposes admission route")
	}
}
