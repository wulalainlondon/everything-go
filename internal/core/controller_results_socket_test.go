package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/controller-results-v9"
	base "everything-go/contracts/task-api/v1"
	cr "everything-go/internal/controllerresults"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func controllerResultsWaitRaw(t *testing.T, c *Client, kind string) []byte {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case raw := <-c.send:
			var v map[string]any
			if json.Unmarshal(raw, &v) == nil && v["type"] == kind {
				return raw
			}
		case <-deadline:
			t.Fatal("exact WS event missing", kind)
		}
	}
}
func controllerResultValidateEvent(t *testing.T, raw []byte, definition string) {
	t.Helper()
	var schema map[string]any
	json.Unmarshal(contract.Schema, &schema)
	schema["$ref"] = "#/$defs/" + definition
	b, _ := json.Marshal(schema)
	if e := base.Validate2020(b, raw); e != nil {
		t.Fatal(e, string(raw))
	}
}
func TestControllerResultsActualProtocol3SocketClosedResponseAndEvents(t *testing.T) {
	h, _, parent, target := unifiedFixtureSessions(t)
	fe := h.exec.(*scopeFixtureExecutor).fakeExec
	useNativeCorrelationFixture(h, fe, target.ResumeID(), "socket-original", "native-socket-original")
	seedControllerResult(t, h, parent, target, "socket-primary", "socket-original", false)
	h.reconcileSessionDispatches(context.Background())
	c := sharedReadClient(t, h, "socket-original-paired")
	c.protocolVersion = 3
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	defer close(c.quit)
	manager := cr.Identity{h.cfg.InstanceID, parent.ID, parent.ResumeID(), 0}
	request := mustPipelineJSON(resultsRequest("read_dispatch_inbox", "", map[string]any{"manager": manager, "cursor": "", "limit": 20}))
	if e := c.dispatchFrame(context.Background(), request); e != nil {
		t.Fatal(e)
	}
	response := controllerResultsWaitRaw(t, c, "task_api_controller_results_response")
	r := resultsDecode(t, response)
	if !r.OK {
		t.Fatal(r.Error)
	}
	second := seedControllerResult(t, h, parent, target, "socket-second", "socket-second", false)
	scope, ok := h.observeControllerResult(context.Background(), second)
	if !ok {
		t.Fatal("observation")
	}
	h.publishControllerResultInvalidation(context.Background(), scope)
	invalidation := controllerResultsWaitRaw(t, c, "controller_results_invalidated")
	controllerResultValidateEvent(t, invalidation, "Invalidation")
	hidden := true
	target.SetMeta(nil, &hidden)
	h.publishControllerResultInvalidation(context.Background(), scope)
	retirement := controllerResultsWaitRaw(t, c, "controller_results_retired")
	controllerResultValidateEvent(t, retirement, "Retirement")
	if directory := os.Getenv("CONTROLLER_RESULTS_SOCKET_OUT"); directory != "" {
		for name, b := range map[string][]byte{"inbox.request.json": request, "inbox.response.json": response, "invalidation.json": invalidation, "retirement.json": retirement} {
			if e := os.WriteFile(filepath.Join(directory, name), b, 0600); e != nil {
				t.Fatal(e)
			}
		}
	}
}
