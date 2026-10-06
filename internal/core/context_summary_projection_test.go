package core

import (
	"encoding/json"
	"testing"
)

func TestSessionSummaryProjectsReportedContextToExistingClientFields(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("qa-context", "QA", "/qa", "claude", "sonnet", "read-only", "")
	s.SetContext(42000, 1000000)
	rows := h.sessionSummaries()
	data, err := json.Marshal(h.client.SessionsList(rows))
	if err != nil {
		t.Fatal(err)
	}
	var event struct {
		Sessions []map[string]any `json:"sessions"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatal(err)
	}
	if len(event.Sessions) != 1 || event.Sessions[0]["context_used"] != float64(42000) || event.Sessions[0]["context_max"] != float64(1000000) {
		t.Fatal("context did not reach the existing client wire contract")
	}
}
