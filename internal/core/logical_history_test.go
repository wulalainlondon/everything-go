package core

import (
	"testing"

	"everything-go/internal/history"
)

type logicalHistoryProvider struct {
	byResume map[string][]map[string]any
}

func (p logicalHistoryProvider) LoadHistory(resumeID string, opts history.Opts) (*history.Result, error) {
	return history.Slice(p.byResume[resumeID], opts), nil
}

func (p logicalHistoryProvider) ResumableSessions(int) ([]history.ResumableSession, error) {
	return nil, nil
}

func TestLoadLogicalSessionHistoryMergesArchivedAndActiveGenerations(t *testing.T) {
	provider := logicalHistoryProvider{byResume: map[string][]map[string]any{
		"old": {
			{"role": "user", "source_message_id": "old-u"},
			{"role": "assistant", "source_message_id": "old-a"},
		},
		"new": {
			{"role": "user", "source_message_id": "new-u"},
			{"role": "assistant", "source_message_id": "new-a"},
		},
	}}
	res, err := loadLogicalSessionHistory(provider, []string{"old", "new"}, history.Opts{
		Mode: "auto", KnownLast: "old-a", Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Kind != "delta" || len(res.Messages) != 2 || res.SourceCount != 4 {
		t.Fatalf("unexpected logical history result: %+v", res)
	}
	if res.Messages[0]["source_message_id"] != "new-u" || res.Messages[1]["source_message_id"] != "new-a" {
		t.Fatalf("generation ordering lost: %+v", res.Messages)
	}
}
