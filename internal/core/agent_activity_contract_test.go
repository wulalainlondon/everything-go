package core

import (
	"encoding/json"
	activitycontract "everything-go/contracts/agent-activity/v2"
	taskcontract "everything-go/contracts/task-api/v1"
	"everything-go/internal/activitywire"
	"testing"
)

func TestActivityGeneratedGoldenContract(t *testing.T) {
	var golden struct {
		Snapshot json.RawMessage
		Report   json.RawMessage
	}
	if e := json.Unmarshal(activitycontract.Golden, &golden); e != nil {
		t.Fatal(e)
	}
	for _, raw := range []json.RawMessage{golden.Snapshot, golden.Report} {
		if e := taskcontract.Validate2020(activitycontract.Schema, raw); e != nil {
			t.Fatal(e)
		}
	}
	var snapshot activitywire.Snapshot
	if json.Unmarshal(golden.Snapshot, &snapshot) != nil {
		t.Fatal("generated snapshot")
	}
	raw, _ := json.Marshal(snapshot)
	if e := taskcontract.Validate2020(activitycontract.Schema, raw); e != nil {
		t.Fatal(e)
	}
	var report activitywire.Report
	if json.Unmarshal(golden.Report, &report) != nil {
		t.Fatal("generated report")
	}
	raw, _ = json.Marshal(report)
	if e := taskcontract.Validate2020(activitycontract.Schema, raw); e != nil {
		t.Fatal(e)
	}
	var v map[string]any
	json.Unmarshal(raw, &v)
	v["canonical_qa"] = "PASS"
	raw, _ = json.Marshal(v)
	if taskcontract.Validate2020(activitycontract.Schema, raw) == nil {
		t.Fatal("textual QA promotion admitted")
	}
}
