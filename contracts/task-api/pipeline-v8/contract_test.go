package pipelinecontract

import (
	"encoding/json"
	"testing"
)

func TestClosedPipelineSeparateContract(t *testing.T) {
	raw := []byte(`{"type":"task_api_pipeline_request","api_version":"1.0.0-rc.8","correlation_id":"caps","operation":"capabilities","input":{}}`)
	if e := Validate(raw); e != nil {
		t.Fatal(e)
	}
	var v map[string]any
	json.Unmarshal(raw, &v)
	v["caller"] = "fake"
	bad, _ := json.Marshal(v)
	if Validate(bad) == nil {
		t.Fatal("caller injection")
	}
	v["api_version"] = "1.0.0-rc.6"
	delete(v, "caller")
	bad, _ = json.Marshal(v)
	if Validate(bad) == nil {
		t.Fatal("old contract smuggling")
	}
	if len(Hash()) != 64 {
		t.Fatal(Hash())
	}
}
