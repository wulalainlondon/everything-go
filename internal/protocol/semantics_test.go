package protocol

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestProtocolSemanticManifest(t *testing.T) {
	data, err := os.ReadFile("../../../contracts/v3/protocol.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Definitions struct {
			RuntimePhases struct {
				Values []string `json:"const"`
			} `json:"runtimePhases"`
			RuntimeStages struct {
				Values []string `json:"const"`
			} `json:"runtimeStages"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(manifest.Definitions.RuntimePhases.Values, RuntimePhases) || !reflect.DeepEqual(manifest.Definitions.RuntimeStages.Values, RuntimeStages) {
		t.Fatalf("generated runtime semantics drifted: %+v", manifest.Definitions)
	}
	for _, domain := range []string{"fcm", "runtime", "work", "external_event", "attachment"} {
		if PayloadVersions[domain] != 3 {
			t.Fatalf("missing payload version for %s", domain)
		}
	}
}
