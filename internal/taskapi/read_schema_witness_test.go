package taskapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	contract "everything-go/contracts/task-api/v1"
	"fmt"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
	"log"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestClosedSchemaWitnessActualMediaValidatorAndService(t *testing.T) {
	raw, err := os.ReadFile("testdata/derived-controller-invalid-response.json")
	if err != nil {
		t.Fatal(err)
	}
	validator, err := contract.NewMedia()
	if err != nil {
		t.Fatal(err)
	}
	original := validator.Validate(raw, "Response")
	if original == nil {
		t.Fatal("original real projected controller defect must remain an invalid fixture")
	}
	expected := []closedSchemaWitness{{"schema_unknown", "other", -1, -1}, {"result.items[].source.binding_kind", "enum", 0, -1}, {"result.items[].receipt_ids[]", "minLength", 0, 0}}
	if got := closedReadSchemaWitness(original); !sameClosedWitnessFacts(got, expected) {
		t.Fatal(got)
	}
	var envelope map[string]any
	if json.Unmarshal(raw, &envelope) != nil {
		t.Fatal("invalid original raw fixture")
	}
	// The real Service re-marshals and runs the original strict media validator.
	// Its diagnostic must consume that actual typed error, not a supplied fake one.
	for _, operation := range []string{"list", "snapshot"} {
		t.Run(operation, func(t *testing.T) {
			var output bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&output)
			defer log.SetOutput(previous)
			service, err := NewService(fixturePolicy{path: "ordinary"}, diagnosticReadFixture{result: envelope["result"]})
			if err != nil {
				t.Fatal(err)
			}
			_, verifier := fixtureCaller(t, "closed-schema-service")
			verifier.context.TaskSchemaHash = contract.MediaHash()
			caller, err := BindCaller(context.Background(), verifier, Invocation{Provider: "fixture", ProcessGeneration: "closed-schema-service"})
			if err != nil {
				t.Fatal(err)
			}
			input := map[string]any{"session_id": "sensitive-never-log-session", "limit": 100}
			if operation == "snapshot" {
				input["views"] = []string{"self"}
			}
			request, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "operation": operation, "correlation_id": "sensitive-never-log-correlation", "input": input})
			response := service.Execute(context.Background(), caller, request)
			if response.OK || response.Error == nil || response.Error.Code != "unknown_acceptance" || response.Error.Acceptance != "unknown" || response.Error.Action != "lookup_original" {
				t.Fatal("closed error contract changed", response)
			}
			line := output.String()
			marker := " schema_witness="
			offset := strings.Index(line, marker)
			if offset < 0 {
				t.Fatal("real validation diagnostic lacks witness")
			}
			var got []closedSchemaWitness
			if json.Unmarshal([]byte(strings.TrimSpace(line[offset+len(marker):])), &got) != nil || !sameClosedWitnessFacts(got, expected) {
				t.Fatal("not original two bounded typed failures", got)
			}
			if len(line) > 1024 || strings.Count(line, "task_api_read_diagnostic ") != 1 {
				t.Fatal("unbounded diagnostic")
			}
			for _, secret := range []string{"controller_origin", "scd_synthetic_original", "Synthetic original controller command", "sensitive-never-log", "r_synthetic_parent", verifier.context.StableScopeID} {
				if secret != "" && strings.Contains(line, secret) {
					t.Fatal("private value escaped")
				}
			}
		})
	}
}

func TestClosedSchemaWitnessUnknownDynamicKeywordAndValueAreClosed(t *testing.T) {
	const secret = "private-password-caller-title-dynamic-id"
	cases := []error{
		errors.New(secret),
		&jsonschema.ValidationError{InstanceLocation: []string{"result", "items", "0", secret}, ErrorKind: &kind.AdditionalProperties{Properties: []string{secret}}},
		&jsonschema.ValidationError{InstanceLocation: []string{"result", "items", "0", "source", "binding_kind"}, ErrorKind: &kind.Enum{Got: secret, Want: []any{secret}}},
		&jsonschema.ValidationError{InstanceLocation: []string{"result", "items", secret, "source", "binding_kind"}, ErrorKind: &kind.Enum{Got: secret}},
		&jsonschema.ValidationError{InstanceLocation: []string{"result", "items", "0", "goal"}, ErrorKind: &kind.AllOf{}},
	}
	for i, err := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			got := closedReadSchemaWitness(err)
			raw, _ := json.Marshal(got)
			if strings.Contains(string(raw), secret) {
				t.Fatal("formatted private error data")
			}
			if i != 2 && !reflect.DeepEqual(got, []closedSchemaWitness{{"schema_unknown", "other", -1, -1}}) {
				t.Fatal("unknown path/keyword leaked", got)
			}
		})
	}
	for _, position := range []string{"-1", "100", "00", "+1", "secret-index", strings.Repeat("9", 1000)} {
		got := closedReadSchemaWitness(&jsonschema.ValidationError{InstanceLocation: []string{"result", "items", position, "goal"}, ErrorKind: &kind.Enum{Got: secret}})
		if got[0].Row != -1 || got[0].Path != "schema_unknown" {
			t.Fatal("noncanonical/out-of-range index exposed", position)
		}
	}
}
func TestClosedSchemaWitnessBoundedTraversalDedupAndMaximumFacts(t *testing.T) {
	leaf := func(row string) *jsonschema.ValidationError {
		return &jsonschema.ValidationError{InstanceLocation: []string{"result", "items", row, "goal"}, ErrorKind: &kind.Enum{Got: "never-export"}}
	}
	root := &jsonschema.ValidationError{}
	for i := 0; i < 12000; i++ {
		root.Causes = append(root.Causes, leaf("0"))
	}
	// Beyond the bounded traversal, row99 must not be visited; duplicate failures
	// cannot make the collector traverse an unbounded list to find another fact.
	root.Causes = append(root.Causes, leaf("99"))
	got := closedReadSchemaWitness(root)
	if len(got) != 1 || got[0].Row != 0 {
		t.Fatal("traversal was not bounded", got)
	}
	root = &jsonschema.ValidationError{}
	for i := 0; i < 100; i++ {
		root.Causes = append(root.Causes, leaf(fmt.Sprint(i)))
	}
	if got = closedReadSchemaWitness(root); len(got) != 4 || got[3].Row != 3 {
		t.Fatal("more than four/unstable witness facts", got)
	}
	// A pathological cyclic typed error cannot recurse forever or leak its value.
	root = &jsonschema.ValidationError{}
	root.Causes = []*jsonschema.ValidationError{root}
	if got = closedReadSchemaWitness(root); len(got) != 1 || got[0].Path != "schema_unknown" {
		t.Fatal(got)
	}
}

func sameClosedWitnessFacts(actual, expected []closedSchemaWitness) bool {
	if len(actual) != len(expected) {
		return false
	}
	seen := map[closedSchemaWitness]bool{}
	for _, fact := range actual {
		if seen[fact] {
			return false
		}
		seen[fact] = true
	}
	for _, fact := range expected {
		if !seen[fact] {
			return false
		}
	}
	return true
}
