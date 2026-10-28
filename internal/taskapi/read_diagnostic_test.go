package taskapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	taskcontract "everything-go/contracts/task-api/v1"
)

type diagnosticReadFixture struct {
	result any
	err    error
}

func (f diagnosticReadFixture) Read(context.Context, AuthorizedCommand) (any, error) {
	return f.result, f.err
}
func (f diagnosticReadFixture) Mutate(context.Context, AuthorizedCommand) (any, error) {
	return nil, f.err
}

func TestReadDiagnosticsClosedStagesAndNoPrivateContents(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(previous)
	const secret = "private-result-password-session-caller-credential"
	for _, operation := range []string{"list", "snapshot"} {
		for _, row := range []struct {
			name, stage, reason string
			result              any
			err                 error
		}{
			{"driver", "gateway_read", "other", nil, errors.New(secret)},
			{"cancelled", "gateway_read", "context_cancelled", nil, fmt.Errorf("%s: %w", secret, context.Canceled)},
			{"deadline", "gateway_read", "context_deadline", nil, fmt.Errorf("%s: %w", secret, context.DeadlineExceeded)},
			{"marshal", "response_marshal", "other", map[string]any{secret: make(chan int)}, nil},
			{"schema", "response_validate", "schema", map[string]any{"unexpected": secret}, nil},
			{"frame", "response_validate", "frame_bound", map[string]any{"unexpected": strings.Repeat(secret, 10000)}, nil},
		} {
			t.Run(operation+"/"+row.name, func(t *testing.T) {
				output.Reset()
				service, err := NewService(fixturePolicy{path: "ordinary"}, diagnosticReadFixture{row.result, row.err})
				if err != nil {
					t.Fatal(err)
				}
				caller, verifier := fixtureCaller(t, "diagnostic-fixture")
				verifier.context.TaskSchemaHash = taskcontract.MediaHash()
				caller, err = BindCaller(context.Background(), verifier, Invocation{Provider: "fixture", ProcessGeneration: "diagnostic-fixture"})
				if err != nil {
					t.Fatal(err)
				}
				correlation := "sensitive-original-correlation-" + row.name
				request, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": taskcontract.Version, "operation": operation, "correlation_id": correlation, "input": map[string]any{"session_id": "synthetic-session", "views": []string{"self"}, "limit": 1}})
				// list does not accept snapshot's view selector.
				if operation == "list" {
					request, _ = json.Marshal(map[string]any{"type": "task_api_request", "api_version": taskcontract.Version, "operation": operation, "correlation_id": correlation, "input": map[string]any{"session_id": "synthetic-session", "limit": 1}})
				}
				response := service.Execute(context.Background(), caller, request)
				if response.OK || response.Error == nil || response.Error.Code != "unknown_acceptance" || response.Error.Acceptance != "unknown" || response.Error.Action != "lookup_original" {
					t.Fatal("diagnostics changed original failure contract", response)
				}
				line := output.String()
				digest := sha256.Sum256([]byte(correlation))
				for _, field := range []string{"correlation_sha256=" + hex.EncodeToString(digest[:]), "operation=" + operation, "stage=" + row.stage, "reason=" + row.reason, "response_bytes=", "contract_sha256=" + taskcontract.MediaHash()} {
					if !strings.Contains(line, field) {
						t.Fatal("bounded original technical field missing", field)
					}
				}
				for _, prohibited := range []string{secret, correlation, "synthetic-session", verifier.context.StableScopeID, verifier.context.SourceRequestID} {
					if strings.Contains(line, prohibited) {
						t.Fatal("technical diagnostic disclosed private value")
					}
				}
				if strings.Count(line, "task_api_read_diagnostic ") != 1 || len(line) > 1024 {
					t.Fatal("diagnostic was not one bounded line")
				}
			})
		}
	}
}

func TestReadDiagnosticsExcludeOtherOperationsAndUnknownStage(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(previous)
	for _, operation := range []string{"get", "read_result", "create_dispatch", "append", "cancel", "private-operation"} {
		readDiagnostic(Request{Operation: operation, CorrelationID: "secret"}, "private-schema", "gateway_read", errors.New("secret"), 0)
	}
	readDiagnostic(Request{Operation: "snapshot", CorrelationID: "secret"}, "private-schema", "private-stage", errors.New("secret"), 0)
	if output.Len() != 0 {
		t.Fatal("diagnostics escaped the two read routes or closed stages")
	}
	readDiagnostic(Request{Operation: "snapshot", CorrelationID: "secret"}, "private-schema", "gateway_read", errors.New("secret"), 0)
	if strings.Contains(output.String(), "private-schema") || !strings.Contains(output.String(), taskcontract.Hash()) {
		t.Fatal("raw schema value escaped fixed contract hash")
	}
}
