package goexec

import (
	"bytes"
	"context"
	"encoding/json"
	resultscontract "everything-go/contracts/task-api/controller-results-v9"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"fmt"
	"github.com/coder/websocket"
	"testing"
	"time"
)

func TestControllerResultsNativeToolExactIndependentContract(t *testing.T) {
	if !taskToolMatches("task_controller_results", "task_api_controller_results_request", "1.0.0-rc.9", "read_dispatch_inbox") {
		t.Fatal("missing exact native gateway")
	}
	for _, tool := range []string{"task_unified", "task_pipeline", "task_get", "task_capabilities"} {
		if taskToolMatches(tool, "task_api_controller_results_request", "1.0.0-rc.9", "read_dispatch_inbox") {
			t.Fatal("contract smuggled", tool)
		}
	}
	for _, version := range []string{"1.0.0-rc.1", "1.0.0-rc.6", "1.0.0-rc.8"} {
		if taskToolMatches("task_controller_results", "task_api_controller_results_request", version, "read_dispatch_inbox") {
			t.Fatal("wrong version", version)
		}
	}
}

type controllerResultCatalogFixture struct{}

func (*controllerResultCatalogFixture) TaskWorkerScope(*session.Session) (*taskapi.ChildScope, error) {
	return nil, nil
}
func (*controllerResultCatalogFixture) TaskTools(*session.Session) ([]map[string]any, error) {
	schema, e := resultscontract.ToolInput()
	return []map[string]any{{"type": "function", "name": "task_controller_results", "inputSchema": schema}}, e
}
func (*controllerResultCatalogFixture) ExecuteTask(context.Context, backend.TaskCaller, []byte) taskapi.Response {
	return taskapi.Response{}
}
func TestControllerResultsNewGoalMetadataInitializesCatalogWithoutModel(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "unused")
	c.appServerMode = "daemon"
	c.remoteConn = &websocket.Conn{}
	c.taskProvider = &controllerResultCatalogFixture{}
	writer := &rpcCaptureWriter{writes: make(chan []byte, 4)}
	c.rpc.setWriter(writer)
	s := session.NewRegistry().Create("new-authorized-repair-session", "synthetic", t.TempDir(), backend.Codex, "", "read-only", "")
	done := make(chan error, 1)
	go func() { done <- c.GetGoal(context.Background(), s) }()
	for _, method := range []string{"thread/start", "thread/goal/get"} {
		var frame struct {
			ID     int            `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		select {
		case raw := <-writer.writes:
			if e := json.Unmarshal(raw, &frame); e != nil {
				t.Fatal(e)
			}
		case <-time.After(time.Second):
			t.Fatal("metadata path missing", method)
		}
		if frame.Method != method {
			t.Fatal("unexpected RPC/model turn", frame.Method)
		}
		result := `{"goal":null}`
		if method == "thread/start" {
			raw, _ := json.Marshal(frame.Params["dynamicTools"])
			if !bytes.Contains(raw, []byte(`"task_controller_results"`)) {
				t.Fatal("catalog not supplied to NEW thread", string(raw))
			}
			result = `{"thread":{"id":"synthetic-new-thread"}}`
		} else if frame.Params["threadId"] != "synthetic-new-thread" {
			t.Fatal(frame.Params)
		}
		c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":%s}`, frame.ID, result)))
	}
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("metadata did not finish")
	}
	if s.ResumeID() != "synthetic-new-thread" || c.state(s.ID).reqID != "" {
		t.Fatal("metadata became a business turn", s.Snapshot())
	}
	select {
	case raw := <-writer.writes:
		t.Fatal("extra model/provider RPC", string(raw))
	default:
	}
}
