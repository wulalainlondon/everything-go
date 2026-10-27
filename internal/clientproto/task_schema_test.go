package clientproto

import (
	"encoding/json"
	"everything-go/internal/protocol"
	"testing"
)

func TestTaskSchemaHelloKeepsExactSelectorWithoutVersionGuess(t *testing.T) {
	for _, hash := range []string{"", "674aada21ce809c5bec8324b192163c4f3c4acf7f5b57f39042eacd9982d23fa", "ca3bac5e7c66b75bd141379035a49d060ca4e16996c08b230094be9d7487c018", "unknown"} {
		raw, _ := json.Marshal(map[string]any{"type": "hello", "protocol_version": 3, "task_api_schema_hash": hash})
		var in protocol.Inbound
		if e := json.Unmarshal(raw, &in); e != nil {
			t.Fatal(e)
		}
		cmd := NewAppV1().ParseCommand(in)
		if cmd.Kind != "hello" || cmd.ProtocolVersion != 3 || cmd.TaskAPISchemaHash != hash {
			t.Fatal("selector changed", cmd)
		}
	}
}
