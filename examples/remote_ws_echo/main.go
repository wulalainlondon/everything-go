package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/coder/websocket"
)

func main() {
	http.HandleFunc("/backend", handleBackend)
	log.Println("remote-ws echo backend on ws://127.0.0.1:9001/backend")
	log.Fatal(http.ListenAndServe("127.0.0.1:9001", nil))
}

func handleBackend(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		log.Printf("accept: %v", err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			log.Printf("read: %v", err)
			return
		}
		var frame map[string]any
		if json.Unmarshal(data, &frame) != nil {
			continue
		}
		switch frame["type"] {
		case "remote_hello":
			write(ctx, conn, map[string]any{
				"type": "remote_hello_ack",
				"capabilities": map[string]bool{
					"history": true, "usage": true, "interactions": true,
				},
			})
		case "turn_start":
			sid, _ := frame["session_id"].(string)
			rid, _ := frame["request_id"].(string)
			content, _ := frame["content"].(string)
			write(ctx, conn, map[string]any{"type": "text_delta", "session_id": sid, "request_id": rid, "delta": "remote echo: " + content})
			write(ctx, conn, map[string]any{"type": "done", "session_id": sid, "request_id": rid})
		case "turn_stop":
			write(ctx, conn, map[string]any{"type": "stopped", "session_id": frame["session_id"], "request_id": frame["request_id"]})
		case "history_request":
			write(ctx, conn, map[string]any{
				"type": "history_result", "rpc_id": frame["rpc_id"], "kind": "snapshot",
				"messages": []any{}, "source_count": 0, "known_id_found": true,
			})
		case "resumable_sessions_request":
			write(ctx, conn, map[string]any{"type": "resumable_sessions_result", "rpc_id": frame["rpc_id"], "sessions": []any{}})
		case "usage_request":
			write(ctx, conn, map[string]any{"type": "usage_result", "rpc_id": frame["rpc_id"], "report": map[string]any{"type": "usage_report"}})
		case "user_input_response":
			write(ctx, conn, map[string]any{
				"type": "interaction_resolved", "session_id": frame["session_id"],
				"request_id": frame["request_id"], "status": "resolved",
			})
		}
	}
}

func write(ctx context.Context, conn *websocket.Conn, v any) {
	data, _ := json.Marshal(v)
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		log.Printf("write: %v", err)
	}
}
