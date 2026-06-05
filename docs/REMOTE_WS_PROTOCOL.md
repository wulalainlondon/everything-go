# Remote WebSocket Executor Protocol

This is the backend-facing protocol used by `everything-go --remote-ws-url`.
It is not the app-facing bridge protocol. The Go bridge speaks the app protocol
to phones, and speaks this smaller protocol to a remote model worker.

## Start

```bash
go build -o everything-go ./cmd/everything-go
./everything-go --port 8767 --executor go --remote-ws-url ws://127.0.0.1:9001/backend
```

Create or switch a session to backend `remote-ws`.

## Connection

The Go bridge opens one persistent WebSocket to the remote backend.
All frames are UTF-8 JSON objects.

Bridge -> backend, immediately after connect:

```json
{ "type": "remote_hello", "version": 1 }
```

Backend -> bridge:

```json
{
  "type": "remote_hello_ack",
  "capabilities": {
    "history": true,
    "usage": true,
    "interactions": true
  }
}
```

Capabilities are optional. Missing or false means unsupported.

## Turn Lifecycle

Bridge -> backend:

```json
{
  "type": "turn_start",
  "session_id": "s1",
  "request_id": "r1",
  "content": "hello",
  "model": "model-name",
  "images": [],
  "files": []
}
```

Backend -> bridge:

```json
{ "type": "text_delta", "session_id": "s1", "request_id": "r1", "delta": "hi" }
{ "type": "thinking_delta", "session_id": "s1", "request_id": "r1", "delta": "..." }
{ "type": "done", "session_id": "s1", "request_id": "r1" }
```

Terminal event rule: every turn must eventually produce exactly one of:

- `done`
- `stopped`
- `error`

If the remote backend disconnects before that, the Go bridge emits
`remote_disconnected` for all active turns.

Bridge -> backend:

```json
{ "type": "turn_stop", "session_id": "s1", "request_id": "r1" }
{ "type": "session_clear", "session_id": "s1" }
{ "type": "session_close", "session_id": "s1" }
```

## Tool Events

Backend -> bridge:

```json
{
  "type": "tool_start",
  "session_id": "s1",
  "request_id": "r1",
  "tool_id": "tool1",
  "name": "Bash",
  "command": "ls"
}
```

Streaming tool output:

```json
{ "type": "tool_delta", "session_id": "s1", "request_id": "r1", "tool_id": "tool1", "delta": "a" }
{ "type": "tool_delta", "session_id": "s1", "request_id": "r1", "tool_id": "tool1", "delta": "b" }
```

The Go bridge accumulates deltas and forwards normalized `tool_result` output
to the app.

Finish:

```json
{ "type": "tool_end", "session_id": "s1", "request_id": "r1", "tool_id": "tool1" }
```

## History Capability

Bridge -> backend:

```json
{
  "type": "history_request",
  "rpc_id": "rpc_1",
  "resume_id": "native-thread-id",
  "opts": { "Limit": 100, "Mode": "snapshot" }
}
```

Backend -> bridge:

```json
{
  "type": "history_result",
  "rpc_id": "rpc_1",
  "kind": "snapshot",
  "messages": [
    { "role": "user", "content": "hello", "source_message_id": "m1" }
  ],
  "source_count": 1,
  "known_id_found": true,
  "has_more_before": false
}
```

Bridge -> backend:

```json
{ "type": "resumable_sessions_request", "rpc_id": "rpc_2", "limit": 100 }
```

Backend -> bridge:

```json
{
  "type": "resumable_sessions_result",
  "rpc_id": "rpc_2",
  "sessions": [
    {
      "id": "remote-session-id",
      "name": "Remote session",
      "claude_uuid": "native-thread-id",
      "last_used": 1780000000,
      "cwd": "/tmp",
      "backend": "remote-ws"
    }
  ]
}
```

## Usage Capability

Bridge -> backend:

```json
{ "type": "usage_request", "rpc_id": "rpc_3" }
```

Backend -> bridge:

```json
{
  "type": "usage_result",
  "rpc_id": "rpc_3",
  "report": { "type": "usage_report" }
}
```

## Interactions Capability

Backend -> bridge:

```json
{
  "type": "user_input_request",
  "session_id": "s1",
  "request_id": "ui_1",
  "kind": "ask_user_question",
  "header": "Confirm",
  "tool_use_id": "toolu_1",
  "requesting_agent": "remote",
  "questions": [
    {
      "question_id": "q1",
      "text": "Continue?",
      "type": "choice",
      "options": [{ "id": "yes", "label": "Yes" }]
    }
  ]
}
```

Bridge -> backend after the phone answers:

```json
{
  "type": "user_input_response",
  "session_id": "s1",
  "request_id": "ui_1",
  "answers": { "q1": "yes" },
  "cancelled": false
}
```

Backend may also resolve an interaction itself:

```json
{ "type": "interaction_resolved", "session_id": "s1", "request_id": "ui_1", "status": "resolved" }
```

If the remote WebSocket disconnects, pending interactions are marked `expired`
on the app side.
