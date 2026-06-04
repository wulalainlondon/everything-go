// Package executor defines the seam between the Go connection core and whatever
// actually runs the AI workload. This is the architectural pivot that unlocks
// the three test configurations:
//
//	GoExecutor      → spawns the CLI + parses streams in Go      (config 2: pure Go)
//	PythonExecutor  → forwards to a Python worker over a socket   (config 3: Go + Python, the "P3" hybrid)
//
// The connection core (hub/client/router) is written ONCE against this
// interface; swapping the Executor swaps the entire backend implementation
// without touching connection, routing, or session management.
package executor

import (
	"context"

	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// Sink is how an Executor pushes normalized wire events back toward the client.
// The value passed to Emit is one of the protocol.* outbound structs; the hub
// marshals it and delivers it to the active connection. Emit must be safe for
// concurrent use.
type Sink interface {
	Emit(event any)
}

// Executor runs AI turns for sessions. Implementations own all runtime-specific
// state (subprocesses, sockets) keyed by Session.ID and report progress via the
// Sink they were constructed with.
type Executor interface {
	// Send delivers a user prompt to the session, spawning the underlying
	// runtime on first use. It returns quickly; the turn streams asynchronously
	// through the Sink (text_chunk/tool_*/done). reqID is stamped onto every
	// emitted event so the client can correlate the streaming turn. images/files
	// are optional message attachments (nil when none).
	Send(ctx context.Context, s *session.Session, reqID, content string, images []protocol.InboundImage, files []protocol.InboundFile) error

	// Stop aborts the current turn. Emits `stopped`.
	Stop(ctx context.Context, s *session.Session) error

	// Clear resets the session's conversation history.
	Clear(ctx context.Context, s *session.Session) error

	// Close tears down all runtime resources for the session.
	Close(ctx context.Context, s *session.Session) error
}

// UsageProvider is an optional backend capability: report quota/usage windows.
// Claude (claude.ai OAuth via bun) and Codex (app-server rate limits) implement
// it; Ollama does not. get_usage skips backends that don't.
type UsageProvider interface {
	FetchUsage(ctx context.Context) (*protocol.UsageReport, error)
}

// ProcInspector is an optional capability: report / kill the live subprocess
// backing a session, for get_tasks and kill_task. Only the Claude backend
// (one subprocess per session) implements it.
type ProcInspector interface {
	PID(s *session.Session) (int, bool)
	KillProc(s *session.Session) bool
}

// InteractionResponder is an optional capability: pause a turn on an
// AskUserQuestion tool_use and accept the user's answer back to resume it. Only
// the Claude backend implements it. RespondUserInput reports whether the id
// matched a pending interaction.
type InteractionResponder interface {
	RespondUserInput(id string, answers map[string]any, cancelled bool) bool
	PendingInteractions(sessionID string) []protocol.UserInputRequestPayload
}
