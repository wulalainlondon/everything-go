// Package protocol defines the external WebSocket wire contract shared with the
// React/Capacitor app. Field names and shapes MUST stay byte-compatible with
// app/src/schemas/bridge.ts (the Zod discriminated union is the source of truth).
//
// Go only needs the envelope ({type, session_id}) to route; the typed outbound
// builders below mirror the exact JSON the Python bridge emits so the same app
// renders identically against either backend.
package protocol

import "encoding/json"

// Inbound is a flat superset of every client→bridge frame we currently handle.
// All fields are optional; only `Type` is always present. New command fields can
// be appended without breaking older ones.
type Inbound struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`

	// hello / pairing
	DeviceID   string `json:"device_id"`
	DeviceName string `json:"device_name"`
	AuthToken  string `json:"auth_token"`

	// new_session
	Name           string `json:"name"`
	Cwd            string `json:"cwd"`
	Backend        string `json:"backend"`
	Model          string `json:"model"`
	Sandbox        string `json:"sandbox"`
	ResumeClaudeID string `json:"resume_claude_id"`

	// message
	Content string `json:"content"`

	// request_history
	Limit     int    `json:"limit"`
	KnownLast string `json:"known_last_source_message_id"`
	Mode      string `json:"mode"`
	Before    string `json:"before_source_message_id"`

	// rename / meta / effort
	Effort string `json:"effort"`
	Pinned *bool  `json:"pinned"`
	Hidden *bool  `json:"hidden"`

	// runtime ops: shell / tasks / processes
	ShellID string `json:"shell_id"`
	Data    string `json:"data"`
	ID      string `json:"id"`    // kill_task target
	PID     int    `json:"pid"`   // kill_process target
	Force   bool   `json:"force"` // kill_process SIGKILL

	// file ops: browse_dir / open_file / fcm
	Path       string `json:"path"`
	ClientHash string `json:"client_hash"`
	Token      string `json:"token"`

	// search
	Query         string         `json:"query"`
	Offset        int            `json:"offset"`
	Filters       *SearchFilters `json:"filters"`
	Cursor        string         `json:"cursor"`
	ProjectDir    string         `json:"project_dir"`
	IncludeHidden bool           `json:"include_hidden"`
	MsgUUID       string         `json:"msg_uuid"`
	Around        int            `json:"around"`

	// WebRTC signaling: webrtc_offer carries SDP; webrtc_ice carries a trickled
	// candidate. SDPMLineIndex is a pointer because 0 is a valid index distinct
	// from "absent" (the app sends null when unknown).
	SDP           string `json:"sdp"`
	Candidate     string `json:"candidate"`
	SDPMid        string `json:"sdpMid"`
	SDPMLineIndex *int   `json:"sdpMLineIndex"`

	// user_input_response: the app's answer to an AskUserQuestion interaction.
	// Answers is keyed by question_id. Cancelled is a pointer to distinguish
	// "not sent" from an explicit false.
	Answers   map[string]any `json:"answers"`
	Cancelled *bool          `json:"cancelled"`

	// message attachments: images (base64, no data-URL prefix) and files. The
	// app puts them on the `message` frame alongside content.
	Images []InboundImage `json:"images"`
	Files  []InboundFile  `json:"files"`

	// permission_response: the user's decision on a gated op.
	Decision string `json:"decision"`

	// file push: push_file carries `path` (reuses Path above); file_push_ack
	// carries file_id.
	FileID string `json:"file_id"`

	// fork_session: optional truncation point; empty = copy the full transcript.
	// Name (above) optionally overrides the "<parent> (fork)" default.
	ForkAfterMessageID string `json:"fork_after_message_id"`

	// feed: feed_push carries the article; fetch/mark_read/delete carry feed_id.
	FeedID         string `json:"feed_id"`
	Title          string `json:"title"`
	HTML           string `json:"html"`
	Source         string `json:"source"`
	URL            string `json:"url"`
	ClientDedupKey string `json:"client_dedup_key"`
	ContentType    string `json:"content_type"`
}

// InboundImage is one attached image on a message (app strips the data-URL
// prefix, so Data is raw base64).
type InboundImage struct {
	Data      string `json:"data"`
	MediaType string `json:"media_type"`
}

// InboundFile is one attached file on a message.
type InboundFile struct {
	Name      string `json:"name"`
	Content   string `json:"content"`
	MediaType string `json:"media_type"`
}

// SearchFilters mirrors the nested `filters` object on request_search.
type SearchFilters struct {
	ProjectDir       string `json:"project_dir"`
	Since            string `json:"since"`
	Role             string `json:"role"`
	ExcludeSubagents bool   `json:"exclude_subagents"`
	Source           string `json:"source"`
	MaxPerSession    int    `json:"max_per_session"`
}

// ParseInbound extracts the typed envelope from a raw frame.
func ParseInbound(raw []byte) (Inbound, error) {
	var in Inbound
	err := json.Unmarshal(raw, &in)
	return in, err
}

// --- Outbound builders (bridge→client) -------------------------------------
// Each returns a struct whose json tags match bridge.ts exactly. The hub
// marshals the returned value and writes it to the socket.

type HelloAck struct {
	Type         string `json:"type"`
	ClientID     string `json:"client_id"`
	DeviceID     string `json:"device_id"`
	DeviceName   string `json:"device_name,omitempty"`
	InstanceID   string `json:"instance_id,omitempty"`
	Gen          string `json:"gen,omitempty"`
	IsLocked     bool   `json:"is_locked"`
	LockedToMe   bool   `json:"locked_to_me"`
	InstanceName string `json:"instance_name,omitempty"`
	RootDir      string `json:"root_dir"`
	DataDir      string `json:"data_dir"`
	LanIP        string `json:"lan_ip,omitempty"`
}

// --- Pairing acks -----------------------------------------------------------

type ClaimAck struct {
	Type       string `json:"type"`
	IsLocked   bool   `json:"is_locked"`
	LockedToMe bool   `json:"locked_to_me"`
}

func NewClaimAck() ClaimAck {
	return ClaimAck{Type: "claim_ack", IsLocked: true, LockedToMe: true}
}

type UnclaimAck struct {
	Type     string `json:"type"`
	IsLocked bool   `json:"is_locked"`
}

func NewUnclaimAck() UnclaimAck {
	return UnclaimAck{Type: "unclaim_ack", IsLocked: false}
}

type Pong struct {
	Type string `json:"type"`
}

func NewPong() Pong { return Pong{Type: "pong"} }

// SessionSummary mirrors SessionSummarySchema.
type SessionSummary struct {
	ID           string  `json:"id"`
	Name         string  `json:"name"`
	IsStreaming  bool    `json:"is_streaming"`
	CreatedAt    float64 `json:"created_at"`
	LastActivity float64 `json:"last_activity,omitempty"`
	Cwd          string  `json:"cwd,omitempty"`
	Model        string  `json:"model,omitempty"`
	Backend      string  `json:"backend,omitempty"`
	Sandbox      string  `json:"sandbox,omitempty"`
	Pinned       bool    `json:"pinned"`
	Hidden       bool    `json:"hidden"`
}

type SessionsList struct {
	Type     string           `json:"type"`
	Sessions []SessionSummary `json:"sessions"`
}

func NewSessionsList(sessions []SessionSummary) SessionsList {
	if sessions == nil {
		sessions = []SessionSummary{}
	}
	return SessionsList{Type: "sessions_list", Sessions: sessions}
}

// SessionsListAppend is one batch of the full session snapshot delivered in
// response to get_all_sessions. Mirrors SessionsListAppendSchema and Python's
// send_all_sessions (batch_size 50; offset/total/done let the app assemble it).
type SessionsListAppend struct {
	Type     string           `json:"type"`
	Sessions []SessionSummary `json:"sessions"`
	Offset   int              `json:"offset"`
	Total    int              `json:"total"`
	Done     bool             `json:"done"`
}

func NewSessionsListAppend(sessions []SessionSummary, offset, total int, done bool) SessionsListAppend {
	if sessions == nil {
		sessions = []SessionSummary{}
	}
	return SessionsListAppend{Type: "sessions_list_append", Sessions: sessions, Offset: offset, Total: total, Done: done}
}

// RestartAck confirms a restart_bridge request was accepted. The app doesn't
// consume it (it just waits for the socket to drop and reconnects), but we emit
// it for wire parity with Python.
type RestartAck struct {
	Type string `json:"type"`
}

func NewRestartAck() RestartAck { return RestartAck{Type: "restart_ack"} }

type SessionCreated struct {
	Type      string  `json:"type"`
	SessionID string  `json:"session_id"`
	Name      string  `json:"name"`
	CreatedAt float64 `json:"created_at"`
	Cwd       string  `json:"cwd"`
	Backend   string  `json:"backend,omitempty"`
	Model     string  `json:"model,omitempty"`
	Sandbox   string  `json:"sandbox,omitempty"`
}

type SessionClosed struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
}

func NewSessionClosed(sessionID string) SessionClosed {
	return SessionClosed{Type: "session_closed", SessionID: sessionID}
}

type SessionWarning struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Message   string `json:"message"`
}

func NewSessionWarning(sessionID, msg string) SessionWarning {
	return SessionWarning{Type: "session_warning", SessionID: sessionID, Message: msg}
}

// --- Streaming / turn events (session-scoped) ------------------------------

type TextChunk struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
	Content   string `json:"content"`
}

func NewTextChunk(sessionID, reqID, content string) TextChunk {
	return TextChunk{Type: "text_chunk", SessionID: sessionID, RequestID: reqID, Content: content}
}

type ThinkingChunk struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
	Content   string `json:"content"`
}

func NewThinkingChunk(sessionID, reqID, content string) ThinkingChunk {
	return ThinkingChunk{Type: "thinking_chunk", SessionID: sessionID, RequestID: reqID, Content: content}
}

type ToolStart struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
	ToolUseID string `json:"tool_use_id"`
	Name      string `json:"name"`
	Command   string `json:"command"`
}

func NewToolStart(sessionID, reqID, toolUseID, name, command string) ToolStart {
	return ToolStart{Type: "tool_start", SessionID: sessionID, RequestID: reqID,
		ToolUseID: toolUseID, Name: name, Command: command}
}

type ToolResult struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
	ToolUseID string `json:"tool_use_id"`
	Output    string `json:"output"`
}

func NewToolResult(sessionID, reqID, toolUseID, output string) ToolResult {
	return ToolResult{Type: "tool_result", SessionID: sessionID, RequestID: reqID,
		ToolUseID: toolUseID, Output: output}
}

type ToolEnd struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
	ToolUseID string `json:"tool_use_id"`
}

func NewToolEnd(sessionID, reqID, toolUseID string) ToolEnd {
	return ToolEnd{Type: "tool_end", SessionID: sessionID, RequestID: reqID, ToolUseID: toolUseID}
}

// TodoItem is one normalized task in the cross-backend todo panel. Mirrors the
// app's TodoItemSchema: id may be empty, content is required, status is one of
// pending|in_progress|completed, activeForm is nullable (→ JSON null).
type TodoItem struct {
	ID         string  `json:"id"`
	Content    string  `json:"content"`
	Status     string  `json:"status"`
	ActiveForm *string `json:"activeForm"`
}

// TodoUpdate is the normalized snapshot of the agent's task list, folding
// Claude TodoWrite/Task*, Codex update_plan, and Gemini write_todos into one
// full-replace event. Mirrors bridge's _evt_todo_update.
type TodoUpdate struct {
	Type      string     `json:"type"`
	SessionID string     `json:"session_id"`
	RequestID string     `json:"request_id,omitempty"`
	Todos     []TodoItem `json:"todos"`
}

func NewTodoUpdate(sessionID, reqID string, todos []TodoItem) TodoUpdate {
	if todos == nil {
		todos = []TodoItem{}
	}
	return TodoUpdate{Type: "todo_update", SessionID: sessionID, RequestID: reqID, Todos: todos}
}

type Done struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
}

func NewDone(sessionID, reqID string) Done {
	return Done{Type: "done", SessionID: sessionID, RequestID: reqID}
}

type Stopped struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
}

func NewStopped(sessionID, reqID string) Stopped {
	return Stopped{Type: "stopped", SessionID: sessionID, RequestID: reqID}
}

type Error struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	Code      string `json:"code,omitempty"`
	Message   string `json:"message"`
}

func NewError(sessionID, code, msg string) Error {
	return Error{Type: "error", SessionID: sessionID, Code: code, Message: msg}
}

type SessionUUID struct {
	Type       string `json:"type"`
	SessionID  string `json:"session_id"`
	ClaudeUUID string `json:"claude_uuid"`
}

func NewSessionUUID(sessionID, claudeUUID string) SessionUUID {
	return SessionUUID{Type: "session_uuid", SessionID: sessionID, ClaudeUUID: claudeUUID}
}

// --- History ----------------------------------------------------------------

type HistorySnapshot struct {
	Type           string           `json:"type"`
	SessionID      string           `json:"session_id"`
	Messages       []map[string]any `json:"messages"`
	SourceCount    int              `json:"source_count"`
	HasMoreBefore  bool             `json:"has_more_before"`
	KnownIDFound   bool             `json:"known_id_found"`
	SnapshotReason string           `json:"snapshot_reason,omitempty"`
}

type HistoryDelta struct {
	Type                 string           `json:"type"`
	SessionID            string           `json:"session_id"`
	AfterSourceMessageID string           `json:"after_source_message_id"`
	Messages             []map[string]any `json:"messages"`
	SourceCount          int              `json:"source_count"`
}

type ResumableSessions struct {
	Type     string `json:"type"`
	Sessions any    `json:"sessions"`
}

func NewResumableSessions(sessions any) ResumableSessions {
	return ResumableSessions{Type: "resumable_sessions", Sessions: sessions}
}

// --- Session mgmt tail ------------------------------------------------------

type SessionRenamed struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Name      string `json:"name"`
}

func NewSessionRenamed(sessionID, name string) SessionRenamed {
	return SessionRenamed{Type: "session_renamed", SessionID: sessionID, Name: name}
}

// SessionForked announces a new session created by forking another's transcript.
type SessionForked struct {
	Type            string  `json:"type"`
	SessionID       string  `json:"session_id"`
	ParentSessionID string  `json:"parent_session_id"`
	Name            string  `json:"name"`
	CreatedAt       float64 `json:"created_at"`
}

func NewSessionForked(sessionID, parentID, name string, createdAt float64) SessionForked {
	return SessionForked{
		Type: "session_forked", SessionID: sessionID,
		ParentSessionID: parentID, Name: name, CreatedAt: createdAt,
	}
}

// ForkError reports why a fork_session request could not be completed
// (parent_busy / no_history_file / fork_point_not_found / copy_failed: …).
type ForkError struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Reason    string `json:"reason"`
}

func NewForkError(sessionID, reason string) ForkError {
	return ForkError{Type: "fork_error", SessionID: sessionID, Reason: reason}
}

// --- Agent tree (subagent hierarchy, read-only) -----------------------------

// AgentToolCall is one tool invocation recorded in an agent's transcript.
type AgentToolCall struct {
	Name string `json:"name"`
	TS   *int64 `json:"ts"`
}

// AgentNode is one node in the subagent tree. Mirrors AgentNodeWire in
// app/src/schemas/bridge.ts — nullable fields are pointers so they serialise to
// JSON null; ToolCalls/Children must always be arrays (never null).
type AgentNode struct {
	AgentID       string          `json:"agent_id"`
	AgentType     string          `json:"agent_type"`
	Description   string          `json:"description"`
	PromptID      *string         `json:"prompt_id"`
	ParentAgentID *string         `json:"parent_agent_id"`
	StartTS       *int64          `json:"start_ts"`
	EndTS         *int64          `json:"end_ts"`
	DurationMS    *int64          `json:"duration_ms"`
	ToolCalls     []AgentToolCall `json:"tool_calls"`
	OutputPreview string          `json:"output_preview"`
	Children      []*AgentNode    `json:"children"`
}

// AgentTree is the agent_tree event payload (response to get_agent_tree).
type AgentTree struct {
	Type        string       `json:"type"`
	SessionID   string       `json:"session_id"`
	ResumeID    string       `json:"resume_id"`
	TotalAgents int          `json:"total_agents"`
	Tree        []*AgentNode `json:"tree"`
}

func NewAgentTree(sessionID, resumeID string, total int, tree []*AgentNode) AgentTree {
	if tree == nil {
		tree = []*AgentNode{}
	}
	return AgentTree{
		Type: "agent_tree", SessionID: sessionID,
		ResumeID: resumeID, TotalAgents: total, Tree: tree,
	}
}

type SessionMetaUpdated struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Pinned    *bool  `json:"pinned,omitempty"`
	Hidden    *bool  `json:"hidden,omitempty"`
}

// --- Runtime ops: shell -----------------------------------------------------

type ShellCreated struct {
	Type    string `json:"type"`
	ShellID string `json:"shell_id"`
}

func NewShellCreated(shellID string) ShellCreated {
	return ShellCreated{Type: "shell_created", ShellID: shellID}
}

type ShellOutput struct {
	Type    string `json:"type"`
	ShellID string `json:"shell_id"`
	Data    string `json:"data"`
}

func NewShellOutput(shellID, data string) ShellOutput {
	return ShellOutput{Type: "shell_output", ShellID: shellID, Data: data}
}

type ShellClosed struct {
	Type    string `json:"type"`
	ShellID string `json:"shell_id"`
}

func NewShellClosed(shellID string) ShellClosed {
	return ShellClosed{Type: "shell_closed", ShellID: shellID}
}

// --- Runtime ops: tasks / processes -----------------------------------------

// Task mirrors the get_tasks row (sessions + shells).
type Task struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	PID         *int   `json:"pid"`
	IsStreaming bool   `json:"is_streaming"`
	Cwd         string `json:"cwd"`
}

type TasksList struct {
	Type  string `json:"type"`
	Tasks []Task `json:"tasks"`
}

func NewTasksList(tasks []Task) TasksList {
	if tasks == nil {
		tasks = []Task{}
	}
	return TasksList{Type: "tasks_list", Tasks: tasks}
}

type TaskKilled struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Success bool   `json:"success"`
}

func NewTaskKilled(id string, success bool) TaskKilled {
	return TaskKilled{Type: "task_killed", ID: id, Success: success}
}

// Process mirrors a get_processes row.
type Process struct {
	PID        int     `json:"pid"`
	CPUPercent float64 `json:"cpu_percent"`
	MemRSSKB   int     `json:"mem_rss_kb"`
	User       string  `json:"user"`
	Command    string  `json:"command"`
	Args       string  `json:"args"`
}

type ProcessesList struct {
	Type      string    `json:"type"`
	Processes []Process `json:"processes"`
}

func NewProcessesList(procs []Process) ProcessesList {
	if procs == nil {
		procs = []Process{}
	}
	return ProcessesList{Type: "processes_list", Processes: procs}
}

type ProcessKilled struct {
	Type    string `json:"type"`
	PID     int    `json:"pid"`
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

func NewProcessKilled(pid int, success bool, message string) ProcessKilled {
	return ProcessKilled{Type: "process_killed", PID: pid, Success: success, Message: message}
}

// --- File ops: browse_dir ---------------------------------------------------

// DirEntry mirrors a dir_listing entry.
type DirEntry struct {
	Name     string `json:"name"`
	IsDir    bool   `json:"is_dir"`
	Size     int64  `json:"size"`
	Modified int64  `json:"modified"`
}

// DirSession mirrors a dir_listing session row (active or resumable).
type DirSession struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	ClaudeUUID string `json:"claude_uuid"`
	LastUsed   int64  `json:"last_used"`
	Backend    string `json:"backend"`
	IsActive   bool   `json:"is_active"`
}

type DirListing struct {
	Type      string       `json:"type"`
	Path      string       `json:"path"`
	Entries   []DirEntry   `json:"entries"`
	Sessions  []DirSession `json:"sessions"`
	Hash      string       `json:"hash"`
	Unchanged bool         `json:"unchanged"`
}

func NewDirListing(path string, entries []DirEntry, sessions []DirSession, hash string, unchanged bool) DirListing {
	if entries == nil {
		entries = []DirEntry{}
	}
	if sessions == nil {
		sessions = []DirSession{}
	}
	return DirListing{Type: "dir_listing", Path: path, Entries: entries, Sessions: sessions, Hash: hash, Unchanged: unchanged}
}

type FileOpened struct {
	Type     string `json:"type"`
	Path     string `json:"path"`
	Name     string `json:"name"`
	Content  string `json:"content,omitempty"`
	Size     int64  `json:"size"`
	MimeType string `json:"mime_type"`
	Error    string `json:"error,omitempty"`
}

func NewFileOpened(path, name, content string, size int64, mimeType, errorMessage string) FileOpened {
	return FileOpened{
		Type:     "file_opened",
		Path:     path,
		Name:     name,
		Content:  content,
		Size:     size,
		MimeType: mimeType,
		Error:    errorMessage,
	}
}

// --- Usage ------------------------------------------------------------------

// UsageWindow mirrors one quota window. Utilization is a 0..1 fraction (or a
// raw token count for the Codex token fallback); both fields are nullable.
type UsageWindow struct {
	Utilization *float64 `json:"utilization"`
	ResetsAt    *string  `json:"resets_at"`
}

type UsageReport struct {
	Type           string       `json:"type"`
	FiveHour       *UsageWindow `json:"five_hour"`
	SevenDay       *UsageWindow `json:"seven_day"`
	SevenDaySonnet *UsageWindow `json:"seven_day_sonnet"`
}

func NewUsageReport(fiveHour, sevenDay, sevenDaySonnet *UsageWindow) UsageReport {
	return UsageReport{Type: "usage_report", FiveHour: fiveHour, SevenDay: sevenDay, SevenDaySonnet: sevenDaySonnet}
}

// --- Status -----------------------------------------------------------------

type StatusResult struct {
	Type      string         `json:"type"`
	SessionID string         `json:"session_id"`
	Status    map[string]any `json:"status"`
}

// --- Git diff ---------------------------------------------------------------

type GitDiffResult struct {
	Type        string  `json:"type"`
	SessionID   string  `json:"session_id"`
	Diff        string  `json:"diff"`
	Error       *string `json:"error"`
	Initialized bool    `json:"initialized"`
}

func NewGitDiffResult(sessionID, diff, errCode string, initialized bool) GitDiffResult {
	var ep *string
	if errCode != "" {
		ep = &errCode
	}
	return GitDiffResult{
		Type: "git_diff_result", SessionID: sessionID, Diff: diff,
		Error: ep, Initialized: initialized,
	}
}

// --- WebRTC signaling (bridge→client) --------------------------------------
// The bridge is always the answerer: it bakes its ICE candidates into the
// answer SDP (Pion, like aiortc, doesn't trickle outbound), so it emits
// webrtc_answer + webrtc_ready only — never webrtc_ice. Shapes mirror
// WebRtcAnswerSchema / WebRtcReadySchema in bridge.ts.

type WebRTCAnswer struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

func NewWebRTCAnswer(sdp string) WebRTCAnswer {
	return WebRTCAnswer{Type: "webrtc_answer", SDP: sdp}
}

type WebRTCReady struct {
	Type string `json:"type"`
}

func NewWebRTCReady() WebRTCReady {
	return WebRTCReady{Type: "webrtc_ready"}
}

// --- Phase 5 read stubs: instances / inbox / feed --------------------------
// The app emits list_instances / get_inbox / feed_list_request on connect. The
// Go bridge implements neither the multi-instance supervisor, the file-push
// inbox, nor the article feed, so it answers with valid empty lists — the same
// shape Python returns on a master bridge with nothing configured. This gives
// the app well-formed data (no schema rejection) instead of silence. Arrays are
// always [] never null, per the app's z.array schemas.

type InstancesList struct {
	Type      string `json:"type"`
	Instances []any  `json:"instances"`
}

func NewInstancesList() InstancesList {
	return InstancesList{Type: "instances_list", Instances: []any{}}
}

// InboxItem is one file-push record as the app consumes it (inbox_list items and
// the file_push broadcast share these fields). PushedAt is omitted on a file_push
// broadcast (zero value) and present on inbox_list, mirroring the Python bridge.
type InboxItem struct {
	FileID   string  `json:"file_id"`
	Filename string  `json:"filename"`
	URL      string  `json:"url,omitempty"`
	Data     string  `json:"data,omitempty"`
	Size     int64   `json:"size"`
	MimeType string  `json:"mime_type"`
	PushedAt float64 `json:"pushed_at,omitempty"`
}

type InboxList struct {
	Type  string      `json:"type"`
	Items []InboxItem `json:"items"`
}

func NewInboxList() InboxList {
	return InboxList{Type: "inbox_list", Items: []InboxItem{}}
}

// NewInboxListItems builds inbox_list from pending items (get_inbox reply).
func NewInboxListItems(items []InboxItem) InboxList {
	if items == nil {
		items = []InboxItem{}
	}
	return InboxList{Type: "inbox_list", Items: items}
}

// FilePush is the broadcast announcing a newly pushed file (or a pending item
// replayed on hello). Carries the inline base64 body (Data) or a Storage URL.
type FilePush struct {
	Type     string `json:"type"`
	FileID   string `json:"file_id"`
	Filename string `json:"filename"`
	URL      string `json:"url,omitempty"`
	Data     string `json:"data,omitempty"`
	Size     int64  `json:"size"`
	MimeType string `json:"mime_type"`
}

func NewFilePush(fileID, filename, url, data string, size int64, mimeType string) FilePush {
	return FilePush{
		Type: "file_push", FileID: fileID, Filename: filename,
		URL: url, Data: data, Size: size, MimeType: mimeType,
	}
}

// PushAck is the direct acknowledgement to the sender that a push_file landed.
type PushAck struct {
	Type     string `json:"type"`
	FileID   string `json:"file_id"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
}

func NewPushAck(fileID, filename string, size int64) PushAck {
	return PushAck{Type: "push_ack", FileID: fileID, Filename: filename, Size: size}
}

type FeedList struct {
	Type  string `json:"type"`
	Items any    `json:"items"`
}

// NewFeedList wraps the feed metadata slice (pass a non-nil slice; the router
// passes feed.Store.List() or an empty slice).
func NewFeedList(items any) FeedList {
	if items == nil {
		items = []any{}
	}
	return FeedList{Type: "feed_list", Items: items}
}

// --- feed write events (bridge↔client), mirrors feed_ops.py -----------------

type FeedAck struct {
	Type   string `json:"type"`
	FeedID string `json:"feed_id"`
}

func NewFeedAck(feedID string) FeedAck { return FeedAck{Type: "feed_ack", FeedID: feedID} }

type FeedNew struct {
	Type string `json:"type"`
	Item any    `json:"item"`
}

func NewFeedNew(item any) FeedNew { return FeedNew{Type: "feed_new", Item: item} }

type FeedDetail struct {
	Type        string `json:"type"`
	FeedID      string `json:"feed_id"`
	HTML        string `json:"html"`
	ContentType string `json:"content_type"`
}

func NewFeedDetail(feedID, html, contentType string) FeedDetail {
	return FeedDetail{Type: "feed_detail", FeedID: feedID, HTML: html, ContentType: contentType}
}

type FeedUpdated struct {
	Type    string `json:"type"`
	FeedID  string `json:"feed_id"`
	Read    bool   `json:"read"`
	Deleted bool   `json:"deleted"`
}

func NewFeedUpdated(feedID string, read, deleted bool) FeedUpdated {
	return FeedUpdated{Type: "feed_updated", FeedID: feedID, Read: read, Deleted: deleted}
}

// --- Interactions: AskUserQuestion (bridge↔app) ----------------------------
// Mirrors bridge/interactions.py. A paused Claude turn surfaces an
// AskUserQuestion tool_use as user_input_request; the app answers with
// user_input_response {request_id, answers}; the bridge writes the answer back
// into the CLI's stdin as a tool_result and emits interaction_resolved. Field
// shapes match UserInputRequestPayloadSchema / InteractionResolvedSchema in
// bridge.ts. The list payload carries NO `type`; the event embeds it.

type UserInputOption struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Recommended bool   `json:"recommended"`
}

type UserInputQuestion struct {
	QuestionID  string            `json:"question_id"`
	Text        string            `json:"text"`
	Header      string            `json:"header"`
	Type        string            `json:"type"`
	Options     []UserInputOption `json:"options"`
	MultiSelect bool              `json:"multi_select"`
	FreeForm    bool              `json:"free_form"`
}

// UserInputRequestPayload is the interaction body shared by the
// user_input_request event and the pending_interactions_list entries.
type UserInputRequestPayload struct {
	RequestID       string              `json:"request_id"`
	SessionID       string              `json:"session_id"`
	Source          string              `json:"source"`
	Kind            string              `json:"kind"`
	Header          string              `json:"header"`
	ToolUseID       string              `json:"tool_use_id,omitempty"`
	RequestingAgent string              `json:"requesting_agent,omitempty"`
	Questions       []UserInputQuestion `json:"questions"`
	CreatedAt       int64               `json:"created_at"`
	Status          string              `json:"status"`
}

// UserInputRequestEvent is the payload plus a type discriminant (embedding
// flattens the payload fields into the same JSON object).
type UserInputRequestEvent struct {
	Type string `json:"type"`
	UserInputRequestPayload
}

func NewUserInputRequest(p UserInputRequestPayload) UserInputRequestEvent {
	return UserInputRequestEvent{Type: "user_input_request", UserInputRequestPayload: p}
}

type PendingInteractionsList struct {
	Type         string                    `json:"type"`
	Interactions []UserInputRequestPayload `json:"interactions"`
}

func NewPendingInteractionsList(items []UserInputRequestPayload) PendingInteractionsList {
	if items == nil {
		items = []UserInputRequestPayload{}
	}
	return PendingInteractionsList{Type: "pending_interactions_list", Interactions: items}
}

type InteractionResolved struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id"`
	SessionID string `json:"session_id,omitempty"`
	Status    string `json:"status,omitempty"`
}

func NewInteractionResolved(requestID, sessionID, status string) InteractionResolved {
	return InteractionResolved{Type: "interaction_resolved", RequestID: requestID, SessionID: sessionID, Status: status}
}

// --- permission approval (bridge↔client), mirrors permission_manager.py ------

type PermissionRequest struct {
	Type           string `json:"type"`
	RequestID      string `json:"request_id"`
	SessionID      string `json:"session_id,omitempty"`
	Action         string `json:"action"`
	Title          string `json:"title"`
	Justification  string `json:"justification"`
	CommandPreview string `json:"command_preview,omitempty"`
	RiskLevel      string `json:"risk_level,omitempty"`
	ExpiresAt      int64  `json:"expires_at,omitempty"`
}

func NewPermissionRequest(requestID, sessionID, action, title, justification, preview, risk string, expiresAt int64) PermissionRequest {
	return PermissionRequest{
		Type: "permission_request", RequestID: requestID, SessionID: sessionID,
		Action: action, Title: title, Justification: justification,
		CommandPreview: preview, RiskLevel: risk, ExpiresAt: expiresAt,
	}
}

type PermissionResult struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id"`
	SessionID string `json:"session_id,omitempty"`
	Action    string `json:"action,omitempty"`
	Decision  string `json:"decision"`
	Message   string `json:"message,omitempty"`
}

func NewPermissionResult(requestID, sessionID, action, decision, message string) PermissionResult {
	return PermissionResult{
		Type: "permission_result", RequestID: requestID, SessionID: sessionID,
		Action: action, Decision: decision, Message: message,
	}
}
