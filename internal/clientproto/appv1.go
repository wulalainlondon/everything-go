// Package clientproto adapts the core session gateway to a concrete client
// wire protocol. AppV1 is the current React/Capacitor bridge protocol.
package clientproto

import (
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
)

// AppV1 builds outbound frames for the mobile app's current JSON protocol.
// Keeping these constructors behind one type lets the core move toward
// transport/session intent without scattering app-specific frame names.
type AppV1 struct{}

func NewAppV1() AppV1 { return AppV1{} }

// Command is the protocol-neutral shape the core router consumes.
type Command struct {
	Kind string

	SessionID string
	RequestID string

	DeviceID   string
	DeviceName string
	AuthToken  string
	ReplayAck  bool
	BatchID    string

	Name           string
	Cwd            string
	Backend        string
	Model          string
	Sandbox        string
	ResumeClaudeID string
	Content        string

	Effort      string
	Pinned      *bool
	Hidden      *bool
	Objective   string
	GoalStatus  string
	TokenBudget *int

	Limit     int
	KnownLast string
	Mode      string
	Before    string

	ShellID string
	Data    string
	ID      string
	PID     int
	Force   bool

	Path             string
	Paths            []string
	ClientHash       string
	ExpectedModified *int64
	Token            string

	Query            string
	Offset           int
	Filters          *protocol.SearchFilters
	Cursor           string
	ProjectDir       string
	IncludeHidden    bool
	IncludeSubagents bool
	MsgUUID          string
	Around           int

	SDP           string
	Candidate     string
	SDPMid        string
	SDPMLineIndex *int

	Answers   map[string]any
	Cancelled *bool
	Decision  string

	FileID string

	ForkAfterMessageID string

	FeedID         string
	Title          string
	HTML           string
	Source         string
	URL            string
	ClientDedupKey string
	ContentType    string

	Images []backend.ImageAttachment
	Files  []backend.FileAttachment
}

func (AppV1) ParseCommand(in protocol.Inbound) Command {
	return Command{
		Kind:      in.Type,
		SessionID: in.SessionID,
		RequestID: in.RequestID,

		DeviceID:   in.DeviceID,
		DeviceName: in.DeviceName,
		AuthToken:  in.AuthToken,
		ReplayAck:  in.ReplayAck,
		BatchID:    in.BatchID,

		Name:           in.Name,
		Cwd:            in.Cwd,
		Backend:        in.Backend,
		Model:          in.Model,
		Sandbox:        in.Sandbox,
		ResumeClaudeID: in.ResumeClaudeID,
		Content:        in.Content,

		Effort:      in.Effort,
		Pinned:      in.Pinned,
		Hidden:      in.Hidden,
		Objective:   in.Objective,
		GoalStatus:  in.Status,
		TokenBudget: in.TokenBudget,

		Limit:     in.Limit,
		KnownLast: in.KnownLast,
		Mode:      in.Mode,
		Before:    in.Before,

		ShellID: in.ShellID,
		Data:    in.Data,
		ID:      in.ID,
		PID:     in.PID,
		Force:   in.Force,

		Path:             in.Path,
		Paths:            in.Paths,
		ClientHash:       in.ClientHash,
		ExpectedModified: in.ExpectedModified,
		Token:            in.Token,

		Query:            in.Query,
		Offset:           in.Offset,
		Filters:          in.Filters,
		Cursor:           in.Cursor,
		ProjectDir:       in.ProjectDir,
		IncludeHidden:    in.IncludeHidden,
		IncludeSubagents: in.IncludeSubagents,
		MsgUUID:          in.MsgUUID,
		Around:           in.Around,

		SDP:           in.SDP,
		Candidate:     in.Candidate,
		SDPMid:        in.SDPMid,
		SDPMLineIndex: in.SDPMLineIndex,

		Answers:   in.Answers,
		Cancelled: in.Cancelled,
		Decision:  in.Decision,

		FileID: in.FileID,

		ForkAfterMessageID: in.ForkAfterMessageID,

		FeedID:         in.FeedID,
		Title:          in.Title,
		HTML:           in.HTML,
		Source:         in.Source,
		URL:            in.URL,
		ClientDedupKey: in.ClientDedupKey,
		ContentType:    in.ContentType,

		Images: inboundImagesToBackend(in.Images),
		Files:  inboundFilesToBackend(in.Files),
	}
}

func inboundImagesToBackend(images []protocol.InboundImage) []backend.ImageAttachment {
	if len(images) == 0 {
		return nil
	}
	out := make([]backend.ImageAttachment, 0, len(images))
	for _, img := range images {
		out = append(out, backend.ImageAttachment{
			Data:      img.Data,
			MediaType: img.MediaType,
		})
	}
	return out
}

func inboundFilesToBackend(files []protocol.InboundFile) []backend.FileAttachment {
	if len(files) == 0 {
		return nil
	}
	out := make([]backend.FileAttachment, 0, len(files))
	for _, f := range files {
		out = append(out, backend.FileAttachment{
			Name:      f.Name,
			Content:   f.Content,
			MediaType: f.MediaType,
		})
	}
	return out
}

type HelloInput struct {
	ClientID     string
	DeviceID     string
	DeviceName   string
	InstanceID   string
	Gen          string
	IsLocked     bool
	LockedToMe   bool
	InstanceName string
	RootDir      string
	DataDir      string
	LanIP        string
	TunnelURL    string
	Backends     []backend.Definition
}

func (AppV1) HelloAck(in HelloInput) protocol.HelloAck {
	return protocol.HelloAck{
		Type:         "hello_ack",
		ClientID:     in.ClientID,
		DeviceID:     in.DeviceID,
		DeviceName:   in.DeviceName,
		InstanceID:   in.InstanceID,
		Gen:          in.Gen,
		IsLocked:     in.IsLocked,
		LockedToMe:   in.LockedToMe,
		InstanceName: in.InstanceName,
		RootDir:      in.RootDir,
		DataDir:      in.DataDir,
		LanIP:        in.LanIP,
		TunnelURL:    in.TunnelURL,
		Backends:     backendDefinitionsToWire(in.Backends),
	}
}

func (AppV1) UsageReport(rep backend.UsageReport) protocol.UsageReport {
	return protocol.NewUsageReport(
		usageWindowToWire(rep.FiveHour),
		usageWindowToWire(rep.SevenDay),
		usageWindowToWire(rep.SevenDaySonnet),
	)
}

func usageWindowToWire(w *backend.UsageWindow) *protocol.UsageWindow {
	if w == nil {
		return nil
	}
	return &protocol.UsageWindow{
		Utilization: w.Utilization,
		ResetsAt:    w.ResetsAt,
	}
}

func backendDefinitionsToWire(defs []backend.Definition) []protocol.BackendDefinition {
	if len(defs) == 0 {
		return nil
	}
	out := make([]protocol.BackendDefinition, 0, len(defs))
	for _, d := range defs {
		models := make([]protocol.ModelDefinition, 0, len(d.Models))
		for _, m := range d.Models {
			models = append(models, protocol.ModelDefinition{ID: m.ID, Label: m.Label})
		}
		out = append(out, protocol.BackendDefinition{
			ID:           d.ID,
			Label:        d.Label,
			DefaultModel: d.DefaultModel,
			Models:       models,
			Capabilities: protocol.BackendCapabilities{
				History:      d.Capabilities.History,
				Usage:        d.Capabilities.Usage,
				Interactions: d.Capabilities.Interactions,
				Sandbox:      d.Capabilities.Sandbox,
				Images:       d.Capabilities.Images,
				Files:        d.Capabilities.Files,
				Remote:       d.Capabilities.Remote,
			},
		})
	}
	return out
}

func (AppV1) SessionsList(sessions []protocol.SessionSummary) protocol.SessionsList {
	return protocol.NewSessionsList(sessions)
}

func (AppV1) Pong() protocol.Pong {
	return protocol.NewPong()
}

func (AppV1) Error(sessionID, code, message string) protocol.Error {
	return protocol.NewError(sessionID, code, message)
}

func (AppV1) ClaimAck() protocol.ClaimAck {
	return protocol.NewClaimAck()
}

func (AppV1) UnclaimAck() protocol.UnclaimAck {
	return protocol.NewUnclaimAck()
}

func (AppV1) SessionClosed(sessionID string) protocol.SessionClosed {
	return protocol.NewSessionClosed(sessionID)
}

func (AppV1) SessionRenamed(sessionID, name string) protocol.SessionRenamed {
	return protocol.NewSessionRenamed(sessionID, name)
}

func (AppV1) SessionMetaUpdated(sessionID string, pinned, hidden *bool) protocol.SessionMetaUpdated {
	return protocol.SessionMetaUpdated{
		Type:      "session_meta_updated",
		SessionID: sessionID,
		Pinned:    pinned,
		Hidden:    hidden,
	}
}

func (AppV1) ForkError(sessionID, reason string) protocol.ForkError {
	return protocol.NewForkError(sessionID, reason)
}

func (AppV1) SessionForked(sessionID, parentID, name string, createdAt float64) protocol.SessionForked {
	return protocol.NewSessionForked(sessionID, parentID, name, createdAt)
}

func (AppV1) HistorySnapshot(sessionID string, messages []map[string]any, sourceCount int, hasMoreBefore, knownIDFound bool, reason string) protocol.HistorySnapshot {
	return protocol.HistorySnapshot{
		Type:           "history_snapshot",
		SessionID:      sessionID,
		Messages:       messages,
		SourceCount:    sourceCount,
		HasMoreBefore:  hasMoreBefore,
		KnownIDFound:   knownIDFound,
		SnapshotReason: reason,
	}
}

func (AppV1) HistoryDelta(sessionID, afterSourceMessageID string, messages []map[string]any, sourceCount int) protocol.HistoryDelta {
	return protocol.HistoryDelta{
		Type:                 "history_delta",
		SessionID:            sessionID,
		AfterSourceMessageID: afterSourceMessageID,
		Messages:             messages,
		SourceCount:          sourceCount,
	}
}

func (AppV1) ResumableSessions(sessions any) protocol.ResumableSessions {
	return protocol.NewResumableSessions(sessions)
}

func (AppV1) ShellCreated(shellID string) protocol.ShellCreated {
	return protocol.NewShellCreated(shellID)
}

func (AppV1) TasksList(tasks []protocol.Task) protocol.TasksList {
	return protocol.NewTasksList(tasks)
}

func (AppV1) TaskKilled(id string, ok bool) protocol.TaskKilled {
	return protocol.NewTaskKilled(id, ok)
}

func (AppV1) ProcessesList(procs []protocol.Process) protocol.ProcessesList {
	return protocol.NewProcessesList(procs)
}

func (AppV1) ProcessKilled(pid int, ok bool, message string) protocol.ProcessKilled {
	return protocol.NewProcessKilled(pid, ok, message)
}

func (AppV1) GitDiffResult(sessionID, diff, errCode string, initialized bool) protocol.GitDiffResult {
	return protocol.NewGitDiffResult(sessionID, diff, errCode, initialized)
}

func (AppV1) InstancesList() protocol.InstancesList {
	return protocol.NewInstancesList()
}

func (AppV1) InboxListItems(items []protocol.InboxItem) protocol.InboxList {
	return protocol.NewInboxListItems(items)
}

func (AppV1) FeedList(items any) protocol.FeedList {
	return protocol.NewFeedList(items)
}

func (AppV1) FeedAck(feedID string) protocol.FeedAck {
	return protocol.NewFeedAck(feedID)
}

func (AppV1) FeedNew(item any) protocol.FeedNew {
	return protocol.NewFeedNew(item)
}

func (AppV1) FeedDetail(feedID, html, contentType string) protocol.FeedDetail {
	return protocol.NewFeedDetail(feedID, html, contentType)
}

func (AppV1) FeedUpdated(feedID string, read, deleted bool) protocol.FeedUpdated {
	return protocol.NewFeedUpdated(feedID, read, deleted)
}

func (AppV1) PendingInteractionsList(items []backend.UserInputPayload) protocol.PendingInteractionsList {
	return protocol.NewPendingInteractionsList(items)
}

func (AppV1) WebRTCReady() protocol.WebRTCReady {
	return protocol.NewWebRTCReady()
}

func (AppV1) WebRTCAnswer(sdp string) protocol.WebRTCAnswer {
	return protocol.NewWebRTCAnswer(sdp)
}

type SessionCreatedInput struct {
	ID        string
	Name      string
	CreatedAt float64
	Cwd       string
	Backend   string
	Model     string
	Sandbox   string
}

type WebRTCOfferInput struct {
	SDP string
}

func (c Command) WebRTCOffer() WebRTCOfferInput {
	return WebRTCOfferInput{SDP: c.SDP}
}

type WebRTCICEInput struct {
	Candidate     string
	SDPMid        string
	SDPMLineIndex *int
}

func (c Command) WebRTCICE() WebRTCICEInput {
	return WebRTCICEInput{
		Candidate:     c.Candidate,
		SDPMid:        c.SDPMid,
		SDPMLineIndex: c.SDPMLineIndex,
	}
}

func (AppV1) SessionCreated(s SessionCreatedInput) protocol.SessionCreated {
	return protocol.SessionCreated{
		Type:      "session_created",
		SessionID: s.ID,
		Name:      s.Name,
		CreatedAt: s.CreatedAt,
		Cwd:       s.Cwd,
		Backend:   s.Backend,
		Model:     s.Model,
		Sandbox:   s.Sandbox,
	}
}
