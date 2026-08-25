// Package runtimejournal persists the authoritative, compact state of Bridge
// sessions. Streaming text remains canonical in the native rollout history;
// this journal records only lifecycle, terminal and per-device delivery/read
// cursors so reconnecting clients can deterministically repair their UI.
package runtimejournal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const maxSessions = 2000

type Terminal struct {
	Revision  uint64 `json:"revision"`
	RequestID string `json:"request_id,omitempty"`
	Status    string `json:"status"`
	At        int64  `json:"at"`
}

type Record struct {
	SessionID       string            `json:"session_id"`
	Revision        uint64            `json:"revision"`
	Phase           string            `json:"phase"`
	Stage           string            `json:"stage,omitempty"`
	StageMessage    string            `json:"stage_message,omitempty"`
	StageStartedAt  int64             `json:"stage_started_at,omitempty"`
	ActiveRequestID string            `json:"active_request_id,omitempty"`
	QueueLength     int               `json:"queue_length"`
	LastTerminal    string            `json:"last_terminal_status,omitempty"`
	LastError       string            `json:"last_error,omitempty"`
	UpdatedAt       int64             `json:"updated_at"`
	CompletedAt     int64             `json:"completed_at,omitempty"`
	Terminals       []Terminal        `json:"terminals,omitempty"`
	AckedByDevice   map[string]uint64 `json:"acked_by_device,omitempty"`
	ReadByDevice    map[string]uint64 `json:"read_by_device,omitempty"`
}

// View is safe to send to one device; internal device maps are never exposed.
type View struct {
	SessionID        string `json:"session_id"`
	Revision         uint64 `json:"revision"`
	Phase            string `json:"phase"`
	Stage            string `json:"stage,omitempty"`
	StageMessage     string `json:"stage_message,omitempty"`
	StageStartedAt   int64  `json:"stage_started_at,omitempty"`
	ActiveRequestID  string `json:"active_request_id,omitempty"`
	QueueLength      int    `json:"queue_length"`
	LastTerminal     string `json:"last_terminal_status,omitempty"`
	LastError        string `json:"last_error,omitempty"`
	UpdatedAt        int64  `json:"updated_at"`
	CompletedAt      int64  `json:"completed_at,omitempty"`
	Unread           int    `json:"unread"`
	DeliveryPending  bool   `json:"delivery_pending"`
	HistoryReconcile bool   `json:"history_reconcile"`
}

type snapshot struct {
	Records map[string]*Record `json:"records"`
}

type Store struct {
	mu      sync.Mutex
	path    string
	records map[string]*Record
	now     func() time.Time
}

func New(dataDir string) *Store {
	s := &Store{records: make(map[string]*Record), now: time.Now}
	if dataDir != "" {
		s.path = filepath.Join(dataDir, "session_runtime.json")
	}
	s.load()
	return s
}

func (s *Store) Update(sessionID, phase, requestID string, queueLength int, terminal, lastError string) (View, bool) {
	if sessionID == "" {
		return View{}, false
	}
	if queueLength < 0 {
		queueLength = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(sessionID)
	if phase == "" {
		phase = r.Phase
	}
	if r.Phase == phase && r.ActiveRequestID == requestID && r.QueueLength == queueLength &&
		r.LastTerminal == terminal && r.LastError == lastError {
		return viewLocked(r, ""), false
	}
	r.Revision++
	r.Phase = phase
	r.Stage = defaultStageForPhase(phase)
	r.StageMessage = ""
	r.StageStartedAt = s.now().UnixMilli()
	r.ActiveRequestID = requestID
	r.QueueLength = queueLength
	r.UpdatedAt = s.now().UnixMilli()
	if terminal != "" {
		r.LastTerminal = terminal
		r.LastError = lastError
		r.CompletedAt = r.UpdatedAt
		r.Terminals = append(r.Terminals, Terminal{Revision: r.Revision, RequestID: requestID, Status: terminal, At: r.CompletedAt})
		if len(r.Terminals) > 128 {
			r.Terminals = append([]Terminal(nil), r.Terminals[len(r.Terminals)-128:]...)
		}
	} else if phase == "running" || phase == "queued" || phase == "waiting" || phase == "stopping" {
		r.LastError = ""
	}
	s.enforceCapLocked()
	s.saveLocked()
	return viewLocked(r, ""), true
}

// Progress advances the fine-grained, server-authoritative activity within an
// active phase. It is separate from Update so a cold thread resume can expose
// each meaningful step without pretending the turn itself changed phase.
func (s *Store) Progress(sessionID, requestID, stage, message string) (View, bool) {
	if sessionID == "" || stage == "" {
		return View{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(sessionID)
	sameRequest := requestID == "" || r.ActiveRequestID == "" || r.ActiveRequestID == requestID
	if sameRequest && stageRank(stage) < stageRank(r.Stage) {
		return viewLocked(r, ""), false
	}
	if r.Stage == stage && r.StageMessage == message && (requestID == "" || r.ActiveRequestID == requestID) {
		return viewLocked(r, ""), false
	}
	r.Revision++
	if requestID != "" {
		r.ActiveRequestID = requestID
	}
	r.Stage = stage
	r.StageMessage = message
	r.StageStartedAt = s.now().UnixMilli()
	r.UpdatedAt = r.StageStartedAt
	s.saveLocked()
	return viewLocked(r, ""), true
}

func stageRank(stage string) int {
	switch stage {
	case "transmitting":
		return 1
	case "received":
		return 2
	case "queued":
		return 3
	case "preparing":
		return 4
	case "resuming_thread", "starting_thread":
		return 5
	case "submitting_turn":
		return 6
	case "waiting_model":
		return 7
	case "thinking", "composing", "running_tool", "waiting_user":
		return 8
	case "stopping":
		return 9
	case "completed", "failed", "interrupted", "closed":
		return 10
	default:
		return 0
	}
}

func (s *Store) Ensure(sessionID, phase string, queueLength int) View {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(sessionID)
	if r.Revision == 0 {
		r.Revision = 1
		r.Phase = phase
		if r.Phase == "" {
			r.Phase = "idle"
		}
		r.QueueLength = queueLength
		r.UpdatedAt = s.now().UnixMilli()
		r.Stage = defaultStageForPhase(r.Phase)
		r.StageStartedAt = r.UpdatedAt
		s.saveLocked()
	}
	return viewLocked(r, "")
}

// Recover converts a stale active phase left by a Bridge process crash into an
// explicit terminal state. It never fabricates completion; clients reconcile
// native history before marking the result read.
func (s *Store) Recover(sessionID string, actuallyActive bool) (View, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(sessionID)
	active := r.Phase == "queued" || r.Phase == "running" || r.Phase == "stopping" || r.Phase == "waiting"
	if !active || actuallyActive {
		return viewLocked(r, ""), false
	}
	r.Revision++
	r.Phase = "interrupted"
	r.QueueLength = 0
	r.LastTerminal = "interrupted"
	r.LastError = "Bridge restarted before the terminal event was observed"
	r.UpdatedAt = s.now().UnixMilli()
	r.Stage = "interrupted"
	r.StageMessage = r.LastError
	r.StageStartedAt = r.UpdatedAt
	r.CompletedAt = r.UpdatedAt
	r.Terminals = append(r.Terminals, Terminal{Revision: r.Revision, RequestID: r.ActiveRequestID, Status: "interrupted", At: r.CompletedAt})
	r.ActiveRequestID = ""
	s.saveLocked()
	return viewLocked(r, ""), true
}

// RecoverAllStale is called once while a new Hub is being constructed, before
// it can accept client commands. Any active phase loaded from the previous
// process therefore represents a turn whose terminal event was not observed.
// Keeping this out of reconnect snapshots avoids racing a live worker that is
// transitioning between its in-memory and persisted states.
func (s *Store) RecoverAllStale() []View {
	s.mu.Lock()
	defer s.mu.Unlock()
	var recovered []View
	for _, r := range s.records {
		active := r.Phase == "queued" || r.Phase == "running" || r.Phase == "stopping" || r.Phase == "waiting"
		if !active {
			continue
		}
		r.Revision++
		r.Phase = "interrupted"
		r.QueueLength = 0
		r.LastTerminal = "interrupted"
		r.LastError = "Bridge restarted before the terminal event was observed"
		r.UpdatedAt = s.now().UnixMilli()
		r.Stage = "interrupted"
		r.StageMessage = r.LastError
		r.StageStartedAt = r.UpdatedAt
		r.CompletedAt = r.UpdatedAt
		r.Terminals = append(r.Terminals, Terminal{Revision: r.Revision, RequestID: r.ActiveRequestID, Status: "interrupted", At: r.CompletedAt})
		r.ActiveRequestID = ""
		recovered = append(recovered, viewLocked(r, ""))
	}
	if len(recovered) > 0 {
		s.saveLocked()
	}
	return recovered
}

func (s *Store) Snapshot(deviceID string, sessionIDs []string) []View {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]View, 0, len(sessionIDs))
	for _, id := range sessionIDs {
		r := s.ensureLocked(id)
		out = append(out, viewLocked(r, deviceID))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })
	return out
}

// Ack advances only the named device. read=true additionally marks terminal
// results through revision as viewed; another device's unread count is intact.
func (s *Store) Ack(deviceID, sessionID string, revision uint64, read bool) View {
	if deviceID == "" || sessionID == "" {
		return View{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(sessionID)
	if revision == 0 || revision > r.Revision {
		revision = r.Revision
	}
	if revision > r.AckedByDevice[deviceID] {
		r.AckedByDevice[deviceID] = revision
	}
	if read && revision > r.ReadByDevice[deviceID] {
		r.ReadByDevice[deviceID] = revision
	}
	s.saveLocked()
	return viewLocked(r, deviceID)
}

func (s *Store) Remove(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[sessionID]; ok {
		delete(s.records, sessionID)
		s.saveLocked()
	}
}

func (s *Store) ensureLocked(sessionID string) *Record {
	r := s.records[sessionID]
	if r == nil {
		r = &Record{SessionID: sessionID, Phase: "idle", AckedByDevice: map[string]uint64{}, ReadByDevice: map[string]uint64{}}
		s.records[sessionID] = r
	}
	if r.AckedByDevice == nil {
		r.AckedByDevice = map[string]uint64{}
	}
	if r.ReadByDevice == nil {
		r.ReadByDevice = map[string]uint64{}
	}
	if r.Stage == "" {
		r.Stage = defaultStageForPhase(r.Phase)
		r.StageStartedAt = r.UpdatedAt
	}
	return r
}

func defaultStageForPhase(phase string) string {
	switch phase {
	case "queued":
		return "queued"
	case "running":
		return "preparing"
	case "stopping":
		return "stopping"
	case "waiting":
		return "waiting_user"
	case "completed":
		return "completed"
	case "failed":
		return "failed"
	case "interrupted":
		return "interrupted"
	case "closed":
		return "closed"
	default:
		return "idle"
	}
}

func viewLocked(r *Record, deviceID string) View {
	unread := 0
	readRevision := r.ReadByDevice[deviceID]
	historyReconcile := false
	for _, terminal := range r.Terminals {
		if terminal.Revision > readRevision {
			historyReconcile = true
		}
		if terminal.Status == "completed" && terminal.Revision > readRevision {
			unread++
		}
	}
	return View{SessionID: r.SessionID, Revision: r.Revision, Phase: r.Phase,
		Stage: r.Stage, StageMessage: r.StageMessage, StageStartedAt: r.StageStartedAt,
		ActiveRequestID: r.ActiveRequestID, QueueLength: r.QueueLength,
		LastTerminal: r.LastTerminal, LastError: r.LastError, UpdatedAt: r.UpdatedAt,
		CompletedAt: r.CompletedAt, Unread: unread,
		DeliveryPending:  deviceID != "" && r.AckedByDevice[deviceID] < r.Revision,
		HistoryReconcile: deviceID != "" && historyReconcile}
}

func (s *Store) enforceCapLocked() {
	if len(s.records) <= maxSessions {
		return
	}
	all := make([]*Record, 0, len(s.records))
	for _, r := range s.records {
		all = append(all, r)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].UpdatedAt < all[j].UpdatedAt })
	for _, r := range all[:len(all)-maxSessions] {
		delete(s.records, r.SessionID)
	}
}

func (s *Store) load() {
	if s.path == "" {
		return
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	var snap snapshot
	if json.Unmarshal(data, &snap) == nil && snap.Records != nil {
		s.records = snap.Records
	}
	for id := range s.records {
		s.ensureLocked(id)
	}
	s.enforceCapLocked()
}

func (s *Store) saveLocked() {
	if s.path == "" || os.MkdirAll(filepath.Dir(s.path), 0o700) != nil {
		return
	}
	data, err := json.Marshal(snapshot{Records: s.records})
	if err != nil {
		return
	}
	tmp := s.path + ".tmp"
	if os.WriteFile(tmp, data, 0o600) == nil {
		_ = os.Rename(tmp, s.path)
	}
}
