package governance

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	ControlMobile  = "mobile"
	ControlDesktop = "desktop"
	ControlShared  = "shared"
	ControlActive  = "active"
	ControlPending = "pending_handoff"
)

type SessionControl struct {
	SessionID string `json:"session_id"`
	ThreadID  string `json:"thread_id"`
	Owner     string `json:"owner"`
	State     string `json:"state"`
	UpdatedAt int64  `json:"updated_at"`
}

type SessionControlStore struct {
	mu      sync.Mutex
	path    string
	entries map[string]SessionControl
}

func NewSessionControlStore(dataDir string) *SessionControlStore {
	s := &SessionControlStore{entries: make(map[string]SessionControl)}
	if dataDir != "" {
		s.path = filepath.Join(dataDir, "session_control.json")
	}
	s.load()
	return s
}

func (s *SessionControlStore) Get(sessionID string) SessionControl {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.entries[sessionID]; ok {
		return entry
	}
	return SessionControl{SessionID: sessionID, Owner: ControlMobile, State: ControlActive}
}

func (s *SessionControlStore) BeginDesktopHandoff(sessionID, threadID string) (SessionControl, error) {
	if sessionID == "" || threadID == "" {
		return SessionControl{}, errors.New("session and Codex thread are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.entries[sessionID]
	if current.Owner == ControlDesktop || current.State == ControlPending {
		return current, errors.New("session is already handed off or handoff is pending")
	}
	entry := SessionControl{SessionID: sessionID, ThreadID: threadID, Owner: ControlMobile, State: ControlPending, UpdatedAt: time.Now().UnixMilli()}
	s.entries[sessionID] = entry
	s.saveLocked()
	return entry, nil
}

func (s *SessionControlStore) CompleteDesktopHandoff(sessionID string) SessionControl {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entries[sessionID]
	entry.SessionID = sessionID
	entry.Owner = ControlDesktop
	entry.State = ControlActive
	entry.UpdatedAt = time.Now().UnixMilli()
	s.entries[sessionID] = entry
	s.saveLocked()
	return entry
}

// MarkDesktopOwner records an externally observed writer (for example a TUI
// that was opened before the Bridge saw the thread). The bool reports whether
// durable state changed so callers can avoid broadcasting the same warning on
// every metadata refresh.
func (s *SessionControlStore) MarkDesktopOwner(sessionID, threadID string) (SessionControl, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.entries[sessionID]
	if current.Owner == ControlDesktop && current.State == ControlActive && current.ThreadID == threadID {
		return current, false
	}
	entry := SessionControl{SessionID: sessionID, ThreadID: threadID, Owner: ControlDesktop, State: ControlActive, UpdatedAt: time.Now().UnixMilli()}
	s.entries[sessionID] = entry
	s.saveLocked()
	return entry, true
}

func (s *SessionControlStore) CancelPending(sessionID string) SessionControl {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entries[sessionID]
	entry.SessionID = sessionID
	entry.Owner = ControlMobile
	entry.State = ControlActive
	entry.UpdatedAt = time.Now().UnixMilli()
	s.entries[sessionID] = entry
	s.saveLocked()
	return entry
}

func (s *SessionControlStore) Reclaim(sessionID, threadID string) SessionControl {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := SessionControl{SessionID: sessionID, ThreadID: threadID, Owner: ControlMobile, State: ControlActive, UpdatedAt: time.Now().UnixMilli()}
	s.entries[sessionID] = entry
	s.saveLocked()
	return entry
}

func (s *SessionControlStore) MobileMayWrite(sessionID string) bool {
	entry := s.Get(sessionID)
	return entry.Owner != ControlDesktop && entry.State != ControlPending
}

func (s *SessionControlStore) List() []SessionControl {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SessionControl, 0, len(s.entries))
	for _, entry := range s.entries {
		out = append(out, entry)
	}
	return out
}

func (s *SessionControlStore) load() {
	if s.path == "" {
		return
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &s.entries)
	if s.entries == nil {
		s.entries = make(map[string]SessionControl)
	}
}

func (s *SessionControlStore) saveLocked() {
	if s.path == "" || os.MkdirAll(filepath.Dir(s.path), 0o700) != nil {
		return
	}
	data, err := json.Marshal(s.entries)
	if err != nil {
		return
	}
	tmp := s.path + ".tmp"
	if os.WriteFile(tmp, data, 0o600) == nil {
		_ = os.Rename(tmp, s.path)
	}
}
