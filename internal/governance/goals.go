package governance

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"

	"everything-go/internal/protocol"
)

type goalRecord struct {
	Goal     *protocol.Goal `json:"goal"`
	Revision uint64         `json:"revision"`
}

type goalStateFile struct {
	Revision uint64                `json:"revision"`
	Items    map[string]goalRecord `json:"items"`
}

// GoalStateStore is the durable, authoritative bridge-side snapshot used to
// heal dropped WebSocket events. Codex remains the source of truth; every
// observed goal_update/goal_cleared refreshes this cache atomically.
type GoalStateStore struct {
	mu       sync.Mutex
	path     string
	revision uint64
	items    map[string]goalRecord
}

func NewGoalStateStore(path string) *GoalStateStore {
	s := &GoalStateStore{path: path, items: make(map[string]goalRecord)}
	s.load()
	return s
}

func (s *GoalStateStore) Apply(event any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	var sessionID string
	var next *protocol.Goal
	switch e := event.(type) {
	case protocol.GoalUpdate:
		sessionID = e.SessionID
		goal := e.Goal
		next = &goal
	case protocol.GoalCleared:
		sessionID = e.SessionID
	default:
		return false
	}
	if sessionID == "" {
		return false
	}

	if current, ok := s.items[sessionID]; ok {
		if current.Goal != nil && next != nil && current.Goal.UpdatedAt > next.UpdatedAt {
			return false
		}
		if reflect.DeepEqual(current.Goal, next) {
			return false
		}
	}
	s.revision++
	s.items[sessionID] = goalRecord{Goal: next, Revision: s.revision}
	s.persistLocked()
	return true
}

func (s *GoalStateStore) Snapshot() protocol.GoalsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.items))
	for id := range s.items {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	items := make([]protocol.GoalSnapshotItem, 0, len(ids))
	for _, id := range ids {
		record := s.items[id]
		var goal *protocol.Goal
		if record.Goal != nil {
			copyGoal := *record.Goal
			goal = &copyGoal
		}
		items = append(items, protocol.GoalSnapshotItem{SessionID: id, Goal: goal, Revision: record.Revision})
	}
	return protocol.NewGoalsSnapshot(s.revision, items)
}

func (s *GoalStateStore) load() {
	if s.path == "" {
		return
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	var state goalStateFile
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("[goal] snapshot load failed: %v", err)
		return
	}
	s.revision = state.Revision
	if state.Items != nil {
		s.items = state.Items
	}
}

func (s *GoalStateStore) persistLocked() {
	if s.path == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		log.Printf("[goal] snapshot mkdir failed: %v", err)
		return
	}
	data, err := json.MarshalIndent(goalStateFile{Revision: s.revision, Items: s.items}, "", "  ")
	if err != nil {
		log.Printf("[goal] snapshot marshal failed: %v", err)
		return
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		log.Printf("[goal] snapshot write failed: %v", err)
		return
	}
	if err := os.Rename(tmp, s.path); err != nil {
		log.Printf("[goal] snapshot replace failed: %v", err)
	}
}
