package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// savedEntry mirrors the per-session record in the Python bridge's
// saved_sessions.json (field-compatible, though everything-go writes its OWN
// file so it never clobbers the Python prod data).
type savedEntry struct {
	Name       string  `json:"name"`
	ResumeID   string  `json:"resume_id"`
	ClaudeUUID string  `json:"claude_uuid"`
	LastUsed   int64   `json:"last_used"`
	Cwd        string  `json:"cwd"`
	Backend    string  `json:"backend"`
	Model      string  `json:"model"`
	Sandbox    string  `json:"sandbox"`
	CreatedAt  float64 `json:"created_at"`
	Pinned     bool    `json:"pinned,omitempty"`
	Hidden     bool    `json:"hidden,omitempty"`
}

const (
	pruneAfterDays = 30
	maxSaved       = 500
)

// Store persists session metadata to a JSON file with atomic writes + pruning.
type Store struct {
	path string
	mu   sync.Mutex
}

func NewStore(path string) *Store { return &Store{path: path} }

// Load reads the saved sessions file. A missing file yields an empty map.
func (st *Store) Load() map[string]savedEntry {
	st.mu.Lock()
	defer st.mu.Unlock()
	data, err := os.ReadFile(st.path)
	if err != nil {
		return map[string]savedEntry{}
	}
	var m map[string]savedEntry
	if json.Unmarshal(data, &m) != nil {
		return map[string]savedEntry{}
	}
	return m
}

// Save atomically writes the given sessions, applying the same prune rules as
// the Python bridge (drop >30d idle; cap at 500, evicting resumable ones first).
func (st *Store) Save(sessions []*Session) error {
	now := time.Now().Unix()
	m := make(map[string]savedEntry, len(sessions))
	for _, s := range sessions {
		snap := s.Snapshot()
		m[snap.ID] = savedEntry{
			Name: snap.Name, ResumeID: snap.ResumeID, ClaudeUUID: snap.ResumeID,
			LastUsed: now, Cwd: snap.Cwd, Backend: snap.Backend, Model: snap.Model,
			Sandbox: snap.Sandbox, CreatedAt: snap.CreatedAt,
			Pinned: snap.Pinned, Hidden: snap.Hidden,
		}
	}

	cutoff := now - pruneAfterDays*24*3600
	for k, v := range m {
		if v.LastUsed <= cutoff {
			delete(m, k)
		}
	}
	if len(m) > maxSaved {
		type kv struct {
			k string
			v savedEntry
		}
		var resumable []kv
		for k, v := range m {
			if v.ResumeID != "" {
				resumable = append(resumable, kv{k, v})
			}
		}
		sort.Slice(resumable, func(i, j int) bool { return resumable[i].v.LastUsed < resumable[j].v.LastUsed })
		for i := 0; i < len(m)-maxSaved && i < len(resumable); i++ {
			delete(m, resumable[i].k)
		}
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := st.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(st.path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, st.path)
}
