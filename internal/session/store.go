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
// saved_sessions.json. Store writes known fields back into the original raw JSON
// object so Python-only metadata survives Go updates.
type savedEntry struct {
	Name              string  `json:"name"`
	ResumeID          string  `json:"resume_id"`
	ClaudeUUID        string  `json:"claude_uuid"`
	LastUsed          int64   `json:"last_used"`
	Cwd               string  `json:"cwd"`
	Backend           string  `json:"backend"`
	Model             string  `json:"model"`
	Sandbox           string  `json:"sandbox"`
	Effort            string  `json:"effort,omitempty"`
	ServiceTier       string  `json:"service_tier,omitempty"`
	CollaborationMode string  `json:"collaboration_mode,omitempty"`
	Personality       string  `json:"personality,omitempty"`
	CreatedAt         float64 `json:"created_at"`
	Pinned            bool    `json:"pinned,omitempty"`
	Hidden            bool    `json:"hidden,omitempty"`
}

const (
	pruneAfterDays = 30
	maxSaved       = 500
)

// Store persists session metadata to a JSON file with atomic writes + pruning.
type Store struct {
	path     string
	mu       sync.Mutex
	knownIDs map[string]bool
}

func NewStore(path string) *Store { return &Store{path: path, knownIDs: map[string]bool{}} }

// Load reads the saved sessions file. A missing file yields an empty map.
func (st *Store) Load() map[string]savedEntry {
	st.mu.Lock()
	defer st.mu.Unlock()

	unlock, err := st.lock()
	if err != nil {
		return map[string]savedEntry{}
	}
	defer unlock()

	raw := st.loadRawLocked()
	out := make(map[string]savedEntry, len(raw))
	for id, obj := range raw {
		out[id] = entryFromRaw(obj)
		st.knownIDs[id] = true
	}
	return out
}

// Save atomically writes the given sessions, applying the same prune rules as
// the Python bridge (drop >30d idle; cap at 500, evicting resumable ones first).
func (st *Store) Save(sessions []*Session) error {
	now := time.Now().Unix()

	st.mu.Lock()
	defer st.mu.Unlock()

	unlock, err := st.lock()
	if err != nil {
		return err
	}
	defer unlock()

	raw := st.loadRawLocked()
	currentIDs := make(map[string]bool, len(sessions))
	for _, s := range sessions {
		snap := s.Snapshot()
		currentIDs[snap.ID] = true
		// last_used must reflect the session's real last activity, NOT now.
		// Stamping every session with now on each Save flattens the whole file to
		// one timestamp, which destroys recency ordering on the client (sessions
		// then tie-break by id, burying any backend whose ids sort late). Fall back
		// to now only when a session has no recorded activity yet.
		lastUsed := int64(snap.LastActivity)
		if lastUsed <= 0 {
			lastUsed = now
		}
		entry := savedEntry{
			Name: snap.Name, ResumeID: snap.ResumeID, ClaudeUUID: snap.ResumeID,
			LastUsed: lastUsed, Cwd: snap.Cwd, Backend: snap.Backend, Model: snap.Model,
			Sandbox: snap.Sandbox, CreatedAt: snap.CreatedAt,
			Effort:      snap.Effort,
			ServiceTier: snap.ServiceTier, CollaborationMode: snap.CollaborationMode, Personality: snap.Personality,
			Pinned: snap.Pinned, Hidden: snap.Hidden,
		}
		obj := raw[snap.ID]
		if obj == nil {
			obj = map[string]json.RawMessage{}
		}
		putKnownFields(obj, entry)
		raw[snap.ID] = obj
		st.knownIDs[snap.ID] = true
	}

	// Remove sessions that this process loaded/created and then deleted from its
	// registry. Entries created by Python after Go started are not knownIDs, so a
	// Go persist won't accidentally delete them.
	for id := range st.knownIDs {
		if !currentIDs[id] {
			delete(raw, id)
			delete(st.knownIDs, id)
		}
	}

	cutoff := now - pruneAfterDays*24*3600
	for k, obj := range raw {
		if currentIDs[k] {
			continue
		}
		if rawInt64(obj, "last_used") <= cutoff {
			delete(raw, k)
			delete(st.knownIDs, k)
		}
	}
	if len(raw) > maxSaved {
		type kv struct {
			k string
			v map[string]json.RawMessage
		}
		var resumable []kv
		for k, v := range raw {
			if rawString(v, "resume_id") != "" || rawString(v, "claude_uuid") != "" {
				resumable = append(resumable, kv{k, v})
			}
		}
		sort.Slice(resumable, func(i, j int) bool {
			return rawInt64(resumable[i].v, "last_used") < rawInt64(resumable[j].v, "last_used")
		})
		for i := 0; i < len(raw)-maxSaved && i < len(resumable); i++ {
			delete(raw, resumable[i].k)
			delete(st.knownIDs, resumable[i].k)
		}
	}

	return st.writeRawLocked(raw)
}

func (st *Store) loadRawLocked() map[string]map[string]json.RawMessage {
	data, err := os.ReadFile(st.path)
	if err != nil {
		return map[string]map[string]json.RawMessage{}
	}
	var raw map[string]map[string]json.RawMessage
	if json.Unmarshal(data, &raw) != nil {
		return map[string]map[string]json.RawMessage{}
	}
	return raw
}

func (st *Store) writeRawLocked(raw map[string]map[string]json.RawMessage) error {
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(st.path), 0o755); err != nil {
		return err
	}
	tmp := st.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, st.path)
}

func putKnownFields(obj map[string]json.RawMessage, entry savedEntry) {
	put := func(key string, v any) {
		data, _ := json.Marshal(v)
		obj[key] = data
	}
	put("name", entry.Name)
	put("resume_id", entry.ResumeID)
	put("claude_uuid", entry.ClaudeUUID)
	put("last_used", entry.LastUsed)
	put("cwd", entry.Cwd)
	put("backend", entry.Backend)
	put("model", entry.Model)
	put("sandbox", entry.Sandbox)
	put("effort", entry.Effort)
	put("service_tier", entry.ServiceTier)
	put("collaboration_mode", entry.CollaborationMode)
	put("personality", entry.Personality)
	put("created_at", entry.CreatedAt)
	if entry.Pinned {
		put("pinned", true)
	} else {
		delete(obj, "pinned")
	}
	if entry.Hidden {
		put("hidden", true)
	} else {
		delete(obj, "hidden")
	}
}

func rawString(obj map[string]json.RawMessage, key string) string {
	var s string
	_ = json.Unmarshal(obj[key], &s)
	return s
}

func rawInt64(obj map[string]json.RawMessage, key string) int64 {
	var i int64
	if json.Unmarshal(obj[key], &i) == nil {
		return i
	}
	var f float64
	if json.Unmarshal(obj[key], &f) == nil {
		return int64(f)
	}
	return 0
}

func rawFloat64(obj map[string]json.RawMessage, key string) float64 {
	var f float64
	if json.Unmarshal(obj[key], &f) == nil {
		return f
	}
	var i int64
	if json.Unmarshal(obj[key], &i) == nil {
		return float64(i)
	}
	return 0
}

func rawBool(obj map[string]json.RawMessage, key string) bool {
	var b bool
	_ = json.Unmarshal(obj[key], &b)
	return b
}

func entryFromRaw(obj map[string]json.RawMessage) savedEntry {
	resumeID := rawString(obj, "resume_id")
	claudeUUID := rawString(obj, "claude_uuid")
	if resumeID == "" {
		resumeID = claudeUUID
	}
	lastUsed := rawInt64(obj, "last_used")
	createdAt := rawFloat64(obj, "created_at")
	if createdAt == 0 {
		createdAt = float64(lastUsed)
	}
	return savedEntry{
		Name:              rawString(obj, "name"),
		ResumeID:          resumeID,
		ClaudeUUID:        claudeUUID,
		LastUsed:          lastUsed,
		Cwd:               rawString(obj, "cwd"),
		Backend:           rawString(obj, "backend"),
		Model:             rawString(obj, "model"),
		Sandbox:           rawString(obj, "sandbox"),
		Effort:            rawString(obj, "effort"),
		ServiceTier:       rawString(obj, "service_tier"),
		CollaborationMode: rawString(obj, "collaboration_mode"),
		Personality:       rawString(obj, "personality"),
		CreatedAt:         createdAt,
		Pinned:            rawBool(obj, "pinned"),
		Hidden:            rawBool(obj, "hidden"),
	}
}
