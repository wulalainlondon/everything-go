// Package session holds the bridge's session state. It is intentionally a plain
// data layer with no knowledge of transport or AI runtimes — both the Go
// connection core and any Executor read/mutate Session through here.
//
// State discipline (the hardening contract):
//   - ID and CreatedAt are immutable after Create and may be read without the
//     lock; every other field is private and reached only through methods that
//     take the per-session mutex. No external package mutates a field directly.
//   - Each session has an explicit lifecycle state (see turn.go: Idle →
//     Streaming → Stopping → Closed) and a single-flight turn queue, so two
//     turns for the same session can never run concurrently.
package session

import (
	"log"
	"strings"
	"sync"
	"time"
)

// Session carries generic, backend-agnostic fields. Runtime-specific state
// (e.g. the live subprocess) belongs to the Executor, keyed by Session.ID.
//
// All mutable fields are private; callers go through the methods below so the
// mutex is always held and reads see a consistent view.
type Session struct {
	mu sync.Mutex

	// ID and CreatedAt are set once at construction and never change, so they
	// are exported and safe to read concurrently without the lock.
	ID        string
	CreatedAt float64 // unix seconds (matches Python time.time())

	name     string
	cwd      string
	backend  string
	model    string
	sandbox  string
	effort   string
	resumeID string // AI-runtime conversation handle (Claude UUID / Codex thread id)
	pinned   bool
	hidden   bool

	lastActivity float64
	contextUsed  int
	contextMax   int

	state State

	// Turn queue (see turn.go). mailbox serializes turns; turnDone signals the
	// in-flight turn's completion to the worker. mailbox is NEVER closed (that
	// would race a concurrent Submit into a send-on-closed-channel panic); the
	// worker is stopped by closing quit instead.
	mailbox  chan func()
	quit     chan struct{}
	workerUp bool
	turnDone chan struct{}
}

// Snapshot is an immutable, lock-free copy of a session's fields for callers
// that need several at once (summaries, task listings, spawn argument building).
type Snapshot struct {
	ID           string
	Name         string
	Cwd          string
	Backend      string
	Model        string
	Sandbox      string
	Effort       string
	ResumeID     string
	CreatedAt    float64
	LastActivity float64
	ContextUsed  int
	ContextMax   int
	Pinned       bool
	Hidden       bool
	Streaming    bool
	State        State
}

func nowSeconds() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// Snapshot returns a consistent copy of all fields under the lock.
func (s *Session) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Session) snapshotLocked() Snapshot {
	return Snapshot{
		ID: s.ID, Name: s.name, Cwd: s.cwd, Backend: s.backend,
		Model: s.model, Sandbox: s.sandbox, Effort: s.effort, ResumeID: s.resumeID,
		CreatedAt: s.CreatedAt, LastActivity: s.lastActivity,
		ContextUsed: s.contextUsed, ContextMax: s.contextMax,
		Pinned: s.pinned, Hidden: s.hidden,
		Streaming: s.state == Streaming || s.state == Stopping,
		State:     s.state,
	}
}

// --- single-field getters (hot paths) --------------------------------------

func (s *Session) Name() string     { s.mu.Lock(); defer s.mu.Unlock(); return s.name }
func (s *Session) Cwd() string      { s.mu.Lock(); defer s.mu.Unlock(); return s.cwd }
func (s *Session) Backend() string  { s.mu.Lock(); defer s.mu.Unlock(); return s.backend }
func (s *Session) ResumeID() string { s.mu.Lock(); defer s.mu.Unlock(); return s.resumeID }

// IsStreaming reports whether a turn is in flight (Streaming or Stopping).
func (s *Session) IsStreaming() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == Streaming || s.state == Stopping
}

// --- mutators ---------------------------------------------------------------

// SetName renames the session (rename_session).
func (s *Session) SetName(name string) {
	s.mu.Lock()
	s.name = name
	s.mu.Unlock()
}

// SetEffort stores the reasoning effort applied on the next claude spawn.
func (s *Session) SetEffort(effort string) {
	s.mu.Lock()
	s.effort = effort
	s.mu.Unlock()
}

// SetMeta applies optional frontend-visible session metadata.
func (s *Session) SetMeta(pinned, hidden *bool) {
	s.mu.Lock()
	if pinned != nil {
		s.pinned = *pinned
	}
	if hidden != nil {
		s.hidden = *hidden
	}
	s.mu.Unlock()
}

// ApplyConfig overwrites backend/model/sandbox, ignoring empty values
// (switch_session_config semantics: only the supplied fields change).
func (s *Session) ApplyConfig(backend, model, sandbox string) {
	s.mu.Lock()
	if backend != "" {
		s.backend = backend
	}
	if model != "" {
		s.model = model
	}
	if sandbox != "" {
		s.sandbox = sandbox
	}
	s.mu.Unlock()
}

// SetResumeID records the AI-runtime conversation handle (called by the executor
// when a turn establishes or clears one).
func (s *Session) SetResumeID(id string) {
	s.mu.Lock()
	s.resumeID = id
	s.mu.Unlock()
}

// SetContext records backend-reported context usage for summaries/status views.
func (s *Session) SetContext(used, max int) {
	s.mu.Lock()
	if used >= 0 {
		s.contextUsed = used
	}
	if max >= 0 {
		s.contextMax = max
	}
	s.mu.Unlock()
}

// SetLastActivity records externally observed activity, such as a native CLI
// JSONL transcript update. It never moves activity backwards.
func (s *Session) SetLastActivity(ts float64) {
	if ts <= 0 {
		return
	}
	s.mu.Lock()
	if ts > s.lastActivity {
		s.lastActivity = ts
	}
	s.mu.Unlock()
}

// Registry is the in-memory session store, owned by the Go connection core.
type Registry struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	store    *Store
}

func NewRegistry() *Registry {
	return &Registry{sessions: make(map[string]*Session)}
}

// AttachStore wires persistence and restores any previously saved sessions.
func (r *Registry) AttachStore(store *Store) {
	r.store = store
	for id, e := range store.Load() {
		resume := e.ResumeID
		if resume == "" {
			resume = e.ClaudeUUID
		}
		created := e.CreatedAt
		if created == 0 {
			created = float64(e.LastUsed)
		}
		r.mu.Lock()
		r.sessions[id] = &Session{
			ID: id, CreatedAt: created,
			name: e.Name, cwd: e.Cwd, backend: e.Backend,
			model: e.Model, sandbox: e.Sandbox, resumeID: resume,
			pinned: e.Pinned, hidden: e.Hidden,
			lastActivity: float64(e.LastUsed),
			state:        Idle,
		}
		r.mu.Unlock()
	}
}

// Persist writes the current sessions to the attached store (no-op if none).
// Safe to call from a goroutine; writes are serialized inside the Store.
func (r *Registry) Persist() {
	if r.store == nil {
		return
	}
	if err := r.store.Save(r.List()); err != nil {
		log.Printf("session persist failed: %v", err)
	}
}

// Create registers a session under the client-supplied id. If the id already
// exists the existing session is returned (idempotent — the app may resend
// new_session on reconnect).
func (r *Registry) Create(id, name, cwd, backend, model, sandbox, resumeID string) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.sessions[id]; ok {
		return s
	}
	now := nowSeconds()
	s := &Session{
		ID: id, CreatedAt: now,
		name: name, cwd: cwd, backend: backend, model: model,
		sandbox: sandbox, resumeID: resumeID, lastActivity: now,
		state: Idle,
	}
	r.sessions[id] = s
	return s
}

// HasResumeID reports whether any registered session already represents the
// native runtime conversation handle.
func (r *Registry) HasResumeID(resumeID string) bool {
	if resumeID == "" {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, s := range r.sessions {
		if s.ResumeID() == resumeID {
			return true
		}
	}
	return false
}

// UpsertExternal registers or refreshes a session discovered from the native
// Claude/Codex JSONL stores. It dedupes by resumeID so a bridge-created session
// and a native watcher event cannot produce two dashboard rows for one thread.
// The returned bool is true when the registry changed enough to merit a client
// sessions_list broadcast.
func (r *Registry) UpsertExternal(id, name, cwd, backend, resumeID string, lastUsed int64) (*Session, bool) {
	if id == "" || resumeID == "" {
		return nil, false
	}
	if name == "" {
		name = resumeID
		if len(name) > 8 {
			name = name[:8]
		}
	}
	now := nowSeconds()
	activity := float64(lastUsed)
	if activity <= 0 {
		activity = now
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, s := range r.sessions {
		if s.ResumeID() != resumeID {
			continue
		}
		before := s.Snapshot()
		s.mu.Lock()
		if s.name == "" || strings.HasPrefix(s.ID, "jl_") {
			s.name = name
		}
		if s.cwd == "" || strings.HasPrefix(s.ID, "jl_") {
			s.cwd = cwd
		}
		if s.backend == "" {
			s.backend = backend
		}
		if activity > s.lastActivity {
			s.lastActivity = activity
		}
		after := s.snapshotLocked()
		s.mu.Unlock()
		// NOTE: lastActivity changes do NOT count as "changed". A session being
		// actively written by its CLI bumps mtime constantly; treating that as a
		// change would re-broadcast sessions_list every tick. It's also pointless:
		// the broadcast's last_activity comes from the search index's last_ts, not
		// this field. Only structural changes (name/cwd/backend) warrant a refresh.
		return s, before.Name != after.Name || before.Cwd != after.Cwd ||
			before.Backend != after.Backend
	}

	if s, ok := r.sessions[id]; ok {
		before := s.Snapshot()
		s.mu.Lock()
		s.name = name
		s.cwd = cwd
		s.backend = backend
		s.resumeID = resumeID
		if activity > s.lastActivity {
			s.lastActivity = activity
		}
		after := s.snapshotLocked()
		s.mu.Unlock()
		// lastActivity excluded from "changed" — see note above.
		return s, before.Name != after.Name || before.Cwd != after.Cwd ||
			before.Backend != after.Backend || before.ResumeID != after.ResumeID
	}

	s := &Session{
		ID: id, CreatedAt: activity,
		name: name, cwd: cwd, backend: backend, resumeID: resumeID,
		lastActivity: activity, state: Idle,
	}
	r.sessions[id] = s
	return s, true
}

func (r *Registry) Get(id string) (*Session, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.sessions[id]
	return s, ok
}

func (r *Registry) Delete(id string) {
	r.mu.Lock()
	s := r.sessions[id]
	delete(r.sessions, id)
	r.mu.Unlock()
	if s != nil {
		s.Close() // stop the turn worker so the goroutine doesn't leak
	}
}

func (r *Registry) List() []*Session {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		out = append(out, s)
	}
	return out
}
