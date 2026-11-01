// Package jarvisrole owns the single local operator's durable assignment. It is
// deliberately separate from presentation profiles and base service grants.
package jarvisrole

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Target struct {
	Authority string `json:"authority_instance_id"`
	Session   string `json:"session_id"`
	Thread    string `json:"thread_id"`
	Revision  uint64 `json:"config_revision"`
}
type Assignment struct {
	SourceRevision uint64 `json:"source_revision"`
	PolicyHash     string `json:"scope_policy_revision"`
	OwnerDomain    string `json:"owner_domain"`
	Authority      string `json:"authority_instance_id"`
	Partner        string `json:"partner_id"`
	Target         Target `json:"target"`
	Revision       uint64 `json:"assignment_revision"`
	Epoch          uint64 `json:"role_epoch"`
	Bundle         string `json:"bundle_id"`
	BundleVersion  int    `json:"bundle_version"`
	Enabled        bool   `json:"enabled"`
	Desired        bool   `json:"desired_residency"`
	OperationID    string `json:"operation_id"`
	AssignedAt     int64  `json:"assigned_at"`
	Actor          string `json:"-"`
	Device         string `json:"-"`
	Generation     uint64 `json:"-"`
}

// Credential witness is persisted without ever exposing it in API responses.
type savedAssignment struct {
	Assignment
	Actor      string `json:"actor"`
	Device     string `json:"device"`
	Generation uint64 `json:"credential_generation"`
}
type Operation struct {
	ID          string `json:"operation_id"`
	Phase       string `json:"phase"`
	Action      string `json:"action"`
	Revision    uint64 `json:"assignment_revision"`
	Epoch       uint64 `json:"role_epoch"`
	Hash        string `json:"payload_hash"`
	CommittedAt int64  `json:"committed_at"`
}
type savedOperation struct {
	Operation
	Actor string `json:"actor"`
}
type state struct {
	AudioLeases map[string]AudioLease     `json:"audio_leases"`
	Version     int                       `json:"version"`
	Authority   string                    `json:"authority"`
	Revision    uint64                    `json:"revision"`
	Assignment  *savedAssignment          `json:"assignment"`
	Operations  map[string]savedOperation `json:"operations"`
}
type Store struct {
	authority string
	mu        sync.Mutex
	path      string
	state     state
}

func Open(root, authority string) (*Store, error) {
	if root == "" || authority == "" {
		return nil, errors.New("role_storage_unavailable")
	}
	if e := os.MkdirAll(root, 0700); e != nil {
		return nil, e
	}
	s := &Store{authority: authority, path: filepath.Join(root, "jarvis-role-v1.json"), state: state{Version: 1, Authority: authority, Operations: map[string]savedOperation{}, AudioLeases: map[string]AudioLease{}}}
	b, e := os.ReadFile(s.path)
	if os.IsNotExist(e) {
		return s, nil
	}
	if e != nil {
		return nil, e
	}
	if e = json.Unmarshal(b, &s.state); e != nil {
		return nil, e
	}
	if s.state.Version != 1 || s.state.Authority != authority || s.state.Operations == nil {
		return nil, errors.New("role_schema_or_authority_invalid")
	}
	if s.state.AudioLeases == nil {
		s.state.AudioLeases = map[string]AudioLease{}
	}
	if a := s.state.Assignment; a != nil {
		if a.Authority != authority || a.Target.Authority != authority || a.OwnerDomain != s.Domain() || a.Target.Session == "" || a.Partner == "" || a.Epoch == 0 || a.Epoch > s.state.Revision || a.Revision != s.state.Revision || a.Actor == "" || a.Device == "" || a.Bundle != "jarvis_operator_v1" || a.BundleVersion != 1 {
			return nil, errors.New("role_assignment_invalid")
		}
	}
	return s, nil
}
func (s *Store) Domain() string {
	h := sha256.Sum256([]byte("jarvis-local-operator-v1\x00" + s.authority))
	return "operator_" + hex.EncodeToString(h[:16])
}
func assignment(a *savedAssignment) *Assignment {
	if a == nil {
		return nil
	}
	v := a.Assignment
	v.Actor = a.Actor
	v.Device = a.Device
	v.Generation = a.Generation
	return &v
}
func (s *Store) Snapshot() (uint64, *Assignment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Revision, assignment(s.state.Assignment)
}
func (s *Store) Lookup(actor, id string) (Operation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.state.Operations[id]
	return v.Operation, ok && v.Actor == actor
}
func (s *Store) Apply(actor, device string, generation uint64, id, action, hash string, expected uint64, a *Assignment) (Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if actor == "" || id == "" || (action != "assign" && action != "revoke") {
		return Operation{}, errors.New("invalid_role_operation")
	}
	if old, ok := s.state.Operations[id]; ok {
		if old.Actor != actor || old.Hash != hash {
			return Operation{}, errors.New("idempotency_conflict")
		}
		return old.Operation, nil
	}
	for _, lease := range s.state.AudioLeases {
		if lease.Phase != "released" && ((s.state.Assignment != nil && lease.Target.Session == s.state.Assignment.Target.Session) || (a != nil && lease.Target.Session == a.Target.Session)) {
			return Operation{}, errors.New("role_audio_busy_or_release_unknown")
		}
	}
	if expected != s.state.Revision {
		return Operation{}, errors.New("assignment_revision_conflict")
	}
	n := state{Version: 1, Authority: s.state.Authority, Revision: s.state.Revision + 1, Operations: map[string]savedOperation{}, AudioLeases: s.state.AudioLeases}
	for k, v := range s.state.Operations {
		n.Operations[k] = v
	}
	if action == "assign" {
		if a == nil || a.Target.Session == "" || a.Target.Authority != s.state.Authority || a.Partner == "" {
			return Operation{}, errors.New("invalid_role_assignment")
		}
		v := *a
		v.OwnerDomain = s.Domain()
		v.Authority = s.state.Authority
		v.Revision = n.Revision
		v.Epoch = n.Revision
		v.Bundle = "jarvis_operator_v1"
		v.BundleVersion = 1
		v.Enabled = true
		v.Desired = true
		v.OperationID = id
		v.AssignedAt = time.Now().UnixMilli()
		n.Assignment = &savedAssignment{v, actor, device, generation}
	}
	o := Operation{id, "committed", action, n.Revision, n.Revision, hash, time.Now().UnixMilli()}
	n.Operations[id] = savedOperation{o, actor}
	if e := s.persist(n); e != nil {
		return Operation{}, e
	}
	s.state = n
	return o, nil
}

// MigrateWitness is used only after an original provider initialization or an
// explicitly committed human context reset. It preserves logical identity and
// invalidates old native callers with a new epoch, without replaying any input.
func (s *Store) MigrateWitness(session, oldThread, newThread string, newRevision uint64, sourceRevision ...uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.state.Assignment
	if a == nil || a.Target.Session != session {
		return nil
	}
	if a.Target.Thread != oldThread {
		return errors.New("role_native_witness_changed")
	}
	n := s.state
	n.Revision++
	v := *a
	v.Target.Thread = newThread
	v.Target.Revision = newRevision
	if len(sourceRevision) > 0 {
		v.SourceRevision = sourceRevision[0]
	}
	if oldThread != "" {
		v.Epoch = n.Revision
	}
	v.Revision = n.Revision
	n.Assignment = &v
	if e := s.persist(n); e != nil {
		return e
	}
	s.state = n
	return nil
}
func (s *Store) persist(n state) error {
	raw, e := json.Marshal(n)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(s.path), ".jarvis-role-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(raw)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, s.path); e != nil {
		return e
	}
	// Rename committed: never return a pre-effect refusal after commit.
	if d, e := os.Open(filepath.Dir(s.path)); e == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
