package agentactivity

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sort"
	"sync"
)

// NativeScope is supplied by the original execution owner, never a UI body.
// Epoch includes the original transport generation and configuration revision.
type NativeScope struct{ Authority, Session, RootThread, Epoch string }

func (s NativeScope) valid() bool {
	return shortID(s.Authority) && shortID(s.Session) && shortID(s.RootThread) && shortID(s.Epoch)
}

var ErrNativeSource = errors.New("native_activity_source_unavailable")
var ErrNativeChanged = errors.New("native_activity_scope_changed")

// NativeActivity is a public provider event, not a child execution boundary.
// Resolved values describe creation only; they never attest a later turn's model.
type NativeActivity struct {
	ID                     string
	ParentTurn             string
	ChildThread            string
	AgentPath              string
	Kind                   string
	CompletedAtMS          int64
	ResolvedCreationModel  *string
	ResolvedCreationEffort *string
}

// DecodeNativeActivity accepts only the installed 0.162.1 app-server shape.
// The caller owns provenance: this decoder cannot establish it from JSON.
func DecodeNativeActivity(raw []byte, root string) (NativeActivity, error) {
	var v struct {
		ThreadID, TurnID string
		CompletedAtMS    int64 `json:"completedAtMs"`
		Item             struct {
			ID, Type, AgentThreadID, AgentPath, Kind string
			Model                                    *string
			ReasoningEffort                          *string
		}
	}
	if len(raw) > 65536 || !uniqueJSON(raw) || json.Unmarshal(raw, &v) != nil || v.ThreadID != root || !shortID(v.TurnID) || v.CompletedAtMS <= 0 || v.Item.Type != "subAgentActivity" || !shortID(v.Item.ID) || !shortID(v.Item.AgentThreadID) || v.Item.AgentThreadID == root || !actorPath(v.Item.AgentPath) {
		return NativeActivity{}, ErrNativeSource
	}
	switch v.Item.Kind {
	case "started", "interacted", "interrupted", "completed":
	default:
		return NativeActivity{}, ErrNativeSource
	}
	if v.Item.Model != nil && (!shortID(*v.Item.Model) || v.Item.Kind != "started") {
		return NativeActivity{}, ErrNativeSource
	}
	if v.Item.ReasoningEffort != nil && (v.Item.Kind != "started" || !nativeEffort(*v.Item.ReasoningEffort)) {
		return NativeActivity{}, ErrNativeSource
	}
	return NativeActivity{v.Item.ID, v.TurnID, v.Item.AgentThreadID, v.Item.AgentPath, v.Item.Kind, v.CompletedAtMS, v.Item.Model, v.Item.ReasoningEffort}, nil
}
func nativeEffort(s string) bool {
	switch s {
	case "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra":
		return true
	}
	return false
}

// DecodeRolloutActivity is the equivalent persisted native item shape. A file
// owner must first validate the exact indexed path, header and captured prefix.
// Parsing a record here does not supply that provenance or discover child files.
func DecodeRolloutActivity(raw []byte, root string) ([]byte, error) {
	var v struct {
		Type    string
		Payload struct {
			Type          string
			ThreadID      string `json:"thread_id"`
			TurnID        string `json:"turn_id"`
			CompletedAtMS int64  `json:"completed_at_ms"`
			Item          struct {
				ID, Type, Kind  string
				AgentThreadID   string `json:"agent_thread_id"`
				AgentPath       string `json:"agent_path"`
				Model           *string
				ReasoningEffort *string `json:"reasoning_effort"`
			}
		}
	}
	if len(raw) > 1<<20 || !uniqueJSON(raw) || json.Unmarshal(raw, &v) != nil || v.Type != "event_msg" || v.Payload.Type != "item_completed" || v.Payload.Item.Type != "SubAgentActivity" {
		return nil, ErrNativeSource
	}
	p := v.Payload
	i := p.Item
	b, err := json.Marshal(map[string]any{"threadId": p.ThreadID, "turnId": p.TurnID, "completedAtMs": p.CompletedAtMS, "item": map[string]any{"id": i.ID, "type": "subAgentActivity", "kind": i.Kind, "agentThreadId": i.AgentThreadID, "agentPath": i.AgentPath, "model": i.Model, "reasoningEffort": i.ReasoningEffort}})
	if err != nil {
		return nil, ErrNativeSource
	}
	if _, err = DecodeNativeActivity(b, root); err != nil {
		return nil, err
	}
	return b, nil
}

// NativeSource retains bounded owner-observed metadata, without prompts, tools,
// private messages or encrypted items. Overflow/conflict fails closed until a
// new owner epoch constructs a fresh source. It is not a durable task journal.
type NativeSource struct {
	mu       sync.Mutex
	scope    NativeScope
	revision uint64
	failed   bool
	rejected uint64
	events   map[string][32]byte
	actors   map[string][]NativeActivity
}

func NewNativeSource(scope NativeScope) *NativeSource {
	return &NativeSource{scope: scope, events: map[string][32]byte{}, actors: map[string][]NativeActivity{}}
}
func (s *NativeSource) Observe(scope NativeScope, raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if scope != s.scope || !scope.valid() {
		return ErrNativeChanged
	}
	if s.failed {
		return ErrNativeSource
	}
	e, err := DecodeNativeActivity(raw, scope.RootThread)
	if err != nil {
		s.rejected++
		s.revision++
		return err
	}
	// Live JSON-RPC and persisted native history serialize the same public
	// event differently. De-duplicate the strict decoded facts, not formatting
	// or ignored provider fields; conflicting decoded facts still quarantine.
	normalized, _ := json.Marshal(e)
	h := sha256.Sum256(normalized)
	if old, ok := s.events[e.ID]; ok {
		if old == h {
			return nil
		}
		s.failed = true
		s.revision++
		return ErrNativeSource
	}
	if len(s.events) >= 1024 || (s.actors[e.ChildThread] == nil && len(s.actors) >= 16) {
		s.failed = true
		s.revision++
		return ErrNativeSource
	}
	for thread, events := range s.actors {
		for _, old := range events {
			if (old.AgentPath == e.AgentPath && thread != e.ChildThread) || (thread == e.ChildThread && old.AgentPath != e.AgentPath) {
				s.failed = true
				s.revision++
				return ErrNativeSource
			}
		}
	}
	s.events[e.ID] = h
	s.actors[e.ChildThread] = append(s.actors[e.ChildThread], e)
	s.revision++
	return nil
}

// NativeRPC is installed by the original provider owner. Both methods are
// read-only; decoded replies have a 64 KiB acceptance bound. This does not
// replace the shared owner's transport limit. ListTurns must request
// itemsView=notLoaded. No scan,
// generation, subscribe, resume or arbitrary client-selected path is allowed.
type NativeRPC interface {
	Scope() NativeScope
	ReadThread(context.Context, NativeReadProof, string) (json.RawMessage, error)
	ListTurns(context.Context, NativeReadProof, string) (json.RawMessage, error)
}

// NativeReadProof cannot be reconstructed from client JSON: its source pointer
// is private. The original owner must validate Scope at its writer barrier and
// Acquire this registry proof only until bytes are written, never over a reply.
type NativeReadProof struct {
	Scope                   NativeScope
	Revision                uint64
	source                  *NativeSource
	publicChild, publicTurn string
}

func (p NativeReadProof) Acquire() (func(), error) { return p.AcquireFor("", "") }
func (p NativeReadProof) AcquireFor(thread, turn string) (func(), error) {
	if p.source == nil || !p.source.mu.TryLock() {
		return nil, ErrNativeChanged
	}
	s := p.source
	if s.scope != p.Scope || s.revision != p.Revision || s.failed || !p.Scope.valid() {
		s.mu.Unlock()
		return nil, ErrNativeChanged
	}
	if thread != "" && thread != p.Scope.RootThread && s.actors[thread] == nil {
		s.mu.Unlock()
		return nil, ErrNativeSource
	}
	if turn != "" && (p.publicChild != thread || p.publicTurn != turn) {
		s.mu.Unlock()
		return nil, ErrNativeSource
	}
	return s.mu.Unlock, nil
}

type NativeRound struct {
	ID, Status             string
	RootChainTurn          *string
	StartedAt, CompletedAt *int64
	// native_root_chain is weaker than exact immediate parent/directive ownership.
	ParentAssociation string
}
type NativeActor struct {
	ThreadID, AgentPath string
	Activities          []NativeActivity
	Rounds              []NativeRound
	RoundsTruncated     bool
}
type NativeProbe struct {
	Scope             NativeScope
	SourceRevision    uint64
	Coverage          string
	Actors            []NativeActor
	InventoryComplete bool
	RejectedEvents    uint64
}

func (s *NativeSource) Probe(ctx context.Context, rpc NativeRPC) (out NativeProbe, err error) {
	// Every error retires the entire collection, including earlier actor reads.
	defer func() {
		if err != nil {
			out = NativeProbe{Scope: out.Scope}
		}
	}()
	s.mu.Lock()
	scope, rev, failed, rejected := s.scope, s.revision, s.failed, s.rejected
	bindings := map[string][]NativeActivity{}
	for id, events := range s.actors {
		for _, e := range events {
			if e.ResolvedCreationModel != nil {
				v := *e.ResolvedCreationModel
				e.ResolvedCreationModel = &v
			}
			if e.ResolvedCreationEffort != nil {
				v := *e.ResolvedCreationEffort
				e.ResolvedCreationEffort = &v
			}
			bindings[id] = append(bindings[id], e)
		}
	}
	s.mu.Unlock()
	out = NativeProbe{Scope: scope, SourceRevision: rev, Coverage: "partial_selected_root_native_activity", Actors: []NativeActor{}, RejectedEvents: rejected}
	if failed || !scope.valid() || rpc == nil || rpc.Scope() != scope {
		return out, ErrNativeSource
	}
	proof := NativeReadProof{Scope: scope, Revision: rev, source: s}
	raw, err := rpc.ReadThread(ctx, proof, scope.RootThread)
	if err != nil {
		return out, ErrNativeSource
	}
	root, err := decodeNativeThread(raw, scope.RootThread)
	if err != nil {
		return out, err
	}
	ids := make([]string, 0, len(bindings))
	for id := range bindings {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if ctx.Err() != nil || rpc.Scope() != scope {
			return out, ErrNativeChanged
		}
		raw, err = rpc.ReadThread(ctx, proof, id)
		if err != nil {
			return out, ErrNativeSource
		}
		child, e := decodeNativeThread(raw, id)
		if e != nil || child.Parent == nil || *child.Parent != scope.RootThread || child.Session != root.Session {
			return out, ErrNativeSource
		}
		raw, err = rpc.ListTurns(ctx, proof, id)
		if err != nil {
			return out, ErrNativeSource
		}
		rounds, more, e := decodeNativeRounds(raw, bindings[id])
		if e != nil {
			return out, e
		}
		out.Actors = append(out.Actors, NativeActor{ThreadID: id, AgentPath: bindings[id][0].AgentPath, Activities: bindings[id], Rounds: rounds, RoundsTruncated: more})
	}
	s.mu.Lock()
	changed := s.scope != scope || s.revision != rev || s.failed
	s.mu.Unlock()
	if changed || rpc.Scope() != scope || ctx.Err() != nil {
		return NativeProbe{Scope: scope}, ErrNativeChanged
	}
	return out, nil
}

type nativeThread struct {
	ID, Session string
	Parent      *string
}

func decodeNativeThread(raw []byte, id string) (nativeThread, error) {
	var v struct {
		Thread struct {
			ID, SessionID  string
			ParentThreadID *string
			Turns          *[]json.RawMessage
		}
	}
	if len(raw) > 65536 || !uniqueJSON(raw) || json.Unmarshal(raw, &v) != nil || v.Thread.ID != id || !shortID(v.Thread.SessionID) || v.Thread.Turns == nil || len(*v.Thread.Turns) != 0 {
		return nativeThread{}, ErrNativeSource
	}
	return nativeThread{v.Thread.ID, v.Thread.SessionID, v.Thread.ParentThreadID}, nil
}
func decodeNativeRounds(raw []byte, events []NativeActivity) ([]NativeRound, bool, error) {
	var v struct {
		Data *[]struct {
			ID, Status, ItemsView  string
			RootTurnID             *string
			StartedAt, CompletedAt *int64
			Items                  *[]json.RawMessage
		}
		NextCursor json.RawMessage
	}
	if len(raw) > 65536 || !uniqueJSON(raw) || json.Unmarshal(raw, &v) != nil || v.Data == nil || len(*v.Data) > 16 || len(v.NextCursor) == 0 {
		return nil, false, ErrNativeSource
	}
	more := string(v.NextCursor) != "null"
	if more {
		var cursor string
		if json.Unmarshal(v.NextCursor, &cursor) != nil || !shortID(cursor) {
			return nil, false, ErrNativeSource
		}
	}
	seen := map[string]bool{}
	rounds := []NativeRound{}
	for _, t := range *v.Data {
		if !shortID(t.ID) || seen[t.ID] || t.Items == nil || len(*t.Items) != 0 || t.ItemsView != "notLoaded" {
			return nil, false, ErrNativeSource
		}
		seen[t.ID] = true
		switch t.Status {
		case "inProgress", "completed", "interrupted", "failed":
		default:
			return nil, false, ErrNativeSource
		}
		if t.RootTurnID != nil && !shortID(*t.RootTurnID) {
			return nil, false, ErrNativeSource
		}
		if (t.StartedAt != nil && *t.StartedAt < 0) || (t.CompletedAt != nil && (*t.CompletedAt < 0 || t.StartedAt != nil && *t.CompletedAt < *t.StartedAt)) {
			return nil, false, ErrNativeSource
		}
		association := "unresolved"
		if t.RootTurnID != nil {
			for _, e := range events {
				if e.ParentTurn == *t.RootTurnID {
					association = "native_root_chain"
					break
				}
			}
		}
		rounds = append(rounds, NativeRound{t.ID, t.Status, t.RootTurnID, t.StartedAt, t.CompletedAt, association})
	}
	return rounds, more, nil
}
