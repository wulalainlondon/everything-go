package goexec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/activitywire"
	"everything-go/internal/agentactivity"
	"everything-go/internal/session"
	"sort"
)

// Only original ordered native events establish the parent as-of boundary.
// History replay maintains its own ordered IDs, excluding future live events.
type hostedBoundaries struct {
	seen  map[string]map[string]bool
	views map[string]activitywire.ParentView
	order []string
}

func (b *hostedBoundaries) activity(raw []byte, root string) {
	v, e := agentactivity.DecodeNativeActivity(raw, root)
	if e != nil {
		return
	}
	if b.seen == nil {
		b.seen = map[string]map[string]bool{}
	}
	if b.seen[v.ParentTurn] == nil {
		if len(b.seen) >= 1024 {
			return
		}
		b.seen[v.ParentTurn] = map[string]bool{}
	}
	b.seen[v.ParentTurn][v.ID] = true
}
func (b *hostedBoundaries) terminal(root, turn string) {
	if len(turn) == 0 || len(turn) > 256 {
		return
	}
	if b.views == nil {
		b.views = map[string]activitywire.ParentView{}
	}
	if _, exists := b.views[turn]; exists {
		return
	}
	ids := []string{}
	for id := range b.seen[turn] {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(root + "\x00" + turn + "\x00native_parent_terminal"))
	b.views[turn] = activitywire.ParentView{TurnID: turn, BoundaryID: hex.EncodeToString(sum[:]), ActivityIDs: ids, Coverage: "partial_observed_before_native_parent_terminal"}
	b.order = append(b.order, turn)
	if len(b.order) > 128 {
		old := b.order[0]
		b.order = b.order[1:]
		delete(b.views, old)
		delete(b.seen, old)
	}
}
func (c *Codex) observeHostedBoundary(s *session.Session, method string, raw []byte) {
	var p struct {
		ThreadID string
		Turn     struct{ ID, Status string }
	}
	if method != "turn/completed" || len(raw) > 65536 || !unambiguousPublicJSON(raw) || json.Unmarshal(raw, &p) != nil {
		return
	}
	switch p.Turn.Status {
	case "completed", "failed", "interrupted":
	default:
		return
	}
	st := c.state(s.ID)
	st.mu.Lock()
	scope := st.hostedActivityScope
	active := st.hostedActivity != nil
	st.mu.Unlock()
	if !active || p.ThreadID != scope.RootThread || (hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: scope.Authority}}).Scope() != scope {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.hostedActivityScope == scope {
		st.hostedBoundaries.terminal(scope.RootThread, p.Turn.ID)
		st.hostedViewRevision++
	}
}
