package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/activitywire"
	"everything-go/internal/agentactivity"
	"everything-go/internal/session"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type hostedActivityProvider interface {
	HostedActivitySnapshot(context.Context, *session.Session, string) (activitywire.Snapshot, error)
	HostedActivityScope(*session.Session, string) agentactivity.NativeScope
	HostedActivityReport(context.Context, *session.Session, string, agentactivity.NativeScope, string, string) (agentactivity.NativePublicReturn, string, error)
}
type activityPin struct {
	snapshot activitywire.Snapshot
	pages    []activitywire.Snapshot
	scope    agentactivity.NativeScope
	expires  time.Time
	session  *session.Session
}
type activityClient struct {
	mu       sync.Mutex
	busy     bool
	pins     map[string]activityPin
	revision int64
}

func decodeActivityQuery(raw []byte) (q activitywire.Query, ok bool) {
	if len(raw) == 0 || len(raw) > 2048 || !utf8.Valid(raw) {
		return
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&q) != nil || d.Decode(new(any)) != io.EOF {
		return
	}
	// Decoder alone accepts duplicate keys. The v2 query has only scalar fields;
	// reject duplicates before admitting the normalized generated type.
	d = json.NewDecoder(bytes.NewReader(raw))
	tok, e := d.Token()
	if e != nil || tok != json.Delim('{') {
		return
	}
	seen := map[string]bool{}
	for d.More() {
		k, e := d.Token()
		key, valid := k.(string)
		if e != nil || !valid || seen[key] {
			return
		}
		switch key {
		case "view", "snapshot_token", "offset", "actor_id", "turn_id", "activity_offset":
		default:
			return
		}
		seen[key] = true
		var v any
		if d.Decode(&v) != nil {
			return
		}
	}
	if q.Offset < 0 || q.Offset > 63 || q.ActivityOffset < 0 || q.ActivityOffset > 1024 || len(q.SnapshotToken) > 128 || len(q.ActorID) > 128 || len(q.TurnID) > 256 {
		return
	}
	switch q.View {
	case "snapshot":
		ok = q.SnapshotToken == "" && q.Offset == 0 && q.ActorID == "" && q.TurnID == "" && q.ActivityOffset == 0
	case "page":
		ok = q.SnapshotToken != "" && q.ActorID == "" && q.TurnID == "" && q.ActivityOffset == 0
	case "timeline":
		ok = q.SnapshotToken != "" && q.ActorID != "" && q.TurnID == "" && q.Offset == 0
	case "report":
		ok = q.SnapshotToken != "" && q.ActorID != "" && q.TurnID != "" && q.Offset == 0 && q.ActivityOffset == 0
	}
	return
}
func (h *Hub) handleActivity(ctx context.Context, c *Client, id, request string, raw []byte) {
	fail := func(code string) {
		out := h.client.Error(id, code, code)
		out.RequestID = request
		out.CommandType = "request_agent_activity"
		c.enqueueEvent(out)
	}
	proof := c.readIdentity.Load()
	if proof == nil || h.pairedTaskDevice(c) == "" {
		fail("activity_forbidden")
		return
	}
	q, ok := decodeActivityQuery(raw)
	if !ok || !strings.HasPrefix(request, "act_") || len(request) > 80 {
		fail("activity_invalid_query")
		return
	}
	s, ok := h.registry.Get(id)
	if !ok || s.State() == session.Closed || s.Snapshot().Hidden || !h.controllerInScope(s) {
		fail("activity_forbidden")
		return
	}
	before := s.Snapshot()
	authorized := func() bool {
		current, exists := h.registry.Get(id)
		after := s.Snapshot()
		return exists && current == s && c.readIdentity.Load() == proof && h.pairedTaskDevice(c) != "" && s.State() != session.Closed && !after.Hidden && h.controllerInScope(s) && before.ConfigRevision == after.ConfigRevision && before.ResumeID == after.ResumeID && before.Backend == after.Backend
	}
	var provider hostedActivityProvider
	if hr, ok := h.exec.(historyRouter); ok {
		if p, found := hr.ProviderFor(s); found {
			provider, _ = p.(hostedActivityProvider)
		}
	}
	if provider == nil {
		fail("activity_unsupported")
		return
	}
	c.activity.mu.Lock()
	if c.activity.busy {
		c.activity.mu.Unlock()
		fail("activity_busy")
		return
	}
	c.activity.busy = true
	c.activity.mu.Unlock()
	defer func() { c.activity.mu.Lock(); c.activity.busy = false; c.activity.mu.Unlock() }()
	var pin activityPin
	if q.View == "snapshot" {
		bounded, cancel := context.WithTimeout(ctx, 4*time.Second)
		out, e := provider.HostedActivitySnapshot(bounded, s, h.cfg.InstanceID)
		cancel()
		if !authorized() {
			fail("activity_forbidden")
			return
		}
		scope := provider.HostedActivityScope(s, h.cfg.InstanceID)
		if e != nil || scope.Authority != h.cfg.InstanceID || scope.Session != id || scope.RootThread != before.ResumeID || scope.Epoch == "" || out.Authority != scope.Authority || out.SessionID != id || out.RootThreadID != scope.RootThread || out.SourceEpoch != scope.Epoch || out.ConfigRevision != int64(before.ConfigRevision) {
			fail("activity_source_unavailable")
			return
		}
		for i, o := range out.Origins {
			out.Origins[i].ActorIDs = []string{}
			for _, a := range out.Actors {
				for _, e := range a.Activities {
					if e.ParentTurnID == o.TurnID {
						out.Origins[i].ActorIDs = append(out.Origins[i].ActorIDs, a.ID)
						break
					}
				}
			}
		}
		for i, v := range out.ParentViews {
			out.ParentViews[i].ActorIDs = []string{}
			ids := map[string]bool{}
			for _, id := range v.ActivityIDs {
				ids[id] = true
			}
			for _, a := range out.Actors {
				for _, e := range a.Activities {
					if ids[e.ID] {
						out.ParentViews[i].ActorIDs = append(out.ParentViews[i].ActorIDs, a.ID)
						break
					}
				}
			}
		}
		if out.ParentViews == nil {
			out.ParentViews = []activitywire.ParentView{}
		}
		out.AggregateActivities = 0
		for _, a := range out.Actors {
			out.AggregateActivities += int64(len(a.Activities))
		}
		out.AggregateOrigins = int64(len(out.Origins))
		out.AggregateWaits = int64(len(out.Waits))
		out.AggregateParentViews = int64(len(out.ParentViews))
		out.AggregateParentActivityIDs = 0
		for _, v := range out.ParentViews {
			out.AggregateParentActivityIDs += int64(len(v.ActivityIDs))
		}
		encoded, e := json.Marshal(out)
		if e != nil || len(encoded) > 4<<20 {
			fail("activity_projection_bound")
			return
		}
		tokenBytes := make([]byte, 16)
		if _, e = rand.Read(tokenBytes); e != nil {
			fail("activity_source_unavailable")
			return
		}
		token := hex.EncodeToString(tokenBytes)
		c.activity.mu.Lock()
		c.activity.revision++
		out.Revision = c.activity.revision
		out.SnapshotToken = token
		hash := sha256.Sum256([]byte(scope.Authority + "\x00" + scope.Session + "\x00" + scope.RootThread))
		out.QueryFingerprint = hex.EncodeToString(hash[:])
		pages, e := activityPages(out)
		if e != nil {
			c.activity.mu.Unlock()
			fail("activity_projection_bound")
			return
		}
		pin = activityPin{snapshot: out, pages: pages, scope: scope, expires: time.Now().Add(60 * time.Second), session: s}
		if c.activity.pins == nil {
			c.activity.pins = map[string]activityPin{}
		}
		for k, p := range c.activity.pins {
			if time.Now().After(p.expires) || p.scope != scope {
				delete(c.activity.pins, k)
			}
		}
		if len(c.activity.pins) >= 2 {
			var oldest string
			var at time.Time
			for k, p := range c.activity.pins {
				if oldest == "" || p.expires.Before(at) {
					oldest = k
					at = p.expires
				}
			}
			delete(c.activity.pins, oldest)
		}
		c.activity.pins[token] = pin
		c.activity.mu.Unlock()
	} else {
		c.activity.mu.Lock()
		pin, ok = c.activity.pins[q.SnapshotToken]
		c.activity.mu.Unlock()
		if !ok || time.Now().After(pin.expires) || pin.session != s || provider.HostedActivityScope(s, h.cfg.InstanceID) != pin.scope || !authorized() {
			fail("activity_snapshot_expired")
			return
		}
	}
	if q.View == "timeline" {
		for _, a := range pin.snapshot.Actors {
			if a.ID != q.ActorID {
				continue
			}
			out, valid := activityTimeline(pin, a, q.ActivityOffset, request)
			if !valid {
				fail("activity_invalid_page")
				return
			}
			if !authorized() || provider.HostedActivityScope(s, h.cfg.InstanceID) != pin.scope {
				fail("activity_forbidden")
				return
			}
			c.enqueueEvent(out)
			return
		}
		fail("activity_invalid_page")
		return
	}
	if q.View == "report" {
		var actor *activitywire.Actor
		for _, a := range pin.snapshot.Actors {
			if a.ID == q.ActorID {
				v := a
				actor = &v
				break
			}
		}
		found := false
		if actor != nil {
			for _, r := range actor.Rounds {
				if r.ID == q.TurnID && r.Status == "completed" {
					found = true
				}
			}
		}
		if !found {
			fail("activity_report_unavailable")
			return
		}
		ref, text, e := provider.HostedActivityReport(ctx, s, h.cfg.InstanceID, pin.scope, actor.ThreadID, q.TurnID)
		if !authorized() || provider.HostedActivityScope(s, h.cfg.InstanceID) != pin.scope {
			fail("activity_forbidden")
			return
		}
		out := activitywire.Report{Type: "agent_activity_report", Schema: "agent-activity.v2", RequestID: request, Authority: h.cfg.InstanceID, SessionID: id, SnapshotToken: pin.snapshot.SnapshotToken, ActorID: q.ActorID, TurnID: q.TurnID, Availability: "unavailable", CanonicalQa: "unverified", RootThreadID: pin.scope.RootThread, SourceEpoch: pin.scope.Epoch, ConfigRevision: pin.snapshot.ConfigRevision, Origin: "native_public_final", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		if e == nil && ref.Scope == pin.scope && ref.ChildThread == actor.ThreadID && ref.NativeTurn == q.TurnID && ref.ItemID != "" && len(text) <= 16384 {
			revision := int64(ref.SourceRevision)
			out.SourceRevision = &revision
			out.ItemID = &ref.ItemID
			out.Text = &text
			out.Availability = "available"
		}
		c.enqueueEvent(out)
		return
	}
	if q.Offset >= int64(len(pin.pages)) {
		fail("activity_invalid_page")
		return
	}
	out := pin.pages[q.Offset]
	out.RequestID = request
	if !authorized() || provider.HostedActivityScope(s, h.cfg.InstanceID) != pin.scope {
		fail("activity_forbidden")
		return
	}
	c.enqueueEvent(out)
}
