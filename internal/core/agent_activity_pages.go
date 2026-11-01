package core

import (
	"encoding/json"
	"errors"
	"everything-go/internal/activitywire"
)

const activityPageBytes = 60 << 10

// All metadata axes and timelines share one immutable normalized token. A page
// has <=100 records and <=60KiB, including repeated bounded actor headers.
func activityPages(base activitywire.Snapshot) ([]activitywire.Snapshot, error) {
	blank := func() activitywire.Snapshot {
		v := base
		v.Actors = []activitywire.Actor{}
		v.Origins = []activitywire.Origin{}
		v.Waits = []activitywire.Wait{}
		v.ParentViews = []activitywire.ParentView{}
		v.NextOffset = nil
		return v
	}
	pages := []activitywire.Snapshot{}
	page := blank()
	records := 0
	add := func(next func(activitywire.Snapshot) activitywire.Snapshot) error {
		candidate := next(page)
		raw, e := json.Marshal(candidate)
		if e != nil {
			return e
		}
		if records >= 100 || len(raw) > activityPageBytes {
			if records == 0 {
				return errors.New("activity_atom_bound")
			}
			pages = append(pages, page)
			if len(pages) >= 64 {
				return errors.New("activity_pages_bound")
			}
			page = blank()
			records = 0
			candidate = next(page)
			raw, e = json.Marshal(candidate)
			if e != nil || len(raw) > activityPageBytes {
				return errors.New("activity_atom_bound")
			}
		}
		page = candidate
		records++
		return nil
	}
	for _, actor := range base.Actors {
		chunks := actor.Activities
		if len(chunks) == 0 {
			chunks = []activitywire.Activity{{}}
		}
		for _, event := range chunks {
			a := actor
			a.Activities = []activitywire.Activity{}
			if event.ID != "" {
				a.Activities = append(a.Activities, event)
			}
			if e := add(func(v activitywire.Snapshot) activitywire.Snapshot {
				v.Actors = append([]activitywire.Actor{}, v.Actors...)
				for i, old := range v.Actors {
					if old.ID == a.ID {
						old.Activities = append(append([]activitywire.Activity{}, old.Activities...), a.Activities...)
						v.Actors[i] = old
						return v
					}
				}
				v.Actors = append(v.Actors, a)
				return v
			}); e != nil {
				return nil, e
			}
		}
	}
	for _, origin := range base.Origins {
		if e := add(func(v activitywire.Snapshot) activitywire.Snapshot {
			v.Origins = append(append([]activitywire.Origin{}, v.Origins...), origin)
			return v
		}); e != nil {
			return nil, e
		}
	}
	for _, wait := range base.Waits {
		if e := add(func(v activitywire.Snapshot) activitywire.Snapshot {
			v.Waits = append(append([]activitywire.Wait{}, v.Waits...), wait)
			return v
		}); e != nil {
			return nil, e
		}
	}
	for _, view := range base.ParentViews {
		ids := view.ActivityIDs
		if len(ids) == 0 {
			ids = []string{""}
		}
		for _, id := range ids {
			fragment := view
			fragment.ActivityIDs = []string{}
			if id != "" {
				fragment.ActivityIDs = append(fragment.ActivityIDs, id)
			}
			if e := add(func(v activitywire.Snapshot) activitywire.Snapshot {
				v.ParentViews = append([]activitywire.ParentView{}, v.ParentViews...)
				for i, old := range v.ParentViews {
					if old.TurnID == fragment.TurnID {
						old.ActivityIDs = append(append([]string{}, old.ActivityIDs...), fragment.ActivityIDs...)
						v.ParentViews[i] = old
						return v
					}
				}
				v.ParentViews = append(v.ParentViews, fragment)
				return v
			}); e != nil {
				return nil, e
			}
		}
	}
	if records > 0 || len(pages) == 0 {
		pages = append(pages, page)
	}
	for i := range pages {
		pages[i].Offset = int64(i)
		if i+1 < len(pages) {
			next := int64(i + 1)
			pages[i].NextOffset = &next
		}
	}
	return pages, nil
}

// Timeline cursor boundaries are calculated from the same immutable event list.
// Variable byte-bounded pages also provide a verified previous cursor.
func activityTimeline(p activityPin, a activitywire.Actor, offset int64, request string) (activitywire.Timeline, bool) {
	out := activitywire.Timeline{Type: "agent_activity_timeline", Schema: "agent-activity.v2", RequestID: request, Authority: p.scope.Authority, SessionID: p.scope.Session, RootThreadID: p.scope.RootThread, SourceEpoch: p.scope.Epoch, ConfigRevision: p.snapshot.ConfigRevision, NativeSourceRevision: p.snapshot.NativeSourceRevision, SnapshotToken: p.snapshot.SnapshotToken, ActorID: a.ID, TotalObserved: int64(len(a.Activities)), Events: []activitywire.Activity{}}
	start, previous := 0, -1
	for {
		out.Offset = int64(start)
		out.Events = []activitywire.Activity{}
		out.NextOffset = nil
		out.PreviousOffset = nil
		if previous >= 0 {
			v := int64(previous)
			out.PreviousOffset = &v
		}
		end := start
		for end < len(a.Activities) && end-start < 100 {
			candidate := out
			candidate.Events = a.Activities[start : end+1]
			raw, e := json.Marshal(candidate)
			if e != nil || len(raw) > activityPageBytes {
				if end == start {
					return out, false
				}
				break
			}
			end++
		}
		out.Events = a.Activities[start:end]
		if end < len(a.Activities) {
			v := int64(end)
			out.NextOffset = &v
		}
		if int64(start) == offset {
			return out, true
		}
		if out.NextOffset == nil || int64(end) > offset {
			return out, false
		}
		previous, start = start, end
	}
}
