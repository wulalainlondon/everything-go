package agentactivity

import "sort"

// WaitJournal is separate from actor execution. Completed-only replay cannot
// reopen a call, and neither a timeout nor a receiver list proves child success.
type WaitJournal struct {
	Entries    map[string]NativeMailboxWait
	Lost       bool
	tombstones map[string]string
}

func (j *WaitJournal) Observe(method string, raw []byte, root string) {
	w, e := DecodeNativeMailboxWait(method, raw, root)
	if e != nil {
		return
	}
	if j.Entries == nil {
		j.Entries = map[string]NativeMailboxWait{}
	}
	if old, ok := j.Entries[w.CallID]; ok {
		if old.ParentTurn != w.ParentTurn {
			j.Lost = true
			return
		}
		if old.Phase == "exited" {
			return
		}
		// An existing call must still retire even when the visible window is full.
		j.Entries[w.CallID] = w
		if w.Phase == "exited" {
			j.rememberExit(w)
		}
		return
	}
	if parent, exists := j.tombstones[w.CallID]; exists {
		if parent != w.ParentTurn {
			j.Lost = true
		}
		return
	}
	if len(j.tombstones) >= 2048 {
		j.Lost = true
		return
	}
	if len(j.Entries) >= 128 {
		oldest := ""
		for id, old := range j.Entries {
			if old.Phase == "exited" && (oldest == "" || id < oldest) {
				oldest = id
			}
		}
		if oldest == "" {
			j.Lost = true
			return
		}
		delete(j.Entries, oldest)
	}
	j.Entries[w.CallID] = w
	if w.Phase == "exited" {
		j.rememberExit(w)
	}
}
func (j *WaitJournal) rememberExit(w NativeMailboxWait) {
	if j.tombstones == nil {
		j.tombstones = map[string]string{}
	}
	if len(j.tombstones) < 2048 {
		j.tombstones[w.CallID] = w.ParentTurn
	} else {
		j.Lost = true
	}
}
func (j *WaitJournal) Snapshot() []NativeMailboxWait {
	out := []NativeMailboxWait{}
	for _, w := range j.Entries {
		if j.Lost && w.Phase == "blocking" {
			w.Phase = "unknown"
		}
		out = append(out, w)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].CallID < out[k].CallID })
	return out
}
