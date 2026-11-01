package goexec

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"everything-go/internal/activitywire"
	"everything-go/internal/agentactivity"
	"everything-go/internal/session"
	"io"
	"os"
	"sync"
	"time"
)

// Recovery reads only the original owner's already-indexed root. No directory
// discovery, child transcript scan, provider generation, or ledger mutation.
// Each call advances <=8MiB, each record/header <=1MiB, total <=256MiB.
type hostedRecovery struct {
	tailPin      *exactFinalPin
	boundaryPins map[string]*exactFinalPin
	revision     uint64
	lastResult   string
	boundaries   hostedBoundaries
	mu           sync.Mutex
	scope        agentactivity.NativeScope
	path         string
	info         os.FileInfo
	pos, lineAt  int64
	pending      []byte
	over         bool
	header       *exactFinalPin
	pins         []*exactFinalPin
	status       string
	stats        activitywire.RecoveryStats
	lineBytes    int64
}

func (c *Codex) recoverHostedPrefix(ctx context.Context, s *session.Session, st *hostedRecovery, source *agentactivity.NativeSource) (result string) {
	if !st.mu.TryLock() {
		return "recovering"
	}
	defer st.mu.Unlock()
	oldPos := st.pos
	defer func() {
		if oldPos != st.pos || st.lastResult != result {
			st.revision++
		}
		st.lastResult = result
	}()
	defer func() {
		st.stats.ScannedBytes = st.pos
		st.stats.PartialTailBytes = st.lineBytes
		if result == "source_changed" || result == "native_source_rejected" {
			st.status = result
			source.Quarantine()
		}
	}()
	if st.status == "source_changed" || st.status == "bound_reached" {
		return st.status
	}
	rpc := hostedActivityRPC{codexVisibilityRPC{c: c, s: s, authority: st.scope.Authority}}
	if rpc.Scope() != st.scope {
		return "source_changed"
	}
	root, path, e := c.exactIndexedPath(st.scope.RootThread)
	if e != nil {
		return "indexed_source_unavailable"
	}
	confined, f, _, _, e := openExactIndexed(root, path, st.scope.RootThread)
	if e != nil {
		return "indexed_source_unavailable"
	}
	defer confined.Close()
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return "indexed_source_unavailable"
	}
	if st.info != nil && (!os.SameFile(st.info, info) || st.path != path || info.Size() < st.pos || !checkPin(f, st.header)) {
		st.status = "source_changed"
		return st.status
	}
	if !checkPin(f, st.tailPin) {
		return "source_changed"
	}
	for _, p := range st.boundaryPins {
		if !checkPin(f, p) {
			return "source_changed"
		}
	}
	for _, p := range st.pins {
		if !checkPin(f, p) {
			st.status = "source_changed"
			return st.status
		}
	}
	if st.header == nil {
		reader := bufio.NewReaderSize(io.NewSectionReader(f, 0, (1<<20)+1), (1<<20)+1)
		line, e := reader.ReadSlice('\n')
		var v struct {
			Type    string
			Payload struct{ ID string }
		}
		if e != nil || len(line) > 1<<20 || !unambiguousPublicJSON(line) || json.Unmarshal(line, &v) != nil || v.Type != "session_meta" || v.Payload.ID != st.scope.RootThread {
			return "header_unavailable"
		}
		st.header = pin(0, line)
		st.pos = int64(len(line))
		st.lineAt = st.pos
		st.path = path
	}
	st.info = info
	remaining := min(info.Size()-st.pos, 8<<20, (256<<20)-st.pos)
	if remaining <= 0 {
		if st.pos < info.Size() {
			st.status = "bound_reached"
			return st.status
		}
		return "captured_prefix_partial_inventory"
	}
	reader := bufio.NewReaderSize(io.NewSectionReader(f, st.pos, remaining), 65536)
	deadline := time.Now().Add(100 * time.Millisecond)
	for ctx.Err() == nil && time.Now().Before(deadline) {
		chunk, e := reader.ReadSlice('\n')
		st.pos += int64(len(chunk))
		st.lineBytes += int64(len(chunk))
		if !st.over {
			if len(st.pending)+len(chunk) <= 1<<20 {
				st.pending = append(st.pending, chunk...)
			} else {
				st.over = true
				st.pending = nil
			}
		}
		if e == nil {
			st.stats.Records++
			if st.over {
				st.stats.OversizedRecords++
			}
			if !st.over {
				if !unambiguousPublicJSON(st.pending) {
					st.stats.MalformedRecords++
				}
				var terminal struct {
					Type    string
					Payload struct {
						Type   string
						TurnID string `json:"turn_id"`
					}
				}
				if unambiguousPublicJSON(st.pending) && json.Unmarshal(st.pending, &terminal) == nil && terminal.Type == "event_msg" && (terminal.Payload.Type == "task_complete" || terminal.Payload.Type == "turn_aborted") {
					st.boundaries.terminal(st.scope.RootThread, terminal.Payload.TurnID)
					if st.boundaryPins == nil {
						st.boundaryPins = map[string]*exactFinalPin{}
					}
					if _, exists := st.boundaryPins[terminal.Payload.TurnID]; !exists {
						st.boundaryPins[terminal.Payload.TurnID] = pin(st.lineAt, st.pending)
					}
					for turn := range st.boundaryPins {
						if _, exists := st.boundaries.views[turn]; !exists {
							delete(st.boundaryPins, turn)
						}
					}
				}
				var metadata struct {
					Type    string
					Payload struct {
						Type string
						Item struct{ Type string }
					}
				}
				if unambiguousPublicJSON(st.pending) && json.Unmarshal(st.pending, &metadata) == nil && metadata.Type == "event_msg" && metadata.Payload.Type == "item_completed" && metadata.Payload.Item.Type == "SubAgentActivity" {
					if _, err := agentactivity.DecodeRolloutActivity(st.pending, st.scope.RootThread); err != nil {
						st.stats.RejectedNativeRecords++
					}
				}

				if b, err := agentactivity.DecodeRolloutActivity(st.pending, st.scope.RootThread); err == nil {
					if rpc.Scope() != st.scope {
						return "source_changed"
					}
					if source.Observe(st.scope, b) != nil {
						return "native_source_rejected"
					}
					st.pins = append(st.pins, pin(st.lineAt, st.pending))
					st.boundaries.activity(b, st.scope.RootThread)
				}
			}
			st.pending = nil
			st.over = false
			st.lineAt = st.pos
			st.lineBytes = 0
			st.tailPin = nil
		}
		if e != nil && e != bufio.ErrBufferFull {
			if e != io.EOF {
				return "indexed_source_unavailable"
			}
			break
		}
	}
	if st.lineBytes > 0 && !st.over {
		// Only <=1MiB admissible pending records can become metadata. Oversized
		// tails remain skipped until their original newline; never export them.
		data := make([]byte, len(st.pending))
		if _, e := f.ReadAt(data, st.lineAt); e != nil || !bytes.Equal(data, st.pending) {
			return "source_changed"
		}
		st.tailPin = pin(st.lineAt, data)
	} else {
		st.tailPin = nil
	}
	// Pin the last complete boundary even when unrelated, to detect a replaced
	// source between bounded calls; all accepted native records are pinned too.
	if !checkPin(f, st.header) || rpc.Scope() != st.scope {
		return "source_changed"
	}
	for _, p := range st.boundaryPins {
		if !checkPin(f, p) {
			return "source_changed"
		}
	}
	for _, p := range st.pins {
		if !checkPin(f, p) {
			return "source_changed"
		}
	}
	currentRoot, currentPath, e := c.exactIndexedPath(st.scope.RootThread)
	if e != nil || currentRoot != root || currentPath != path {
		return "source_changed"
	}
	newRoot, newFile, _, _, e := openExactIndexed(root, path, st.scope.RootThread)
	if e != nil {
		return "source_changed"
	}
	newInfo, e := newFile.Stat()
	newFile.Close()
	newRoot.Close()
	if e != nil || !os.SameFile(info, newInfo) || newInfo.Size() < st.pos {
		return "source_changed"
	}
	if len(st.pins) > 1024 {
		st.status = "bound_reached"
		return st.status
	}
	if st.pos < info.Size() {
		return "recovering"
	}
	if st.over || len(st.pending) > 0 {
		return "partial_tail"
	}
	return "captured_prefix_partial_inventory"
}
