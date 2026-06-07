package core

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"everything-go/internal/clientproto"
	"everything-go/internal/session"
)

// fork_session copies a session's JSONL transcript into a brand-new independent
// session, optionally truncated at a chosen message. Mirrors
// bridge/handlers/fork_ops.py. The fork is a real new resume id (a fresh .jsonl
// in the same project dir), so resuming it never touches the parent's history.

// transcriptLocator is the optional capability a history provider implements to
// point at a session's on-disk transcript (the Claude backend does; Codex/Ollama
// in Go do not, so those sessions fork-fail with no_history_file).
type transcriptLocator interface {
	TranscriptPath(resumeID string) (string, bool)
}

// transcriptPath resolves the parent session's .jsonl via its backend provider.
func (h *Hub) transcriptPath(s *session.Session) (string, bool) {
	hr, ok := h.exec.(historyRouter)
	if !ok {
		return "", false
	}
	prov, ok := hr.ProviderFor(s)
	if !ok {
		return "", false
	}
	loc, ok := prov.(transcriptLocator)
	if !ok {
		return "", false
	}
	return loc.TranscriptPath(s.ResumeID())
}

func (h *Hub) handleFork(c *Client, cmd clientproto.Command) {
	parentID := cmd.SessionID
	parent, ok := h.registry.Get(parentID)
	if !ok {
		c.enqueueEvent(h.client.Error(parentID, "", "Unknown session: "+parentID))
		return
	}
	// A fork copies the transcript file; forking mid-turn would race the backend
	// still appending to it. Refuse while the parent has a turn in flight.
	if parent.IsStreaming() {
		c.enqueueEvent(h.client.ForkError(parentID, "parent_busy"))
		return
	}
	src, ok := h.transcriptPath(parent)
	if !ok {
		c.enqueueEvent(h.client.ForkError(parentID, "no_history_file"))
		return
	}

	newResumeID := newUUID()
	dstPath := filepath.Join(filepath.Dir(src), newResumeID+".jsonl")
	tmpPath := dstPath + ".tmp"

	if after := cmd.ForkAfterMessageID; after != "" {
		offset, found := findForkOffset(src, after)
		if !found {
			c.enqueueEvent(h.client.ForkError(parentID, "fork_point_not_found"))
			return
		}
		if err := copyPrefix(src, tmpPath, offset); err != nil {
			_ = os.Remove(tmpPath)
			c.enqueueEvent(h.client.ForkError(parentID, "copy_failed: "+err.Error()))
			return
		}
	} else if err := copyFileContents(src, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		c.enqueueEvent(h.client.ForkError(parentID, "copy_failed: "+err.Error()))
		return
	}
	if err := os.Rename(tmpPath, dstPath); err != nil {
		_ = os.Remove(tmpPath)
		c.enqueueEvent(h.client.ForkError(parentID, "copy_failed: "+err.Error()))
		return
	}

	snap := parent.Snapshot()
	name := cmd.Name
	if name == "" {
		name = snap.Name + " (fork)"
	}
	newID := "s_" + randHex(4)
	fork := h.registry.Create(newID, name, snap.Cwd, snap.Backend, snap.Model, snap.Sandbox, newResumeID)
	if snap.Effort != "" {
		fork.SetEffort(snap.Effort)
	}
	go h.registry.Persist()

	fsnap := fork.Snapshot()
	log.Printf("[fork] %s → %s (resume=%s, after=%q)", parentID, newID, newResumeID, cmd.ForkAfterMessageID)
	// Announce the new session, then the refreshed list (parity with fork_ops.py).
	h.Emit(h.client.SessionForked(newID, parentID, fsnap.Name, fsnap.CreatedAt))
	h.Emit(h.client.SessionsList(h.sessionSummaries()))
}

// findForkOffset returns the byte offset just past the line identified by
// sourceMessageID. The claude_cli backend ids messages as "claude:<uuid>:line:<N>"
// (1-indexed); for that form we count bytes to line N. Otherwise we match a
// JSONL row by its uuid/id field. Mirrors find_fork_byte_offset.
func findForkOffset(path, sourceMessageID string) (int64, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer f.Close()
	r := bufio.NewReader(f)

	if i := strings.LastIndex(sourceMessageID, ":line:"); i >= 0 {
		target, err := strconv.Atoi(sourceMessageID[i+len(":line:"):])
		if err != nil {
			return 0, false
		}
		var offset int64
		lineNo := 0
		for {
			line, rerr := r.ReadBytes('\n')
			if len(line) > 0 {
				lineNo++
				offset += int64(len(line))
				if lineNo == target {
					return offset, true
				}
			}
			if rerr != nil {
				break
			}
		}
		return 0, false
	}

	// Fallback: match by uuid field in the row content.
	var offset int64
	for {
		line, rerr := r.ReadBytes('\n')
		if len(line) > 0 {
			offset += int64(len(line))
			if rowMatchesID(line, sourceMessageID) {
				return offset, true
			}
		}
		if rerr != nil {
			break
		}
	}
	return 0, false
}

// rowMatchesID checks a JSONL row's uuid / message.id / id against want.
func rowMatchesID(line []byte, want string) bool {
	var row struct {
		UUID    string `json:"uuid"`
		ID      string `json:"id"`
		Message struct {
			ID string `json:"id"`
		} `json:"message"`
	}
	if err := json.Unmarshal(line, &row); err != nil {
		return false
	}
	switch want {
	case row.UUID, row.Message.ID, row.ID:
		return want != ""
	}
	return false
}

// copyPrefix writes the first n bytes of src to dst.
func copyPrefix(src, dst string, n int64) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.CopyN(out, in, n); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// copyFileContents copies the whole of src to dst.
func copyFileContents(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// newUUID returns a random RFC 4122 v4 UUID string (the new resume id).
func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// randHex returns n random bytes hex-encoded (2n chars).
func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
