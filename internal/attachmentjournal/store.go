// Package attachmentjournal persists bridge-discovered media and documents.
// Records contain stable local facts only; delivery URLs are intentionally
// materialized by core at send time because tunnel addresses are ephemeral.
package attachmentjournal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"everything-go/internal/protocol"
)

const (
	maxRecords = 10000
	ttl        = 30 * 24 * time.Hour
)

// Record is the canonical, URL-free representation of one attachment mention.
// RequestID is part of the identity, so the same path referenced by different
// turns remains two independently ordered attachments.
type Record struct {
	AttachmentID  string          `json:"attachment_id"`
	SessionID     string          `json:"session_id"`
	RequestID     string          `json:"request_id,omitempty"`
	Kind          string          `json:"kind"` // media | document
	Path          string          `json:"path"`
	MIMEType      string          `json:"mime_type"`
	DisplayName   string          `json:"display_name,omitempty"`
	MediaType     string          `json:"media_type,omitempty"`
	DocumentType  string          `json:"document_type,omitempty"`
	CreatedAt     int64           `json:"created_at"`
	Sequence      uint64          `json:"sequence"`
	AckedByDevice map[string]bool `json:"acked_by_device,omitempty"`
}

type snapshot struct {
	NextSequence uint64    `json:"next_sequence"`
	Records      []*Record `json:"records"`
}

// Store is a bounded persistent attachment registry. Acknowledgements are
// retained per device; one device can never consume another device's cursor.
type Store struct {
	mu           sync.Mutex
	path         string
	nextSequence uint64
	records      []*Record
	byID         map[string]*Record
	now          func() time.Time
}

func New(dataDir string) *Store {
	s := &Store{byID: make(map[string]*Record), now: time.Now}
	if dataDir != "" {
		s.path = filepath.Join(dataDir, "attachment_journal.json")
	}
	s.load()
	return s
}

// Add canonicalizes a media/document event. added=false means the exact same
// session/request/path attachment was already registered (idempotent rescan).
func (s *Store) Add(event any) (Record, bool) {
	r, ok := recordFromEvent(event)
	if !ok {
		return Record{}, false
	}
	r.AttachmentID = stableID(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.byID[r.AttachmentID]; existing != nil {
		return cloneRecord(existing), false
	}
	s.gcLocked()
	s.nextSequence++
	r.Sequence = s.nextSequence
	r.CreatedAt = s.now().UnixMilli()
	r.AckedByDevice = make(map[string]bool)
	stored := r
	s.records = append(s.records, &stored)
	s.byID[r.AttachmentID] = &stored
	s.enforceCapLocked()
	s.saveLocked()
	return cloneRecord(&stored), true
}

// Pending returns a device's unacknowledged records for one session in
// canonical order. Replays are session-scoped so restoring one conversation
// can never ACK attachments belonging to another conversation that is not yet
// present in the client UI.
func (s *Store) Pending(deviceID, sessionID string, limit int) []Record {
	if deviceID == "" || sessionID == "" || limit <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gcLocked() {
		s.saveLocked()
	}
	out := make([]Record, 0, limit)
	for _, r := range s.records {
		if r.SessionID != sessionID {
			continue
		}
		if r.AckedByDevice[deviceID] {
			continue
		}
		out = append(out, cloneRecord(r))
		if len(out) == limit {
			break
		}
	}
	return out
}

// Ack commits only the named device's delivery state.
func (s *Store) Ack(deviceID string, attachmentIDs []string) int {
	if deviceID == "" || len(attachmentIDs) == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	committed := 0
	for _, id := range attachmentIDs {
		r := s.byID[id]
		if r == nil || r.AckedByDevice[deviceID] {
			continue
		}
		if r.AckedByDevice == nil {
			r.AckedByDevice = make(map[string]bool)
		}
		r.AckedByDevice[deviceID] = true
		committed++
	}
	if committed > 0 {
		s.saveLocked()
	}
	return committed
}

func recordFromEvent(event any) (Record, bool) {
	switch e := event.(type) {
	case protocol.Media:
		return Record{SessionID: e.SessionID, RequestID: e.RequestID, Kind: "media", Path: e.Path,
			MIMEType: mimeForMedia(e.MediaType, e.Path), DisplayName: filepath.Base(e.Path), MediaType: e.MediaType}, e.Path != ""
	case protocol.Document:
		name := e.Title
		if name == "" {
			name = filepath.Base(e.Path)
		}
		return Record{SessionID: e.SessionID, RequestID: e.RequestID, Kind: "document", Path: e.Path,
			MIMEType: mimeForDocument(e.DocType), DisplayName: name, DocumentType: e.DocType}, e.Path != ""
	default:
		return Record{}, false
	}
}

func stableID(r Record) string {
	sum := sha256.Sum256([]byte(r.SessionID + "\x00" + r.RequestID + "\x00" + r.Kind + "\x00" + r.Path))
	return "att_" + hex.EncodeToString(sum[:12])
}

func mimeForMedia(kind, path string) string {
	if kind == "video" {
		switch filepath.Ext(path) {
		case ".mov":
			return "video/quicktime"
		default:
			return "video/mp4"
		}
	}
	switch filepath.Ext(path) {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	default:
		return "image/png"
	}
}

func mimeForDocument(kind string) string {
	if kind == "pdf" {
		return "application/pdf"
	}
	return "text/html"
}

func cloneRecord(r *Record) Record {
	out := *r
	out.AckedByDevice = make(map[string]bool, len(r.AckedByDevice))
	for id, acked := range r.AckedByDevice {
		out.AckedByDevice[id] = acked
	}
	return out
}

func (s *Store) load() {
	if s.path == "" {
		return
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	var snap snapshot
	if json.Unmarshal(data, &snap) != nil {
		return
	}
	s.nextSequence = snap.NextSequence
	for _, r := range snap.Records {
		if r == nil || r.AttachmentID == "" || r.Path == "" {
			continue
		}
		if r.AckedByDevice == nil {
			r.AckedByDevice = make(map[string]bool)
		}
		s.records = append(s.records, r)
		s.byID[r.AttachmentID] = r
		if r.Sequence > s.nextSequence {
			s.nextSequence = r.Sequence
		}
	}
	s.gcLocked()
	s.enforceCapLocked()
}

// caller holds s.mu (or load is still single-threaded).
func (s *Store) gcLocked() bool {
	cutoff := s.now().Add(-ttl).UnixMilli()
	kept := s.records[:0]
	changed := false
	for _, r := range s.records {
		if r.CreatedAt < cutoff {
			delete(s.byID, r.AttachmentID)
			changed = true
			continue
		}
		kept = append(kept, r)
	}
	s.records = kept
	return changed
}

func (s *Store) enforceCapLocked() {
	if len(s.records) <= maxRecords {
		return
	}
	sort.SliceStable(s.records, func(i, j int) bool { return s.records[i].Sequence < s.records[j].Sequence })
	drop := len(s.records) - maxRecords
	for _, r := range s.records[:drop] {
		delete(s.byID, r.AttachmentID)
	}
	s.records = s.records[drop:]
}

func (s *Store) saveLocked() {
	if s.path == "" {
		return
	}
	if os.MkdirAll(filepath.Dir(s.path), 0o700) != nil {
		return
	}
	data, err := json.Marshal(snapshot{NextSequence: s.nextSequence, Records: s.records})
	if err != nil {
		return
	}
	tmp := s.path + ".tmp"
	if os.WriteFile(tmp, data, 0o600) == nil {
		_ = os.Rename(tmp, s.path)
	}
}
