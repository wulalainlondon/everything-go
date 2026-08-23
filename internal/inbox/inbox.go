// Package inbox persists files pushed from desktop to mobile. Small files are
// stored inline; large files retain a canonical local path so their URL can be
// regenerated from current tunnel/network state on every delivery.
package inbox

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// inlineMaxBytes caps a file we will base64-inline (Python: _PUSH_INLINE_MAX_BYTES).
const inlineMaxBytes = 50 * 1024 * 1024

// ttl drops inbox entries older than this on load / Pending (Python: 7 days).
const ttl = 7 * 24 * time.Hour

// mimeOverrides covers extensions Go's mime package may not know but that the
// app needs for the right open/install intent — the APK case in particular.
var mimeOverrides = map[string]string{
	".apk": "application/vnd.android.package-archive",
}

func randID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return "push_" + hex.EncodeToString(b) // push_<12 hex>, mirrors Python
}

func nowSecs() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// Entry is one persisted file-push record. JSON tags mirror push_registry.py's
// dict so an inbox.json written by either side is interchangeable.
type Entry struct {
	FileID          string   `json:"file_id"`
	Filename        string   `json:"filename"`
	Size            int64    `json:"size"`
	MimeType        string   `json:"mime_type"`
	Data            string   `json:"data,omitempty"` // base64 inline body
	URL             string   `json:"url,omitempty"`
	LocalPath       string   `json:"local_path,omitempty"`
	TargetDeviceIDs []string `json:"target_device_ids"`
	AckedDeviceIDs  []string `json:"acked_device_ids"`
	PushedAt        float64  `json:"pushed_at"`
}

// Item is the client-facing view of an entry, used for both the file_push
// broadcast and the inbox_list reply. PushedAt is omitted on the broadcast
// (matching Python, which only includes it in inbox_list).
type Item struct {
	FileID   string
	Filename string
	Size     int64
	MimeType string
	Data     string
	URL      string
	PushedAt float64
}

func (e *Entry) item(urlBuilder func(string) string) Item {
	url := e.URL
	if e.LocalPath != "" && urlBuilder != nil {
		if info, err := os.Stat(e.LocalPath); err == nil && !info.IsDir() {
			url = urlBuilder(e.LocalPath)
		}
	}
	return Item{
		FileID: e.FileID, Filename: e.Filename, Size: e.Size,
		MimeType: e.MimeType, Data: e.Data, URL: url, PushedAt: e.PushedAt,
	}
}

// Store holds the push registry and persists it. Safe for concurrent use.
type Store struct {
	dir        string // <data_dir>
	mu         sync.Mutex
	reg        map[string]*Entry
	urlBuilder func(string) string
}

// New opens (or creates) the inbox rooted at dataDir, loading inbox.json and
// dropping any entries already past their TTL.
func New(dataDir string) *Store {
	s := &Store{dir: dataDir, reg: map[string]*Entry{}}
	s.load()
	return s
}

func (s *Store) SetURLBuilder(builder func(string) string) {
	s.mu.Lock()
	s.urlBuilder = builder
	s.mu.Unlock()
}

func (s *Store) path() string { return filepath.Join(s.dir, "inbox.json") }

func (s *Store) load() {
	data, err := os.ReadFile(s.path())
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &s.reg)
	if s.reg == nil {
		s.reg = map[string]*Entry{}
	}
	s.gcLocked() // drop expired on load, like Python's load_inbox filter
}

// caller holds s.mu
func (s *Store) save() {
	_ = os.MkdirAll(s.dir, 0o755)
	if data, err := json.Marshal(s.reg); err == nil {
		_ = os.WriteFile(s.path(), data, 0o644)
	}
}

func guessMime(name string) string {
	ext := filepath.Ext(name)
	if m, ok := mimeOverrides[ext]; ok {
		return m
	}
	if m := mime.TypeByExtension(ext); m != "" {
		// Drop any "; charset=..." parameter to match Python's guess_type, which
		// returns the bare type.
		if i := indexByte(m, ';'); i >= 0 {
			return m[:i]
		}
		return m
	}
	return "application/octet-stream"
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// Push reads absPath, base64-inlines it, registers it for every target device
// (all connected devices except the sender) and persists. Returns the stored
// item for broadcast. Files above the inline cap are served from their original
// path instead of being copied into inbox.json.
func (s *Store) Push(absPath, senderDevice string, targets []string) (Item, error) {
	info, err := os.Stat(absPath)
	if err != nil {
		return Item{}, fmt.Errorf("File not found: %s", absPath)
	}
	if info.IsDir() {
		return Item{}, errors.New("path is a directory")
	}
	var encoded, localPath string
	if info.Size() > inlineMaxBytes {
		localPath, err = filepath.EvalSymlinks(absPath)
		if err != nil {
			return Item{}, fmt.Errorf("Push failed: %v", err)
		}
	} else {
		raw, readErr := os.ReadFile(absPath)
		if readErr != nil {
			return Item{}, fmt.Errorf("Push failed: %v", readErr)
		}
		encoded = base64.StdEncoding.EncodeToString(raw)
	}
	name := filepath.Base(absPath)
	e := &Entry{
		FileID:          randID(),
		Filename:        name,
		Size:            info.Size(),
		MimeType:        guessMime(name),
		Data:            encoded,
		LocalPath:       localPath,
		TargetDeviceIDs: append([]string{}, targets...),
		AckedDeviceIDs:  []string{},
		PushedAt:        nowSecs(),
	}
	s.mu.Lock()
	s.reg[e.FileID] = e
	s.save()
	s.mu.Unlock()
	return e.item(s.urlBuilder), nil
}

// Pending returns the un-acked, un-expired items targeted at deviceID (or
// untargeted items), newest-first. Mirrors pending_file_push_items.
func (s *Store) Pending(deviceID string) []Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gcLocked()
	var out []Item
	for _, e := range s.reg {
		target := e.TargetDeviceIDs
		if len(target) > 0 && deviceID != "" && !contains(target, deviceID) {
			continue
		}
		if deviceID != "" && contains(e.AckedDeviceIDs, deviceID) {
			continue
		}
		if e.Data == "" && e.URL == "" && e.LocalPath == "" {
			continue // nothing deliverable
		}
		item := e.item(s.urlBuilder)
		if item.Data == "" && item.URL == "" {
			continue
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PushedAt > out[j].PushedAt })
	return out
}

// Ack records that deviceID received fileID and, once every target device (or
// any device, if untargeted) has acked, deletes the entry. Returns true if the
// entry was deleted. Mirrors handle_file_push_ack (minus the Storage blob
// delete, which Go never creates).
func (s *Store) Ack(fileID, deviceID string) (deleted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.reg[fileID]
	if e == nil {
		return false
	}
	if deviceID != "" && !contains(e.AckedDeviceIDs, deviceID) {
		e.AckedDeviceIDs = append(e.AckedDeviceIDs, deviceID)
		sort.Strings(e.AckedDeviceIDs)
	}
	should := false
	if len(e.TargetDeviceIDs) > 0 {
		should = subset(e.TargetDeviceIDs, e.AckedDeviceIDs)
	} else {
		should = len(e.AckedDeviceIDs) > 0
	}
	if !should {
		s.save()
		return false
	}
	delete(s.reg, fileID)
	s.save()
	return true
}

// gcLocked drops entries past their TTL. Caller holds mu.
func (s *Store) gcLocked() {
	cutoff := nowSecs() - ttl.Seconds()
	changed := false
	for id, e := range s.reg {
		if e.PushedAt < cutoff {
			delete(s.reg, id)
			changed = true
		}
	}
	if changed {
		s.save()
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// subset reports whether every element of need is present in have.
func subset(need, have []string) bool {
	for _, n := range need {
		if !contains(have, n) {
			return false
		}
	}
	return true
}
