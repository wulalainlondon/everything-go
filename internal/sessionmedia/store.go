// Package sessionmedia stores rebuildable, conversation-bound media projections.
// It never grants access: callers must validate the current session on every read.
package sessionmedia

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp"
	_ "modernc.org/sqlite"
)

const MaxImageBytes = 20 << 20
const maxVaultBytes = 2 << 30

type Entry struct {
	ID           string `json:"id"`
	Message      string `json:"message_id"`
	Thread       string `json:"thread"`
	Request      string `json:"request_id,omitempty"`
	Author       string `json:"author"`
	Provenance   string `json:"provenance"`
	Timestamp    int64  `json:"timestamp"`
	Ordinal      int    `json:"ordinal"`
	Sort         string `json:"-"`
	MIME         string `json:"mime_type"`
	SHA256       string `json:"sha256,omitempty"`
	Bytes        int64  `json:"size_bytes"`
	Width        int    `json:"width"`
	Height       int    `json:"height"`
	Availability string `json:"availability"`
	VersionKnown bool   `json:"version_known"`
}
type Source struct {
	Path       string `json:"path,omitempty"`
	Attachment string `json:"attachment,omitempty"`
	Hash       string `json:"hash,omitempty"`
}
type Record struct {
	Entry  Entry
	Source Source
}
type Checkpoint struct {
	Binding   string `json:"binding"`
	Epoch     string `json:"epoch"`
	Secret    string `json:"secret"`
	Before    string `json:"before"`
	Last      string `json:"last"`
	Started   bool   `json:"started"`
	Complete  bool   `json:"complete"`
	MoreAfter bool   `json:"more_after"`
	Revision  uint64 `json:"revision"`
}
type Store struct {
	free func(string) (uint64, error)
	mu   sync.Mutex
	root string
	db   *sql.DB
}

func New(root string) *Store { return &Store{root: root, free: availableBytes} }
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
func (s *Store) open() error {
	if s.db != nil {
		return nil
	}
	if s.root == "" {
		return errors.New("gallery storage unavailable")
	}
	if e := os.MkdirAll(filepath.Join(s.root, "blobs"), 0700); e != nil {
		return e
	}
	db, e := sql.Open("sqlite", filepath.Join(s.root, "index.sqlite"))
	if e != nil {
		return e
	}
	db.SetMaxOpenConns(1)
	_, e = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=3000;
 CREATE TABLE IF NOT EXISTS checkpoints(session TEXT PRIMARY KEY, value BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS pictures(session TEXT, id TEXT, ordering TEXT, author TEXT, entry BLOB, source BLOB, PRIMARY KEY(session,id));
 CREATE INDEX IF NOT EXISTS picture_order ON pictures(session,ordering DESC);
 CREATE TABLE IF NOT EXISTS assets(hash TEXT PRIMARY KEY, bytes INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS captures(identity TEXT PRIMARY KEY, hash TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS derivatives(original TEXT,variant TEXT,version INTEGER,digest TEXT,bytes INTEGER,PRIMARY KEY(original,variant));`)
	if e != nil {
		db.Close()
		return e
	}
	_ = os.Chmod(filepath.Join(s.root, "index.sqlite"), 0600)
	s.db = db
	return nil
}
func Random() string {
	var b [24]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func ID(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(h[:])
}
func (s *Store) Load(session, binding string) (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked(session, binding)
}
func (s *Store) loadLocked(session, binding string) (Checkpoint, error) {
	if e := s.open(); e != nil {
		return Checkpoint{}, e
	}
	var raw []byte
	e := s.db.QueryRow("SELECT value FROM checkpoints WHERE session=?", session).Scan(&raw)
	if e != nil && e != sql.ErrNoRows {
		return Checkpoint{}, e
	}
	var c Checkpoint
	if len(raw) > 0 && json.Unmarshal(raw, &c) != nil {
		return c, errors.New("gallery checkpoint invalid")
	}
	if len(raw) > 0 {
		if c.Binding == "" || len(c.Epoch) != 48 || len(c.Secret) != 48 {
			return c, errors.New("gallery checkpoint invalid")
		}
		if _, e := hex.DecodeString(c.Epoch + c.Secret); e != nil {
			return c, errors.New("gallery checkpoint invalid")
		}
	}
	if c.Binding != binding {
		c = Checkpoint{Binding: binding, Epoch: Random(), Secret: Random()}
		tx, e := s.db.Begin()
		if e != nil {
			return c, e
		}
		defer tx.Rollback()
		if _, e = tx.Exec("DELETE FROM pictures WHERE session=?", session); e != nil {
			return c, e
		}
		raw, _ := json.Marshal(c)
		if _, e = tx.Exec("INSERT OR REPLACE INTO checkpoints VALUES(?,?)", session, raw); e != nil {
			return c, e
		}
		return c, tx.Commit()
	}
	return c, nil
}

// Commit the entire source page and its next checkpoint in one transaction.
func (s *Store) Commit(session string, c Checkpoint, records []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitLocked(session, c, records, nil)
}
func (s *Store) CommitCAS(session string, expected, c Checkpoint, records []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitLocked(session, c, records, &expected)
}
func (s *Store) commitLocked(session string, c Checkpoint, records []Record, expected *Checkpoint) error {
	if e := s.open(); e != nil {
		return e
	}
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var old []byte
	e = tx.QueryRow("SELECT value FROM checkpoints WHERE session=?", session).Scan(&old)
	if e != nil && e != sql.ErrNoRows {
		return e
	}
	var previous Checkpoint
	_ = json.Unmarshal(old, &previous)
	if expected != nil && (previous.Epoch != expected.Epoch || previous.Binding != expected.Binding || previous.Revision != expected.Revision) {
		return errors.New("gallery checkpoint changed")
	}
	c.Revision = previous.Revision + 1
	if previous.Binding != c.Binding {
		if _, e = tx.Exec("DELETE FROM pictures WHERE session=?", session); e != nil {
			return e
		}
	}
	for _, r := range records {
		b, e := json.Marshal(r.Entry)
		if e != nil {
			return e
		}
		src, _ := json.Marshal(r.Source)
		if _, e = tx.Exec("INSERT INTO pictures VALUES(?,?,?,?,?,?) ON CONFLICT(session,id) DO UPDATE SET ordering=excluded.ordering, author=excluded.author, entry=excluded.entry, source=excluded.source", session, r.Entry.ID, r.Entry.Sort, r.Entry.Author, b, src); e != nil {
			return e
		}
	}
	raw, _ := json.Marshal(c)
	if _, e = tx.Exec("INSERT OR REPLACE INTO checkpoints VALUES(?,?)", session, raw); e != nil {
		return e
	}
	return tx.Commit()
}

type cursor struct {
	Epoch  string `json:"e"`
	Filter string `json:"f"`
	After  string `json:"a"`
	High   string `json:"h"`
}

func cursorMAC(c Checkpoint, session, body string) []byte {
	h := hmac.New(sha256.New, []byte(c.Secret))
	h.Write([]byte(session + "\x00" + body))
	return h.Sum(nil)
}
func (s *Store) List(session string, c Checkpoint, filter, token string, limit int) ([]Entry, string, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listLocked(session, c, filter, token, limit)
}

// Every component of a response is captured under the same store lock/read view.
func (s *Store) View(session, binding, filter, token string, limit int) (Checkpoint, []Entry, string, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, e := s.loadLocked(session, binding)
	if e != nil {
		return c, nil, "", 0, e
	}
	items, next, count, e := s.listLocked(session, c, filter, token, limit)
	return c, items, next, count, e
}
func (s *Store) listLocked(session string, c Checkpoint, filter, token string, limit int) ([]Entry, string, int, error) {
	if e := s.open(); e != nil {
		return nil, "", 0, e
	}
	if filter != "all" && filter != "user" && filter != "assistant" {
		return nil, "", 0, errors.New("invalid filter")
	}
	cur := cursor{Epoch: c.Epoch, Filter: filter, High: "~", After: "~"}
	if token != "" {
		parts := strings.Split(token, ".")
		if len(parts) != 2 || len(token) > 4096 {
			return nil, "", 0, errors.New("invalid cursor")
		}
		raw, e := base64.RawURLEncoding.DecodeString(parts[0])
		mac, e2 := base64.RawURLEncoding.DecodeString(parts[1])
		if e != nil || e2 != nil || !hmac.Equal(mac, cursorMAC(c, session, parts[0])) || json.Unmarshal(raw, &cur) != nil || cur.Epoch != c.Epoch || cur.Filter != filter {
			return nil, "", 0, errors.New("stale cursor")
		}
	}
	if token != "" && cur.High != "~" {
		var newer int
		if e := s.db.QueryRow("SELECT count(*) FROM pictures WHERE session=? AND author LIKE ? AND ordering>?", session, map[string]string{"all": "%", "user": "user", "assistant": "assistant"}[filter], cur.High).Scan(&newer); e != nil {
			return nil, "", 0, e
		}
		if newer > 0 {
			return nil, "", 0, errors.New("gallery updated; refresh required")
		}
	}
	if limit < 1 || limit > 60 {
		limit = 60
	}
	author := "%"
	if filter != "all" {
		author = filter
	}
	rows, e := s.db.Query("SELECT ordering,entry FROM pictures WHERE session=? AND author LIKE ? AND ordering<? AND ordering<=? ORDER BY ordering DESC LIMIT ?", session, author, cur.After, cur.High, limit+1)
	if e != nil {
		return nil, "", 0, e
	}
	items := []Entry{}
	orders := []string{}
	for rows.Next() {
		var order string
		var raw []byte
		if e = rows.Scan(&order, &raw); e != nil {
			rows.Close()
			return nil, "", 0, e
		}
		var entry Entry
		if e = json.Unmarshal(raw, &entry); e != nil {
			rows.Close()
			return nil, "", 0, e
		}
		items = append(items, entry)
		orders = append(orders, order)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, "", 0, e
	}
	more := len(items) > limit
	if more {
		items = items[:limit]
		orders = orders[:limit]
	}
	var count int
	if e = s.db.QueryRow("SELECT count(*) FROM pictures WHERE session=? AND author LIKE ?", session, author).Scan(&count); e != nil {
		return nil, "", 0, e
	}
	if token == "" && len(orders) > 0 {
		cur.High = orders[0]
	}
	if len(orders) > 0 {
		cur.After = orders[len(orders)-1]
	}
	next := ""
	if more || !c.Complete || c.MoreAfter {
		b, _ := json.Marshal(cur)
		body := base64.RawURLEncoding.EncodeToString(b)
		next = body + "." + base64.RawURLEncoding.EncodeToString(cursorMAC(c, session, body))
	}
	return items, next, count, nil
}
func (s *Store) Get(session, id string) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.open(); e != nil {
		return Record{}, e
	}
	var raw, src []byte
	var r Record
	e := s.db.QueryRow("SELECT entry,source FROM pictures WHERE session=? AND id=?", session, id).Scan(&raw, &src)
	if e != nil {
		return r, e
	}
	if e = json.Unmarshal(raw, &r.Entry); e == nil {
		e = json.Unmarshal(src, &r.Source)
	}
	return r, e
}
func (s *Store) Read(hash string) ([]byte, error) {
	if len(hash) != 64 {
		return nil, errors.New("invalid asset")
	}
	if _, e := hex.DecodeString(hash); e != nil {
		return nil, e
	}
	b, e := readRegular(filepath.Join(s.root, "blobs", hash))
	if e != nil {
		return nil, e
	}
	if IDBytes(b) != hash {
		return nil, errors.New("asset changed")
	}
	return b, nil
}
func IDBytes(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func readRegular(path string) ([]byte, error) {
	before, e := os.Lstat(path)
	if e != nil {
		return nil, e
	}
	if !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > MaxImageBytes {
		return nil, errors.New("image size or file type unsupported")
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !os.SameFile(before, opened) {
		return nil, errors.New("image changed")
	}
	b, e := io.ReadAll(io.LimitReader(f, MaxImageBytes+1))
	after, se := os.Lstat(path)
	if e != nil || se != nil || len(b) > MaxImageBytes || int64(len(b)) != before.Size() || !os.SameFile(before, after) || after.Size() != before.Size() || after.ModTime() != before.ModTime() {
		return nil, errors.New("image changed")
	}
	return b, nil
}
func ImageInfo(b []byte) (string, int, int, error) {
	c, kind, e := image.DecodeConfig(bytes.NewReader(b))
	if e != nil || c.Width <= 0 || c.Height <= 0 || int64(c.Width)*int64(c.Height) > 64000000 {
		return "", 0, 0, errors.New("image dimensions unsupported")
	}
	mime := map[string]string{"jpeg": "image/jpeg", "png": "image/png", "webp": "image/webp", "gif": "image/gif"}[kind]
	if mime == "" {
		return "", 0, 0, errors.New("image format unsupported")
	}
	return mime, c.Width, c.Height, nil
}
func (s *Store) Put(b []byte) (string, error) {
	if len(b) == 0 || len(b) > MaxImageBytes {
		return "", errors.New("image size unsupported")
	}
	mime, _, _, e := ImageInfo(b)
	if e != nil {
		return "", e
	}
	if e = validateImage(b, mime); e != nil {
		return "", errors.New("image incomplete or undecodable")
	}
	hash := IDBytes(b)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.open(); e != nil {
		return "", e
	}
	var n int
	if e := s.db.QueryRow("SELECT count(*) FROM assets WHERE hash=?", hash).Scan(&n); e != nil {
		return "", e
	}
	if n > 0 {
		if existing, e := readRegular(filepath.Join(s.root, "blobs", hash)); e == nil && IDBytes(existing) == hash {
			return hash, nil
		}
	}
	free, e := s.free(s.root)
	if e != nil {
		return "", e
	}
	if free < uint64((512<<20)+len(b)+(80<<10)) {
		return "", errors.New("gallery disk reserve unavailable")
	}
	if n == 0 {
		var total int64
		if e := s.db.QueryRow("SELECT COALESCE(sum(bytes + 81920),0) FROM assets").Scan(&total); e != nil {
			return "", e
		}
		var previews int64
		if e := s.db.QueryRow("SELECT COALESCE(sum(bytes),0) FROM derivatives WHERE variant='preview'").Scan(&previews); e != nil {
			return "", e
		}
		if total+previews+int64(len(b))+81920 > maxVaultBytes {
			return "", errors.New("gallery retained storage full")
		}
		if _, e := s.db.Exec("INSERT INTO assets VALUES(?,?)", hash, len(b)); e != nil {
			return "", e
		}
	}
	f, e := os.CreateTemp(filepath.Join(s.root, "blobs"), ".capture-")
	if e != nil {
		return "", e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	_ = f.Chmod(0600)
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e == nil {
		e = os.Rename(tmp, filepath.Join(s.root, "blobs", hash))
	}
	return hash, e
}

// Capture freezes a trusted attachment mention once; replay cannot adopt new
// bytes at a reused path. Historical reads and live output share this identity.
func (s *Store) Capture(session, request, path string) (string, error) {
	identity := ID(session, request, path)
	s.mu.Lock()
	if e := s.open(); e != nil {
		s.mu.Unlock()
		return "", e
	}
	var hash string
	e := s.db.QueryRow("SELECT hash FROM captures WHERE identity=?", identity).Scan(&hash)
	s.mu.Unlock()
	if e == nil {
		return hash, nil
	}
	if e != sql.ErrNoRows {
		return "", e
	}
	b, e := readRegular(path)
	if e != nil {
		return "", e
	}
	hash, e = s.Put(b)
	if e != nil {
		return "", e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, e = s.db.Exec("INSERT OR IGNORE INTO captures VALUES(?,?)", identity, hash)
	if e != nil {
		return "", e
	}
	e = s.db.QueryRow("SELECT hash FROM captures WHERE identity=?", identity).Scan(&hash)
	return hash, e
}
func Thumbnail(b []byte) ([]byte, error) {
	_, w, h, e := ImageInfo(b)
	if e != nil {
		return nil, e
	}
	src, _, e := image.Decode(bytes.NewReader(b))
	if e != nil {
		return nil, e
	}
	if w > 320 || h > 320 {
		if w >= h {
			h = max(1, h*320/w)
			w = 320
		} else {
			w = max(1, w*320/h)
			h = 320
		}
	}
	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	draw.ApproxBiLinear.Scale(dst, dst.Bounds(), src, src.Bounds(), draw.Src, nil)
	var out bytes.Buffer
	if e = jpeg.Encode(&out, dst, &jpeg.Options{Quality: 78}); e != nil {
		return nil, e
	}
	if out.Len() > 80<<10 {
		return nil, fmt.Errorf("thumbnail exceeds budget")
	}
	return out.Bytes(), nil
}

// Thumbnail is a reconstructable derivative, covered by the original asset's
// reserved storage budget. A missing cache never changes the original version.
func (s *Store) Thumbnail(hash string, b []byte) ([]byte, error) {
	if IDBytes(b) != hash {
		return nil, errors.New("original digest mismatch")
	}
	path := filepath.Join(s.root, "blobs", hash+".thumb.jpg")
	if cached, e := readRegular(path); e == nil && len(cached) <= 80<<10 {
		if mime, _, _, e := ImageInfo(cached); e == nil && mime == "image/jpeg" {
			return cached, nil
		}
	}
	thumb, e := Thumbnail(b)
	if e != nil {
		return nil, e
	}
	f, e := os.CreateTemp(filepath.Join(s.root, "blobs"), ".thumbnail-")
	if e != nil {
		return thumb, nil
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	_ = f.Chmod(0600)
	_, e = f.Write(thumb)
	ce := f.Close()
	if e == nil && ce == nil {
		_ = os.Rename(tmp, path)
	}
	return thumb, nil
}
