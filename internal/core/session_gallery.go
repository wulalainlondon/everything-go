package core

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/session"
	"everything-go/internal/sessionmedia"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
)

type sessionGallery struct {
	store       *sessionmedia.Store
	scan        chan struct{}
	jobsMu      sync.Mutex
	jobs        map[string]*galleryScanJob
	subscribers int
	decode      chan struct{}
	io          galleryIOAdmission
}

func (h *Hub) sessionGallery() *sessionGallery {
	h.galleryInit.Do(func() {
		root := ""
		if h.cfg.DataDir != "" {
			root = filepath.Join(h.cfg.DataDir, "session-media")
		}
		h.gallery = &sessionGallery{store: sessionmedia.New(root), decode: make(chan struct{}, 1), scan: make(chan struct{}, 1), jobs: map[string]*galleryScanJob{}}
	})
	return h.gallery
}

type galleryBinding struct {
	provider backend.HistoryProvider
	threads  []string
	key      string
	revision string
}

func (h *Hub) galleryBinding(s *session.Session) (galleryBinding, error) {
	router, ok := h.exec.(historyRouter)
	if !ok {
		return galleryBinding{}, errors.New("history unsupported")
	}
	provider, ok := router.ProviderFor(s)
	if !ok {
		return galleryBinding{}, errors.New("history unsupported")
	}
	generations, ok := provider.(historyGenerationProvider)
	if !ok {
		return galleryBinding{}, errors.New("history source identity unsupported")
	}
	ids := s.ResumeIDs()
	if len(ids) == 0 {
		return galleryBinding{}, errors.New("history source not ready")
	}
	snap := s.Snapshot()
	parts := []string{h.cfg.InstanceID, s.ID, snap.Backend, snap.ResumeID, strconv.FormatUint(snap.SourceRevision, 10)}
	witnesses := []string{}
	revisions, supported := provider.(interface{ HistorySourceRevision(string) (string, error) })
	for _, id := range ids {
		g, e := generations.HistorySourceGeneration(id)
		if e != nil || g == "" {
			return galleryBinding{}, errors.New("history source unavailable")
		}
		parts = append(parts, id, g)
		if supported {
			v, err := revisions.HistorySourceRevision(id)
			if err != nil || v == "" {
				supported = false
			} else {
				witnesses = append(witnesses, id, v)
			}
		}
	}
	revision := ""
	if supported {
		revision = sessionmedia.ID(witnesses...)
	}
	return galleryBinding{provider: provider, threads: ids, key: sessionmedia.ID(parts...), revision: revision}, nil
}
func (h *Hub) serveSessionGallery(w http.ResponseWriter, r *http.Request, operation string) {
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Access-Control-Expose-Headers", "X-Bridge-Image-SHA256")
	if r.Method != http.MethodGet && !(r.Method == http.MethodPost && (operation == "advance" || operation == "validate")) {
		http.Error(w, "method", 405)
		return
	}
	sid := r.URL.Query().Get("session_id")
	device := h.inboxDevice(r)
	s, e := h.intakeSession(sid)
	if e != nil || device == "" {
		http.Error(w, "session permission", 403)
		return
	}
	binding, e := h.galleryBinding(s)
	if e != nil {
		http.Error(w, e.Error(), 503)
		return
	}
	current := func() bool {
		if r.Context().Err() != nil {
			return false
		}
		original, e := h.intakeSession(sid)
		if e != nil || original != s || h.inboxDevice(r) != device || !reflect.DeepEqual(s.ResumeIDs(), binding.threads) {
			return false
		}
		now, e := h.galleryBinding(s)
		return e == nil && now.key == binding.key
	}
	gallery := h.sessionGallery()
	if operation == "capabilities" {
		if !current() {
			http.Error(w, "source changed", 409)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema": 1, "authority_instance_id": h.cfg.InstanceID, "session_id": sid, "loading_version": 1, "indexed_list": binding.revision != "", "status": binding.revision != "", "advance": binding.revision != "", "validate": binding.revision != "", "preview": binding.revision != ""})
		return
	}
	if operation == "list" || operation == "status" || operation == "advance" {
		filter := r.URL.Query().Get("filter")
		if filter == "" {
			filter = "all"
		}
		token := r.URL.Query().Get("cursor")
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		// Reject a stale cursor before a legacy request can advance anything.
		if token != "" {
			if _, _, _, _, e := gallery.store.ViewGuarded(sid, binding.key, filter, token, 1, current); e != nil {
				http.Error(w, "gallery cursor changed; refresh", 409)
				return
			}
		}
		if operation == "advance" || operation == "list" && r.URL.Query().Get("mode") != "indexed" {
			if e := h.joinGalleryAdvance(r.Context(), s, binding, current); e != nil {
				w.Header().Set("Retry-After", "1")
				http.Error(w, e.Error(), 503)
				return
			}
		}
		if !current() {
			http.Error(w, "source changed", 409)
			return
		}
		c, items, next, count, e := gallery.store.ViewGuarded(sid, binding.key, filter, token, limit, current)
		if e != nil {
			http.Error(w, "gallery cursor changed; refresh", 409)
			return
		}
		if !current() {
			http.Error(w, "source changed", 409)
			return
		}
		coverage := "indexing"
		if c.Started && c.Complete && !c.MoreAfter {
			coverage = "complete"
		}
		result := map[string]any{"loading_version": 1, "loading_capabilities": map[string]bool{"indexed_list": binding.revision != "", "status": binding.revision != "", "advance": binding.revision != "", "validate": binding.revision != "", "preview": binding.revision != ""}, "schema": 1, "authority_instance_id": h.cfg.InstanceID, "session_id": sid, "snapshot": c.Epoch, "revision": c.Revision, "coverage": coverage, "count": count, "dirty": binding.revision != "" && c.HistoryRevision != binding.revision}
		if operation == "list" {
			result["items"] = items
			result["next_cursor"] = next
			result["has_more"] = next != ""
		}
		_ = json.NewEncoder(w).Encode(result)
		return
	}

	priority := 1
	if operation == "original" || operation == "validate" && r.URL.Query().Get("priority") == "foreground" {
		priority = 0
	}
	releaseIO, e := gallery.io.acquire(r.Context(), binding.key, priority)
	if e != nil {
		w.Header().Set("Retry-After", "1")
		http.Error(w, e.Error(), 503)
		return
	}
	defer releaseIO()
	c, e := gallery.store.CheckpointView(sid, binding.key, current)
	if e != nil || !c.Started || r.URL.Query().Get("snapshot") != c.Epoch {
		http.Error(w, "gallery source changed", 409)
		return
	}
	if operation == "validate" {
		h.validateGallery(w, r, sid, binding, c, current)
		return
	}
	record, e := gallery.store.RecordView(sid, c, r.URL.Query().Get("id"), current)
	if e != nil {
		http.Error(w, "picture not found", 404)
		return
	}
	if operation == "message" {
		// Read the exact native row and its following context, without changing the
		// active chat window, read receipts or event/delivery journals.
		res, e := loadLogicalSessionHistory(binding.provider, binding.threads, history.Opts{Scoped: true, Mode: "continuation", KnownLast: record.Entry.Message, Limit: 12})
		if e != nil || !res.KnownIDFound || len(res.Messages) == 0 || res.Messages[0]["source_message_id"] != record.Entry.Message {
			http.Error(w, "original message unavailable", 404)
			return
		}
		rows := h.projectMediaHistory(s, res.Messages)
		if !current() {
			http.Error(w, "source changed", 409)
			return
		}
		bounded, err := boundScopedHistoryFrame(&history.Result{Messages: rows}, clientproto.Command{Mode: "continuation"})
		if err != nil {
			http.Error(w, "original message exceeds history window budget", 413)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema": 1, "authority_instance_id": h.cfg.InstanceID, "session_id": sid, "message_id": record.Entry.Message, "messages": bounded.Messages, "source_threads": binding.threads})
		return
	}
	if operation != "thumbnail" && operation != "original" && operation != "preview" && operation != "validate" {
		http.NotFound(w, r)
		return
	}

	if record.Source.Hash != "" && (operation == "thumbnail" || operation == "preview") {
		cached, mime, hit, err := gallery.store.VerifiedDerivative(record.Source.Hash, operation)
		if err != nil {
			http.Error(w, "original unavailable or changed", 404)
			return
		}
		if hit {
			h.writeGalleryBytes(w, cached, mime, current)
			return
		}
	}
	if operation != "original" {
		select {
		case gallery.decode <- struct{}{}:
			defer func() { <-gallery.decode }()
		case <-r.Context().Done():
			return
		}
	}

	var b []byte
	if record.Source.Hash != "" {
		b, e = gallery.store.Read(record.Source.Hash)
	} else if record.Source.Attachment != "" {
		// Reuse the original admitted-object relation and its current read guard.
		q := r.URL.Query()
		q.Set("attachment_id", record.Source.Attachment)
		q.Set("sha256", record.Entry.SHA256)
		copyReq := r.Clone(r.Context())
		copyURL := *r.URL
		copyURL.RawQuery = q.Encode()
		copyReq.URL = &copyURL
		capture := newGalleryCapture()
		h.serveMediaAttachment(capture, copyReq)
		if capture.status != 200 {
			http.Error(w, "original attachment unavailable", capture.status)
			return
		}
		b = capture.body.Bytes()
	} else {
		e = errors.New("original unavailable")
	}
	if e != nil || len(b) > sessionmedia.MaxImageBytes || record.Entry.SHA256 != "" && sessionmedia.IDBytes(b) != record.Entry.SHA256 {
		http.Error(w, "original unavailable or changed", 404)
		return
	}
	mime, _, _, e := sessionmedia.ImageInfo(b)
	if e != nil {
		http.Error(w, "image unsupported", 415)
		return
	}
	if operation == "thumbnail" || operation == "preview" {
		b, e = gallery.store.Derivative(sessionmedia.IDBytes(b), b, operation, current)
		mime = "image/jpeg"
		if e != nil {
			http.Error(w, "thumbnail unavailable", 415)
			return
		}
	}
	h.writeGalleryBytes(w, b, mime, current)
}
func (h *Hub) writeGalleryBytes(w http.ResponseWriter, b []byte, mime string, current func() bool) {
	if !current() {
		http.Error(w, "source changed", 409)
		return
	}
	w.Header().Set("Content-Type", mime)
	w.Header().Set("X-Bridge-Image-SHA256", sessionmedia.IDBytes(b))
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	guarded := &mediaReadWriter{ResponseWriter: w, allowed: current}
	for offset := 0; offset < len(b); offset += 64 << 10 {
		if _, e := guarded.Write(b[offset:min(len(b), offset+(64<<10))]); e != nil {
			return
		}
	}
}

type galleryCapture struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func newGalleryCapture() *galleryCapture      { return &galleryCapture{header: http.Header{}, status: 200} }
func (c *galleryCapture) Header() http.Header { return c.header }
func (c *galleryCapture) WriteHeader(n int)   { c.status = n }
func (c *galleryCapture) Write(b []byte) (int, error) {
	if c.body.Len()+len(b) > sessionmedia.MaxImageBytes {
		return 0, errors.New("image too large")
	}
	return c.body.Write(b)
}

func (h *Hub) advanceGallery(s *session.Session, b galleryBinding, c sessionmedia.Checkpoint, current func() bool) (sessionmedia.Checkpoint, error) {
	return h.advanceGalleryCommit(s, b, c, current, func(next sessionmedia.Checkpoint, records []sessionmedia.Record) error {
		return h.sessionGallery().store.CommitCAS(s.ID, c, next, records)
	})
}
func (h *Hub) advanceGalleryCommit(s *session.Session, b galleryBinding, c sessionmedia.Checkpoint, current func() bool, commit func(sessionmedia.Checkpoint, []sessionmedia.Record) error) (sessionmedia.Checkpoint, error) {
	opts := history.Opts{Scoped: true, Mode: "snapshot", Limit: 100}
	if c.Started {
		if !c.Complete {
			opts.Before = c.Before
		} else if c.Last != "" {
			opts.Mode = "continuation"
			opts.KnownLast = c.Last
		}
	}
	page, e := loadLogicalSessionHistory(b.provider, b.threads, opts)
	if e != nil {
		return c, e
	}
	if c.Started && (opts.Before != "" || opts.KnownLast != "") && !page.KnownIDFound {
		return c, errors.New("gallery history anchor changed; refresh required")
	}
	records := []sessionmedia.Record{}
	for _, row := range h.projectMediaHistory(s, page.Messages) {
		if !current() {
			return c, errors.New("gallery source changed")
		}
		candidates, err := h.galleryRecordsGuarded(s, b.threads, row, current)
		if err != nil {
			return c, err
		}
		for _, candidate := range candidates {
			replaced := false
			for i, existing := range records {
				if existing.Entry.ID == candidate.Entry.ID {
					if existing.Entry.Provenance != "generated" {
						records[i] = candidate
					}
					replaced = true
					break
				}
			}
			if !replaced {
				records = append(records, candidate)
			}
		}
	}
	if !c.Started {
		c.Complete = !page.HasMoreBefore
		c.MoreAfter = page.HasMoreAfter
	} else if opts.Before != "" {
		c.Complete = !page.HasMoreBefore
		// Backfill used the initial newest anchor. Reconcile appends before
		// declaring coverage complete, even when the initial tail was closed.
		if c.Complete {
			c.MoreAfter = true
		}
	} else {
		c.MoreAfter = page.HasMoreAfter
	}
	if len(page.Messages) > 0 {
		first, _ := page.Messages[0]["source_message_id"].(string)
		last, _ := page.Messages[len(page.Messages)-1]["source_message_id"].(string)
		if !c.Started || opts.Before != "" {
			if c.Started && first == c.Before {
				return c, errors.New("gallery backfill made no progress")
			}
			c.Before = first
		}
		if !c.Started || opts.Before == "" {
			c.Last = last
		}
	}
	if len(page.Messages) == 0 && (page.HasMoreBefore || page.HasMoreAfter) {
		return c, errors.New("gallery requires a smaller history range")
	}
	c.Started = true
	c.HistoryRevision = b.revision
	if !current() {
		return c, errors.New("gallery source changed")
	}
	e = commit(c, records)
	if e == nil {
		c.Revision++
	}
	return c, e
}
func maps(raw any) []map[string]any {
	switch v := raw.(type) {
	case []map[string]any:
		return v
	case []map[string]string:
		out := []map[string]any{}
		for _, r := range v {
			m := map[string]any{}
			for k, s := range r {
				m[k] = s
			}
			out = append(out, m)
		}
		return out
	case []any:
		out := []map[string]any{}
		for _, r := range v {
			if m, ok := r.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}
func str(v any) string { s, _ := v.(string); return s }
func stringsOf(v any) []string {
	if a, ok := v.([]string); ok {
		return a
	}
	out := []string{}
	if a, ok := v.([]any); ok {
		for _, p := range a {
			if s, ok := p.(string); ok {
				out = append(out, s)
			}
		}
	}
	return out
}
func (h *Hub) galleryRecords(s *session.Session, threads []string, row map[string]any) []sessionmedia.Record {
	records, _ := h.galleryRecordsGuarded(s, threads, row, func() bool { return true })
	return records
}
func (h *Hub) galleryRecordsGuarded(s *session.Session, threads []string, row map[string]any, current func() bool) ([]sessionmedia.Record, error) {
	if !current() {
		return nil, sessionmedia.ErrSourceRetired
	}
	message := str(row["source_message_id"])
	thread := str(row["source_session_id"])
	at := historySourceThread(message, threads)
	if at < 0 || threads[at] != thread {
		return nil, nil
	}
	line, e := strconv.Atoi(message[strings.LastIndex(message, ":")+1:])
	if e != nil || line < 1 {
		return nil, nil
	}
	role := str(row["role"])
	request := str(row["request_id"])
	if role != "user" && role != "assistant" {
		return nil, nil
	}
	timestamp := int64(0)
	switch t := row["timestamp"].(type) {
	case int64:
		timestamp = t
	case int:
		timestamp = int64(t)
	case float64:
		timestamp = int64(t)
	}
	out := []sessionmedia.Record{}
	store := h.sessionGallery().store
	makeEntry := func(n int, provenance string) sessionmedia.Entry {
		return sessionmedia.Entry{ID: sessionmedia.ID(s.ID, message, strconv.Itoa(n), provenance), Message: message, Thread: thread, Request: request, Author: role, Provenance: provenance, Timestamp: timestamp, Ordinal: n, Sort: fmt.Sprintf("%08d:%020d:%08d", at, line, n), Availability: "unknown"}
	}
	populate := func(entry *sessionmedia.Entry, hash string) {
		if !current() {
			return
		}
		entry.SHA256 = hash
		mime, w, height, size, e := store.VerifyOriginal(hash)
		if e != nil {
			entry.Availability = "unsupported"
			return
		}
		entry.MIME = mime
		entry.Width = w
		entry.Height = height
		entry.Bytes = size
		entry.Availability = "available"
	}
	if role == "user" {
		for n, img := range maps(row["images"]) {
			if !current() {
				return nil, sessionmedia.ErrSourceRetired
			}
			entry := makeEntry(n, "uploaded")
			source := sessionmedia.Source{}
			if id := str(img["attachment_id"]); id != "" {
				source.Attachment = id
				entry.SHA256 = str(img["sha256"])
				entry.MIME = str(img["media_type"])
				switch n := img["size_bytes"].(type) {
				case int64:
					entry.Bytes = n
				case int:
					entry.Bytes = int64(n)
				case float64:
					entry.Bytes = int64(n)
				}
				entry.Availability = "available"
				entry.VersionKnown = true
			} else if data := str(img["data"]); len(data) > 0 && len(data) <= (sessionmedia.MaxImageBytes+2)/3*4 {
				if raw, e := base64.StdEncoding.DecodeString(data); e == nil {
					if hash, e := store.PutGuarded(raw, current); e == nil {
						source.Hash = hash
						populate(&entry, hash)
						entry.VersionKnown = true
					} else if galleryResourceError(e) {
						return nil, e
					}
				}
			}
			out = append(out, sessionmedia.Record{Entry: entry, Source: source})
		}
		if !current() {
			return nil, sessionmedia.ErrSourceRetired
		}
		return out, nil
	}
	paths := stringsOf(row["generated_image_paths"])
	for n, path := range paths {
		if !current() {
			return nil, sessionmedia.ErrSourceRetired
		}
		if request == "" || filepath.Clean(path) != path || filepath.Base(filepath.Dir(path)) != thread || filepath.Base(filepath.Dir(filepath.Dir(path))) != "generated_images" || filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(path)))) != ".codex" {
			continue
		}
		entry := makeEntry(n, "generated")
		entry.ID = sessionmedia.ID(s.ID, message, strconv.Itoa(n), "generated")
		source := sessionmedia.Source{Path: path}
		hash, e := store.CaptureGuarded(s.ID, request, path, current)
		if galleryResourceError(e) {
			return nil, e
		}
		if e == nil {
			source.Hash = hash
			populate(&entry, hash)
		} else {
			entry.Availability = "missing"
		}
		out = append(out, sessionmedia.Record{Entry: entry, Source: source})
	}
	if request != "" && row["history_read_result_verified"] == true {
		for n, record := range h.attachments.ForRequest(s.ID, request) {
			if !current() {
				return nil, sessionmedia.ErrSourceRetired
			}
			id := sessionmedia.ID(s.ID, message, strconv.Itoa(1000+n), "attached")
			found := false
			for _, r := range out {
				found = found || r.Source.Path == record.Path
			}
			if found {
				continue
			}
			entry := makeEntry(1000+n, "attached")
			entry.ID = id
			source := sessionmedia.Source{Path: record.Path}
			if hash, e := store.CaptureGuarded(s.ID, request, record.Path, current); e == nil {
				source.Hash = hash
				populate(&entry, hash)
			} else {
				if galleryResourceError(e) {
					return nil, e
				}
				entry.Availability = "missing"
			}
			out = append(out, sessionmedia.Record{Entry: entry, Source: source})
		}
	}
	// Deduplicate alternate representations on this exact native row only.
	// Distinct Markdown occurrences keep their own ordinal, even for one asset.
	represented := map[string]bool{}
	for _, prior := range out {
		represented[prior.Source.Path] = true
	}
	for n, path := range galleryMarkdownPaths(str(row["content"]), str(row["source_cwd"])) {
		if !current() {
			return nil, sessionmedia.ErrSourceRetired
		}
		if filepath.IsAbs(path) && h.TypedMediaPathPrivate(path) {
			continue
		}
		if represented[path] {
			continue
		}
		entry := makeEntry(2000+n, "attached")
		source := sessionmedia.Source{Path: path}
		if !filepath.IsAbs(path) {
			entry.Availability = "unknown"
		} else if hash, e := store.CaptureGuarded(s.ID, message, path, current); e == nil {
			source.Hash = hash
			populate(&entry, hash)
		} else {
			if galleryResourceError(e) {
				return nil, e
			}
			entry.Availability = "missing"
		}
		out = append(out, sessionmedia.Record{Entry: entry, Source: source})
	}

	if !current() {
		return nil, sessionmedia.ErrSourceRetired
	}
	return out, nil
}

// Only actual Markdown image nodes grant a local attachment relation. Code,
// comments and raw HTML nodes never become filesystem reads.
func galleryMarkdownPaths(content, cwd string) []string {
	lines := strings.Split(content, "\n")
	// Conservatively exclude indented examples even in ambiguous paragraph
	// continuation positions; they never authorize a local file read.
	for i, line := range lines {
		if strings.HasPrefix(line, "    ") || strings.HasPrefix(strings.TrimLeft(line, " "), "\t") {
			lines[i] = ""
		}
	}
	source := []byte(strings.Join(lines, "\n"))
	document := goldmark.DefaultParser().Parse(text.NewReader(source))
	destinations := []string{}
	_ = ast.Walk(document, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		if image, ok := node.(*ast.Image); entering && ok {
			destinations = append(destinations, string(image.Destination))
		}
		return ast.WalkContinue, nil
	})
	out := []string{}
	for _, raw := range destinations {
		u, e := url.Parse(raw)
		if e != nil || u.Scheme != "" || u.Host != "" || u.RawQuery != "" || u.Fragment != "" || strings.HasPrefix(raw, "//") {
			continue
		}
		p := u.Path
		if strings.ContainsAny(p, "\x00\r\n\\") || p == "" {
			continue
		}
		if !filepath.IsAbs(p) {
			if !filepath.IsAbs(cwd) {
				switch strings.ToLower(filepath.Ext(p)) {
				case ".png", ".jpg", ".jpeg", ".webp", ".gif":
					out = append(out, p)
				}
				continue
			}
			p = filepath.Join(cwd, p)
		}
		p = filepath.Clean(p)
		switch strings.ToLower(filepath.Ext(p)) {
		case ".png", ".jpg", ".jpeg", ".webp", ".gif":
			out = append(out, p)
		}
	}
	return out
}

func galleryResourceError(e error) bool {
	return errors.Is(e, sessionmedia.ErrResourceBusy) || errors.Is(e, sessionmedia.ErrSourceRetired)
}
