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
	"regexp"
	"strconv"
	"strings"
	"sync"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/session"
	"everything-go/internal/sessionmedia"
)

type sessionGallery struct {
	store  *sessionmedia.Store
	scan   sync.Mutex
	decode chan struct{}
}

func (h *Hub) sessionGallery() *sessionGallery {
	h.galleryInit.Do(func() {
		root := ""
		if h.cfg.DataDir != "" {
			root = filepath.Join(h.cfg.DataDir, "session-media")
		}
		h.gallery = &sessionGallery{store: sessionmedia.New(root), decode: make(chan struct{}, 2)}
	})
	return h.gallery
}

type galleryBinding struct {
	provider backend.HistoryProvider
	threads  []string
	key      string
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
	parts := []string{h.cfg.InstanceID, s.ID, s.Backend(), s.ResumeID()}
	for _, id := range ids {
		g, e := generations.HistorySourceGeneration(id)
		if e != nil || g == "" {
			return galleryBinding{}, errors.New("history source unavailable")
		}
		parts = append(parts, id, g)
	}
	return galleryBinding{provider, ids, sessionmedia.ID(parts...)}, nil
}
func (h *Hub) serveSessionGallery(w http.ResponseWriter, r *http.Request, operation string) {
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Access-Control-Expose-Headers", "X-Bridge-Image-SHA256")
	if r.Method != http.MethodGet {
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
	if operation == "list" {
		if !gallery.scan.TryLock() {
			http.Error(w, "gallery busy", 503)
			return
		}
		defer gallery.scan.Unlock()
		c, e := gallery.store.Load(sid, binding.key)
		if e != nil {
			http.Error(w, "gallery checkpoint unavailable", 503)
			return
		}
		// A stale pagination token must never advance a changed source or bind to it.
		filter := r.URL.Query().Get("filter")
		if filter == "" {
			filter = "all"
		}
		token := r.URL.Query().Get("cursor")
		if _, _, _, e = gallery.store.List(sid, c, filter, token, 1); e != nil {
			http.Error(w, "gallery cursor changed; refresh", 409)
			return
		}
		c, e = h.advanceGallery(s, binding, c, current)
		if e != nil {
			http.Error(w, e.Error(), 503)
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		items, next, count, e := gallery.store.List(sid, c, filter, token, limit)
		if e != nil {
			http.Error(w, "gallery cursor changed; refresh", 409)
			return
		}
		if !current() {
			http.Error(w, "source changed", 409)
			return
		}
		coverage := "indexing"
		if c.Complete && !c.MoreAfter {
			coverage = "complete"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema": 1, "authority_instance_id": h.cfg.InstanceID, "session_id": sid, "snapshot": c.Epoch, "coverage": coverage, "count": count, "items": items, "next_cursor": next, "has_more": next != ""})
		return
	}
	c, e := gallery.store.Load(sid, binding.key)
	if e != nil || !c.Started || r.URL.Query().Get("snapshot") != c.Epoch {
		http.Error(w, "gallery source changed", 409)
		return
	}
	record, e := gallery.store.Get(sid, r.URL.Query().Get("id"))
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
		_ = json.NewEncoder(w).Encode(map[string]any{"schema": 1, "authority_instance_id": h.cfg.InstanceID, "session_id": sid, "message_id": record.Entry.Message, "messages": bounded.Messages})
		return
	}
	if operation != "thumbnail" && operation != "original" {
		http.NotFound(w, r)
		return
	}
	select {
	case gallery.decode <- struct{}{}:
		defer func() { <-gallery.decode }()
	case <-r.Context().Done():
		return
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
	if operation == "thumbnail" {
		b, e = gallery.store.Thumbnail(sessionmedia.IDBytes(b), b)
		mime = "image/jpeg"
		if e != nil {
			http.Error(w, "thumbnail unavailable", 415)
			return
		}
	}
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
		if _, e = guarded.Write(b[offset:min(len(b), offset+(64<<10))]); e != nil {
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
		for _, candidate := range h.galleryRecords(s, b.threads, row) {
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
	if !current() {
		return c, errors.New("gallery source changed")
	}
	e = h.sessionGallery().store.Commit(s.ID, c, records)
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
	message := str(row["source_message_id"])
	thread := str(row["source_session_id"])
	at := historySourceThread(message, threads)
	if at < 0 || threads[at] != thread {
		return nil
	}
	line, e := strconv.Atoi(message[strings.LastIndex(message, ":")+1:])
	if e != nil || line < 1 {
		return nil
	}
	role := str(row["role"])
	request := str(row["request_id"])
	if role != "user" && role != "assistant" {
		return nil
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
		entry.SHA256 = hash
		b, e := store.Read(hash)
		if e != nil {
			entry.Availability = "missing"
			return
		}
		mime, w, height, e := sessionmedia.ImageInfo(b)
		if e != nil {
			entry.Availability = "unsupported"
			return
		}
		entry.MIME = mime
		entry.Width = w
		entry.Height = height
		entry.Bytes = int64(len(b))
		entry.Availability = "available"
	}
	if role == "user" {
		for n, img := range maps(row["images"]) {
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
					if hash, e := store.Put(raw); e == nil {
						source.Hash = hash
						populate(&entry, hash)
						entry.VersionKnown = true
					}
				}
			}
			out = append(out, sessionmedia.Record{Entry: entry, Source: source})
		}
		return out
	}
	paths := stringsOf(row["generated_image_paths"])
	for n, path := range paths {
		if request == "" || filepath.Clean(path) != path || filepath.Base(filepath.Dir(path)) != thread || filepath.Base(filepath.Dir(filepath.Dir(path))) != "generated_images" || filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(path)))) != ".codex" {
			continue
		}
		entry := makeEntry(n, "generated")
		entry.ID = sessionmedia.ID(s.ID, request, path)
		source := sessionmedia.Source{Path: path}
		hash, e := store.Capture(s.ID, request, path)
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
			id := sessionmedia.ID(s.ID, request, record.Path)
			if prior, e := store.Get(s.ID, id); e == nil && prior.Entry.Provenance == "generated" {
				continue
			}
			found := false
			for _, r := range out {
				found = found || r.Entry.ID == id
			}
			if found {
				continue
			}
			entry := makeEntry(1000+n, "attached")
			entry.ID = id
			source := sessionmedia.Source{Path: record.Path}
			if hash, e := store.Capture(s.ID, request, record.Path); e == nil {
				source.Hash = hash
				populate(&entry, hash)
			} else {
				entry.Availability = "missing"
			}
			out = append(out, sessionmedia.Record{Entry: entry, Source: source})
		}
	}
	// Explicit image Markdown is an attachment reference on this exact source
	// row. Plain path mentions, remote URLs and tool outputs are not admitted.
	for n, path := range galleryMarkdownPaths(str(row["content"]), str(row["source_cwd"])) {
		duplicate := false
		for _, r := range out {
			duplicate = duplicate || r.Source.Path == path
		}
		if duplicate {
			continue
		}
		entry := makeEntry(2000+n, "attached")
		if request != "" {
			entry.ID = sessionmedia.ID(s.ID, request, path)
			if prior, e := store.Get(s.ID, entry.ID); e == nil && prior.Entry.Provenance == "generated" {
				continue
			}
		}
		source := sessionmedia.Source{Path: path}
		if !filepath.IsAbs(path) {
			entry.Availability = "unknown"
		} else if hash, e := store.Capture(s.ID, message, path); e == nil {
			source.Hash = hash
			populate(&entry, hash)
		} else {
			entry.Availability = "missing"
		}
		out = append(out, sessionmedia.Record{Entry: entry, Source: source})
	}

	return out
}

var galleryImageMarkdown = regexp.MustCompile(`!\[[^\]\r\n]*\]\((?:<([^>\r\n]+)>|([^\s)]+))\)`)

func galleryMarkdownPaths(content, cwd string) []string {
	out := []string{}
	for _, match := range galleryImageMarkdown.FindAllStringSubmatch(galleryVisibleMarkdown(content), -1) {
		raw := match[1]
		if raw == "" {
			raw = match[2]
		}
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

// A literal example inside code is not an attachment in the rendered message.
func galleryVisibleMarkdown(content string) string {
	lines := strings.Split(content, "\n")
	fence := byte(0)
	length := 0
	for i, line := range lines {
		trim := strings.TrimLeft(line, " ")
		indent := len(line) - len(trim)
		run := 0
		if len(trim) > 0 && (trim[0] == '`' || trim[0] == '~') {
			for run < len(trim) && trim[run] == trim[0] {
				run++
			}
		}
		if fence != 0 {
			lines[i] = ""
			if indent <= 3 && run >= length && trim[0] == fence && strings.TrimSpace(trim[run:]) == "" {
				fence = 0
			}
			continue
		}
		if indent >= 4 {
			lines[i] = ""
			continue
		}
		if run >= 3 {
			fence = trim[0]
			length = run
			lines[i] = ""
			continue
		}
		raw := []byte(strings.ReplaceAll(line, "\\!", "  "))
		for at := 0; at < len(raw); at++ {
			if raw[at] != '`' {
				continue
			}
			end := at
			for end < len(raw) && raw[end] == '`' {
				end++
			}
			delimiter := strings.Repeat("`", end-at)
			close := strings.Index(string(raw[end:]), delimiter)
			if close < 0 {
				continue
			}
			finish := end + close + len(delimiter)
			for n := at; n < finish; n++ {
				raw[n] = ' '
			}
			at = finish - 1
		}
		lines[i] = string(raw)
	}
	return strings.Join(lines, "\n")
}
