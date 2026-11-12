package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/session"
	"everything-go/internal/sessionmedia"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// Historical reads use current session permission plus the original admitted
// message relation. Uploader device equality is deliberately not a read grant.
func (h *Hub) serveMediaAttachment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method", 405)
		return
	}
	device := h.inboxDevice(r)
	sid := r.URL.Query().Get("session_id")
	id := r.URL.Query().Get("attachment_id")
	hash := r.URL.Query().Get("sha256")
	if device == "" {
		http.Error(w, "paired device required", 403)
		return
	}
	if _, e := h.intakeSession(sid); e != nil {
		http.Error(w, "session permission", 403)
		return
	}
	done, e := h.media.begin()
	if e != nil {
		http.Error(w, "media unavailable", 503)
		return
	}
	allowed := false
	for _, b := range h.media.data.Batches {
		if b.Session != sid {
			continue
		}
		if b.State != "admitted" && !(b.State == "prepared" && b.Device == device) {
			continue
		}
		for _, object := range b.Objects {
			if object == id {
				allowed = true
			}
		}
	}
	m, e := h.unifiedUpload(sid, id)
	if !allowed || e != nil || m.SHA256 != hash {
		done()
		http.Error(w, "attachment relation unavailable", 403)
		return
	}
	lease := "reader_" + randomID()
	if h.media.data.Readers == nil {
		h.media.data.Readers = map[string]mediaReadLease{}
	}
	h.media.data.Readers[lease] = mediaReadLease{id, sid, m.Path, m.SHA256, m.SizeBytes, "history"}
	if e = h.media.save(); e != nil {
		done()
		http.Error(w, "reader lease unknown", 503)
		return
	}
	done()
	defer func() {
		release, e := h.media.begin()
		if e != nil {
			return
		}
		defer release()
		delete(h.media.data.Readers, lease)
		_ = h.media.save()
	}()
	before, e := os.Lstat(m.Path)
	if e != nil || !before.Mode().IsRegular() || before.Size() != m.SizeBytes {
		http.Error(w, "object changed", 409)
		return
	}
	f, e := os.Open(m.Path)
	if e != nil {
		http.Error(w, "object unavailable", 404)
		return
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !os.SameFile(before, opened) || h.inboxDevice(r) != device {
		http.Error(w, "object or permission changed", 409)
		return
	}
	digest := sha256.New()
	n, he := io.Copy(digest, io.LimitReader(f, m.SizeBytes+1))
	after, se := os.Lstat(m.Path)
	if he != nil || se != nil || n != m.SizeBytes || hex.EncodeToString(digest.Sum(nil)) != m.SHA256 || !os.SameFile(before, after) || after.Size() != before.Size() || after.ModTime() != before.ModTime() {
		http.Error(w, "object digest changed", 409)
		return
	}
	if _, e = f.Seek(0, io.SeekStart); e != nil {
		http.Error(w, "object read unavailable", 409)
		return
	}
	w.Header().Set("Content-Type", m.MediaType)
	w.Header().Set("ETag", `"`+m.SHA256+`"`)
	w.Header().Set("X-Bridge-Attachment-SHA256", m.SHA256)
	w.Header().Set("X-Bridge-Attachment-Bytes", strconv.FormatInt(m.SizeBytes, 10))
	w.Header().Set("Cache-Control", "private, no-store")
	http.ServeContent(&mediaReadWriter{ResponseWriter: w, allowed: func() bool { _, e := h.intakeSession(sid); return e == nil && h.inboxDevice(r) == device }}, r, m.Name, before.ModTime(), f)
}

type mediaReadWriter struct {
	http.ResponseWriter
	allowed func() bool
}

func (w *mediaReadWriter) Write(p []byte) (int, error) {
	if !w.allowed() {
		return 0, errors.New("media permission revoked")
	}
	return w.ResponseWriter.Write(p)
}

// The legacy absolute-path media route must not expose new private object/index
// files. Existing historical non-typed media behavior remains unchanged.
func (h *Hub) TypedMediaPathPrivate(path string) bool {
	if sessionmedia.PrivatePath(h.cfg.DataDir, path) {
		return true
	}
	if h.media == nil {
		return false
	}
	relative, e := filepath.Rel(h.media.root, path)
	if e != nil || relative == ".." || strings.HasPrefix(relative, "../") {
		return false
	}
	parts := strings.Split(relative, "/")
	if len(parts) < 3 {
		return true
	}
	dir := filepath.Join(h.media.root, parts[0], parts[1])
	raw, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if e == nil {
		var m uploadedVideoManifest
		if json.Unmarshal(raw, &m) != nil {
			return true
		}
		return m.Scope != nil
	}
	raw, e = os.ReadFile(filepath.Join(dir, "upload-state.json"))
	if e != nil {
		return true
	}
	var state resumableUploadState
	return json.Unmarshal(raw, &state) != nil || state.Scope != nil
}

// Only a provider-journal-proven original request may supply this projection.
// The upload path never becomes a history read grant, nor an uploader-only rule.
func (h *Hub) projectMediaHistory(s *session.Session, rows []map[string]any) []map[string]any {
	if h.media == nil || s == nil {
		return rows
	}
	done, e := h.media.begin()
	if e != nil {
		return rows
	}
	defer done()
	out := append([]map[string]any(nil), rows...)
	for n, row := range rows {
		if row["role"] != "user" {
			continue
		}
		request, _ := row["request_id"].(string)
		if request == "" {
			continue
		}
		var found *mediaBatch
		for _, b := range h.media.data.Batches {
			if b.Session == s.ID && b.ID == request && b.State == "admitted" {
				if found != nil {
					found = nil
					break
				}
				found = b
			}
		}
		if found == nil {
			continue
		}
		images := []map[string]any{}
		files := []map[string]any{}
		for _, i := range found.Items {
			id := found.Objects[i.UploadRequestID]
			if id == "" {
				continue
			}
			v := map[string]any{"attachment_id": id, "media_type": i.MediaType, "sha256": i.SHA256, "size_bytes": i.SizeBytes}
			if i.UploadKind == "image" {
				images = append(images, v)
			} else {
				v["name"] = i.Name
				v["upload_kind"] = i.UploadKind
				files = append(files, v)
			}
		}
		clone := make(map[string]any, len(row)+2)
		for k, v := range row {
			clone[k] = v
		}
		if len(images) > 0 {
			clone["images"] = images
		}
		if len(files) > 0 {
			clone["files"] = files
		}
		clone["media_batch_id"] = found.ID
		// Exact admitted business content is retained even after Codex deletes its
		// temporary localImage staging path. No filesystem path is fetched here.
		if text, ok := row["content"].(string); ok {
			lines := strings.Split(text, "\n")
			kept := make([]string, 0, len(lines))
			for _, line := range lines {
				if mediaHistoryWrapper.MatchString(line) || line == "</image>" {
					continue
				}
				kept = append(kept, line)
			}
			clone["content"] = strings.TrimSpace(strings.Join(kept, "\n"))
		}
		out[n] = clone
	}
	return out
}

var mediaHistoryWrapper = regexp.MustCompile(`^<image name=\[Image #[1-9][0-9]*\] path="[^"\r\n<>]+">$`)
