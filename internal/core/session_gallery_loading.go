package core

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"everything-go/internal/session"
	"everything-go/internal/sessionmedia"
)

// Job lifetime is independent of the durable catalog epoch. Ending an HTTP
// interest cancels only its worker, never a lawful committed image snapshot.
type galleryScanJob struct {
	done    chan struct{}
	cancel  context.CancelFunc
	readers map[*struct{ identity byte }]func() bool
	err     error
}

func (h *Hub) joinGalleryAdvance(ctx context.Context, s *session.Session, b galleryBinding, current func() bool) error {
	g := h.sessionGallery()
	key := sessionmedia.ID(s.ID, b.key)
	reader := new(struct{ identity byte })
	g.jobsMu.Lock()
	job := g.jobs[key]
	if g.subscribers >= 64 || (job != nil && len(job.readers) >= 16) || (job == nil && len(g.jobs) >= 32) {
		g.jobsMu.Unlock()
		return errors.New("gallery capacity pending")
	}
	if job == nil {
		if !current() {
			g.jobsMu.Unlock()
			return errors.New("gallery source changed")
		}
		checkpoint, err := g.store.Load(s.ID, b.key)
		if err != nil {
			g.jobsMu.Unlock()
			return err
		}
		worker, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		job = &galleryScanJob{done: make(chan struct{}), cancel: cancel, readers: map[*struct{ identity byte }]func() bool{}}
		g.jobs[key] = job
		go func(job *galleryScanJob) {
			legalLocked := func() bool {
				if worker.Err() != nil || g.jobs[key] != job {
					return false
				}
				for _, valid := range job.readers {
					if valid() {
						return true
					}
				}
				return false
			}
			legal := func() bool { g.jobsMu.Lock(); defer g.jobsMu.Unlock(); return legalLocked() }
			var err error
			select {
			case g.scan <- struct{}{}:
				if !legal() {
					err = errors.New("gallery scan interest ended")
				} else {
					c := checkpoint
					if err == nil {
						_, err = h.advanceGalleryCommit(s, b, c, legal, func(next sessionmedia.Checkpoint, records []sessionmedia.Record) error {
							g.jobsMu.Lock()
							defer g.jobsMu.Unlock()
							if !legalLocked() {
								return errors.New("gallery scan retired")
							}
							return g.store.CommitCAS(s.ID, c, next, records)
						})
					}
				}
				<-g.scan
			case <-worker.Done():
				err = worker.Err()
			}
			g.jobsMu.Lock()
			job.err = err
			close(job.done)
			if g.jobs[key] == job {
				delete(g.jobs, key)
			}
			g.jobsMu.Unlock()
			cancel()
		}(job)
	}
	job.readers[reader] = current
	g.subscribers++
	g.jobsMu.Unlock()
	defer func() {
		g.jobsMu.Lock()
		delete(job.readers, reader)
		g.subscribers--
		if len(job.readers) == 0 {
			job.cancel()
			if g.jobs[key] == job {
				delete(g.jobs, key)
			}
		}
		g.jobsMu.Unlock()
	}()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-job.done:
		return job.err
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.New("gallery advance deadline")
	}
}

type galleryValidationItem struct {
	ID      string `json:"id"`
	SHA     string `json:"sha256"`
	Purpose string `json:"purpose"`
}

func (h *Hub) validateGallery(w http.ResponseWriter, r *http.Request, sid string, b galleryBinding, c sessionmedia.Checkpoint, current func() bool) {
	items := []galleryValidationItem{{r.URL.Query().Get("id"), r.URL.Query().Get("sha256"), r.URL.Query().Get("purpose")}}
	if r.Method == http.MethodPost {
		var body struct {
			Items []galleryValidationItem `json:"items"`
		}
		d := json.NewDecoder(io.LimitReader(r.Body, 16385))
		d.DisallowUnknownFields()
		if d.Decode(&body) != nil || d.Decode(new(any)) != io.EOF || len(body.Items) < 1 || len(body.Items) > 12 {
			http.Error(w, "invalid validation batch", 400)
			return
		}
		items = body.Items
	}
	out := []map[string]any{}
	for _, item := range items {
		if item.Purpose != "thumbnail_display" && item.Purpose != "full_preview" && item.Purpose != "download" && item.Purpose != "quote" {
			http.Error(w, "invalid validation purpose", 400)
			return
		}
		record, e := h.sessionGallery().store.Get(sid, item.ID)
		if e != nil || record.Entry.Availability != "available" {
			http.Error(w, "original unavailable", 404)
			return
		}
		var hash, mime string
		var size int64
		var width, height int
		if record.Source.Hash != "" {
			hash = record.Source.Hash
			mime, width, height, size, e = h.sessionGallery().store.VerifyOriginal(hash)
		} else if record.Source.Attachment != "" {
			q := r.URL.Query()
			q.Set("attachment_id", record.Source.Attachment)
			q.Set("sha256", record.Entry.SHA256)
			copyReq := r.Clone(r.Context())
			copyReq.Method = http.MethodGet
			u := *r.URL
			u.RawQuery = q.Encode()
			copyReq.URL = &u
			capture := newGalleryCapture()
			h.serveMediaAttachment(capture, copyReq)
			if capture.status != 200 {
				http.Error(w, "original attachment unavailable", capture.status)
				return
			}
			bytes := capture.body.Bytes()
			hash = sessionmedia.IDBytes(bytes)
			size = int64(len(bytes))
			mime, width, height, e = sessionmedia.ImageInfo(bytes)
		} else {
			e = errors.New("original missing")
		}
		if e != nil || item.SHA != "" && item.SHA != hash || record.Entry.SHA256 != "" && record.Entry.SHA256 != hash {
			http.Error(w, "original unavailable or changed", 409)
			return
		}
		if !current() {
			http.Error(w, "source changed", 409)
			return
		}
		out = append(out, map[string]any{"id": item.ID, "purpose": item.Purpose, "sha256": hash, "mime_type": mime, "size_bytes": size, "width": width, "height": height, "allowed": true})
	}
	if !current() {
		http.Error(w, "source changed", 409)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"schema": 1, "authority_instance_id": h.cfg.InstanceID, "session_id": sid, "snapshot": c.Epoch, "items": out})
}
