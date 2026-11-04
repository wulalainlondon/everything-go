package core

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"everything-go/internal/backend"
)

func (h *Hub) resolveMediaImages(sid string, images []backend.ImageAttachment) ([]backend.ImageAttachment, error) {
	out := make([]backend.ImageAttachment, 0, len(images))
	for _, img := range images {
		if img.AttachmentID == "" {
			if img.VerifiedPath != "" {
				return nil, errors.New("client image path not allowed")
			}
			out = append(out, img)
			continue
		}
		if img.Data != "" || img.VerifiedPath != "" || img.SizeBytes <= 0 || img.SizeBytes > backend.MediaImageMaxBytes || !jarvisSHA.MatchString(img.SHA256) {
			return nil, errors.New("invalid typed image ref")
		}
		m, e := h.unifiedUpload(sid, img.AttachmentID)
		if e != nil || m.Kind != "image" || m.MediaType != img.MediaType || m.SizeBytes != img.SizeBytes || m.SHA256 != img.SHA256 {
			return nil, errors.New("image ref binding mismatch")
		}
		if e = verifyStoredAttachment(filepath.Join(h.cfg.DataDir, "uploads"), m.Path, m.SizeBytes, m.SHA256); e != nil {
			return nil, e
		}
		img.VerifiedPath = m.Path
		out = append(out, img)
	}
	return out, nil
}

func (h *Hub) hasTypedFiles(sid string, files []backend.FileAttachment) bool {
	for _, f := range files {
		if f.AttachmentID != "" {
			m, e := h.unifiedUpload(sid, f.AttachmentID)
			if e == nil && m.Scope != nil {
				return true
			}
		} else if f.RemotePath != "" {
			raw, e := os.ReadFile(filepath.Join(filepath.Dir(f.RemotePath), "manifest.json"))
			var m uploadedVideoManifest
			if e == nil && json.Unmarshal(raw, &m) == nil && m.Scope != nil {
				return true
			}
		}
	}
	return false
}

// Count client descriptors before any full-file verification or model staging.
// This placeholder can only reach the metadata counter, never a file opener.
func mediaRefPreflight(provider, content string, images []backend.ImageAttachment) error {
	preview := append([]backend.ImageAttachment(nil), images...)
	for n := range preview {
		if preview[n].AttachmentID != "" {
			preview[n].VerifiedPath = "/metadata-counter-only"
		}
	}
	return backend.ValidateMediaBudget(provider, content, preview, nil)
}
