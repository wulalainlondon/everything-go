package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/taskapi"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type unifiedUploadBinding struct {
	Session string `json:"session_id"`
	Upload  string `json:"upload_id"`
	Hash    string `json:"sha256"`
	Size    int64  `json:"size_bytes"`
	Device  string `json:"device_id"`
	Path    string `json:"path"`
}

func (h *Hub) unifiedUpload(session, id string) (uploadedVideoManifest, error) {
	var m uploadedVideoManifest
	root, e := os.OpenRoot(filepath.Join(h.cfg.DataDir, "uploads"))
	if e != nil {
		return m, e
	}
	defer root.Close()
	if len(id) != 34 || !strings.HasPrefix(id, "u_") {
		return m, unifiedError("invalid_argument")
	}
	f, e := root.Open(filepath.Join(attachmentSafe(session, "session"), id, "manifest.json"))
	if e != nil {
		return m, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, 65537))
	if e != nil || len(b) > 65536 || json.Unmarshal(b, &m) != nil || m.SessionID != session || m.UploadID != id || !jarvisSHA.MatchString(m.SHA256) {
		return m, unifiedError("invalid_argument")
	}
	return m, nil
}
func (h *Hub) verifyUnifiedUpload(b unifiedUploadBinding) error {
	m, e := h.unifiedUpload(b.Session, b.Upload)
	if e != nil {
		return e
	}
	if m.SHA256 != b.Hash || m.SizeBytes != b.Size || m.DeviceID != b.Device || m.Path != b.Path {
		return unifiedError("scope_changed")
	}
	root, e := filepath.Abs(filepath.Join(h.cfg.DataDir, "uploads"))
	if e != nil {
		return e
	}
	return verifyStoredAttachment(root, m.Path, m.SizeBytes, m.SHA256)
}
func (h *Hub) prepareUnifiedMedia(ctx context.Context, c taskapi.VerifiedContext, target jarvisTarget, content string, images []intakeImage, files []intakeFile) (string, []backend.ImageAttachment, []backend.FileAttachment, []unifiedUploadBinding, error) {
	if !intakeHasInput(intakeInput{Goal: content, Images: images, Files: files}) {
		return "", nil, nil, nil, unifiedError("invalid_argument")
	}
	if len(images) > 12 || len(files) > 12 {
		return "", nil, nil, nil, unifiedError("invalid_argument")
	}
	outImages := []backend.ImageAttachment{}
	outFiles := []backend.FileAttachment{}
	bindings := []unifiedUploadBinding{}
	for _, i := range images {
		if i.MediaType != "image/jpeg" && i.MediaType != "image/png" && i.MediaType != "image/webp" {
			return "", nil, nil, nil, unifiedError("invalid_argument")
		}
		b, e := base64.StdEncoding.DecodeString(i.Data)
		if e != nil || len(b) == 0 || len(i.Data) > 200000 {
			return "", nil, nil, nil, unifiedError("invalid_argument")
		}
		outImages = append(outImages, backend.ImageAttachment{Data: i.Data, MediaType: i.MediaType})
	}
	for _, f := range files {
		if f.AttachmentID != "" {
			if c.BindingKind != "paired_human" {
				return "", nil, nil, nil, unifiedError("permission")
			}
			m, e := h.unifiedUpload(target.SessionID, f.AttachmentID)
			if e != nil {
				return "", nil, nil, nil, e
			}
			if m.DeviceID != strings.TrimPrefix(c.StableScopeID, "device:") || f.Name != "" && m.Name != f.Name || f.MediaType != "" && m.MediaType != f.MediaType || f.SizeBytes != 0 && m.SizeBytes != f.SizeBytes {
				return "", nil, nil, nil, unifiedError("permission")
			}
			binding := unifiedUploadBinding{m.SessionID, m.UploadID, m.SHA256, m.SizeBytes, m.DeviceID, m.Path}
			if e = h.verifyUnifiedUpload(binding); e != nil {
				return "", nil, nil, nil, e
			}
			bindings = append(bindings, binding)
		}
		outFiles = append(outFiles, backend.FileAttachment{AttachmentID: f.AttachmentID, Name: f.Name, Content: f.Content, MediaType: f.MediaType, SizeBytes: f.SizeBytes})
	}
	if strings.TrimSpace(content) == "" && len(outImages) == 0 && len(outFiles) == 0 {
		return "", nil, nil, nil, unifiedError("invalid_argument")
	}
	prepared, resolved, e := h.resolveUploadedVideos(target.SessionID, content, outFiles)
	if e != nil {
		return "", nil, nil, nil, e
	}
	if e = h.unifiedTarget(target, true); e != nil {
		return "", nil, nil, nil, e
	}
	return prepared, outImages, resolved, bindings, nil
}

func (h *Hub) prepareUnifiedEditMedia(ctx context.Context, c taskapi.VerifiedContext, r unifiedResolved, in unifiedInput, p queuedPayload) (queuedPayload, []unifiedUploadBinding, bool, error) {
	images, files := in.Images, in.Files
	explicit := images != nil || files != nil
	if len(r.Entry.EffectiveBinding) == 0 && p.UnifiedBinding != nil {
		r.Entry.EffectiveBinding, _ = json.Marshal(p.UnifiedBinding)
	}
	if files == nil {
		var binding unifiedInputBinding
		if json.Unmarshal(r.Entry.EffectiveBinding, &binding) == nil && binding.InputFiles != nil {
			files = binding.InputFiles
			explicit = true
		}
	}
	// Formal media intake retained exact user-supplied refs, so the original
	// verified media projection can be rebuilt with the new user text.

	if r.Canonical != nil {
		var meta apiMetadata
		if json.Unmarshal(r.Canonical.Metadata, &meta) == nil && meta.Request.Operation == "intake" {
			var original intakeInput
			if json.Unmarshal(meta.Request.Input, &original) == nil {
				if images == nil && original.Images != nil {
					images = original.Images
					explicit = true
				}
				if files == nil && original.Files != nil {
					files = original.Files
					explicit = true
				}
			}
		}
	}

	if !explicit {
		if strings.TrimSpace(in.Content) == "" && len(p.Images) == 0 && len(p.Files) == 0 {
			return p, nil, false, unifiedError("invalid_argument")
		}
		p.Content = in.Content
		return p, nil, false, nil
	}
	if images == nil {
		images = []intakeImage{}
		for _, i := range p.Images {
			images = append(images, intakeImage{Data: i.Data, MediaType: i.MediaType})
		}
	}
	if files == nil {
		files = []intakeFile{}
		for _, f := range p.Files {
			files = append(files, intakeFile{AttachmentID: f.AttachmentID, Name: f.Name, Content: f.Content, MediaType: f.MediaType, SizeBytes: f.SizeBytes})
		}
	}
	content, newImages, newFiles, bindings, e := h.prepareUnifiedMedia(ctx, c, r.Target, in.Content, images, files)
	if e != nil {
		return p, nil, false, e
	}
	p.Content = content
	p.Images = newImages
	p.Files = newFiles
	return p, bindings, true, nil
}

func unifiedOriginalMediaFiles(r unifiedResolved, in unifiedInput) []intakeFile {
	if in.Files != nil {
		return in.Files
	}
	if len(r.Entry.EffectiveBinding) == 0 {
		var p queuedPayload
		if json.Unmarshal(r.Entry.Payload, &p) == nil && p.UnifiedBinding != nil {
			r.Entry.EffectiveBinding, _ = json.Marshal(p.UnifiedBinding)
		}
	}
	var binding unifiedInputBinding
	if json.Unmarshal(r.Entry.EffectiveBinding, &binding) == nil && binding.InputFiles != nil {
		return binding.InputFiles
	}
	if r.Canonical != nil {
		var meta apiMetadata
		var original intakeInput
		if json.Unmarshal(r.Canonical.Metadata, &meta) == nil && meta.Request.Operation == "intake" && json.Unmarshal(meta.Request.Input, &original) == nil {
			return original.Files
		}
	}
	return nil
}
