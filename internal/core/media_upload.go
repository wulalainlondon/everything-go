package core

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
)

type mediaUploadScope struct {
	BatchID      string `json:"media_batch_id,omitempty"`
	Revision     uint64 `json:"media_batch_revision,omitempty"`
	BodyHash     string `json:"media_body_hash,omitempty"`
	ItemID       string `json:"item_id,omitempty"`
	ItemRevision uint64 `json:"item_revision,omitempty"`
	SHA256       string `json:"expected_sha256,omitempty"`
	Generation   uint64 `json:"credential_generation,omitempty"`
}

func (u *attachmentUploads) initCommand(cmd clientproto.Command) {
	if cmd.MediaBatchID == "" {
		if cmd.UploadKind == "image" {
			u.sendError(cmd.UploadRequestID, "", "Typed image batch required")
			return
		}
		u.initKind(cmd.SessionID, cmd.UploadRequestID, cmd.Name, cmd.MediaType, cmd.SizeBytes, cmd.UploadKind)
		return
	}
	h := u.client.hub
	actor, e := h.mediaActor(u.client)
	if e != nil {
		u.sendError(cmd.UploadRequestID, "", "Media caller unavailable")
		return
	}
	done, e := h.media.begin()
	if e != nil {
		u.sendError(cmd.UploadRequestID, "", "Media store unavailable")
		return
	}
	defer done()
	key := mediaKey(strings.TrimPrefix(actor.StableScopeID, "device:"), actor.NamespaceGeneration, cmd.MediaBatchID)
	b := h.media.data.Batches[key]
	if b == nil || b.Session != cmd.SessionID || b.State != "prepared" || b.Revision != cmd.MediaBatchRevision || b.BodyHash != cmd.MediaBodyHash {
		u.sendError(cmd.UploadRequestID, "", "Media batch cancelled, unknown or changed")
		return
	}
	if e = h.mediaBatchTarget(b); e != nil {
		u.sendError(cmd.UploadRequestID, "", "Media source changed")
		return
	}
	s, _ := h.registry.Get(b.Session)
	if s == nil || h.SessionRoleEpoch(s) != b.RoleEpoch {
		u.sendError(cmd.UploadRequestID, "", "Media role source changed")
		return
	}
	var selected *protocol.MediaBatchItem
	for j := range b.Items {
		if b.Items[j].UploadRequestID == cmd.UploadRequestID {
			selected = &b.Items[j]
		}
	}
	if selected == nil || selected.AttachmentID != "" || selected.ItemID != cmd.ItemID || selected.ItemRevision != cmd.ItemRevision || selected.Name != cmd.Name || selected.MediaType != cmd.MediaType || selected.SizeBytes != cmd.SizeBytes || selected.SHA256 != cmd.SHA256 || selected.UploadKind != cmd.UploadKind {
		u.sendError(cmd.UploadRequestID, "", "Media item immutable descriptor mismatch")
		return
	}
	u.initialScope = &mediaUploadScope{cmd.MediaBatchID, b.Revision, b.BodyHash, selected.ItemID, selected.ItemRevision, selected.SHA256, actor.NamespaceGeneration}
	u.lifecycleHeld = true
	defer func() { u.initialScope = nil; u.lifecycleHeld = false }()
	u.initKind(cmd.SessionID, cmd.UploadRequestID, cmd.Name, cmd.MediaType, cmd.SizeBytes, cmd.UploadKind)
}
func (u *attachmentUploads) guardUpload(up *activeAttachmentUpload) (func(), error) {
	if up.Scope == nil {
		m := u.client.hub.media
		if m != nil && m.err == nil && m.root == u.root {
			return m.begin()
		}
		return func() {}, nil
	}
	h := u.client.hub
	actor, e := h.mediaActor(u.client)
	if e != nil {
		return nil, e
	}
	if actor.NamespaceGeneration != up.Scope.Generation || strings.TrimPrefix(actor.StableScopeID, "device:") != up.deviceID {
		return nil, errors.New("media upload owner changed")
	}
	done, e := h.media.begin()
	if e != nil {
		return nil, e
	}
	b := h.media.data.Batches[mediaKey(up.deviceID, up.Scope.Generation, up.Scope.BatchID)]
	if b == nil || b.State != "prepared" || b.BodyHash != up.Scope.BodyHash || b.Revision != up.Scope.Revision {
		done()
		return nil, errors.New("media batch cancelled or uncertain")
	}
	if e = h.mediaBatchTarget(b); e != nil {
		done()
		return nil, e
	}
	s, _ := h.registry.Get(b.Session)
	if s == nil || h.SessionRoleEpoch(s) != b.RoleEpoch {
		done()
		return nil, errors.New("media role source changed")
	}
	return done, nil
}
func mediaVideoHint(kind, name, mime string) bool {
	return kind == "video" || strings.HasPrefix(strings.ToLower(mime), "video/") || isVideoExtension(filepath.Ext(name))
}
func mediaPrefixKind(p []byte) string {
	// Classic QuickTime MOV may start with a media/movie/wide atom, without ftyp.
	if len(p) >= 8 {
		switch string(p[4:8]) {
		case "moov", "mdat", "wide", "pnot":
			return "video"
		}
	}

	if len(p) >= 12 && string(p[4:8]) == "ftyp" {
		brand := string(p[8:12])
		if strings.HasPrefix(brand, "hei") || strings.HasPrefix(brand, "mif") || brand == "avif" || brand == "avis" {
			return "image"
		}
		return "video"
	}
	if len(p) >= 12 && string(p[:4]) == "RIFF" {
		if string(p[8:12]) == "AVI " {
			return "video"
		}
		if string(p[8:12]) == "WEBP" {
			return "image"
		}
	}
	if bytes.HasPrefix(p, []byte{0x1a, 0x45, 0xdf, 0xa3}) || bytes.HasPrefix(p, []byte("FLV")) || bytes.HasPrefix(p, []byte{0, 0, 1, 0xba}) {
		return "video"
	}
	mt := http.DetectContentType(p)
	if strings.HasPrefix(mt, "image/") {
		return "image"
	}
	if strings.HasPrefix(mt, "video/") {
		return "video"
	}
	if bytes.HasPrefix(p, []byte("%PDF-")) || bytes.HasPrefix(p, []byte("PK\x03\x04")) || bytes.HasPrefix(p, []byte{0x1f, 0x8b}) || strings.HasPrefix(mt, "text/") || strings.HasPrefix(mt, "audio/") {
		return "nonvideo"
	}
	return "unknown"
}
func (u *attachmentUploads) validatePrefix(up *activeAttachmentUpload, chunk []byte) error {
	const probeBytes = 512
	if up.received >= probeBytes {
		return nil
	}
	size := int(up.received)
	if size > probeBytes {
		size = probeBytes
	}
	prefix := make([]byte, size)
	if size > 0 {
		if _, e := up.file.ReadAt(prefix, 0); e != nil {
			return e
		}
	}
	remaining := probeBytes - len(prefix)
	if remaining > len(chunk) {
		remaining = len(chunk)
	}
	prefix = append(prefix, chunk[:remaining]...)
	final := up.received+int64(len(chunk)) == up.expected
	return validateMediaPrefix(up, prefix, final)
}
func validateMediaPrefix(up *activeAttachmentUpload, prefix []byte, final bool) error {
	kind := mediaPrefixKind(prefix)
	// A fragmented first CBV2 frame cannot establish a non-video exception.
	// Defer inconclusive probes only within the same bounded 512-byte prefix.
	certain := len(prefix) >= 512 || final || kind == "video" || kind == "image"
	if !certain {
		return nil
	}
	if kind == "video" && up.expected > mediaVideoMaxBytes && !up.LegacyLarge {
		return errors.New("Video exceeds the 1 GiB limit")
	}
	if up.kind == "image" && kind != "image" {
		return errors.New("Image bytes do not match prepared media")
	}
	if up.kind == "video" && kind != "video" {
		return errors.New("Video container does not match declaration")
	}
	if up.kind == "file" && up.expected > mediaVideoMaxBytes && !up.LegacyLarge && kind != "nonvideo" {
		return errors.New("Unclassified large file requires a known non-video header")
	}
	if up.kind == "file" && mediaVideoHint(up.kind, up.originalName, up.mediaType) && kind != "video" {
		return errors.New("Video metadata contradicts file container")
	}
	if up.kind == "file" && kind == "video" && !backend.IsAudioType(up.mediaType) && !mediaVideoHint(up.kind, up.originalName, up.mediaType) {
		return errors.New("Video container contradicts file declaration")
	}
	return nil
}
func preparedImageDimensions(path, mime string) (int, int, error) {
	f, e := os.Open(path)
	if e != nil {
		return 0, 0, e
	}
	defer f.Close()
	prefix := make([]byte, 64)
	n, _ := io.ReadFull(f, prefix)
	prefix = prefix[:n]
	if http.DetectContentType(prefix) != mime {
		return 0, 0, errors.New("prepared image MIME mismatch")
	}
	if _, e = f.Seek(0, io.SeekStart); e != nil {
		return 0, 0, e
	}
	if mime != "image/webp" {
		v, _, e := image.DecodeConfig(io.LimitReader(f, backend.MediaImageMaxBytes+1))
		if e != nil {
			return 0, 0, e
		}
		if v.Width <= 0 || v.Height <= 0 || v.Width > 2560 || v.Height > 2560 {
			return 0, 0, errors.New("prepared image geometry exceeds policy")
		}
		return v.Width, v.Height, nil
	}
	if len(prefix) < 30 {
		return 0, 0, errors.New("invalid WebP header")
	}
	var w, h int
	switch string(prefix[12:16]) {
	case "VP8X":
		if prefix[20]&2 != 0 {
			return 0, 0, errors.New("animated WebP unsupported")
		}
		w = 1 + int(prefix[24]) + (int(prefix[25]) << 8) + (int(prefix[26]) << 16)
		h = 1 + int(prefix[27]) + (int(prefix[28]) << 8) + (int(prefix[29]) << 16)
	case "VP8 ":
		if string(prefix[23:26]) != "\x9d\x01\x2a" {
			return 0, 0, errors.New("invalid WebP frame")
		}
		w = int(binary.LittleEndian.Uint16(prefix[26:28]) & 0x3fff)
		h = int(binary.LittleEndian.Uint16(prefix[28:30]) & 0x3fff)
	case "VP8L":
		if prefix[20] != 0x2f {
			return 0, 0, errors.New("invalid WebP lossless frame")
		}
		bits := binary.LittleEndian.Uint32(prefix[21:25])
		w = 1 + int(bits&0x3fff)
		h = 1 + int((bits>>14)&0x3fff)
	default:
		return 0, 0, errors.New("unsupported WebP header")
	}
	if w <= 0 || h <= 0 || w > 2560 || h > 2560 {
		return 0, 0, errors.New("prepared image geometry exceeds policy")
	}
	return w, h, nil
}
func (u *attachmentUploads) scopeEvent(v map[string]any, up *activeAttachmentUpload) map[string]any {
	if up.Scope != nil {
		v["media_batch_id"] = up.Scope.BatchID
		v["media_batch_revision"] = up.Scope.Revision
		v["item_id"] = up.Scope.ItemID
		v["item_revision"] = up.Scope.ItemRevision
		if v["type"] == "attachment_upload_complete" && u.lifecycleHeld {
			b := u.client.hub.media.data.Batches[mediaKey(up.deviceID, up.Scope.Generation, up.Scope.BatchID)]
			if b != nil {
				for _, i := range b.Items {
					if i.UploadRequestID == up.requestID {
						v["release_descriptor"] = mediaReleaseDescriptor(b, i)
					}
				}
			}
		}
	}
	v["size_bytes"] = up.expected
	return v
}
func (u *attachmentUploads) finishTyped(up *activeAttachmentUpload, m *uploadedVideoManifest) error {
	if backend.IsAudioType(up.mediaType) {
		if up.Scope == nil || up.expected > backend.MediaAudioMaxBytes || backend.AudioExtension(up.mediaType) == "" {
			return errors.New("recording requires typed media admission and bounded audio")
		}
		duration, err := probeUploadedAudio(up.finalPath, up.mediaType)
		if err != nil {
			return err
		}
		m.DurationMS = duration
	}
	if up.Scope == nil {
		return nil
	}
	if hex.EncodeToString(up.digest.Sum(nil)) != up.Scope.SHA256 {
		return errors.New("Prepared media digest mismatch")
	}
	if up.kind == "image" {
		w, h, e := preparedImageDimensions(up.finalPath, up.mediaType)
		if e != nil {
			return e
		}
		m.Width = w
		m.Height = h
	}
	m.Scope = up.Scope
	return nil
}

func (u *attachmentUploads) uploadLeaseID(up *activeAttachmentUpload) string {
	return "upload/" + up.id + "/" + u.client.clientID
}

// Called under the original lifecycle guard from the correlated cancellation.
func (u *attachmentUploads) closeCancelledBatch(id string) {
	for _, up := range u.active {
		if up.Scope == nil || up.Scope.BatchID != id {
			continue
		}
		if up.file != nil {
			_ = up.file.Sync()
			if e := up.file.Close(); e != nil {
				continue
			}
			up.file = nil
		}
		_ = u.persistState(up)
		delete(u.client.hub.media.data.Readers, u.uploadLeaseID(up))
		_ = u.client.hub.media.save()
		delete(u.active, up.id)
	}
	u.client.uploadActive.Store(len(u.active) > 0)
}
