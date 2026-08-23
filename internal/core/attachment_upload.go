package core

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"everything-go/internal/backend"
)

const (
	attachmentMagicV1     = "CBV1"
	attachmentMagicV2     = "CBV2"
	attachmentIDBytes     = 34
	attachmentOffsetBytes = 8
	maxVideoUploadBytes   = int64(512 * 1024 * 1024)
	attachmentChunkBytes  = 512 * 1024
	staleUploadMaxAge     = 24 * time.Hour
)

var safeAttachmentComponent = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

type activeAttachmentUpload struct {
	id, requestID, sessionID, deviceID string
	name, mediaType                    string
	expected, received                 int64
	partPath, finalPath, statePath     string
	file                               *os.File
	digest                             hash.Hash
}

type attachmentUploads struct {
	root    string
	client  *Client
	active  map[string]*activeAttachmentUpload
	cleaned bool
}

type uploadedVideoManifest struct {
	Version    int    `json:"version"`
	UploadID   string `json:"upload_id"`
	SessionID  string `json:"session_id"`
	DeviceID   string `json:"device_id"`
	Name       string `json:"name"`
	MediaType  string `json:"media_type"`
	SizeBytes  int64  `json:"size_bytes"`
	SHA256     string `json:"sha256"`
	Path       string `json:"path"`
	DurationMS int64  `json:"duration_ms,omitempty"`
	Width      int    `json:"width,omitempty"`
	Height     int    `json:"height,omitempty"`
}

type resumableUploadState struct {
	Version      int    `json:"version"`
	UploadID     string `json:"upload_id"`
	RequestID    string `json:"upload_request_id"`
	SessionID    string `json:"session_id"`
	DeviceID     string `json:"device_id"`
	Name         string `json:"name"`
	MediaType    string `json:"media_type"`
	ExpectedSize int64  `json:"expected_size"`
	ReceivedSize int64  `json:"received_size"`
	PartPath     string `json:"part_path"`
	FinalPath    string `json:"final_path"`
}

func newAttachmentUploads(c *Client, dataDir string) *attachmentUploads {
	return &attachmentUploads{
		root: filepath.Join(dataDir, "uploads"), client: c,
		active: make(map[string]*activeAttachmentUpload),
	}
}

func attachmentSafe(value, fallback string) string {
	value = strings.Trim(safeAttachmentComponent.ReplaceAllString(value, "_"), "._")
	if value == "" {
		return fallback
	}
	if len(value) > 120 {
		return value[:120]
	}
	return value
}

func isVideoExtension(ext string) bool {
	switch strings.ToLower(ext) {
	case ".mp4", ".mov", ".m4v", ".webm", ".mkv", ".avi", ".3gp", ".3gpp":
		return true
	default:
		return false
	}
}

func resumableAttachmentID(deviceID, requestID string) string {
	sum := sha256.Sum256([]byte(deviceID + "\x00" + requestID))
	return "u_" + hex.EncodeToString(sum[:16])
}

func (u *attachmentUploads) init(sessionID, requestID, name, mediaType string, size int64) {
	if !u.cleaned {
		u.cleanupStale()
		u.cleaned = true
	}
	if requestID == "" {
		u.sendError(requestID, "", "Missing upload_request_id")
		return
	}
	if _, ok := u.client.hub.registry.Get(sessionID); !ok {
		u.sendError(requestID, "", "Unknown session")
		return
	}
	ext := strings.ToLower(filepath.Ext(name))
	if size <= 0 {
		u.sendError(requestID, "", "Video is empty")
		return
	}
	if size > maxVideoUploadBytes {
		u.sendError(requestID, "", "Video exceeds the 512 MB limit")
		return
	}
	if !strings.HasPrefix(mediaType, "video/") || !isVideoExtension(ext) {
		u.sendError(requestID, "", "Unsupported video format")
		return
	}
	id := resumableAttachmentID(u.client.deviceID, requestID)
	if existing := u.active[id]; existing != nil {
		u.client.enqueueEvent(map[string]any{
			"type": "attachment_upload_ready", "upload_request_id": requestID,
			"upload_id": id, "chunk_size": attachmentChunkBytes,
			"protocol_version": 2, "received_bytes": existing.received,
		})
		return
	}
	safeName := attachmentSafe(strings.TrimSuffix(filepath.Base(name), filepath.Ext(name)), "recording") + ext
	dir := filepath.Join(u.root, attachmentSafe(sessionID, "session"), id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		u.sendError(requestID, id, "Cannot create attachment store")
		return
	}
	partPath := filepath.Join(dir, "."+safeName+".part")
	finalPath := filepath.Join(dir, safeName)
	statePath := filepath.Join(dir, "upload-state.json")

	if complete, ok := u.completedUpload(requestID, sessionID, id, finalPath); ok {
		u.client.enqueueEvent(complete)
		return
	}

	received, digest, err := u.loadResumableState(
		statePath, partPath, id, requestID, sessionID, u.client.deviceID,
		safeName, mediaType, size,
	)
	if err != nil {
		u.sendError(requestID, id, err.Error())
		return
	}
	f, err := os.OpenFile(partPath, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		u.sendError(requestID, id, "Cannot create upload file")
		return
	}
	if _, err := f.Seek(received, io.SeekStart); err != nil {
		_ = f.Close()
		u.sendError(requestID, id, "Cannot resume upload file")
		return
	}
	u.active[id] = &activeAttachmentUpload{
		id: id, requestID: requestID, sessionID: sessionID, deviceID: u.client.deviceID,
		name: safeName, mediaType: mediaType, expected: size, received: received,
		partPath: partPath, finalPath: finalPath, statePath: statePath,
		file: f, digest: digest,
	}
	if err := u.persistState(u.active[id]); err != nil {
		u.discard(u.active[id])
		u.sendError(requestID, id, "Cannot save upload state")
		return
	}
	u.client.uploadActive.Store(true)
	u.client.enqueueEvent(map[string]any{
		"type": "attachment_upload_ready", "upload_request_id": requestID,
		"upload_id": id, "chunk_size": attachmentChunkBytes,
		"protocol_version": 2, "received_bytes": received,
	})
}

func (u *attachmentUploads) cleanupStale() {
	cutoff := time.Now().Add(-staleUploadMaxAge)
	var staleDirs []string
	_ = filepath.WalkDir(u.root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || entry.Name() != "upload-state.json" {
			return nil
		}
		info, statErr := entry.Info()
		if statErr == nil && info.ModTime().Before(cutoff) {
			staleDirs = append(staleDirs, filepath.Dir(path))
		}
		return nil
	})
	for _, dir := range staleDirs {
		if _, err := os.Stat(filepath.Join(dir, "manifest.json")); errors.Is(err, os.ErrNotExist) {
			_ = os.RemoveAll(dir)
		}
	}
}

func (u *attachmentUploads) writeFrame(data []byte) bool {
	if len(data) < len(attachmentMagicV1) {
		return false
	}
	magic := string(data[:len(attachmentMagicV1)])
	if magic != attachmentMagicV1 && magic != attachmentMagicV2 {
		return false
	}
	header := len(attachmentMagicV1) + attachmentIDBytes
	if magic == attachmentMagicV2 {
		header += attachmentOffsetBytes
	}
	if len(data) <= header {
		return true
	}
	id := string(data[len(attachmentMagicV1) : len(attachmentMagicV1)+attachmentIDBytes])
	up := u.active[id]
	if up == nil {
		return true
	}
	if magic == attachmentMagicV2 {
		offset := int64(binary.BigEndian.Uint64(data[header-attachmentOffsetBytes : header]))
		if offset != up.received {
			u.sendAck(up)
			return true
		}
	}
	chunk := data[header:]
	if up.received+int64(len(chunk)) > up.expected {
		u.fail(up, "Received more bytes than declared")
		return true
	}
	n, err := up.file.Write(chunk)
	if err != nil || n != len(chunk) {
		u.fail(up, "Failed to write upload")
		return true
	}
	_, _ = up.digest.Write(chunk)
	up.received += int64(len(chunk))
	if err := up.file.Sync(); err != nil {
		u.fail(up, "Failed to sync upload")
		return true
	}
	if err := u.persistState(up); err != nil {
		u.fail(up, "Failed to save upload progress")
		return true
	}
	if magic == attachmentMagicV2 {
		u.sendAck(up)
	}
	return true
}

func (u *attachmentUploads) finish(id string) {
	up := u.active[id]
	if up == nil {
		u.sendError("", id, "Unknown or expired upload")
		return
	}
	if up.received != up.expected {
		u.fail(up, fmt.Sprintf("Incomplete upload (%d/%d bytes)", up.received, up.expected))
		return
	}
	if err := up.file.Sync(); err != nil {
		u.fail(up, "Failed to sync upload")
		return
	}
	if err := up.file.Close(); err != nil {
		u.fail(up, "Failed to close upload")
		return
	}
	up.file = nil
	if err := os.Rename(up.partPath, up.finalPath); err != nil {
		u.fail(up, "Failed to finalize upload")
		return
	}
	manifest := uploadedVideoManifest{
		Version: 1, UploadID: up.id, SessionID: up.sessionID, DeviceID: up.deviceID,
		Name: up.name, MediaType: up.mediaType, SizeBytes: up.expected,
		SHA256: hex.EncodeToString(up.digest.Sum(nil)), Path: up.finalPath,
	}
	manifest.DurationMS, manifest.Width, manifest.Height = probeVideo(up.finalPath)
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil || os.WriteFile(filepath.Join(filepath.Dir(up.finalPath), "manifest.json"), raw, 0o600) != nil {
		_ = os.Remove(up.finalPath)
		u.fail(up, "Failed to save upload manifest")
		return
	}
	_ = os.Remove(up.statePath)
	delete(u.active, id)
	u.client.uploadActive.Store(len(u.active) > 0)
	u.client.enqueueEvent(map[string]any{
		"type": "attachment_upload_complete", "upload_request_id": up.requestID,
		"upload_id": up.id, "name": up.name, "media_type": up.mediaType,
		"size_bytes": up.expected, "sha256": manifest.SHA256,
		"remote_path": up.finalPath, "path": up.finalPath,
		"duration_ms": manifest.DurationMS, "width": manifest.Width, "height": manifest.Height,
	})
}

func probeVideo(path string) (int64, int, int) {
	out, err := exec.Command(
		"ffprobe", "-v", "error", "-show_entries", "format=duration:stream=width,height",
		"-select_streams", "v:0", "-of", "json", path,
	).Output()
	if err != nil {
		return 0, 0, 0
	}
	var result struct {
		Streams []struct{ Width, Height int } `json:"streams"`
		Format  struct{ Duration string }     `json:"format"`
	}
	if json.Unmarshal(out, &result) != nil {
		return 0, 0, 0
	}
	seconds, _ := strconv.ParseFloat(result.Format.Duration, 64)
	if len(result.Streams) == 0 {
		return int64(seconds * 1000), 0, 0
	}
	return int64(seconds * 1000), result.Streams[0].Width, result.Streams[0].Height
}

func (u *attachmentUploads) cancel(id string) {
	if up := u.active[id]; up != nil {
		u.discard(up)
	}
	u.client.uploadActive.Store(len(u.active) > 0)
}

func (u *attachmentUploads) close() {
	for _, up := range u.active {
		if up.file != nil {
			_ = up.file.Sync()
			_ = up.file.Close()
			up.file = nil
		}
		_ = u.persistState(up)
		delete(u.active, up.id)
	}
	u.client.uploadActive.Store(false)
}

func (u *attachmentUploads) fail(up *activeAttachmentUpload, message string) {
	requestID, id := up.requestID, up.id
	u.discard(up)
	u.client.uploadActive.Store(len(u.active) > 0)
	u.sendError(requestID, id, message)
}

func (u *attachmentUploads) discard(up *activeAttachmentUpload) {
	delete(u.active, up.id)
	if up.file != nil {
		_ = up.file.Close()
	}
	_ = os.Remove(up.partPath)
	_ = os.Remove(up.statePath)
	_ = os.Remove(filepath.Dir(up.partPath))
}

func (u *attachmentUploads) sendAck(up *activeAttachmentUpload) {
	u.client.enqueueEvent(map[string]any{
		"type": "attachment_upload_ack", "upload_request_id": up.requestID,
		"upload_id": up.id, "received_bytes": up.received,
	})
}

func (u *attachmentUploads) persistState(up *activeAttachmentUpload) error {
	state := resumableUploadState{
		Version: 2, UploadID: up.id, RequestID: up.requestID,
		SessionID: up.sessionID, DeviceID: up.deviceID,
		Name: up.name, MediaType: up.mediaType,
		ExpectedSize: up.expected, ReceivedSize: up.received,
		PartPath: up.partPath, FinalPath: up.finalPath,
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp := up.statePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, up.statePath)
}

func (u *attachmentUploads) loadResumableState(
	statePath, partPath, id, requestID, sessionID, deviceID, name, mediaType string,
	expected int64,
) (int64, hash.Hash, error) {
	digest := sha256.New()
	raw, err := os.ReadFile(statePath)
	if errors.Is(err, os.ErrNotExist) {
		if info, statErr := os.Stat(partPath); statErr == nil && info.Size() > 0 {
			return 0, nil, errors.New("Upload state is missing")
		}
		return 0, digest, nil
	}
	if err != nil {
		return 0, nil, errors.New("Cannot read upload state")
	}
	var state resumableUploadState
	if json.Unmarshal(raw, &state) != nil ||
		state.Version != 2 ||
		state.UploadID != id ||
		state.RequestID != requestID ||
		state.SessionID != sessionID ||
		state.DeviceID != deviceID ||
		state.Name != name ||
		state.MediaType != mediaType ||
		state.ExpectedSize != expected ||
		state.PartPath != partPath {
		return 0, nil, errors.New("Upload resume metadata does not match")
	}
	f, err := os.Open(partPath)
	if err != nil {
		return 0, nil, errors.New("Upload partial file is missing")
	}
	defer f.Close()
	n, err := io.Copy(digest, f)
	if err != nil || n != state.ReceivedSize || n > expected {
		return 0, nil, errors.New("Upload partial file is inconsistent")
	}
	return n, digest, nil
}

func (u *attachmentUploads) completedUpload(
	requestID, sessionID, id, finalPath string,
) (map[string]any, bool) {
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(finalPath), "manifest.json"))
	if err != nil {
		return nil, false
	}
	var manifest uploadedVideoManifest
	if json.Unmarshal(raw, &manifest) != nil ||
		manifest.UploadID != id ||
		manifest.SessionID != sessionID ||
		manifest.Path != finalPath {
		return nil, false
	}
	info, err := os.Stat(finalPath)
	if err != nil || info.Size() != manifest.SizeBytes {
		return nil, false
	}
	return map[string]any{
		"type": "attachment_upload_complete", "upload_request_id": requestID,
		"upload_id": manifest.UploadID, "name": manifest.Name,
		"media_type": manifest.MediaType, "size_bytes": manifest.SizeBytes,
		"sha256": manifest.SHA256, "remote_path": manifest.Path, "path": manifest.Path,
		"duration_ms": manifest.DurationMS, "width": manifest.Width, "height": manifest.Height,
	}, true
}

func (u *attachmentUploads) sendError(requestID, id, message string) {
	u.client.enqueueEvent(map[string]any{
		"type": "attachment_upload_error", "upload_request_id": requestID,
		"upload_id": id, "message": message,
	})
}

func (h *Hub) resolveUploadedVideos(sessionID, content string, files []backend.FileAttachment) (string, []backend.FileAttachment, error) {
	inline := make([]backend.FileAttachment, 0, len(files))
	var videos []uploadedVideoManifest
	root, err := filepath.Abs(filepath.Join(h.cfg.DataDir, "uploads"))
	if err != nil {
		return content, nil, err
	}
	for _, file := range files {
		if file.RemotePath == "" {
			inline = append(inline, file)
			continue
		}
		candidate, err := filepath.Abs(file.RemotePath)
		if err != nil {
			return content, nil, err
		}
		rel, err := filepath.Rel(root, candidate)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return content, nil, errors.New("uploaded video path is outside the attachment store")
		}
		raw, err := os.ReadFile(filepath.Join(filepath.Dir(candidate), "manifest.json"))
		if err != nil {
			return content, nil, errors.New("uploaded video manifest is missing")
		}
		var manifest uploadedVideoManifest
		if json.Unmarshal(raw, &manifest) != nil || manifest.SessionID != sessionID || manifest.Path != candidate {
			return content, nil, errors.New("uploaded video does not belong to this session")
		}
		info, err := os.Stat(candidate)
		if err != nil || info.Size() != manifest.SizeBytes {
			return content, nil, errors.New("uploaded video is missing or incomplete")
		}
		videos = append(videos, manifest)
	}
	if len(videos) == 0 {
		return content, inline, nil
	}
	var b strings.Builder
	b.WriteString(content)
	b.WriteString("\n\n[Bridge attached video files — inspect the original files at these absolute paths]\n")
	for _, video := range videos {
		fmt.Fprintf(&b, "- name=%s; path=%s; media_type=%s; size_bytes=%d; sha256=%s",
			video.Name, video.Path, video.MediaType, video.SizeBytes, video.SHA256)
		if video.DurationMS > 0 {
			fmt.Fprintf(&b, "; duration_ms=%d", video.DurationMS)
		}
		if video.Width > 0 && video.Height > 0 {
			fmt.Fprintf(&b, "; dimensions=%dx%d", video.Width, video.Height)
		}
		b.WriteByte('\n')
	}
	b.WriteString("Use local video tools (for example ffprobe/ffmpeg frame extraction) to inspect them; do not claim to have reviewed the video unless you actually opened or sampled it.")
	return b.String(), inline, nil
}
