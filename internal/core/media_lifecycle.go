package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

const mediaVideoMaxBytes int64 = 1073741824
const mediaControlMaxBytes = 262144

type mediaBatch struct {
	ExecutionThread string                    `json:"execution_thread,omitempty"`
	ExecutionSource uint64                    `json:"execution_source_revision,omitempty"`
	ID              string                    `json:"id"`
	Revision        uint64                    `json:"revision"`
	Session         string                    `json:"session"`
	Thread          string                    `json:"thread"`
	Config          uint64                    `json:"config"`
	Device          string                    `json:"device"`
	Generation      uint64                    `json:"generation"`
	SourceRevision  uint64                    `json:"source_revision"`
	RoleEpoch       uint64                    `json:"role_epoch"`
	Content         string                    `json:"content"`
	Items           []protocol.MediaBatchItem `json:"items"`
	BodyHash        string                    `json:"body_hash"`
	State           string                    `json:"state"`
	CancelID        string                    `json:"cancel_id,omitempty"`
	CancelItem      string                    `json:"cancel_item,omitempty"`
	Objects         map[string]string         `json:"objects"`
	Updated         int64                     `json:"updated"`
}
type mediaLegacyWitness struct {
	MetadataHash string                `json:"metadata_hash"`
	FileSize     int64                 `json:"file_size"`
	FileHash     string                `json:"file_hash"`
	Pending      *resumableUploadState `json:"pending,omitempty"`
}
type mediaReadLease struct {
	ObjectID string `json:"object_id"`
	Session  string `json:"session_id"`
	Path     string `json:"path"`
	Hash     string `json:"sha256"`
	Size     int64  `json:"size"`
	Purpose  string `json:"purpose"`
}
type mediaGCTombstone struct {
	ObjectID string `json:"object_id"`
	Session  string `json:"session_id"`
	Path     string `json:"path"`
	Hash     string `json:"sha256"`
	Size     int64  `json:"size"`
	State    string `json:"state"`
}
type mediaLifecycleState struct {
	Readers     map[string]mediaReadLease     `json:"readers,omitempty"`
	GC          map[string]mediaGCTombstone   `json:"gc,omitempty"`
	Credentials map[string]uint64             `json:"credentials,omitempty"`
	Version     int                           `json:"version"`
	Batches     map[string]*mediaBatch        `json:"batches"`
	Legacy      map[string]mediaLegacyWitness `json:"legacy"`
	Created     int64                         `json:"created"`
}
type mediaLifecycle struct {
	gcMu  sync.Mutex
	fault func(string) error
	mu    sync.Mutex
	root  string
	data  mediaLifecycleState
	err   error
}

func openMediaLifecycle(dataDir string) *mediaLifecycle {
	m := &mediaLifecycle{root: filepath.Join(dataDir, "uploads")}
	if dataDir == "" {
		m.err = errors.New("media store unavailable")
		return m
	}
	m.err = os.MkdirAll(m.root, 0700)
	if m.err != nil {
		return m
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f, e := m.lock()
	if e != nil {
		m.err = e
		return m
	}
	defer m.unlock(f)
	raw, e := os.ReadFile(m.path())
	if e == nil {
		m.err = json.Unmarshal(raw, &m.data)
		if m.data.Version != 1 || m.data.Batches == nil || m.data.Legacy == nil {
			m.err = errors.New("media lifecycle journal unavailable")
		}
		return m
	}
	if !errors.Is(e, os.ErrNotExist) {
		m.err = e
		return m
	}
	m.data = mediaLifecycleState{Version: 1, Batches: map[string]*mediaBatch{}, Legacy: map[string]mediaLegacyWitness{}, Created: time.Now().UnixMilli()}
	// The first server-owned policy-era inventory is made before advertising the
	// new writer. Client flags/mtime alone never create a legacy exception.
	m.err = m.captureLegacy()
	if m.err == nil {
		m.err = m.save()
	}
	return m
}
func (m *mediaLifecycle) path() string { return filepath.Join(m.root, "media-lifecycle-v1.json") }
func (m *mediaLifecycle) lock() (*os.File, error) {
	f, e := os.OpenFile(filepath.Join(m.root, ".media-lifecycle.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); e != nil {
		f.Close()
		return nil, e
	}
	return f, nil
}
func (m *mediaLifecycle) unlock(f *os.File) {
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
}
func (m *mediaLifecycle) begin() (func(), error) {
	if m == nil {
		return nil, errors.New("media lifecycle unavailable")
	}
	m.mu.Lock()
	if m.err != nil {
		m.mu.Unlock()
		return nil, m.err
	}
	f, e := m.lock()
	if e != nil {
		m.mu.Unlock()
		return nil, e
	}
	raw, e := os.ReadFile(m.path())
	if e == nil {
		var fresh mediaLifecycleState
		e = json.Unmarshal(raw, &fresh)
		if e == nil && (fresh.Version != 1 || fresh.Batches == nil || fresh.Legacy == nil) {
			e = errors.New("media journal invalid")
		}
		if e == nil {
			if fresh.Readers == nil {
				fresh.Readers = map[string]mediaReadLease{}
			}
			m.data = fresh
		}
	}
	if e != nil {
		m.unlock(f)
		m.mu.Unlock()
		return nil, e
	}
	return func() { m.unlock(f); m.mu.Unlock() }, nil
}
func (m *mediaLifecycle) save() error { return mediaAtomicJSON(m.path(), m.data) }
func mediaAtomicJSON(path string, v any) error {
	raw, e := json.Marshal(v)
	if e != nil {
		return e
	}
	dir := filepath.Dir(path)
	info, e := os.Lstat(dir)
	if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("media directory replaced")
	}
	f, e := os.CreateTemp(dir, ".media-write-")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(raw)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	after, e2 := os.Lstat(dir)
	if e2 != nil || !os.SameFile(info, after) {
		return errors.New("media directory identity changed")
	}
	if e != nil {
		return e
	}
	if e = os.Rename(tmp, path); e != nil {
		return e
	}
	d, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func mediaKey(device string, generation uint64, id string) string {
	return fmt.Sprintf("%s/%d/%s", device, generation, id)
}
func (h *Hub) mediaActor(c *Client) (taskapi.VerifiedContext, error) {
	b, e := h.taskHuman(c)
	if e != nil {
		return taskapi.VerifiedContext{}, e
	}
	return b.Check(c.ctx)
}
func (h *Hub) mediaTarget(sid string, thread string, revision uint64) error {
	s, e := h.intakeSession(sid)
	if e != nil {
		return e
	}
	if e = h.intakeWritePolicy(s); e != nil {
		return e
	}
	v := s.SettingsSnapshot()
	if v.ResumeID != thread || v.ConfigRevision != revision {
		return errors.New("media source changed")
	}
	return nil
}
func mediaPolicy() map[string]any {
	return map[string]any{"version": 1, "maximum_items": 8, "image_max_bytes": int64(4194304), "video_max_bytes": mediaVideoMaxBytes, "file_max_bytes": maxFileUploadBytes, "control_max_bytes": mediaControlMaxBytes, "model_max_serialized_bytes": backend.MediaModelMaxBytes, "chunk_size": attachmentChunkBytes, "maximum_active_uploads_per_client": 2, "purposes": []string{"image", "video", "file"}}
}
func mediaHash(v any) string {
	b, _ := json.Marshal(v)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func (h *Hub) handleMediaBatch(c *Client, cmd clientproto.Command) {
	op := strings.TrimPrefix(cmd.Kind, "media_batch_")
	event := map[string]any{"type": "media_batch_receipt", "operation": op, "request_id": cmd.RequestID, "authority_instance_id": h.cfg.InstanceID, "session_id": cmd.SessionID, "media_batch_id": cmd.MediaBatchID}
	fail := func(code string) {
		event["state"] = "rejected_known_none"
		event["known_effect"] = "none"
		if code == "store_unavailable" {
			event["state"] = "admitting_unknown"
			event["known_effect"] = "unknown"
		}
		event["error_code"] = code
		c.enqueueEvent(event)
	}
	caller, e := h.mediaActor(c)
	if e != nil {
		fail("caller_unbound")
		return
	}
	s, e := h.intakeSession(cmd.SessionID)
	if e != nil {
		fail("permission")
		return
	}
	if op == "capabilities" {
		v := s.SettingsSnapshot()
		var thread any
		if v.ResumeID != "" {
			thread = v.ResumeID
		}
		policy := mediaPolicy()
		available := h.media != nil && h.media.err == nil && h.intakeWritePolicy(s) == nil
		event["state"] = "capabilities"
		event["available"] = available
		event["policy"] = policy
		event["policy_hash"] = mediaHash(policy)
		event["task_api_schema_hash"] = contract.MediaRefsHash()
		event["target"] = map[string]any{"instance_id": h.cfg.InstanceID, "session_id": s.ID, "expected_native_thread_id": thread, "expected_config_revision": v.ConfigRevision}
		if !available {
			event["reason"] = "unavailable"
		}
		c.enqueueEvent(event)
		return
	}
	if cmd.MediaBatchID == "" || len(cmd.MediaBatchID) > 200 || cmd.RequestID == "" {
		fail("invalid_argument")
		return
	}
	done, e := h.media.begin()
	if e != nil {
		fail("store_unavailable")
		return
	}
	defer done()
	device := strings.TrimPrefix(caller.StableScopeID, "device:")
	key := mediaKey(device, caller.NamespaceGeneration, cmd.MediaBatchID)
	batch := h.media.data.Batches[key]
	if op == "prepare" {
		thread := cmd.ExpectedNativeThreadID
		if cmd.ExpectedConfigRevision == nil || cmd.MediaBatchRevision == 0 || cmd.MediaBatchRevision > 9007199254740991 || len(cmd.MediaItems) == 0 || len(cmd.MediaItems) > 8 {
			fail("invalid_argument")
			return
		}
		if e = h.mediaTarget(cmd.SessionID, thread, *cmd.ExpectedConfigRevision); e != nil {
			fail("source_changed")
			return
		}
		b := &mediaBatch{ID: cmd.MediaBatchID, Revision: cmd.MediaBatchRevision, Session: s.ID, Thread: thread, Config: *cmd.ExpectedConfigRevision, SourceRevision: s.SettingsSnapshot().SourceRevision, RoleEpoch: h.SessionRoleEpoch(s), Device: device, Generation: caller.NamespaceGeneration, Content: cmd.Content, Items: cmd.MediaItems, Objects: map[string]string{}, State: "prepared", Updated: time.Now().UnixMilli()}
		frozen := *b
		frozen.Updated = 0
		frozen.State = ""
		b.BodyHash = mediaHash(frozen)
		raw, _ := json.Marshal(b)
		if len(raw) > mediaControlMaxBytes {
			fail("control_budget")
			return
		}
		seen := map[string]bool{}
		for _, item := range b.Items {
			if item.ItemID == "" || item.ItemRevision == 0 || item.ItemRevision > 9007199254740991 || len(item.ItemID) > 200 || len(item.UploadRequestID) > 200 || item.UploadRequestID == "" || seen[item.ItemID] || seen["u/"+item.UploadRequestID] || !jarvisSHA.MatchString(item.SHA256) || !validMediaDescriptor(item) {
				fail("invalid_item")
				return
			}
			seen[item.ItemID] = true
			seen["u/"+item.UploadRequestID] = true
		}
		if batch != nil {
			if batch.BodyHash != b.BodyHash {
				fail("idempotency_conflict")
				return
			}
		} else {
			for _, item := range b.Items {
				if item.AttachmentID == "" {
					continue
				}
				if tomb, ok := h.media.data.GC[item.AttachmentID]; ok && tomb.State != "" {
					fail("object_tombstoned")
					return
				}
				m, e := h.unifiedUpload(s.ID, item.AttachmentID)
				if e == nil && m.Scope != nil {
					original := h.media.data.Batches[mediaKey(device, m.Scope.Generation, m.Scope.BatchID)]
					if original == nil || original.State == "admitting_unknown" {
						fail("reuse_original_unresolved")
						return
					}
				}
				if e != nil || m.DeviceID != device || (m.Scope != nil && m.Scope.Generation != caller.NamespaceGeneration) || (m.Scope == nil && h.media.data.Credentials[device] != caller.NamespaceGeneration) || m.MediaType != item.MediaType || m.SizeBytes != item.SizeBytes || m.SHA256 != item.SHA256 || m.Kind != item.UploadKind {
					fail("reuse_scope_changed")
					return
				}
				b.Objects[item.UploadRequestID] = m.UploadID
			}
			h.media.data.Batches[key] = b
			if e = h.media.save(); e != nil {
				event["state"] = "admitting_unknown"
				event["known_effect"] = "unknown"
				event["error_code"] = "store_unavailable"
				c.enqueueEvent(event)
				return
			}
			batch = b
		}
	} else if op == "cancel" {
		if batch == nil { // A cancel arriving before prepare/init is a durable fence.
			batch = &mediaBatch{ID: cmd.MediaBatchID, Session: s.ID, Device: device, Generation: caller.NamespaceGeneration, State: "cancelled_before_admission", CancelID: cmd.RequestID, CancelItem: cmd.UploadRequestID, Objects: map[string]string{}, Updated: time.Now().UnixMilli()}
			h.media.data.Batches[key] = batch
		} else if batch.Session != s.ID {
			fail("permission")
			return
		} else if batch.State == "prepared" {
			found := false
			for _, i := range batch.Items {
				if i.UploadRequestID == cmd.UploadRequestID {
					found = true
				}
			}
			if !found {
				fail("invalid_item")
				return
			}
			batch.State = "cancelled_before_admission"
			batch.CancelID = cmd.RequestID
			batch.CancelItem = cmd.UploadRequestID
			batch.Updated = time.Now().UnixMilli()
		}
		// admitting_unknown is never downgraded by absence of a queue row.
		if e = h.media.save(); e != nil {
			fail("store_unavailable")
			return
		}
	} else if op != "status" {
		fail("unsupported")
		return
	}
	if batch != nil && batch.Session == s.ID && batch.State == "admitting_unknown" {
		if entry, found, e := h.messageQueue.Get(s.ID, batch.ID); e == nil && found {
			var queued queuedPayload
			if json.Unmarshal(entry.Payload, &queued) == nil && mediaQueueBinding(batch, queued) {
				batch.State = "admitted"
				_ = h.media.save()
			}
		}
	}
	if batch == nil || batch.Session != s.ID {
		event["state"] = "not_found"
		event["known_effect"] = "unknown"
		c.enqueueEvent(event)
		return
	}
	if op == "cancel" && batch.State == "cancelled_before_admission" && c.uploads != nil {
		c.uploads.closeCancelledBatch(batch.ID)
	}
	h.mediaBatchReceipt(event, batch)
	c.enqueueEvent(event)
}
func (h *Hub) mediaBatchReceipt(event map[string]any, b *mediaBatch) {
	event["state"] = b.State
	event["media_batch_revision"] = b.Revision
	event["body_hash"] = b.BodyHash
	event["known_effect"] = "unknown"
	if b.State == "prepared" || b.State == "cancelled_before_admission" {
		event["known_effect"] = "none"
	}
	if b.State == "admitted" {
		event["known_effect"] = "admitted"
	}
	event["cancel_request_id"] = b.CancelID
	event["cancelled_upload_request_id"] = b.CancelItem
	items := []map[string]any{}
	for _, i := range b.Items {
		v := map[string]any{"item_id": i.ItemID, "item_revision": i.ItemRevision, "upload_request_id": i.UploadRequestID, "size_bytes": i.SizeBytes, "media_type": i.MediaType, "sha256": i.SHA256, "received_bytes": int64(0), "state": "pending"}
		id := resumableAttachmentID(b.Device, i.UploadRequestID)
		raw, e := os.ReadFile(filepath.Join(h.media.root, attachmentSafe(b.Session, "session"), id, "upload-state.json"))
		var pending resumableUploadState
		if e == nil && json.Unmarshal(raw, &pending) == nil && pending.Scope != nil && pending.Scope.BatchID == b.ID && pending.Scope.BodyHash == b.BodyHash && pending.Scope.Generation == b.Generation && pending.RequestID == i.UploadRequestID && pending.Scope.ItemID == i.ItemID && pending.Scope.ItemRevision == i.ItemRevision && pending.ReceivedSize >= 0 && pending.ReceivedSize <= i.SizeBytes {
			if st, e := os.Lstat(pending.PartPath); e == nil && st.Mode().IsRegular() && st.Size() >= pending.ReceivedSize {
				v["upload_id"] = id
				v["received_bytes"] = pending.ReceivedSize
				v["state"] = "uploading"
			}
		}
		if id := b.Objects[i.UploadRequestID]; id != "" {
			v["upload_id"] = id
			v["received_bytes"] = i.SizeBytes
			v["state"] = "complete"
		}
		items = append(items, v)
	}
	event["items"] = items
}
func validMediaDescriptor(i protocol.MediaBatchItem) bool {
	if i.SizeBytes <= 0 || len(i.Name) > 500 || len(i.MediaType) > 150 {
		return false
	}
	switch i.UploadKind {
	case "image":
		return i.SizeBytes <= backend.MediaImageMaxBytes && (i.MediaType == "image/jpeg" || i.MediaType == "image/png" || i.MediaType == "image/webp")
	case "video":
		return i.SizeBytes <= mediaVideoMaxBytes && strings.HasPrefix(i.MediaType, "video/") && isVideoExtension(filepath.Ext(i.Name))
	case "file":
		return i.SizeBytes <= maxFileUploadBytes
	default:
		return false
	}
}

// Caller already holds lifecycle guard; queue/intake transaction follows it.
// PREPARE is durable before admission. A crash after reserving admission always
// leaves an UNKNOWN pin, even if the separate queue transaction is absent.
func (h *Hub) mediaBeginAdmission(caller taskapi.VerifiedContext, sid, batchID, requestID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) (*mediaBatch, error) {
	if len(images)+len(files) > 8 {
		return nil, errors.New("maximum 8 media items")
	}
	if batchID == "" {
		if h.hasTypedFiles(sid, files) {
			return nil, errors.New("media batch required for typed file")
		}
		for _, i := range images {
			if i.AttachmentID != "" {
				return nil, errors.New("media batch required")
			}
		}
		return nil, nil
	}
	device := strings.TrimPrefix(caller.StableScopeID, "device:")
	b := h.media.data.Batches[mediaKey(device, caller.NamespaceGeneration, batchID)]
	if b == nil || b.Session != sid || b.ID != requestID || b.Content != content {
		return nil, errors.New("media batch mismatch")
	}
	if b.State == "admitted" {
		return b, nil
	}
	if b.State != "prepared" {
		return nil, errors.New("media batch cancelled or uncertain; lookup original")
	}
	if e := h.mediaBatchTarget(b); e != nil {
		return nil, e
	}
	s, _ := h.registry.Get(sid)
	if h.SessionRoleEpoch(s) != b.RoleEpoch {
		return nil, errors.New("media role source changed")
	}
	supplied := map[string]int{}
	expectedInput := map[string]protocol.MediaBatchItem{}
	for _, i := range b.Items {
		if id := b.Objects[i.UploadRequestID]; id != "" {
			expectedInput[id] = i
		}
	}
	for _, i := range images {
		if i.AttachmentID == "" || i.Data != "" {
			return nil, errors.New("typed batch requires immutable refs")
		}
		wanted, ok := expectedInput[i.AttachmentID]
		if !ok || wanted.UploadKind != "image" || wanted.MediaType != i.MediaType || wanted.SHA256 != i.SHA256 || wanted.SizeBytes != i.SizeBytes {
			return nil, errors.New("image descriptor mismatch")
		}
		supplied[i.AttachmentID]++
	}
	for _, f := range files {
		if f.AttachmentID == "" || f.Content != "" {
			return nil, errors.New("typed batch requires immutable refs")
		}
		wanted, ok := expectedInput[f.AttachmentID]
		if !ok || wanted.UploadKind == "image" || wanted.MediaType != f.MediaType || wanted.SHA256 != f.SHA256 || wanted.SizeBytes != f.SizeBytes || wanted.Name != f.Name {
			return nil, errors.New("file descriptor mismatch")
		}
		supplied[f.AttachmentID]++
	}
	if len(images)+len(files) != len(b.Items) {
		return nil, errors.New("full media set required")
	}
	for _, i := range b.Items {
		id := b.Objects[i.UploadRequestID]
		if id == "" || supplied[id] < 1 {
			return nil, errors.New("media object not ready or duplicate")
		}
		supplied[id]--
		if supplied[id] == 0 {
			delete(supplied, id)
		}
		if tomb, ok := h.media.data.GC[id]; ok && tomb.State != "" {
			return nil, errors.New("media object tombstoned")
		}
		m, e := h.unifiedUpload(sid, id)
		if e != nil || m.DeviceID != device || m.SHA256 != i.SHA256 || m.SizeBytes != i.SizeBytes || m.MediaType != i.MediaType || m.Kind != i.UploadKind {
			return nil, errors.New("media object binding changed")
		}
		info, e := os.Lstat(m.Path)
		if e != nil || !info.Mode().IsRegular() || info.Size() != i.SizeBytes {
			return nil, errors.New("media object changed")
		}
	}
	b.State = "admitting_unknown"
	b.Updated = time.Now().UnixMilli()
	if e := h.media.save(); e != nil {
		return nil, e
	}
	if e := h.media.inject("prepare_before_queue"); e != nil {
		return nil, e
	}
	return b, nil
}
func (h *Hub) mediaAdmitted(b *mediaBatch) error {
	if b == nil || b.State == "admitted" {
		return nil
	}
	if e := h.media.inject("queue_commit_before_receipt"); e != nil {
		return e
	}
	b.State = "admitted"
	b.Updated = time.Now().UnixMilli()
	return h.media.save()
}

// A typed object with any PREPARE/UNKNOWN/admitted/history pin is immutable and
// excluded from every old cleaner. Cancellation never implies physical erase.
func (m *mediaLifecycle) pinnedID(id string) bool {
	for _, r := range m.data.Readers {
		if r.ObjectID == id {
			return true
		}
	}
	for _, b := range m.data.Batches {
		for _, object := range b.Objects {
			if object == id {
				return true
			}
		}
		for _, i := range b.Items {
			if resumableAttachmentID(b.Device, i.UploadRequestID) == id {
				return true
			}
		}
	}
	return false
}

var _ = context.Background
var _ = session.Closed

func (m *mediaLifecycle) protectDirectory(dir string) bool {
	// Called outside lifecycle guard by the original stale cleaner. Any new
	// prepared upload is retained even before its final object ID is published.
	done, e := m.begin()
	if e != nil {
		return true
	}
	defer done()
	return m.pinnedID(filepath.Base(dir))
}

func (h *Hub) mediaBatchTarget(b *mediaBatch) error {
	if e := h.mediaTarget(b.Session, b.Thread, b.Config); e != nil {
		return e
	}
	s, ok := h.registry.Get(b.Session)
	if !ok || s.SettingsSnapshot().SourceRevision != b.SourceRevision || h.SessionRoleEpoch(s) != b.RoleEpoch {
		return errors.New("media source revision changed")
	}
	return nil
}

func (m *mediaLifecycle) inject(stage string) error {
	if m.fault != nil {
		return m.fault(stage)
	}
	return nil
}

func mediaQueueBinding(b *mediaBatch, p queuedPayload) bool {
	return b != nil && p.MediaBatchID == b.ID && p.MediaBodyHash == b.BodyHash && p.OwnerDevice == b.Device && p.RoleEpoch == b.RoleEpoch && p.Configuration != nil && p.Configuration.Revision == b.Config
}
