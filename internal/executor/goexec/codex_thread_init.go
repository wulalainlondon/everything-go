package goexec

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

const (
	codexThreadInitSlowAfter = 15 * time.Second
	codexThreadInitTimeout   = 60 * time.Second
)

// Only well-formed protocol-level method/parameter rejection proves this start
// was refused before initialization. Internal/server errors can follow effects.
func definiteThreadInitRejection(raw string) bool {
	var reply struct {
		Code    *int    `json:"code"`
		Message *string `json:"message"`
	}
	return json.Unmarshal([]byte(raw), &reply) == nil && reply.Code != nil && reply.Message != nil && (*reply.Code == -32601 || *reply.Code == -32602)
}

type codexThreadInitError struct {
	Code  string
	Cause error
}

func (e *codexThreadInitError) Error() string {
	if e.Cause == nil {
		return e.Code
	}
	return e.Code + ": " + e.Cause.Error()
}
func (e *codexThreadInitError) Unwrap() error { return e.Cause }
func codexThreadInitCode(err error) string {
	var init *codexThreadInitError
	if errors.As(err, &init) {
		return init.Code
	}
	return "thread_initialization_failed"
}

// This receipt is admission evidence, not permission to adopt a thread or replay
// input. A crash after bytes but before the ACK remains pending on reopen.
type codexThreadInitRecord struct {
	ContextGeneration uint64 `json:"context_generation"`
	Schema            int    `json:"schema"`
	SessionID         string `json:"session_id"`
	RequestID         string `json:"request_id,omitempty"`
	RPCID             int    `json:"rpc_id"`
	TransportEpoch    uint64 `json:"transport_epoch"`
	ConfigRevision    uint64 `json:"config_revision"`
	SourceRevision    uint64 `json:"source_revision"`
	RoleEpoch         uint64 `json:"role_epoch"`
	StartedAt         int64  `json:"started_at"`
	Status            string `json:"status"`
	ThreadID          string `json:"thread_id,omitempty"`
}

func (c *Codex) threadInitPath(id string) string {
	return filepath.Join(c.dataDir, "codex-thread-starts", fmt.Sprintf("%x.json", sha256.Sum256([]byte(id))))
}

// Caller holds threadInitMu. Missing-after-observed and malformed records fail
// closed; neither reconnect nor a changed configuration clears an unknown start.
func (c *Codex) loadThreadInit(id string) (*codexThreadInitRecord, error) {
	previous, observed := c.threadInitRecords[id]
	if c.dataDir == "" {
		if observed {
			return &previous, nil
		}
		return nil, nil
	}
	path := c.threadInitPath(id)
	info, err := os.Lstat(path)
	if os.IsNotExist(err) && !observed {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 16<<10 {
		return nil, errors.New("invalid initialization receipt")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (16<<10)+1))
	if err != nil {
		return nil, err
	}
	var record codexThreadInitRecord
	if len(data) > 16<<10 || json.Unmarshal(data, &record) != nil || record.Schema != 1 || record.SessionID != id || record.RPCID <= 0 || record.StartedAt <= 0 ||
		(record.Status != "pending" && record.Status != "acknowledged" && record.Status != "rejected") || (record.Status == "acknowledged" && record.ThreadID == "") {
		return nil, errors.New("invalid initialization receipt")
	}
	if observed && record != previous {
		return nil, errors.New("initialization receipt changed unexpectedly")
	}
	if c.threadInitRecords == nil {
		c.threadInitRecords = make(map[string]codexThreadInitRecord)
	}
	c.threadInitRecords[id] = record
	return &record, nil
}

func threadInitAllows(record *codexThreadInitRecord, snap session.Snapshot) bool {
	return record == nil || record.Status == "rejected" || (record.Status == "acknowledged" && ((snap.ResumeID != "" && snap.ResumeID == record.ThreadID) || snap.ContextGeneration > record.ContextGeneration))
}

func (c *Codex) threadInitReady(snap session.Snapshot) error {
	c.threadInitMu.Lock()
	defer c.threadInitMu.Unlock()
	record, err := c.loadThreadInit(snap.ID)
	if err != nil || !threadInitAllows(record, snap) {
		return &codexThreadInitError{Code: "thread_initialization_unknown"}
	}
	return nil
}

// Caller holds threadInitMu. Sync the intent before dispatch and retain it until
// an exact response. Atomic replacement preserves pending on a failed ACK save.
func (c *Codex) saveThreadInit(record codexThreadInitRecord) error {
	if c.threadInitRecords == nil {
		c.threadInitRecords = make(map[string]codexThreadInitRecord)
	}
	if c.dataDir == "" {
		c.threadInitRecords[record.SessionID] = record
		return nil
	}
	path := c.threadInitPath(record.SessionID)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("invalid initialization directory")
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".thread-start-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	// Keep the in-process guard even if directory durability cannot be confirmed.
	c.threadInitRecords[record.SessionID] = record
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (c *Codex) threadInitSourceCurrent(ctx context.Context, s *session.Session, snap session.Snapshot, epoch, role uint64) bool {
	now := s.Snapshot()
	if ctx.Err() != nil || now.State == session.Closed || now.State == session.Stopping || now.Hidden || now.ResumeID != snap.ResumeID || now.ConfigRevision != snap.ConfigRevision || now.SourceRevision != snap.SourceRevision || session.ConfigurationFrom(now) != session.ConfigurationFrom(snap) || c.rpc.transportEpoch.Load() != epoch {
		return false
	}
	if provider, ok := c.controlProvider.(backend.SessionRoleProvider); ok && provider.SessionRoleEpoch(s) != role {
		return false
	}
	return true
}

func (c *Codex) startThreadOnce(ctx context.Context, s *session.Session, snap session.Snapshot, requestID string, role uint64, params map[string]any) (json.RawMessage, error) {
	// Preserve the same maintenance/health gates as ordinary mutation RPCs.
	c.toolRPCGate.RLock()
	defer c.toolRPCGate.RUnlock()
	if c.appServerMode == "daemon" {
		if err := c.healthAdmission(); err != nil {
			return nil, err
		}
	}
	// Serialize independent executors sharing a data directory as well as the
	// per-session in-process ensureMu. A persisted pending intent survives unlock.
	if c.dataDir != "" {
		if err := os.MkdirAll(filepath.Dir(c.threadInitPath(s.ID)), 0700); err != nil {
			return nil, &codexThreadInitError{Code: "thread_initialization_unknown", Cause: err}
		}
		unlock, err := lockCodexThreadInit(c.threadInitPath(s.ID) + ".lock")
		if err != nil {
			return nil, &codexThreadInitError{Code: "thread_initialization_unknown", Cause: err}
		}
		defer unlock()
	}
	epoch := c.rpc.transportEpoch.Load()
	soft, hard := codexThreadInitSlowAfter, codexThreadInitTimeout
	if c.threadInitSlowAfter > 0 {
		soft = c.threadInitSlowAfter
	}
	if c.threadInitTimeout > 0 {
		hard = c.threadInitTimeout
	}
	var progressMu sync.Mutex
	waiting := true
	var timer *time.Timer
	var record codexThreadInitRecord
	raw, err := c.rpc.requestAtEpochObserved(ctx, "thread/start", params, hard, &epoch, nil, func(id int) error {
		if !c.threadInitSourceCurrent(ctx, s, snap, epoch, role) {
			return &codexThreadInitError{Code: "thread_initialization_failed"}
		}
		c.threadInitMu.Lock()
		defer c.threadInitMu.Unlock()
		previous, err := c.loadThreadInit(s.ID)
		if err != nil || !threadInitAllows(previous, snap) {
			return &codexThreadInitError{Code: "thread_initialization_unknown"}
		}
		record = codexThreadInitRecord{ContextGeneration: snap.ContextGeneration, Schema: 1, SessionID: s.ID, RequestID: requestID, RPCID: id, TransportEpoch: epoch, ConfigRevision: snap.ConfigRevision, SourceRevision: snap.SourceRevision, RoleEpoch: role, StartedAt: time.Now().UnixMilli(), Status: "pending"}
		if err = c.saveThreadInit(record); err != nil {
			return &codexThreadInitError{Code: "thread_initialization_unknown", Cause: err}
		}
		if !c.threadInitSourceCurrent(ctx, s, snap, epoch, role) {
			return &codexThreadInitError{Code: "thread_initialization_unknown"}
		}
		log.Printf("[codex] thread/start session=%s request=%s rpc=%d transport=%d status=pending", s.ID, requestID, id, epoch)
		timer = time.AfterFunc(soft, func() {
			progressMu.Lock()
			defer progressMu.Unlock()
			if waiting && c.threadInitSourceCurrent(ctx, s, snap, epoch, role) {
				c.sink.Emit(backend.NewTurnProgress(s.ID, requestID, "starting_thread", "對話初始化較久，正在等待同一筆建立請求；不會重送。"))
			}
		})
		return nil
	})
	if timer != nil {
		timer.Stop()
	}
	progressMu.Lock()
	waiting = false
	progressMu.Unlock()
	if err != nil {
		var timeout *rpcTimeoutError
		if errors.As(err, &timeout) {
			if c.appServerMode == "daemon" {
				c.scheduleHealthCheck("thread/start", params)
			}
			return nil, &codexThreadInitError{Code: "thread_initialization_timeout", Cause: fmt.Errorf("%w; server outcome unknown; request was not retried", err)}
		}
		var response *rpcResponseError
		if errors.As(err, &response) && record.RPCID > 0 && definiteThreadInitRejection(response.raw) {
			record.Status = "rejected"
			c.threadInitMu.Lock()
			saveErr := c.saveThreadInit(record)
			c.threadInitMu.Unlock()
			if saveErr != nil {
				return nil, &codexThreadInitError{Code: "thread_initialization_unknown", Cause: saveErr}
			}
			return nil, &codexThreadInitError{Code: "thread_initialization_failed", Cause: err}
		}
		if record.RPCID > 0 {
			return nil, &codexThreadInitError{Code: "thread_initialization_unknown", Cause: err}
		}
		return nil, err
	}
	// Only this original live waiter can bind an ACK. After final timeout or
	// cancellation the plumber retires its id; late responses never adopt/submit.
	var response struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Thread.ID == "" || !c.threadInitSourceCurrent(ctx, s, snap, epoch, role) {
		return nil, &codexThreadInitError{Code: "thread_initialization_unknown"}
	}
	record.Status, record.ThreadID = "acknowledged", response.Thread.ID
	c.threadInitMu.Lock()
	err = c.saveThreadInit(record)
	c.threadInitMu.Unlock()
	if err != nil {
		return nil, &codexThreadInitError{Code: "thread_initialization_unknown", Cause: err}
	}
	log.Printf("[codex] thread/start session=%s request=%s rpc=%d transport=%d status=acknowledged thread=%s", s.ID, requestID, record.RPCID, epoch, record.ThreadID)
	return raw, nil
}
