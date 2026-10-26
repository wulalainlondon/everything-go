package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/recovery"
	"everything-go/internal/session"
)

// Canary enablement is explicit and backend-only. A private kill-switch file
// disables NEW automatic dispatches without interrupting accepted native work.
func (h *Hub) networkRecoveryEnabled(sid string) bool {
	if os.Getenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE") != "true" {
		return false
	}
	if _, e := os.Lstat(filepath.Join(h.cfg.DataDir, "codex-network-autocontinue.disabled")); e == nil || !os.IsNotExist(e) {
		return false
	}
	if os.Getenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_ALL_ORDINARY") == "true" {
		return true
	}
	for _, v := range strings.Split(os.Getenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_SESSIONS"), ",") {
		if strings.TrimSpace(v) == sid && sid != "" {
			return true
		}
	}
	return false
}

type networkRecovery struct {
	hub           *Hub
	session       *session.Session
	source        recovery.AutomaticSource
	generation    uint64
	epoch         string
	owner         string
	configuration session.Configuration
	cwd           string
	payload       queuedPayload
	fileLeases    []recovery.FileLease
}

func (h *Hub) prepareNetworkRecovery(s *session.Session, e messagequeue.Entry, p queuedPayload) (recovery.AutomaticController, error) {
	if !h.networkRecoveryEnabled(s.ID) || s.Backend() != backend.Codex || h.messageQueue == nil {
		return nil, nil
	}
	if !strings.HasPrefix(e.RequestID, "r_") || strings.HasPrefix(e.RequestID, "ui_async_") || p.Origin != nil || p.WorkspaceTask != nil || p.VoiceContext || p.ExternalPeer != "" || len(p.ExternalEventIDs) > 0 || p.PartnerRole != nil || p.OwnerDevice == "" || p.CallbackBinding == "" || (p.MessagePurpose != "" && p.MessagePurpose != "instruction") || strings.HasPrefix(s.ID, "s_dg_") || strings.HasPrefix(s.ID, "pm_") {
		return nil, nil
	}
	if err := h.intakeWritePolicy(s); err != nil {
		return nil, nil
	}
	existing, found, err := h.messageQueue.Recovery(s.ID, e.RequestID)
	if err != nil {
		return nil, err
	}
	// Old queued/failed work is never opted in by a service upgrade.
	if !found && e.CreatedAt < h.networkRecoveryStarted {
		return nil, nil
	}
	cfg := session.ConfigurationFrom(s.Snapshot())
	b, _ := json.Marshal(struct {
		Configuration session.Configuration
		Cwd           string
	}{cfg, s.Cwd()})
	source := recovery.AutomaticSource{SourceRevision: s.Snapshot().SourceRevision, ControlRevision: h.controls.Get(s.ID).Revision, Authority: h.cfg.InstanceID, Session: s.ID, Request: e.RequestID, Thread: s.ResumeID(), Configuration: recovery.Hash(b), Principal: p.CallbackBinding, Scope: recovery.Hash([]byte(cfg.Backend + ":" + cfg.Model))}

	for _, binding := range h.pairing.DeviceBindings() {
		if binding.DeviceID == p.OwnerDevice && recovery.Hash([]byte(binding.Token)) == source.Principal {
			source.CredentialGeneration = binding.Generation
		}
	}
	n := &networkRecovery{hub: h, session: s, source: source, generation: 1, epoch: h.networkRecoveryEpoch, owner: p.OwnerDevice, configuration: cfg, cwd: s.Cwd(), payload: p}
	if found {
		source.Provider = existing.Source.Provider
		source.Scope = existing.Source.Scope
		n.source = source
		if existing.Source != source {
			return nil, recovery.ErrGuard
		}
		n.generation = existing.Generation
		if existing.State != "retry_wait" {
			return nil, recovery.ErrGuard
		}
		if err = h.messageQueue.AdoptRecovery(s.ID, e.RequestID, existing.Generation, n.epoch); err != nil {
			return nil, err
		}
	} else {
		for _, f := range p.Files {
			if f.RemotePath != "" {
				lease, e := recovery.CaptureFileLease(s.ID, f.AttachmentID, f.RemotePath, f.SizeBytes)
				if e != nil {
					return nil, e
				}
				n.fileLeases = append(n.fileLeases, lease)
			}
		}
		if _, err = h.messageQueue.BeginRecovery(source, n.epoch, n.now().UnixMilli(), n.fileLeases...); err != nil {
			if errors.Is(err, messagequeue.ErrRecoveryInputPressure) {
				return nil, nil
			}
			return nil, err
		}
	}
	if found {
		n.fileLeases, err = h.messageQueue.RecoveryFileLeases(s.ID, e.RequestID)
		if err != nil {
			return nil, err
		}
	}
	if err = n.Guard(); err != nil {
		return nil, err
	}
	return n, nil
}
func (n *networkRecovery) Resume() (recovery.Run, error) {
	r, ok, e := n.hub.messageQueue.Recovery(n.source.Session, n.source.Request)
	if e != nil {
		return r, e
	}
	if !ok {
		return r, recovery.ErrGuard
	}
	return r, nil
}
func (n *networkRecovery) Guard() error { return n.guardSources(n.source, n.source) }

func (n *networkRecovery) guardSources(current, persisted recovery.AutomaticSource) error {
	h := n.hub
	s, ok := h.registry.Get(current.Session)
	if !ok || s != n.session || !h.networkRecoveryEnabled(s.ID) || s.State() == session.Closed || s.State() == session.Stopping || s.ActiveQueuedID() != current.Request || s.ResumeID() != current.Thread || s.Cwd() != n.cwd || s.Snapshot().SourceRevision != current.SourceRevision || h.controls.Get(s.ID).Revision != current.ControlRevision || session.ConfigurationFrom(s.Snapshot()) != n.configuration || session.ConfigurationFrom(s.SettingsSnapshot()) != n.configuration || s.Snapshot().Hidden || !h.controls.MobileMayWrite(s.ID) {
		return recovery.ErrGuard
	}
	if e := h.intakeWritePolicy(s); e != nil {
		return recovery.ErrGuard
	}
	paired := false
	for _, b := range h.pairing.DeviceBindings() {
		if b.DeviceID == n.owner && recovery.Hash([]byte(b.Token)) == current.Principal && b.Generation == current.CredentialGeneration {
			paired = true
			break
		}
	}
	if !paired {
		return recovery.ErrGuard
	}
	if n.payload.OwnerCredentialGeneration != 0 && !h.jarvisCredentialCurrent(n.owner, n.payload.OwnerCredentialGeneration) {
		return recovery.ErrGuard
	}
	r, e := n.Resume()
	if e != nil {
		return e
	}
	if r.Generation != n.generation || r.OwnerEpoch != n.epoch || r.Source != persisted || r.State == "cancelled" || r.State == "disabled" || r.LeaseUntil <= n.now().UnixMilli() {
		return recovery.ErrGuard
	}
	if e = h.messageQueue.RecoveryNoNewInput(s.ID, current.Request); e != nil {
		return e
	}

	return nil
}
func (n *networkRecovery) BeforeDispatch(ctx context.Context, inputHash string, transport uint64) (recovery.AutomaticNext, error) {
	if ctx.Err() != nil {
		return recovery.AutomaticNext{}, ctx.Err()
	}
	h := n.hub
	h.messageQueueMu.Lock()
	defer h.messageQueueMu.Unlock()
	if e := n.Guard(); e != nil {
		return recovery.AutomaticNext{}, e
	}
	before, e := n.Resume()
	if e != nil {
		return recovery.AutomaticNext{}, e
	}
	_, a, e := h.messageQueue.ClaimRecovery(n.source.Session, n.source.Request, n.generation, n.epoch, transport, inputHash, n.now().UnixMilli())
	if errors.Is(e, recovery.ErrDispatchDeferred) || errors.Is(e, recovery.ErrDispatchPaused) {
		h.updateRuntime(n.source.Session, "queued", n.source.Request, n.session.QueueLen(), "", "")
		h.updateRuntimeProgress(n.source.Session, n.source.Request, "queued", "模型恢復正在等待共用容量；尚未重送。")
		h.publishMessageQueue(n.source.Session)
	}
	if e != nil {
		return recovery.AutomaticNext{}, e
	}
	return recovery.AutomaticNext{Attempt: a, Continuation: before.Kind == "continue", ParentTurn: before.ParentTurn}, nil
}
func (n *networkRecovery) Accepted(attempt, thread, turn string) error {
	return n.hub.messageQueue.RecoveryAccepted(n.source.Session, n.source.Request, attempt, thread, turn, n.generation, n.epoch, n.now().UnixMilli())
}
func (n *networkRecovery) Failed(o recovery.AutomaticOutcome) (bool, error) {
	if e := n.Guard(); e != nil {
		return false, e
	}
	r, retry, e := n.hub.messageQueue.ScheduleRecovery(n.source.Session, n.source.Request, n.generation, n.epoch, o, n.now().UnixMilli(), recoveryJitter())
	if e != nil {
		return false, e
	}
	message := recoveryUserMessage(recoveryPublic(r))
	if retry {
		message = fmt.Sprintf("模型連線／容量暫時失敗，%d 秒後自動續跑 · 第 %d/%d 次", max(0, (r.NextAt-n.now().UnixMilli()+999)/1000), r.Attempts+1, r.MaxAttempts)
	}
	n.hub.updateRuntime(n.source.Session, "queued", n.source.Request, n.session.QueueLen(), "", "")
	n.hub.Emit(backend.NewTurnProgress(n.source.Session, n.source.Request, "queued", message))
	n.hub.publishMessageQueue(n.source.Session)
	return retry, nil
}
func (n *networkRecovery) NativeFinished(status string) error {
	return n.hub.messageQueue.FinishRecovery(n.source.Session, n.source.Request, status)
}
func (n *networkRecovery) Wait(ctx context.Context) error {
	for {
		if e := n.Guard(); e != nil {
			reason := "source_changed"
			if !n.hub.networkRecoveryEnabled(n.source.Session) {
				reason = "disabled"
			}
			if er := n.hub.messageQueue.InvalidateRecovery(n.source.Session, n.source.Request, reason); er != nil {
				return er
			}
			return e
		}
		r, e := n.Resume()
		if e != nil {
			return e
		}
		if r.State != "retry_wait" {
			return recovery.ErrGuard
		}
		left := time.UnixMilli(r.NextAt).Sub(n.now())
		if left <= 0 {
			return nil
		}
		if left > time.Second {
			left = time.Second
		}
		timer := time.NewTimer(left)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Deterministic per-call bounded entropy, never a persistent clock adjustment.
func recoveryJitter() float64 { return float64(time.Now().UnixNano()%4001)/10000 - .2 }

var _ recovery.AutomaticController = (*networkRecovery)(nil)

// WriterGuard holds only the canonical admission boundary during the bounded
// transport write, never during a response wait/backoff/native execution.
func (n *networkRecovery) WriterGuard() (func(), error) {
	if e := n.Guard(); e != nil {
		return nil, e
	}
	for _, lease := range n.fileLeases {
		if e := recovery.VerifyFileMetadata(lease); e != nil {
			return nil, e
		}
	}
	h := n.hub
	// Transport wmu is already held. Never wait for a core operation which may
	// itself need the transport; this final check performs no native RPC.
	if !h.messageQueueMu.TryLock() {
		return nil, recovery.ErrGuard
	}
	if e := n.Guard(); e != nil {
		h.messageQueueMu.Unlock()
		return nil, e
	}
	return h.messageQueueMu.Unlock, nil
}

func (h *Hub) invalidateNetworkRecovery(sid, reason string) {
	if h.messageQueue == nil {
		return
	}
	h.messageQueueMu.Lock()
	_ = h.messageQueue.InvalidateRecovery(sid, "", reason)
	h.messageQueueMu.Unlock()
	if release, ok := h.networkRecoveryBarriers.LoadAndDelete(sid); ok {
		release.(func())()
	}
}
func (h *Hub) holdNetworkRecoveryBarrier(sid, rid string) {
	if h.messageQueue == nil {
		return
	}
	r, found, e := h.messageQueue.Recovery(sid, rid)
	if e != nil || !found {
		return
	}
	if r.State != "paused_uncertain" && r.State != "paused_budget" && r.State != "waiting_user" && r.State != "reconciling" {
		return
	}
	if s, ok := h.registry.Get(sid); ok {
		release := s.HoldQueue()
		if _, loaded := h.networkRecoveryBarriers.LoadOrStore(sid, release); loaded {
			release()
		}
	}
}
func (h *Hub) cancelNetworkRecovery(c *Client, cmd clientproto.Command, e messagequeue.Entry) bool {
	r, found, err := h.messageQueue.Recovery(e.SessionID, e.RequestID)
	if err != nil || !found {
		return false
	}
	if r.State != "retry_wait" && r.State != "paused_budget" && r.State != "paused_uncertain" && r.State != "waiting_user" && r.State != "reconciling" {
		return false
	}
	raw, err := h.messageQueue.RecoveryInput(e.SessionID, e.RequestID)
	var p queuedPayload
	if err != nil || json.Unmarshal(raw, &p) != nil || h.pairedTaskDevice(c) == "" || p.OwnerDevice != h.pairedTaskDevice(c) || p.CallbackBinding != callbackBinding(c) || !h.controls.MobileMayWrite(e.SessionID) {
		h.queueResult(c, cmd, "cancel", "rejected", "Original recovery owner changed", e)
		return true
	}
	if safe, er := h.messageQueue.RecoveryWaitingCancellable(e.SessionID, e.RequestID); er != nil || !safe {
		h.queueResult(c, cmd, "cancel", "rejected", "原生接受狀態尚待核對；未假稱已停止。", e)
		return true
	}
	if err = h.messageQueue.InvalidateRecovery(e.SessionID, e.RequestID, "cancelled"); err != nil {
		h.queueResult(c, cmd, "cancel", "rejected", err.Error(), e)
		return true
	}
	if release, ok := h.networkRecoveryBarriers.LoadAndDelete(e.SessionID); ok {
		release.(func())()
	}
	if s, ok := h.registry.Get(e.SessionID); ok && s.ActiveQueuedID() == e.RequestID {
		s.MarkStopping()
		go func() { _ = h.exec.Stop(context.Background(), s) }()
	}
	h.queueResult(c, cmd, "cancel", "accepted", "Automatic recovery cancelled; no active native turn replayed", e)
	h.publishMessageQueue(e.SessionID)
	return true
}
func recoveryPublic(r recovery.Run) protocol.AutomaticRecoverySnapshot {
	return protocol.AutomaticRecoverySnapshot{RequestID: r.Source.Request, Mode: recovery.AutomaticMode, Revision: r.Revision, Generation: r.Generation, State: r.State, Reason: r.Reason, Attempts: r.Attempts, MaxAttempts: r.MaxAttempts, NextAt: r.NextAt, Deadline: r.Deadline, NativeStatus: r.NativeStatus, ObjectiveVerified: r.ObjectiveVerified}
}

func (n *networkRecovery) BindThread(thread string) error {
	if thread == "" {
		return recovery.ErrGuard
	}
	if n.source.Thread != "" {
		if n.source.Thread != thread {
			return recovery.ErrGuard
		}
		return n.Guard()
	}
	// Only the one admitted empty->owned thread transition may advance source.
	// Any config/hidden/thread ABA creates additional revisions and stays retired.
	candidate := n.source
	candidate.Thread = thread
	candidate.SourceRevision++
	if candidate.SourceRevision == 0 {
		return recovery.ErrGuard
	}
	if e := n.guardSources(candidate, n.source); e != nil {
		return e
	}
	if e := n.hub.messageQueue.BindRecoveryThread(n.source.Session, n.source.Request, thread, n.generation, n.epoch, candidate.SourceRevision); e != nil {
		return e
	}
	n.source = candidate
	return n.Guard()
}

func (n *networkRecovery) now() time.Time {
	if n.hub.networkRecoveryNow != nil {
		return n.hub.networkRecoveryNow()
	}
	return time.Now()
}

func (n *networkRecovery) BindProvider(provider string) error {
	if n.source.Provider == provider {
		return n.Guard()
	}
	if e := n.hub.messageQueue.BindRecoveryProvider(n.source.Session, n.source.Request, provider, n.generation, n.epoch); e != nil {
		return e
	}
	r, e := n.Resume()
	if e != nil {
		return e
	}
	n.source = r.Source
	return n.Guard()
}

func recoveryUserMessage(r protocol.AutomaticRecoverySnapshot) string {
	switch r.State {
	case "retry_wait":
		return fmt.Sprintf("模型服務暫時忙碌，等待後端自動恢復 · 第 %d/%d 次", r.Attempts+1, r.MaxAttempts)
	case "reconciling":
		return "連線中斷，正在核對原任務；未重送。"
	case "paused_budget":
		return "自動恢復已暫停：已達次數、時間或無進展上限。"
	case "waiting_user":
		return "自動恢復已暫停：需要你確認原工作。"
	default:
		return "自動恢復已暫停：原結果或執行條件需要核對。"
	}
}
func (h *Hub) networkRecoveryReadiness() map[string]any {
	mode := "disabled"
	enabled := os.Getenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE") == "true"
	all := os.Getenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_ALL_ORDINARY") == "true"
	count := 0
	for _, s := range strings.Split(os.Getenv("EVERYTHING_GO_CODEX_NETWORK_AUTOCONTINUE_SESSIONS"), ",") {
		if strings.TrimSpace(s) != "" {
			count++
		}
	}
	_, e := os.Lstat(filepath.Join(h.cfg.DataDir, "codex-network-autocontinue.disabled"))
	disabled := e == nil || !os.IsNotExist(e)
	if enabled && all {
		mode = "all_new_ordinary"
	} else if enabled && count > 0 {
		mode = "exact_canary_allowlist"
	}
	ready := h.messageQueue != nil && h.messageQueue.RecoveryStorageReady() == nil
	return map[string]any{"schema_version": 1, "enabled": enabled && !disabled && ready && (all || count > 0), "configured_enabled": enabled, "all_ordinary": all, "allowlist_count": count, "mode": mode, "disabled": disabled, "durable_store_ready": ready, "max_attempts": 5, "deadline_minutes": 30, "old_failed_replay": false, "objective_verification": "not_inferred_from_native_completed", "accepted_tool_coverage": "opaque_or_unverifiable_tools_pause"}
}

func (n *networkRecovery) ObserveFailure(o recovery.AutomaticOutcome) error {
	return n.hub.messageQueue.ObserveRecoveryFailure(n.source.Session, n.source.Request, o, n.now().UnixMilli())
}

func (n *networkRecovery) VerifyInputs() error {
	if e := n.Guard(); e != nil {
		return e
	}
	for _, lease := range n.fileLeases {
		if e := recovery.VerifyFileLease(lease); e != nil {
			return e
		}
	}
	return n.Guard()
}

// A committed newer ordinary input invalidates the old generation in Enqueue's
// transaction. Release only its in-memory pause; unknown native ownership is
// still checked by the original executor admission before any new writer.
func (h *Hub) releaseNetworkRecoveryBarrier(sid string) {
	if release, ok := h.networkRecoveryBarriers.LoadAndDelete(sid); ok {
		release.(func())()
	}
}
