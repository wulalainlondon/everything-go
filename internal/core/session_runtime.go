package core

import (
	"context"
	"log"
	"sort"
	"time"

	"everything-go/internal/fcm"
	"everything-go/internal/protocol"
	"everything-go/internal/runtimejournal"
	"everything-go/internal/workitems"
)

const (
	messageReceiptTTL = 24 * time.Hour
	messageReceiptMax = 4096
)

func messageReceiptKey(sessionID, requestID string) string { return sessionID + "\x00" + requestID }

func (h *Hub) reserveMessageRequest(sessionID, requestID string) bool {
	if sessionID == "" || requestID == "" {
		return true
	}
	now := time.Now()
	cutoff := now.Add(-messageReceiptTTL).UnixMilli()
	key := messageReceiptKey(sessionID, requestID)
	h.messageMu.Lock()
	defer h.messageMu.Unlock()
	if _, exists := h.messageReceipts[key]; exists {
		return false
	}
	h.messageReceipts[key] = now.UnixMilli()
	if len(h.messageReceipts) <= messageReceiptMax {
		return true
	}
	type receipt struct {
		key string
		at  int64
	}
	all := make([]receipt, 0, len(h.messageReceipts))
	for candidate, at := range h.messageReceipts {
		if at < cutoff {
			delete(h.messageReceipts, candidate)
			continue
		}
		all = append(all, receipt{candidate, at})
	}
	if len(h.messageReceipts) > messageReceiptMax {
		sort.Slice(all, func(i, j int) bool { return all[i].at < all[j].at })
		for _, candidate := range all[:len(h.messageReceipts)-messageReceiptMax] {
			delete(h.messageReceipts, candidate.key)
		}
	}
	return true
}

func (h *Hub) releaseMessageRequest(sessionID, requestID string) {
	if sessionID == "" || requestID == "" {
		return
	}
	h.messageMu.Lock()
	delete(h.messageReceipts, messageReceiptKey(sessionID, requestID))
	h.messageMu.Unlock()
}

func runtimeEvent(view runtimejournal.View) protocol.SessionRuntime {
	return protocol.SessionRuntime{
		Type: "session_runtime", SessionID: view.SessionID, Revision: view.Revision,
		Phase: view.Phase, Stage: view.Stage, StageMessage: view.StageMessage,
		StageStartedAt: view.StageStartedAt, ActiveStartedAt: view.ActiveStartedAt,
		ActiveRequestID: view.ActiveRequestID, QueueLength: view.QueueLength,
		LastTerminalStatus: view.LastTerminal, LastError: view.LastError,
		UpdatedAt: view.UpdatedAt, CompletedAt: view.CompletedAt, Unread: view.Unread,
		DeliveryPending: view.DeliveryPending, HistoryReconcile: view.HistoryReconcile,
	}
}

func (h *Hub) updateRuntimeProgress(sessionID, requestID, stage, message string) {
	view, changed := h.runtimes.Progress(sessionID, requestID, stage, message)
	if changed {
		h.broadcastRuntime(view)
		h.notifyRuntimeStatus(view)
	}
}

func (h *Hub) runtimeSnapshot(deviceID string) protocol.SessionRuntimeSnapshot {
	sessions := h.registry.List()
	ids := make([]string, 0, len(sessions))
	for _, s := range sessions {
		snap := s.Snapshot()
		ids = append(ids, snap.ID)
		phase := snap.State.String()
		if phase == "streaming" {
			phase = "running"
		}
		h.runtimes.Ensure(snap.ID, phase, s.QueueLen())
	}
	views := h.runtimes.Snapshot(deviceID, ids)
	items := make([]protocol.SessionRuntime, 0, len(views))
	for _, view := range views {
		items = append(items, runtimeEvent(view))
	}
	return protocol.SessionRuntimeSnapshot{Type: "session_runtime_snapshot", Items: items}
}

func (h *Hub) updateRuntime(sessionID, phase, requestID string, queueLength int, terminal, message string) {
	view, changed := h.runtimes.Update(sessionID, phase, requestID, queueLength, terminal, message)
	if !changed {
		return
	}
	h.publishRuntime(view)
}

func (h *Hub) publishRuntime(view runtimejournal.View) {
	h.broadcastRuntime(view)
	h.notifyRuntimeStatus(view)
	h.projectWorkRun(view.SessionID, view.ActiveRequestID, view.Phase, view.LastError)
}

func (h *Hub) updateRuntimeQueueLength(sessionID string, queueLength int) {
	view, changed := h.runtimes.UpdateQueueLength(sessionID, queueLength)
	if !changed {
		return
	}
	h.broadcastRuntime(view)
	h.notifyRuntimeStatus(view)
}

func (h *Hub) sessionQueueLength(sessionID string) int {
	if current, ok := h.registry.Get(sessionID); ok {
		return current.QueueLen()
	}
	return 0
}

func (h *Hub) recordTerminalRuntime(event any) (runtimejournal.View, bool, bool) {
	var sessionID, requestID, phase, terminal, message string
	switch e := event.(type) {
	case protocol.Done:
		sessionID, requestID, phase, terminal = e.SessionID, e.RequestID, "completed", "completed"
	case protocol.Stopped:
		sessionID, requestID, phase, terminal = e.SessionID, e.RequestID, "interrupted", "interrupted"
	case protocol.Error:
		if e.SessionID == "" {
			return runtimejournal.View{}, false, false
		}
		sessionID, requestID, phase, terminal, message = e.SessionID, e.RequestID, "failed", "failed", e.Message
	default:
		return runtimejournal.View{}, false, false
	}
	view, changed := h.runtimes.Update(sessionID, phase, requestID, h.sessionQueueLength(sessionID), terminal, message)
	return view, changed, true
}

func (h *Hub) notifyRuntimeStatus(view runtimejournal.View) {
	if h.runtimeStatusPush == nil || view.SessionID == "" || view.Revision == 0 {
		return
	}
	name := view.SessionID
	if s, ok := h.registry.Get(view.SessionID); ok {
		if candidate := s.Name(); candidate != "" {
			name = candidate
		}
	}
	replyURL, fallbackURL, capability, expiresAt := h.notificationReplyAction(view.SessionID)
	go h.runtimeStatusPush(h.cfg.InstanceID, view.SessionID, name, view.Phase, view.Stage,
		view.StageMessage, view.Revision, view.UpdatedAt, view.ActiveStartedAt, view.ActiveRequestID, view.QueueLength,
		fcm.ReplyAction{URL: replyURL, FallbackURL: fallbackURL, Capability: capability, ExpiresAt: expiresAt})
}

func (h *Hub) projectWorkRun(sessionID, requestID, phase, reason string) {
	if h.work == nil {
		return
	}
	status := phase
	switch phase {
	case "completed":
		status = "succeeded"
	case "waiting":
		status = "waiting_user"
	case "running", "failed", "interrupted":
	default:
		return
	}
	update, err := h.work.AdvanceRun(context.Background(), sessionID, requestID, status, reason)
	if err != nil {
		log.Printf("[work] project run session=%s request=%s: %v", sessionID, requestID, err)
		return
	}
	if !update.Changed {
		if status != "succeeded" {
			h.completeRelayRun(requestID, status, reason, "")
		}
		h.projectAutomationRun(requestID, status, reason)
		return
	}
	if status != "succeeded" {
		h.completeRelayRun(requestID, status, reason, "")
	}
	h.projectAutomationRun(requestID, status, reason)
	h.broadcastWorkRevision(update.Item.ActivityRevision)
	if update.Attention != "" {
		h.notifyWorkAttention(update.Item, update.Attention)
	}
}

func (h *Hub) notifyWorkAttention(item workitems.WorkItem, kind string) {
	if h.workAttentionPush == nil || item.ID == "" || kind == "" {
		return
	}
	go h.workAttentionPush(h.cfg.InstanceID, item.ID, item.Title, item.ActivityRevision, kind)
}

func (h *Hub) broadcastRuntime(view runtimejournal.View) {
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
		deviceView := h.runtimes.Snapshot(c.deviceID, []string{view.SessionID})
		if len(deviceView) == 1 {
			c.enqueueEvent(runtimeEvent(deviceView[0]))
		}
	}
}

func (h *Hub) driveRuntimeState(event any) {
	switch e := event.(type) {
	case protocol.TurnProgress:
		h.updateRuntimeProgress(e.SessionID, e.RequestID, e.Stage, e.Message)
	case protocol.ThinkingChunk:
		h.updateRuntimeProgress(e.SessionID, e.RequestID, "thinking", "")
	case protocol.TextChunk:
		h.updateRuntimeProgress(e.SessionID, e.RequestID, "composing", "")
	case protocol.ToolStart:
		h.updateRuntimeProgress(e.SessionID, e.RequestID, "running_tool", e.Name)
	case protocol.ToolEnd:
		h.updateRuntimeProgress(e.SessionID, e.RequestID, "thinking", "")
	case protocol.TodoUpdate:
		h.updateRuntimeProgress(e.SessionID, e.RequestID, "running_tool", "Updating task plan")
	case protocol.Done, protocol.Stopped, protocol.Error:
		// Terminal events are recorded and published by Hub.Emit's two-phase
		// ordering boundary before the next queued turn is released.
		return
	case protocol.UserInputRequestEvent:
		h.updateRuntime(e.SessionID, "waiting", e.RequestID, 0, "", "")
	case protocol.InteractionResolved:
		if e.SessionID != "" {
			h.updateRuntime(e.SessionID, "running", "", 0, "", "")
			h.updateRuntimeProgress(e.SessionID, "", "thinking", "")
		}
	}
}
