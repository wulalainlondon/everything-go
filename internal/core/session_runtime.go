package core

import (
	"context"
	"log"

	"everything-go/internal/protocol"
	"everything-go/internal/runtimejournal"
)

func runtimeEvent(view runtimejournal.View) protocol.SessionRuntime {
	return protocol.SessionRuntime{
		Type: "session_runtime", SessionID: view.SessionID, Revision: view.Revision,
		Phase: view.Phase, Stage: view.Stage, StageMessage: view.StageMessage,
		StageStartedAt: view.StageStartedAt, ActiveRequestID: view.ActiveRequestID, QueueLength: view.QueueLength,
		LastTerminalStatus: view.LastTerminal, LastError: view.LastError,
		UpdatedAt: view.UpdatedAt, CompletedAt: view.CompletedAt, Unread: view.Unread,
		DeliveryPending: view.DeliveryPending, HistoryReconcile: view.HistoryReconcile,
	}
}

func (h *Hub) updateRuntimeProgress(sessionID, requestID, stage, message string) {
	view, changed := h.runtimes.Progress(sessionID, requestID, stage, message)
	if changed {
		h.broadcastRuntime(view)
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
	h.broadcastRuntime(view)
	h.projectWorkRun(sessionID, requestID, phase, message)
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
		h.projectAutomationRun(requestID, status, reason)
		return
	}
	h.projectAutomationRun(requestID, status, reason)
	h.broadcastWorkRevision(update.Item.ActivityRevision)
	if update.Attention != "" && h.fcm != nil {
		go h.fcm.NotifyWorkAttention(h.cfg.InstanceID, update.Item.ID, update.Item.Title,
			update.Item.ActivityRevision, update.Attention)
	}
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
	case protocol.Done:
		h.updateRuntime(e.SessionID, "completed", e.RequestID, 0, "completed", "")
	case protocol.Stopped:
		h.updateRuntime(e.SessionID, "interrupted", e.RequestID, 0, "interrupted", "")
	case protocol.Error:
		if e.SessionID != "" {
			h.updateRuntime(e.SessionID, "failed", e.RequestID, 0, "failed", e.Message)
		}
	case protocol.UserInputRequestEvent:
		h.updateRuntime(e.SessionID, "waiting", e.RequestID, 0, "", "")
	case protocol.InteractionResolved:
		if e.SessionID != "" {
			h.updateRuntime(e.SessionID, "running", "", 0, "", "")
			h.updateRuntimeProgress(e.SessionID, "", "thinking", "")
		}
	}
}
