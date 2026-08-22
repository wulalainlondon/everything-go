package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

const handoffPollInterval = 50 * time.Millisecond

func (h *Hub) handleDesktopHandoff(c *Client, sessionID string) {
	if h.cfg.CodexRemote == "" {
		c.enqueueEvent(h.client.Error(sessionID, "shared_app_server_disabled", "Desktop handoff requires the shared Codex app-server daemon."))
		return
	}
	s, ok := h.registry.Get(sessionID)
	if !ok {
		c.enqueueEvent(h.client.Error(sessionID, "no_session", "unknown session"))
		return
	}
	snap := s.Snapshot()
	if snap.Backend != backend.Codex || snap.ResumeID == "" {
		c.enqueueEvent(h.client.Error(sessionID, "handoff_unavailable", "Desktop handoff requires a resumable Codex session."))
		return
	}
	entry, err := h.controls.BeginDesktopHandoff(sessionID, snap.ResumeID)
	if err != nil {
		c.enqueueEvent(controlEvent(entry, "", err.Error()))
		return
	}
	c.enqueueEvent(controlEvent(entry, "", "Waiting for the current turn and queued turns to finish."))

	go func() {
		// Once pending_handoff is durable, route rejects every new mobile write.
		// Drain turns that were already accepted before the lease transition.
		for s.IsStreaming() || s.QueueLen() > 0 {
			time.Sleep(handoffPollInterval)
		}
		// Require a second stable-idle observation so the worker cannot be in the
		// tiny gap between pulling an already queued function and BeginTurn.
		time.Sleep(handoffPollInterval)
		if s.IsStreaming() || s.QueueLen() > 0 {
			h.handleDesktopHandoffWait(s, sessionID)
			return
		}
		h.completeDesktopHandoff(sessionID, snap.ResumeID)
	}()
}

func (h *Hub) handleDesktopHandoffWait(s interface {
	IsStreaming() bool
	QueueLen() int
}, sessionID string) {
	for s.IsStreaming() || s.QueueLen() > 0 {
		time.Sleep(handoffPollInterval)
	}
	entry := h.controls.Get(sessionID)
	h.completeDesktopHandoff(sessionID, entry.ThreadID)
}

func (h *Hub) completeDesktopHandoff(sessionID, threadID string) {
	s, ok := h.registry.Get(sessionID)
	if !ok {
		h.controls.CancelPending(sessionID)
		return
	}
	if controller, ok := h.exec.(backend.SessionControlExecutor); ok {
		if err := controller.ReleaseSession(context.Background(), s); err != nil {
			entry := h.controls.CancelPending(sessionID)
			h.Emit(controlEvent(entry, "", "Desktop handoff failed while detaching the Bridge: "+err.Error()))
			return
		}
	}
	h.registry.Persist()
	entry := h.controls.CompleteDesktopHandoff(sessionID)
	command := fmt.Sprintf("codex resume %s --remote %s", threadID, h.cfg.CodexRemote)
	h.Emit(controlEvent(entry, command, "Desktop now owns new user turns. Mobile input is suspended until reclaim."))
}

func (h *Hub) handleDesktopReclaim(c *Client, sessionID string) {
	s, ok := h.registry.Get(sessionID)
	if !ok {
		c.enqueueEvent(h.client.Error(sessionID, "no_session", "unknown session"))
		return
	}
	if s.IsStreaming() {
		c.enqueueEvent(h.client.Error(sessionID, "reclaim_busy", "Cannot reclaim while a Codex turn is active."))
		return
	}
	go func() {
		if controller, ok := h.exec.(backend.SessionControlExecutor); ok {
			if err := controller.ClaimSession(context.Background(), s); err != nil {
				if errors.Is(err, backend.ErrThreadActiveWriter) {
					entry := h.controls.Get(sessionID)
					c.enqueueEvent(controlEvent(entry, "", "Desktop still owns this thread. Exit the desktop TUI before reclaiming mobile control."))
					return
				}
				c.enqueueEvent(h.client.Error(sessionID, "reclaim_failed", "Could not reclaim the Codex thread: "+err.Error()))
				return
			}
		}
		entry := h.controls.Reclaim(sessionID, s.ResumeID())
		h.Emit(controlEvent(entry, "", "Mobile control reclaimed."))
	}()
}

func (h *Hub) markDesktopWriter(s *session.Session) governance.SessionControl {
	entry, changed := h.controls.MarkDesktopOwner(s.ID, s.ResumeID())
	if changed {
		h.Emit(controlEvent(entry, "", "This thread is already open in a desktop terminal. Mobile remains read-only until the terminal exits and control is reclaimed."))
	}
	return entry
}

func controlEvent(entry governance.SessionControl, command, message string) protocol.SessionControlState {
	return protocol.NewSessionControlState(entry.SessionID, entry.ThreadID, entry.Owner, entry.State, command, message, entry.UpdatedAt)
}

func (h *Hub) rejectMobileWrite(c *Client, sessionID string) bool {
	entry := h.controls.Get(sessionID)
	if entry.Owner != governance.ControlDesktop && entry.State != governance.ControlPending {
		return false
	}
	message := "This session is being handed off to the desktop; mobile input is temporarily suspended."
	if entry.Owner == governance.ControlDesktop {
		message = "This session is currently controlled by the desktop. Reclaim mobile control before sending a new turn."
	}
	c.enqueueEvent(h.client.Error(sessionID, "session_controlled_by_desktop", message))
	return true
}
