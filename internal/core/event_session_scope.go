package core

import (
	"everything-go/internal/session"
	"strings"
)

func (h *Hub) eventSessionAllowed(s *session.Session) bool {
	if s == nil || !h.controllerInScope(s) || s.Snapshot().Hidden || s.State() == session.Closed || !h.controls.MobileMayWrite(s.ID) || strings.HasPrefix(s.ID, "s_dg_") || strings.HasPrefix(s.ID, "pm_") {
		return false
	}
	policy, err := h.PMConfiguration(s.ID)
	return err == nil && policy == nil
}
func (h *Hub) eventAssignmentSendAllowed(s *session.Session, payload queuedPayload) bool {
	if payload.OwnerCredentialGeneration != 0 && !h.jarvisCredentialCurrent(payload.OwnerDevice, payload.OwnerCredentialGeneration) {
		return false
	}
	return len(payload.ExternalEventIDs) == 0 || h.eventSessionAllowed(s)
}
