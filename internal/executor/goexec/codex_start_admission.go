package goexec

import (
	"encoding/json"
	"errors"
	"everything-go/internal/recovery"
	"everything-go/internal/session"
	"time"
)

// Fixed, non-secret correlation reasons. They are observations, never permission
// to replay or proof that a provider capacity hint caused the original failure.
type codexAcceptanceError struct {
	Reason  string
	Failure recovery.Failure
}

func (e *codexAcceptanceError) Error() string { return e.Reason }
func codexCorrelationReason(err error) string {
	var e *codexAcceptanceError
	if errors.As(err, &e) {
		return e.Reason
	}
	return "native_acceptance_unknown"
}

type codexStartAdmission struct {
	ThreadID, RequestID             string
	Epoch, Boundary, ConfigRevision uint64
	Source                          *session.Session
}

// Use the existing public 0.160 thread/read metadata (no history). Restart
// quarantine or a local idle flag cannot certify an idle provider thread.
func (c *Codex) readStartAdmission(s *session.Session, st *codexState, requestID string) (*codexStartAdmission, error) {
	st.mu.Lock()
	threadID, active, boundary := st.threadID, (st.turnActive && !(st.turnStartPending && st.reqID == requestID && st.currentTurnID == "")) || st.compactActive, st.nativeBoundary
	st.mu.Unlock()
	if active {
		return nil, &codexAcceptanceError{Reason: "native_submission_busy"}
	}
	if threadID == "" {
		return nil, &codexAcceptanceError{Reason: "native_acceptance_unknown"}
	}
	epoch := c.rpc.transportEpoch.Load()
	revision := s.Snapshot().ConfigRevision
	raw, err := c.rpc.requestAtEpoch("thread/read", map[string]any{"threadId": threadID, "includeTurns": false}, 15*time.Second, &epoch)
	if err != nil || len(raw) > 64<<10 {
		return nil, &codexAcceptanceError{Reason: "native_idle_unverified"}
	}
	var response struct {
		Thread struct {
			ID     string `json:"id"`
			Status struct {
				Type string `json:"type"`
			} `json:"status"`
		} `json:"thread"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Thread.ID != threadID {
		return nil, &codexAcceptanceError{Reason: "native_idle_unverified"}
	}
	if response.Thread.Status.Type == "active" {
		return nil, &codexAcceptanceError{Reason: "native_submission_busy"}
	}
	if response.Thread.Status.Type != "idle" {
		return nil, &codexAcceptanceError{Reason: "native_idle_unverified"}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.threadID != threadID || (st.turnActive && !(st.turnStartPending && st.reqID == requestID && st.currentTurnID == "")) || st.compactActive || st.nativeBoundary != boundary || c.rpc.transportEpoch.Load() != epoch || s.ResumeID() != threadID || s.Snapshot().ConfigRevision != revision {
		return nil, &codexAcceptanceError{Reason: "native_source_changed"}
	}
	return &codexStartAdmission{ThreadID: threadID, RequestID: requestID, Epoch: epoch, Boundary: boundary, ConfigRevision: revision, Source: s}, nil
}

func (a *codexStartAdmission) sourceKnown() bool {
	if a == nil {
		return true
	}
	if a.Source == nil {
		return false
	}
	s := a.Source.Snapshot()
	return s.ResumeID == a.ThreadID && s.ConfigRevision == a.ConfigRevision && s.State != session.Closed && !s.Hidden
}
