package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/nativewatch"
)

func (h *Hub) observeNativeContext(a nativewatch.ContextActivity) {
	s, ok := h.registry.Get(a.Session.ID)
	if !ok {
		return
	}
	snap := s.Snapshot()
	if snap.Backend != "codex" || snap.ResumeID == "" || snap.ResumeID != a.Session.ResumeID {
		return
	}
	e := backend.ContextTelemetry{SessionID: s.ID, Backend: "codex", NativeGeneration: snap.ResumeID, SourceRevision: snap.SourceRevision, EventID: a.EventID, ReportedAtMS: a.ReportedAtMS, Origin: "codex_rollout_token_usage", MaxOrigin: "codex_rollout_token_usage", UsedTokens: a.Used, MaxTokens: a.Max}
	if a.Completed {
		e.CompletionState = "completed"
		if p, ok := h.exec.(backend.MaintenanceProvider); ok {
			for _, r := range p.MaintenanceRecords() {
				if r.SessionID == s.ID && r.ThreadID == snap.ResumeID && r.TurnID == a.TurnID && r.OperationID != "" && a.Offset >= r.LogOffset {
					raw, _ := json.Marshal([]string{"codex_maintenance", r.OperationID})
					sum := sha256.Sum256(raw)
					e.Aliases = []string{hex.EncodeToString(sum[:])}
					e.CompletionState = "observed"
				}
			}
		}
	}
	h.observeContextTelemetry(e)
}
