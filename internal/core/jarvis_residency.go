package core

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/jarvisrole"
	"time"
)

type jarvisResidentStatus struct {
	Desired    bool   `json:"desired"`
	Actual     string `json:"actual"`
	Epoch      uint64 `json:"role_epoch"`
	Thread     string `json:"thread_id"`
	CheckedAt  int64  `json:"checked_at"`
	FreshUntil int64  `json:"fresh_until"`
	Reason     string `json:"reason"`
	Provider   string `json:"provider"`
	Recoveries int    `json:"recoveries"`
	Generating bool   `json:"model_generating"`
	ToolState  string `json:"tool_state"`
}

func (h *Hub) jarvisResidentSnapshot(a *jarvisrole.Assignment) jarvisResidentStatus {
	h.jarvisResidentMu.Lock()
	v := h.jarvisResident
	h.jarvisResidentMu.Unlock()
	if a == nil {
		return jarvisResidentStatus{Actual: "inactive", Reason: "no_assignment", ToolState: "unavailable"}
	}
	if v.Epoch != a.Epoch {
		v = jarvisResidentStatus{Desired: true, Actual: "pending", Epoch: a.Epoch, Thread: a.Target.Thread, Reason: "awaiting_provider_observation", ToolState: "unknown"}
	}
	if h.currentJarvisRole() == nil {
		v.Actual = "suspended"
		v.Reason = "assignment_source_or_pairing_changed"
		v.FreshUntil = 0
		v.ToolState = "unavailable"
	}
	if v.FreshUntil > 0 && time.Now().UnixMilli() > v.FreshUntil {
		v.Actual = "retry_wait"
		v.Reason = "provider_observation_stale"
		v.ToolState = "unknown"
	}
	return v
}
func (h *Hub) reconcileJarvisResidency(ctx context.Context) {
	a := h.currentJarvisRole()
	if a == nil {
		return
	}
	now := time.Now().UnixMilli()
	h.jarvisResidentMu.Lock()
	if h.jarvisResidentRunning || now < h.jarvisResidentNext {
		h.jarvisResidentMu.Unlock()
		return
	}
	h.jarvisResidentRunning = true
	h.jarvisResidentNext = now + 30000
	h.jarvisResidentMu.Unlock()
	go func() {
		defer func() { h.jarvisResidentMu.Lock(); h.jarvisResidentRunning = false; h.jarvisResidentMu.Unlock() }()
		v := jarvisResidentStatus{Desired: true, Epoch: a.Epoch, Thread: a.Target.Thread, Actual: "limited", Reason: "provider_residency_unsupported", ToolState: "unavailable"}
		s, ok := h.registry.Get(a.Target.Session)
		if !ok {
			return
		}
		v.Provider = s.Backend()
		current := func() bool {
			b := h.currentJarvisRole()
			return ctx.Err() == nil && b != nil && b.Epoch == a.Epoch && b.Target == a.Target
		}
		if !current() {
			return
		}
		if a.Target.Thread == "" {
			v.Actual = "pending"
			v.Reason = "awaiting_first_native_thread"
			v.ToolState = "unknown"
		} else if p, ok := h.exec.(backend.ResidentExecutor); ok {
			call, cancel := context.WithTimeout(ctx, 25*time.Second)
			o, e := p.MaintainResident(call, s, a.Target.Thread, a.Epoch, current)
			cancel()
			if !current() {
				return
			}
			v.Actual = o.State
			v.CheckedAt = o.CheckedAt
			v.FreshUntil = o.FreshUntil
			v.Reason = o.Reason
			v.Generating = o.Generating
			v.ToolState = o.ToolState
			if o.Recovered {
				v.Recoveries = 1
			}
			if e != nil {
				v.Actual = "retry_wait"
				v.Reason = "provider_observation_or_recovery_failed"
				v.ToolState = "unknown"
				v.FreshUntil = 0
			}
		}
		if !current() {
			return
		}
		h.jarvisResidentMu.Lock()
		if h.jarvisResident.Epoch == a.Epoch {
			v.Recoveries += h.jarvisResident.Recoveries
		}
		h.jarvisResident = v
		h.jarvisResidentMu.Unlock()
	}()
}
