package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"time"
)

// MaintainResident uses metadata plus exact same-thread subscription. It never
// sends input, compacts, resets, starts a new thread, forks, or claims desktop.
func (c *Codex) MaintainResident(ctx context.Context, s *session.Session, thread string, roleEpoch uint64, current func() bool) (backend.ResidentObservation, error) {
	out := backend.ResidentObservation{Thread: thread, State: "limited", Reason: "shared_daemon_required", ToolState: "unavailable"}
	if c.appServerMode != "daemon" {
		return out, nil
	}
	if thread == "" || s.ResumeID() != thread || !current() {
		return out, errors.New("resident_source_changed")
	}
	if e := ctx.Err(); e != nil {
		return out, e
	}
	if e := c.ensureServer(); e != nil {
		return out, e
	}
	c.mu.Lock()
	other := c.activeThreadOwner[thread]
	route := c.threadToSession[thread]
	c.mu.Unlock()
	if (other != nil && other != s) || (route != nil && route != s) {
		return out, errors.New("resident_original_thread_route_owned_elsewhere")
	}
	st := c.state(s.ID)
	st.ensureMu.Lock()
	defer st.ensureMu.Unlock()
	epoch := c.rpc.transportEpoch.Load()
	read := func() (string, error) {
		if !current() || s.ResumeID() != thread || ctx.Err() != nil {
			return "", errors.New("resident_source_changed")
		}
		raw, e := c.rpc.requestAtEpoch("thread/read", map[string]any{"threadId": thread, "includeTurns": false}, 5*time.Second, &epoch)
		if e != nil {
			if isQueuedThreadUnloaded(e, thread) {
				return "notLoaded", nil
			}
			return "", e
		}
		var response struct {
			Thread struct {
				ID     string `json:"id"`
				Status struct {
					Type string `json:"type"`
				} `json:"status"`
			} `json:"thread"`
		}
		if len(raw) > 64<<10 || json.Unmarshal(raw, &response) != nil || response.Thread.ID != thread || c.rpc.transportEpoch.Load() != epoch {
			return "", errors.New("resident_observation_invalid")
		}
		return response.Thread.Status.Type, nil
	}
	status, e := read()
	if e != nil {
		return out, e
	}
	st.mu.Lock()
	busy := st.turnActive || st.compactActive
	subscribed := st.threadID == thread
	toolsEpoch := st.residentRoleEpoch
	nativeToolsObserved := st.residentToolsEpoch == roleEpoch && st.residentToolsTransport == epoch
	st.mu.Unlock()
	if status == "active" || busy {
		out.State = "active"
		out.Reason = "original_native_turn_active"
		out.Generating = status == "active"
		out.ToolState = "refresh_pending"
		if subscribed && toolsEpoch == roleEpoch && nativeToolsObserved {
			out.ToolState = "loaded"
		}
	} else {
		if status != "idle" && status != "notLoaded" && status != "systemError" {
			return out, errors.New("resident_provider_state_unverified")
		}
		if !subscribed || status == "notLoaded" || toolsEpoch != roleEpoch {
			if !current() || ctx.Err() != nil || s.ResumeID() != thread {
				return out, errors.New("resident_source_changed")
			}
			params := map[string]any{"threadId": thread, "excludeTurns": true}
			c.applySessionControlThreadTools(s, params)
			if e := c.applyTaskAPIThreadTools(s, params); e != nil {
				return out, e
			}
			raw, e := c.rpc.requestAtEpoch("thread/resume", params, 10*time.Second, &epoch)
			if e != nil {
				return out, e
			}
			var resumed struct {
				Thread struct {
					ID string `json:"id"`
				} `json:"thread"`
			}
			if len(raw) > 64<<10 || json.Unmarshal(raw, &resumed) != nil || resumed.Thread.ID != thread {
				return out, errors.New("resident_original_thread_mismatch")
			}
			// No fallback to ensureThread: its legacy recovery can create a successor.
			if !current() || ctx.Err() != nil || s.ResumeID() != thread || c.rpc.transportEpoch.Load() != epoch {
				return out, errors.New("resident_source_changed")
			}
			st.mu.Lock()
			st.threadID = thread
			st.residentRoleEpoch = roleEpoch
			st.mu.Unlock()
			c.mu.Lock()
			c.threadToSession[thread] = s
			c.mu.Unlock()
			status, e = read()
			if e != nil {
				return out, e
			}
			out.Recovered = true
		}
		if status != "idle" && status != "active" && status != "systemError" {
			return out, errors.New("resident_provider_state_unverified")
		}
		out.State = "ready"
		if status == "systemError" {
			out.State = "limited"
			out.Reason = "provider_system_error"
		}
		if status != "systemError" {
			out.Reason = "exact_thread_subscription_and_metadata_observed"
		}
		out.ToolState = "refresh_pending"
		if nativeToolsObserved && toolsEpoch == roleEpoch {
			out.ToolState = "loaded"
		}
		if status == "active" {
			out.State = "active"
			out.Generating = true
		}
	}
	if !current() || ctx.Err() != nil || c.rpc.transportEpoch.Load() != epoch {
		return out, errors.New("resident_source_changed")
	}
	out.CheckedAt = time.Now().UnixMilli()
	out.FreshUntil = out.CheckedAt + 45000
	return out, nil
}
