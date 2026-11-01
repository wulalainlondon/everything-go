package core

import (
	"context"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/jarvisrole"
	"everything-go/internal/linebridge"
	"time"
)

// Role does not invent external accounts/devices. These observations come from
// actual installed adapters and existing subject scopes; Status never starts a
// LINE runtime, logs in, or performs a connectivity probe.
func (h *Hub) jarvisServiceReadiness(a *jarvisrole.Assignment) []map[string]any {
	now := time.Now().UnixMilli()
	line := map[string]any{"id": "line", "state": "unavailable", "reason": "line_service_not_installed", "checked_at": now, "authority_source": "original_operator_service_scope", "resources": []any{}}
	browser := map[string]any{"id": "browser", "state": "unavailable", "reason": "browser_control_not_installed", "checked_at": now, "authority_source": "original_native_caller_and_selected_frontend"}
	media := map[string]any{"id": "voice_media", "state": "unavailable", "reason": "media_entry_not_installed", "checked_at": now, "authority_source": "original_active_voice_and_device_capability"}
	if h.lineService != nil {
		line["state"] = "waiting_source"
		line["reason"] = "jarvis_source_required"
		if a != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			c := linebridge.Caller{Subject: "session:" + a.Target.Session}
			raw, e := h.lineService.Execute(ctx, c, linebridge.Request{Method: "accounts"})
			if e != nil {
				line["state"] = "unknown"
				line["reason"] = "scoped_account_metadata_unavailable"
			} else if accounts, ok := raw.([]linebridge.Account); ok {
				resources := []any{}
				line["state"] = "unavailable"
				line["reason"] = "no_operator_authorized_accounts"
				for i, account := range accounts {
					if i >= 16 {
						break
					}
					access, ae := h.lineService.Access(ctx, c, account.ID)
					status, se := h.lineService.Status(ctx, c, account.ID)
					resource := map[string]any{"account_ref": account.ID, "binding_revision": account.Revision, "enabled": account.Enabled, "health_state": "unknown", "reason": "live_health_not_observed", "scopes": []string{}}
					if ae == nil {
						resource["scopes"] = access.Scopes
					} else {
						resource["reason"] = "subject_scope_unavailable"
					}
					if se == nil {
						resource["runtime_state"] = status.RuntimeState
						resource["last_health"] = status.LastHealth
						if status.LastHealth != nil {
							resource["health_state"] = status.LastHealth.State
							resource["health_checked_at"] = status.LastHealth.CheckedAt
							if now-status.LastHealth.CheckedAt > 60000 {
								resource["health_state"] = "stale"
								resource["reason"] = "last_health_observation_stale"
							} else {
								resource["reason"] = "original_binding_health_observation"
							}
						}
					}
					resources = append(resources, resource)
					if account.Enabled && ae == nil {
						line["state"] = "configured"
						line["reason"] = "original_scopes_available_live_connectivity_separate"
					}
				}
				line["resources"] = resources
			}
			cancel()
		}
	}
	if h.browserControl != nil {
		browser["state"] = "waiting_source"
		browser["reason"] = "original_frontend_not_selected"
		if a != nil {
			st := h.browserControl
			st.mu.Lock()
			binding, ok := st.binding[a.Target.Thread]
			if ok && binding.session == a.Target.Session && binding.client.live() {
				browser["state"] = "selected"
				browser["reason"] = "selected_frontend_engine_capability_requires_native_observation"
				browser["native_capability_state"] = "unknown"
				for _, op := range st.operations {
					if op.thread == a.Target.Thread && op.binding == binding.token && op.method == "capabilities" && !op.pending && len(op.state) > 0 {
						browser["native_capability_state"] = "observed_receipt_available"
						browser["native_capability_receipt"] = op.state
						break
					}
				}
			}
			st.mu.Unlock()
		}
	}
	if h.unifiedMedia != nil {
		media["state"] = "waiting_source"
		media["reason"] = "active_voice_required"
		if a != nil {
			h.voiceMu.Lock()
			for c, call := range h.voiceClients {
				if call.session != nil && call.session.ID == a.Target.Session && c.live() {
					if call.stopping {
						media["state"] = "unavailable"
						media["reason"] = "voice_release_unsettled"
					} else if call.kind != "line" {
						media["state"] = "bound"
						media["reason"] = "actual_call_capability_observed_playback_and_audio_separate"
						media["capabilities"] = map[string]bool{"visible_player": call.mediaCapable, "native_music": call.externalMusicCapable, "media_session": call.mediaSessionCapable, "voice_volume": call.voiceAudioCapable, "system_media_volume": call.systemVolumeCapable}
						media["platform"] = call.externalMusicPlatform
						media["native_permission_state"] = "unknown_until_original_native_effect_or_observation"
					}
					break
				}
			}
			h.voiceMu.Unlock()
		}
	}
	return []map[string]any{media, line, browser}
}

// Existing service authorizers remain the final base-scope check. A current
// role witness is additive evidence, never a bypass. After demotion only that
// independently existing base service scope can admit a new action.
func (h *Hub) jarvisExternalAdmission(c backend.SessionControlCaller, baseAllowed bool) error {
	if c.Parent == nil || !baseAllowed {
		return errors.New("original_service_scope_unavailable")
	}
	if c.RoleEpoch == 0 {
		return nil
	}
	a := h.currentJarvisRole()
	if a != nil && a.Target.Session == c.Parent.ID && a.Epoch == c.RoleEpoch {
		return nil
	}
	// No role-derived service scope is persisted. Independently configured base
	// permissions survive demotion; exact account/call/view checks still follow.
	return nil
}
