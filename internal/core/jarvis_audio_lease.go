package core

import (
	"encoding/json"
	"everything-go/internal/jarvisrole"
	"net/http"
	"time"
)

func (h *Hub) serveJarvisAudioLease(w http.ResponseWriter, r *http.Request, device, path string) {
	fail := func(status int, code string) {
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "ok": false, "error_code": code})
	}
	var in struct {
		Authority string            `json:"authority_instance_id"`
		ID        string            `json:"lease_id"`
		SpeechID  string            `json:"speech_id"`
		Target    jarvisrole.Target `json:"target"`
		Epoch     uint64            `json:"expected_role_epoch"`
		Phase     string            `json:"phase"`
		Expected  uint64            `json:"expected_lease_revision"`
	}
	if r.Method != "POST" {
		fail(405, "method_not_allowed")
		return
	}
	if !jarvisDecode(w, r, &in) {
		return
	}
	if in.Authority != h.cfg.InstanceID || !jarvisID.MatchString(in.ID) {
		fail(403, "audio_lease_scope_invalid")
		return
	}
	actor := jarvisRequestPrincipal(r, device)
	if path == "role/audio-lease/status" {
		v, ok := h.jarvisRoles.AudioLease(actor, in.ID)
		if !ok {
			fail(404, "audio_lease_not_found")
			return
		}
		v.Actor = ""
		v.InputHash = ""
		_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "phase": "observed", "audio_lease": v})
		return
	}
	h.jarvisRoleMu.Lock()
	defer h.jarvisRoleMu.Unlock()
	if in.Phase == "acquired" {
		s, ok := h.registry.Get(in.Target.Session)
		if !ok || !h.jarvisRoleEligible(s) || h.contextAdmissionBlocked(s) || in.Target.Authority != h.cfg.InstanceID || s.ResumeID() != in.Target.Thread || s.SettingsSnapshot().ConfigRevision != in.Target.Revision || len(in.SpeechID) < 8 || len(in.SpeechID) > 128 {
			fail(409, "audio_lease_source_changed")
			return
		}
	}
	if h.inboxDevice(r) != device {
		fail(403, "pairing_changed")
		return
	}
	v, e := h.jarvisRoles.SetAudioLease(actor, jarvisrole.AudioLease{ID: in.ID, Target: in.Target, Epoch: in.Epoch, Phase: in.Phase, SpeechID: in.SpeechID}, in.Expected, jarvisHash(in))
	if e != nil {
		fail(409, e.Error())
		return
	}
	v.Actor = ""
	v.InputHash = ""
	_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "phase": "committed", "audio_lease": v})
}

// Requires voiceMu held until the role commit. Expired preparation permits are
// not live calls; any actual/stopping entry remains blocking until exact close.
func (h *Hub) jarvisRoleVoiceBusyLocked(target string) bool {
	if h.jarvisVoiceTransitions[target] > 0 {
		return true
	}
	for _, call := range h.voiceClients {
		if call.session != nil && call.session.ID == target {
			return true
		}
	}
	for _, permit := range h.voicePermits {
		if permit.session == target && time.Now().Before(permit.expires) {
			return true
		}
	}
	return false
}

func (h *Hub) beginJarvisVoiceTransition(session string) func() {
	h.voiceMu.Lock()
	if h.jarvisVoiceTransitions == nil {
		h.jarvisVoiceTransitions = map[string]int{}
	}
	h.jarvisVoiceTransitions[session]++
	h.voiceMu.Unlock()
	return func() {
		h.voiceMu.Lock()
		h.jarvisVoiceTransitions[session]--
		if h.jarvisVoiceTransitions[session] <= 0 {
			delete(h.jarvisVoiceTransitions, session)
		}
		h.voiceMu.Unlock()
	}
}
