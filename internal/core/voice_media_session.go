package core

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

var mediaSessionError = regexp.MustCompile(`^[a-z0-9_]{0,128}$`)
var mediaSessionCommandID = regexp.MustCompile(`^vm_[a-f0-9]{64}$`)

type callMediaReceipt struct {
	before        *protocol.MediaSessionState // immutable fresh pre-admission ownership snapshot, never a later read
	fingerprint   string
	command       protocol.VoiceMediaCommand
	result        backend.VoiceMediaResult
	causal        *protocol.MediaSessionState
	observation   *protocol.MediaSessionState // read-only original-status response, never causal
	observeIssued bool                        // set before original-status enqueue; never inferred from native content
	receivedAt    int64
	settled       chan struct{}
	observed      chan struct{}
	closed        bool
}
type callMediaSession struct {
	sequence   uint64
	receipts   map[string]*callMediaReceipt
	byCommand  map[string]*callMediaReceipt
	current    *callMediaReceipt
	snapshot   *protocol.MediaSessionState
	snapshotAt int64
}

func cloneMediaSession(x *protocol.MediaSessionState) *protocol.MediaSessionState {
	if x == nil {
		return nil
	}
	b, _ := json.Marshal(x)
	var v protocol.MediaSessionState
	_ = json.Unmarshal(b, &v)
	return &v
}
func sessionSampleAge(x *protocol.MediaSessionState, received int64) (int64, bool) {
	if x == nil || received <= 0 || x.ObservationAgeMS < 0 {
		return -1, false
	}
	elapsed := time.Now().UnixMilli() - received
	if elapsed < 0 {
		return -1, false
	}
	age := elapsed + x.ObservationAgeMS
	return age, age >= 0 && age <= 5000
}
func callMediaResult(m *callMediaSession, r *callMediaReceipt) backend.VoiceMediaResult {
	out := r.result
	out.MediaSessionReceipt = cloneMediaSession(r.causal)
	out.MediaSession = cloneMediaSession(m.snapshot)
	out.Observed = false
	out.ActionConfirmed = false
	out.PlaybackConfirmed = false
	out.AudioCheck = "unknown"
	if r.causal == nil && r.observeIssued && r.command.MediaSession.Action != "observe" {
		// Once a read is issued, an older native's observe-stage terminal can
		// be indistinguishable from its read response. Preserve uncertainty;
		// do not fabricate an original failure, success, or completion.
		out.State, out.ErrorCode = "unknown", "media_outcome_unknown"
	}
	if r.causal != nil {
		age, fresh := sessionSampleAge(r.causal, r.receivedAt)
		out.ObservationAgeMS = &age
		out.MediaSessionReceipt.ObservationAgeMS = age
		if !fresh {
			out.MediaSessionReceipt.ActionConfirmed = false
			out.MediaSessionReceipt.PlaybackConfirmed = false
		}
		out.State = r.causal.Phase
		out.ErrorCode = r.causal.Error
		out.Title = r.causal.Title
		if r.causal.Volume != nil {
			out.Volume = *r.causal.Volume
		}
		out.LastObservedMS = r.receivedAt
		out.LastObservedState = r.causal.Phase
		out.Observed = fresh
		out.ActionConfirmed = fresh && r.causal.ActionConfirmed
		out.PlaybackConfirmed = fresh && r.causal.PlaybackConfirmed
		out.NeedsUserAction = r.causal.Phase == "awaiting_gesture"
		if fresh {
			out.AudioCheck = "media_session_output_unverified"
		} else {
			out.AudioCheck = "stale"
		}
	}
	// A retired remote operation remains attributable, not a claim that its old
	// source/item is still current after another transaction or metadata change.
	if r != m.current || (m.snapshot != nil && r.causal != nil &&
		(m.snapshot.Revision > r.causal.Revision || r.causal.PlaybackConfirmed &&
			(!m.snapshot.PlaybackConfirmed || m.snapshot.ActiveSource != r.causal.ActiveSource || m.snapshot.MediaID != r.causal.MediaID))) {
		out.ActionConfirmed, out.PlaybackConfirmed = false, false
		if out.MediaSessionReceipt != nil {
			out.MediaSessionReceipt.ActionConfirmed = false
			out.MediaSessionReceipt.PlaybackConfirmed = false
		}
	}
	// Current snapshots are separately aged and cannot confirm the remote action.
	if out.MediaSession != nil {
		age, fresh := sessionSampleAge(m.snapshot, m.snapshotAt)
		out.MediaSession.ObservationAgeMS = age
		out.MediaSession.ActionConfirmed = false
		if !fresh {
			out.MediaSession.PlaybackConfirmed = false
		}
	}
	return out
}

// Only a negotiated call enters this route. The native coordinator alone owns
// pause/destroy/prepare/play claims; Go queues exactly one admission, not steps.
func (h *Hub) executeCallMediaSession(ctx context.Context, parent *session.Session, voiceID, requestID, operationID, transport string, expected *clientVoiceCall, method string, input backend.VoiceMediaRequest) (any, bool, error) {
	h.voiceMu.Lock()
	var owner *Client
	var call *clientVoiceCall
	for c, v := range h.voiceClients {
		if c.live() && v.session == parent && v.voiceID == voiceID {
			if call != nil {
				h.voiceMu.Unlock()
				return nil, true, &backend.VoiceMediaNotDispatchedError{Cause: errors.New("media_endpoint_ambiguous")}
			}
			owner, call = c, v
		}
	}
	if call == nil || !call.mediaSessionCapable {
		h.voiceMu.Unlock()
		return nil, false, nil
	}
	refuse := func(code string) (any, bool, error) {
		h.voiceMu.Unlock()
		return nil, true, &backend.VoiceMediaNotDispatchedError{Cause: errors.New(code)}
	}
	s, ok := h.registry.Get(parent.ID)
	if ctx.Err() != nil {
		return refuse("media_context_expired")
	}
	if !ok || s != parent || !h.controls.MobileMayWrite(s.ID) || call.stopping || call.kind == "line" || expected != nil && expected != call {
		return refuse("voice_media_active_voice_required")
	}
	if !call.mediaCapable || call.platformNativeMusicProvider != "ytmusic_android" && call.platformNativeMusicProvider != "apple_music_ios" {
		return refuse("media_session_not_supported")
	}
	if input.Query != "" || input.Delta != nil {
		return refuse("media_session_arguments_invalid")
	}
	for _, v := range []string{input.Title, input.Artist, input.ExpectedMediaID} {
		if !safeSessionLabel(v, 200) {
			return refuse("media_session_arguments_invalid")
		}
	}
	if input.PositionMS != nil && (*input.PositionMS < 0 || *input.PositionMS > 9007199254740991) {
		return refuse("media_session_position_invalid")
	}
	if input.ExpectedRevision != nil && *input.ExpectedRevision > 9007199254740991 {
		return refuse("media_session_revision_invalid")
	}
	if input.AdoptCurrent && input.ExpectedMediaID == "" || !input.AdoptCurrent && input.ExpectedMediaID != "" {
		return refuse("media_session_explicit_adoption_required")
	}
	if input.Source != "" && input.Source != "native_music" {
		return refuse("media_session_source_invalid")
	}
	if call.callMedia == nil {
		call.callMedia = &callMediaSession{receipts: map[string]*callMediaReceipt{}, byCommand: map[string]*callMediaReceipt{}}
	}
	m := call.callMedia
	target, action := input.Source, input.Action
	id, uri := "", ""
	switch method {
	case "play_music":
		if action != "" || input.PositionMS != nil {
			return refuse("media_session_arguments_invalid")
		}
		action = "play"
		if target != "" && target != "native_music" {
			return refuse("media_session_source_invalid")
		}
		target = "native_music"
		if call.platformNativeMusicProvider == "apple_music_ios" {
			id = appleMusicSongID(input.URL)
			if id == "" {
				return refuse("external_music_uri_invalid")
			}
			uri = input.URL
		} else {
			u, e := url.Parse(input.URL)
			if e != nil || u.Scheme != "https" || u.Host != "music.youtube.com" || u.User != nil || u.Path != "/watch" || u.Fragment != "" || len(u.RawQuery) != 13 || !youtubeVideoID.MatchString(u.Query().Get("v")) {
				return refuse("external_music_uri_invalid")
			}
			id = u.Query().Get("v")
			uri = input.URL
		}
	case "control", "music_control":
		if input.URL != "" || input.Title != "" || input.Artist != "" {
			return refuse("media_session_arguments_invalid")
		}
		if method == "music_control" {
			if target != "" && target != "native_music" {
				return refuse("media_session_source_invalid")
			}
			target = "native_music"
		}
		if target == "" {
			if _, fresh := sessionSampleAge(m.snapshot, m.snapshotAt); !fresh {
				return refuse("media_session_source_required")
			}
			target = m.snapshot.ActiveSource
			if target == "none" {
				target = m.snapshot.ReservedSource
			}
		}
		if target != "native_music" {
			return refuse("media_session_source_required")
		}
		if !slices.Contains([]string{"pause", "resume", "stop", "next", "previous", "seek"}, action) {
			return refuse("media_session_action_invalid")
		}
		if (action == "seek") != (input.PositionMS != nil) {
			return refuse("media_session_position_invalid")
		}
		if (action == "next" || action == "previous") && target != "native_music" {
			return refuse("media_session_action_unsupported")
		}
	case "status":
		if action != "" || input.URL != "" || input.Title != "" || input.Artist != "" || input.PositionMS != nil || input.Volume != nil || input.AdoptCurrent || input.ExpectedMediaID != "" || input.ExpectedRevision != nil {
			return refuse("media_session_arguments_invalid")
		}
		action = "observe"
		if target == "" {
			target = "native_music"
			if m.snapshot != nil && m.snapshot.ActiveSource != "none" {
				target = m.snapshot.ActiveSource
			}
		}
	default:
		return refuse("voice_media_tool_unknown")
	}
	if input.Volume != nil {
		return refuse("media_session_gain_unsupported")
	}
	if target != "native_music" {
		return refuse("youtube_playback_retired")
	}
	if method != "status" && input.Operation != "" {
		return refuse("media_session_arguments_invalid")
	}
	if input.OriginalCommandID != "" {
		if method != "status" {
			return refuse("media_session_arguments_invalid")
		}
		r := m.byCommand[input.OriginalCommandID]
		if r == nil {
			return refuse("media_session_original_receipt_unavailable")
		}
		if r.command.MediaSession == nil || r.command.MediaSession.Target != "native_music" {
			return refuse("youtube_playback_retired")
		}
		if r.observed == nil {
			r.observed = make(chan struct{})
		}
		wait := r.observed
		r.observeIssued = true
		cmd := r.command
		copy := *cmd.MediaSession
		copy.Action = "observe"
		cmd.MediaSession = &copy
		event := voiceEvent(clientproto.Command{SessionID: s.ID, RequestID: call.requestID, VoiceID: voiceID}, "media", "")
		event.ThreadID = s.ResumeID()
		event.Media = &cmd
		h.voiceMu.Unlock()
		owner.enqueueEvent(event)
		waitMediaSession(ctx, wait, 800*time.Millisecond)
		h.voiceMu.Lock()
		defer h.voiceMu.Unlock()
		if h.voiceClients[owner] != call || call.stopping {
			return backend.VoiceMediaResult{CommandID: r.command.CommandID, Sequence: r.command.Sequence, Provider: "call_media_session", State: "voice_ended", AudioCheck: "stale"}, true, nil
		}
		return callMediaResult(m, r), true, nil
	}
	encoded, _ := json.Marshal([]any{method, input})
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(encoded))
	key := transport + "|" + operationID
	if old := m.receipts[key]; old != nil {
		if old.fingerprint != fingerprint {
			return refuse("voice_media_intent_conflict")
		}
		out := callMediaResult(m, old)
		h.voiceMu.Unlock()
		return out, true, nil
	}
	if len(m.receipts) >= 64 {
		return refuse("voice_media_command_limit")
	}
	m.sequence++
	commandID := fmt.Sprintf("vm_%x", sha256.Sum256([]byte(h.cfg.InstanceID+"\x00"+s.ID+"\x00"+voiceID+"\x00session\x00"+transport+"\x00"+operationID)))
	nested := &protocol.MediaSessionCommand{OperationID: commandID, Target: target, Action: action, URI: uri, MediaID: id, Title: input.Title, Artist: input.Artist, PositionMS: input.PositionMS, Volume: input.Volume, ExpectedRevision: input.ExpectedRevision, AdoptCurrent: input.AdoptCurrent, ExpectedMediaID: input.ExpectedMediaID}
	cmd := protocol.VoiceMediaCommand{Provider: "call_media_session", Action: "session", CommandID: commandID, Sequence: m.sequence, MediaSession: nested}
	r := &callMediaReceipt{fingerprint: fingerprint, command: cmd, settled: make(chan struct{}), result: backend.VoiceMediaResult{Provider: "call_media_session", Endpoint: h.mediaEndpoint(owner), Transport: transport, Action: action, CommandID: commandID, Sequence: m.sequence, State: "queued", AudioCheck: "unknown", DisplayLabel: input.DisplayLabel}}
	if _, fresh := sessionSampleAge(m.snapshot, m.snapshotAt); fresh {
		r.before = cloneMediaSession(m.snapshot)
	}
	m.receipts[key] = r
	m.byCommand[commandID] = r
	m.current = r
	event := voiceEvent(clientproto.Command{SessionID: s.ID, RequestID: call.requestID, VoiceID: voiceID}, "media", "")
	event.ThreadID = s.ResumeID()
	event.Media = &r.command
	h.voiceMu.Unlock()
	owner.enqueueEvent(event)
	waitMediaSession(ctx, r.settled, 2*time.Second)
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if h.voiceClients[owner] != call || call.stopping {
		return backend.VoiceMediaResult{Provider: "call_media_session", State: "voice_ended", CommandID: commandID, Sequence: r.command.Sequence, AudioCheck: "stale"}, true, nil
	}
	return callMediaResult(m, r), true, nil
}
func waitMediaSession(ctx context.Context, ch <-chan struct{}, limit time.Duration) {
	t := time.NewTimer(limit)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-ch:
	case <-t.C:
	}
}

func validCallMediaState(x *protocol.MediaSessionState) bool {
	if x == nil || !slices.Contains([]string{"receipt", "snapshot"}, x.Kind) || x.Version == 0 || x.Version > 9007199254740991 || x.Revision > 9007199254740991 || x.ObservationAgeMS < -1 || x.ObservationAgeMS > 9007199254740991 {
		return false
	}
	for _, source := range []string{x.ActiveSource, x.ReservedSource} {
		if !slices.Contains([]string{"none", "native_music"}, source) {
			return false
		}
	}
	if !slices.Contains([]string{"idle", "preparing", "switching", "playing", "paused", "stopped", "awaiting_gesture", "unknown", "error"}, x.Phase) || !slices.Contains([]string{"observe", "pause_old", "prepare_target", "start_target", "confirmed", "refused"}, x.Stage) {
		return false
	}
	if !safeSessionLabel(x.Title, 200) || !safeSessionLabel(x.Artist, 200) || len(x.MediaID) > 2048 || len(x.BindingID) > 512 || strings.ContainsAny(x.MediaID+x.BindingID, "\r\n\x00") || !mediaSessionError.MatchString(x.Error) {
		return false
	}
	if len(x.SupportedActions) > 16 {
		return false
	}
	for _, a := range x.SupportedActions {
		if !slices.Contains([]string{"play", "pause", "resume", "stop", "next", "previous", "seek", "observe", "cancel"}, a) {
			return false
		}
	}
	if x.Volume != nil && (*x.Volume < 0 || *x.Volume > 100) {
		return false
	}
	if x.PositionMS != nil && (*x.PositionMS < 0 || *x.PositionMS > 9007199254740991) || x.DurationMS != nil && (*x.DurationMS <= 0 || *x.DurationMS > 9007199254740991) {
		return false
	}
	if x.NativeMatch != nil && !validNativeMetadataEvidence(x) {
		return false
	}
	if x.PlaybackConfirmed && (x.Phase != "playing" || x.ActiveSource == "none" || (x.MediaID == "" && !validNativeMetadataEvidence(x)) || x.ObservationAgeMS < 0 || x.ObservationAgeMS > 5000 || x.Error != "") {
		return false
	}
	return true
}
func sessionCausalMatches(r *callMediaReceipt, x *protocol.MediaSessionState) bool {
	cmd := r.command.MediaSession
	if x.NativeMatch != nil {
		return nativeMetadataCausalMatches(r, x)
	}
	if x.PlaybackConfirmed && (x.ActiveSource != cmd.Target || cmd.MediaID != "" && x.MediaID != cmd.MediaID) {
		return false
	}
	if !x.ActionConfirmed {
		return true
	}
	if x.Error != "" || x.Stage != "confirmed" || x.ObservationAgeMS < 0 || x.ObservationAgeMS > 5000 {
		return false
	}
	switch cmd.Action {
	case "play", "resume":
		return x.PlaybackConfirmed && x.ActiveSource == cmd.Target
	case "pause":
		return x.Phase == "paused" && x.ActiveSource == cmd.Target
	case "stop":
		return (x.Phase == "stopped" || x.Phase == "idle") && (x.ActiveSource == "none" || x.ActiveSource == cmd.Target) && (x.ReservedSource == "none" || x.ReservedSource == cmd.Target)
	case "seek":
		return x.ActiveSource == cmd.Target && x.PositionMS != nil && cmd.PositionMS != nil && absMediaPosition(*x.PositionMS-*cmd.PositionMS) <= 3000
	case "next", "previous":
		return x.ActiveSource == "native_music" && x.MediaID != "" && x.BindingID != ""
	default:
		return false
	}
}
func absMediaPosition(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// Older Android can terminate discovery before dispatch while retaining its
// initial observe stage. Only this first pending, pre-read refusal is causal.
// Once any original-status read was issued, the same wire shape is ambiguous.
func legacyPreReadMediaRefusal(r *callMediaReceipt, x *protocol.MediaSessionState) bool {
	return !r.observeIssued && !r.closed && r.causal == nil &&
		x.Stage == "observe" && (x.Phase == "error" || x.Phase == "unknown") && x.Error != "" &&
		!x.Dispatched && !x.ActionConfirmed && !x.PlaybackConfirmed && x.ObservationAgeMS == -1
}

func (h *Hub) handleCallMediaState(c *Client, cmd clientproto.Command) {
	x := cmd.MediaSessionState
	if !validCallMediaState(x) || !mediaSessionCommandID.MatchString(cmd.MediaCommandID) {
		return
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	call := h.voiceClients[c]
	if call == nil || !c.live() || call.stopping || call.kind == "line" || !call.mediaSessionCapable || call.voiceID != cmd.VoiceID || call.requestID != cmd.RequestID || call.session.ID != cmd.SessionID || call.session.ResumeID() != cmd.ThreadID || !h.controls.MobileMayWrite(cmd.SessionID) || x.NativeProvider != call.platformNativeMusicProvider {
		return
	}
	if call.callMedia == nil {
		call.callMedia = &callMediaSession{receipts: map[string]*callMediaReceipt{}, byCommand: map[string]*callMediaReceipt{}}
	}
	actual, exists := h.registry.Get(cmd.SessionID)
	if !exists || actual != call.session {
		return
	}
	m := call.callMedia
	if x.Kind == "receipt" {
		r := m.byCommand[cmd.MediaCommandID]
		if r == nil || cmd.MediaSequence != r.command.Sequence || x.OperationID != cmd.MediaCommandID {
			return
		}
		// Native statusOriginal retains the original outer command ID/sequence.
		// Except for a strictly identified pre-read legacy refusal, its observe
		// stage describes current state, not a new mutation outcome. Older
		// natives may retain Dispatched; that still cannot confirm an effect.
		if x.Stage == "observe" && r.command.MediaSession.Action != "observe" && !legacyPreReadMediaRefusal(r, x) {
			if x.ActionConfirmed || r.observation != nil && (x.Version <= r.observation.Version || x.Revision < r.observation.Revision) ||
				r.causal != nil && (x.Version <= r.causal.Version || x.Revision < r.causal.Revision) {
				return
			}
			r.observation = cloneMediaSession(x)
			if r.observed != nil {
				close(r.observed)
				r.observed = nil
			}
		} else {
			// A terminal mutation receipt is immutable. Later native feedback
			// may arrive as a separate snapshot, never a rewrite of its outcome.
			if r.closed && r.causal != nil && r.command.MediaSession.Action != "observe" {
				return
			}
			if !sessionCausalMatches(r, x) || r.causal != nil && (x.Version <= r.causal.Version || x.Revision < r.causal.Revision) ||
				r.observation != nil && (x.Version <= r.observation.Version || x.Revision < r.observation.Revision) {
				return
			}
			r.causal = cloneMediaSession(x)
			r.receivedAt = time.Now().UnixMilli()
			if r.observed != nil {
				close(r.observed)
				r.observed = nil
			}
			// Only causal effect feedback (or an actual observe command's own
			// receipt) reaches completion. Current observations never settle it.
			if !r.closed && (x.ActionConfirmed || r.command.MediaSession.Action == "observe" || x.Stage == "refused" || x.Phase == "error" || x.Phase == "unknown") {
				r.closed = true
				close(r.settled)
			}
		}
	} else if cmd.MediaSequence != x.Revision || !(x.OperationID == cmd.MediaCommandID || strings.HasPrefix(x.OperationID, "local_") && len(x.OperationID) <= 160 && !strings.ContainsAny(x.OperationID, "\r\n\x00")) {
		return
	}
	if m.snapshot != nil && (x.Version <= m.snapshot.Version || x.Revision < m.snapshot.Revision) {
		return
	}
	m.snapshot = cloneMediaSession(x)
	m.snapshot.Kind = "snapshot"
	m.snapshot.NativeMatch = nil
	m.snapshot.ActionConfirmed = false
	m.snapshotAt = time.Now().UnixMilli()
}

func safeSessionLabel(s string, max int) bool {
	if !utf8.ValidString(s) || utf8.RuneCountInString(s) > max {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func configureMediaSession(call *clientVoiceCall, requested bool) {
	call.mediaSessionCapable = requested && call.mediaCapable
	call.platformNativeMusicProvider = ""
	if !call.mediaSessionCapable {
		return
	}
	switch call.externalMusicPlatform {
	case "ios":
		call.platformNativeMusicProvider = "apple_music_ios"
	case "android":
		call.platformNativeMusicProvider = "ytmusic_android"
	default:
		call.mediaSessionCapable = false
	}
}
