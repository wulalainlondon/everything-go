package core

import (
	"context"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/mediaservice"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type unifiedVoiceMedia struct {
	store    *mediaservice.Store
	server   *mediaservice.Server
	resolver backend.MediaOriginResolver
	launcher string
}

func (h *Hub) StartUnifiedVoiceMedia(resolver backend.MediaOriginResolver, manifest string) (func(), error) {
	if resolver == nil || h.unifiedMedia != nil {
		return nil, errors.New("media_configuration_invalid")
	}
	store, e := mediaservice.Open(h.cfg.DataDir)
	if e != nil {
		return nil, e
	}
	expectedManifest, launcher, e := mediaservice.Paths(h.cfg.MediaControlDir)
	if e != nil {
		store.Close()
		return nil, e
	}
	if h.cfg.MediaControlDir != "" && manifest != expectedManifest {
		store.Close()
		return nil, errors.New("media_configuration_invalid")
	}
	u := &unifiedVoiceMedia{store: store, resolver: resolver, launcher: launcher}
	server, e := mediaservice.OpenServer(h.cfg.DataDir, h.cfg.InstanceID, manifest, func(ctx context.Context, q mediaservice.Request) (mediaservice.Result, error) {
		return h.executeUnifiedMedia(ctx, u, q)
	})
	if e != nil {
		store.Close()
		return nil, e
	}
	u.server = server
	binary, e := os.Executable()
	if e != nil {
		server.Close()
		store.Close()
		return nil, e
	}
	if e = os.MkdirAll(filepath.Dir(u.launcher), 0700); e != nil {
		server.Close()
		store.Close()
		return nil, e
	}
	// Fixed path, atomically replaced alongside the adopted signed release.
	const marker = "# Everything Go managed media entry v1\n"
	if st, err := os.Lstat(u.launcher); err == nil {
		old, readErr := os.ReadFile(u.launcher)
		if !st.Mode().IsRegular() || st.Mode()&os.ModeSymlink != 0 || readErr != nil || !strings.HasPrefix(string(old), "#!/bin/sh\n"+marker) {
			server.Close()
			store.Close()
			return nil, errors.New("media_launcher_conflict")
		}
	} else if !os.IsNotExist(err) {
		server.Close()
		store.Close()
		return nil, err
	}
	script := "#!/bin/sh\n" + marker + "exec " + shellArgument(binary) + " media --manifest " + shellArgument(manifest) + " \"$@\"\n"
	tmp, e := os.CreateTemp(filepath.Dir(u.launcher), ".bridge-media-*")
	if e != nil {
		server.Close()
		store.Close()
		return nil, e
	}
	name := tmp.Name()
	if e = tmp.Chmod(0700); e == nil {
		_, e = tmp.WriteString(script)
	}
	if e == nil {
		e = tmp.Sync()
	}
	tmp.Close()
	if e == nil {
		e = os.Rename(name, u.launcher)
	}
	os.Remove(name)
	if e != nil {
		server.Close()
		store.Close()
		return nil, e
	}
	h.unifiedMedia = u
	return func() { server.Close(); store.Close() }, nil
}

func (h *Hub) executeUnifiedMedia(ctx context.Context, u *unifiedVoiceMedia, q mediaservice.Request) (mediaservice.Result, error) {
	// Native command admission can reach us slightly after process startup.
	var origin backend.MediaOrigin
	var e error
	timer := time.NewTicker(25 * time.Millisecond)
	defer timer.Stop()
	for {
		origin, e = u.resolver.ResolveMediaOrigin(q.Thread, q.ID)
		if e == nil {
			break
		}
		select {
		case <-ctx.Done():
			return mediaservice.Result{}, errors.New("origin_context_unavailable")
		case <-timer.C:
		}
	}
	return h.executeMediaWithOrigin(ctx, u, origin, q, "local_user_socket")
}
func (h *Hub) UnifiedVoiceMediaEnabled() bool { return h.unifiedMedia != nil }
func (h *Hub) ExecuteScopedVoiceMedia(ctx context.Context, caller backend.SessionControlCaller, origin backend.MediaOrigin, intent, method string, input backend.VoiceMediaRequest) (any, error) {
	if h.unifiedMedia == nil || caller.Parent == nil || caller.ToolCallID == "" || caller.TurnID != origin.TurnID || caller.Parent.ID != origin.SessionID || caller.Parent.ResumeID() != origin.ThreadID || !mediaservice.IntentID.MatchString(intent) {
		return nil, errors.New("media_origin_invalid")
	}
	if e := h.jarvisExternalAdmission(caller, caller.VoiceID != "" && caller.VoiceID == origin.VoiceID); e != nil {
		return nil, e
	}
	operation := input.Operation
	input.Operation = ""
	if operation != "" && (method != "status" || !mediaservice.IntentID.MatchString(operation)) {
		return nil, errors.New("media_operation_invalid")
	}
	return h.executeMediaWithOrigin(ctx, h.unifiedMedia, origin, mediaservice.Request{ID: intent, Thread: origin.ThreadID, Method: method, Operation: operation, Input: input}, "native_tool")
}
func (h *Hub) executeMediaWithOrigin(ctx context.Context, u *unifiedVoiceMedia, origin backend.MediaOrigin, q mediaservice.Request, transport string) (mediaservice.Result, error) {
	s, ok := h.registry.Get(origin.SessionID)
	if !ok || s.ResumeID() != origin.ThreadID || origin.ThreadID != q.Thread || !h.controls.MobileMayWrite(s.ID) {
		return mediaservice.Result{}, errors.New("media_origin_invalid")
	}
	h.voiceMu.Lock()
	var call *clientVoiceCall
	for c, v := range h.voiceClients {
		if c.live() && !v.stopping && v.session == s && v.voiceID == origin.VoiceID && v.mediaGeneration == origin.Generation && v.kind != "line" {
			if call != nil {
				h.voiceMu.Unlock()
				return mediaservice.Result{}, errors.New("media_endpoint_ambiguous")
			}
			call = v
		}
	}
	h.voiceMu.Unlock()
	if call == nil && q.Method != "status" {
		return mediaservice.Result{}, errors.New("media_voice_expired")
	}
	return u.store.Execute(ctx, origin, q, func() (backend.VoiceMediaResult, error) {
		if call == nil {
			return backend.VoiceMediaResult{}, errors.New("media_voice_expired")
		}

		value, e := h.executeVoiceMedia(ctx, s, origin.VoiceID, call.requestID, q.ID, transport, call, q.Method, q.Input)
		if e != nil {
			result, _ := value.(backend.VoiceMediaResult)
			return result, e
		}
		result, ok := value.(backend.VoiceMediaResult)
		if !ok {
			return result, errors.New("media_result_invalid")
		}
		return result, nil
	}, func(old backend.VoiceMediaResult) (backend.VoiceMediaResult, error) {
		if call == nil {
			return backend.VoiceMediaResult{}, errors.New("media_voice_expired")
		}
		if old.Provider == "call_media_session" {
			input := backend.VoiceMediaRequest{OriginalCommandID: old.CommandID}
			value, err := h.executeVoiceMedia(ctx, s, origin.VoiceID, call.requestID, q.ID, transport, call, "status", input)
			result, _ := value.(backend.VoiceMediaResult)
			return result, err
		}
		// Legacy histories are immutable too: do not ask whichever provider
		// happens to be current. Only exact volume receipt lookup is causal.
		if result, ok := h.systemVolumeReceipt(call, q.Operation); ok && result.CommandID == old.CommandID {
			return result, nil
		}
		if result, ok := h.voiceVolumeReceipt(call, q.Operation); ok && result.CommandID == old.CommandID {
			return result, nil
		}
		return h.observeOriginalLegacyMedia(ctx, call, old)
	})
}

func (u *unifiedVoiceMedia) instructions() string {
	return fmt.Sprintf(`Native Music control is installed at the fixed local entry %s through the existing exec_command tool, including older native conversations. The service verifies the original provider-observed request and exact call generation. Never supply caller/thread/voice/device identities or switch transports after uncertainty. When delegating, include this entry and the requested action; project hot reload is not assumed.
Only official YouTube Music on Android and native Apple Music on iOS are supported. For a NEW requested song use --id vop_UNIQUE_INTENT --method play_music --url VERIFIED_NATIVE_SONG_URL --title EXACT_SONG_TITLE [--artist EXACT_ARTIST] [--label DISPLAY_LABEL]. Android requires canonical https://music.youtube.com/watch?v=VIDEO_ID for a verified recording. iOS requires a real https://music.apple.com song or album track ?i=SONG_ID. --title contains the exact song title only; artist is separate. Optional --label/display_label is display-only, never a native matching expectation or a substitute for title/artist/ID. Do not auto-split labels, guess localized aliases or invent song IDs. Resolve before dispatch; no automatic dispatch-driven search, fallback player, purchase or permission grant.
Use --method music_control --action pause|resume|stop|next|previous|seek (--position-ms for seek); control with --source native_music is the same native-only path. Native Music has no gain setter. Platform foreground, permissions, membership, supported native sessions and action availability apply; refusal is not playback. Android notification-listener access and an official supported session may be required. music_open_required means the user must tap the current call native controls, never a second play workaround. Unsupported iOS native Music cannot fall back to another app.
Observe --id vop_UNIQUE_QUERY --method status [--operation ORIGINAL_INTENT]. Keep every original intent and its immutable outcome; read does not restart playback, change provider, settle an unknown action from current playing or renew causal proof. media_session is a local snapshot, media_session_receipt is causal to the original command. Verify returned endpoint and original call. Queued/accepted/handoff/playing is not proof the human heard sound. Foreign Music must not be paused without explicit consent for the exact fresh item; only then supply --adopt-current=true --expected-media-id EXACT_ITEM. Never infer consent or force routes.
Assistant playback gain remains --method voice_volume (--volume0..100 or explicit --delta); explicit phone STREAM_MUSIC gain remains --method media_volume with --volume or --delta where advertised. Query without a gain argument is read-only. These independent capabilities never change the source, microphone, Music item or earbud-private gain. Never reinterpret an unsupported player volume request as either gain. Ambiguous volume targets require clarification; no automatic increase/retry. On uncertainty observe the original operation. No private APIs, cookies, ADB, other endpoint or LINE/Spotify substitution.`, shellArgument(u.launcher))
}
