package core

import (
	"context"
	"crypto/sha256"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/mediaservice"
	"everything-go/internal/protocol"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeOnlyRejectsEveryRetiredAdmissionBeforeNativeQueue(t *testing.T) {
	volume := 60
	for _, negotiated := range []bool{false, true} {
		for _, tc := range []struct {
			method string
			in     backend.VoiceMediaRequest
		}{
			{"play_youtube", backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}},
			{"play_youtube", backend.VoiceMediaRequest{URL: "not-even-a-url", AdoptCurrent: true, ExpectedMediaID: "foreign"}},
			{"control", backend.VoiceMediaRequest{Source: "youtube", Action: "pause"}},
			{"control", backend.VoiceMediaRequest{Source: "youtube", Action: "resume"}},
			{"control", backend.VoiceMediaRequest{Source: "youtube", Action: "volume", Volume: &volume}},
			{"play_music", backend.VoiceMediaRequest{Source: "youtube", URL: "https://music.youtube.com/watch?v=M7lc1UVf-VE"}},
		} {
			t.Run(tc.method+tc.in.Action+map[bool]string{true: "v1", false: "legacy"}[negotiated], func(t *testing.T) {
				h, c, call, caller := sessionMediaFixture(t)
				configureMediaSession(call, negotiated)
				_, err := h.ExecuteVoiceMedia(context.Background(), caller, tc.method, tc.in)
				var refused *backend.VoiceMediaNotDispatchedError
				if !errors.As(err, &refused) || refused.Error() != "youtube_playback_retired" {
					t.Fatal(err)
				}
				if call.callMedia != nil || call.media != nil || call.externalMusic != nil {
					t.Fatal("retired admission claimed a source")
				}
				select {
				case e := <-c.send:
					t.Fatal("retired command enqueued", e)
				default:
				}
			})
		}
	}
}
func TestNativeOnlyPlatformSongAndStrictURLRemain(t *testing.T) {
	for _, platform := range []string{"android", "ios"} {
		t.Run(platform, func(t *testing.T) {
			h, c, call, caller := sessionMediaFixture(t)
			c.clientSurface = platform
			call.externalMusicPlatform = platform
			configureMediaSession(call, true)
			uri := "https://music.apple.com/us/song/123456789"
			expected := "123456789"
			if platform == "android" {
				uri = "https://music.youtube.com/watch?v=M7lc1UVf-VE"
				expected = "M7lc1UVf-VE"
			}
			done := make(chan any, 1)
			go func() {
				v, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{URL: uri, Title: "Yellow", Artist: "Coldplay", DisplayLabel: "Coldplay - Yellow"})
				if e != nil {
					done <- e
				} else {
					done <- v
				}
			}()
			cmd := nextSessionCommand(t, c)
			if cmd.MediaSession.Target != "native_music" || cmd.MediaSession.URI != uri || cmd.MediaSession.MediaID != expected || cmd.MediaSession.Title != "Yellow" || cmd.MediaSession.Volume != nil {
				t.Fatal(cmd)
			}
			x := sessionState(cmd, 1)
			x.NativeProvider = call.platformNativeMusicProvider
			sessionFeedback(h, c, call, cmd, x)
			got, ok := (<-done).(backend.VoiceMediaResult)
			if !ok || !got.PlaybackConfirmed || got.DisplayLabel != "Coldplay - Yellow" {
				t.Fatal(got)
			}
			caller.ToolCallID = "invalid-native-url"
			for _, bad := range []string{"https://youtube.com/watch?v=M7lc1UVf-VE", "https://music.youtube.com.evil/watch?v=M7lc1UVf-VE", "https://music.youtube.com/watch?v=bad", "https://music.apple.com.evil/song/123456789"} {
				if _, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{URL: bad}); e == nil {
					t.Fatal("invalid native URL", bad)
				}
			}
			select {
			case e := <-c.send:
				t.Fatal("invalid URL queued", e)
			default:
			}
		})
	}
}
func TestNativeOnlyLegacyIOSCannotFallbackOrAcceptHandoff(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	for c, call := range h.voiceClients {
		c.clientSurface = "ios"
		call.mediaCapable = true
		call.externalMusicCapable = true
		call.musicProvider = "ytmusic_ios"
		if _, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{URL: "https://music.youtube.com/watch?v=M7lc1UVf-VE"}); e == nil || e.Error() != "music_provider_retired" {
			t.Fatal(e)
		}
		if validExternalMusic(&protocol.ExternalMusicState{Provider: "ytmusic_ios", PackageName: "com.google.ios.youtubemusic", State: "unknown", ObservationAgeMS: -1}) {
			t.Fatal("retired live report accepted")
		}
		select {
		case e := <-c.send:
			t.Fatal("fallback queued", e)
		default:
		}
	}
}
func TestNativeOnlyHistoricalUnknownNeverReroutesOrMutatesLedger(t *testing.T) {
	h, c, call, caller := sessionMediaFixture(t)
	dir := t.TempDir()
	store, e := mediaservice.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	o := backend.MediaOrigin{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, Generation: call.mediaGeneration, TurnID: caller.TurnID}
	q := mediaservice.Request{ID: "vop_old_video_unknown", Thread: o.ThreadID, Method: "play_youtube", Input: backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}}
	oldCommand := "vm_" + strings.Repeat("a", 64)
	_, e = store.Execute(context.Background(), o, q, func() (backend.VoiceMediaResult, error) {
		return backend.VoiceMediaResult{State: "unknown", Provider: "call_media_session", CommandID: oldCommand, MediaSessionReceipt: &protocol.MediaSessionState{Kind: "receipt", Version: 1, Revision: 1, ObservationAgeMS: -1, ActiveSource: "youtube", ReservedSource: "youtube", Phase: "unknown", Error: "original_unknown"}}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	db := filepath.Join(dir, "media-control/operations.db")
	before, _ := os.ReadFile(db)
	hash := sha256.Sum256(before)
	u := &unifiedVoiceMedia{store: store}
	for _, status := range []bool{false, true} {
		request := q
		if status {
			request = mediaservice.Request{ID: "vop_old_video_query", Thread: o.ThreadID, Method: "status", Operation: q.ID}
		}
		r, e := h.executeMediaWithOrigin(context.Background(), u, o, request, "native_tool")
		if e != nil || r.State != "unknown" || !r.Historical || r.CommandID != oldCommand || r.ActionConfirmed || r.PlaybackConfirmed || r.MediaSessionReceipt.Error != "original_unknown" {
			t.Fatal(r, e)
		}
	}
	after, _ := os.ReadFile(db)
	if sha256.Sum256(after) != hash {
		t.Fatal("read changed original receipt")
	}
	select {
	case e := <-c.send:
		t.Fatal("history observed another source", e)
	default:
	}
}
func TestNativeOnlyIgnoresLegacyPlayerCallbacksAndPrompts(t *testing.T) {
	h, c, call, caller := sessionMediaFixture(t)
	call.media = &voiceMediaSession{state: "unknown"}
	before := call.media.state
	configureMediaSession(call, false)
	h.handleVoiceMediaState(c, clientproto.Command{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaState: "playing"})
	if call.media.state != before {
		t.Fatal("old video callback renewed proof")
	}
	for _, text := range []string{(&unifiedVoiceMedia{launcher: "fixed"}).instructions()} {
		for _, old := range []string{"play_youtube", "--source youtube", "ytmusic_ios", "visible official player"} {
			if strings.Contains(text, old) {
				t.Fatal("retired instructions", old)
			}
		}
		for _, keep := range []string{"play_music", "music_control", "voice_volume", "media_volume", "music.youtube.com", "music.apple.com", "exact song title"} {
			if !strings.Contains(text, keep) {
				t.Fatal("native contract missing", keep)
			}
		}
	}
}

func TestNativeOnlyLegacyUnknownPlatformNeverDefaultsToAndroid(t *testing.T) {
	for _, platform := range []string{"", "desktop", "other"} {
		h, _, caller, _ := controllerFixture(t)
		for c, call := range h.voiceClients {
			c.clientSurface = platform
			call.mediaCapable = true
			call.externalMusicCapable = true
			_, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{URL: "https://music.youtube.com/watch?v=M7lc1UVf-VE"})
			if e == nil || e.Error() != "external_music_not_supported" {
				t.Fatal(platform, e)
			}
			select {
			case e := <-c.send:
				t.Fatal("unidentified platform queued", e)
			default:
			}
		}
	}
}
