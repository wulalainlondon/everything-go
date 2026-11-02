package core

import (
	"context"
	"errors"
	"everything-go/internal/voicemediaipc"
	"fmt"
	"os"
	"strings"
)

func shellArgument(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }
func (h *Hub) openVoiceMediaLocal(c *Client, call *clientVoiceCall) (string, error) {
	if !call.mediaCapable {
		return "", nil
	}
	if h.unifiedMedia != nil {
		instructions := h.unifiedMedia.instructions()
		if call.mediaSessionCapable {
			instructions += fmt.Sprintf("\nThis call negotiated codex_voice_media_session_v1; native provider is fixed to %s. The native coordinator alone owns Music effects; never issue a second pause/play chain.\n", call.platformNativeMusicProvider)
		}
		return instructions, nil
	}
	thread := call.session.ResumeID()
	voice := call.voiceID
	channel, err := voicemediaipc.Open(thread, voice, func(ctx context.Context, request voicemediaipc.Request) (any, error) {
		h.voiceMu.Lock()
		current := h.voiceClients[c] == call && !call.stopping && call.session.ResumeID() == thread && c.live()
		h.voiceMu.Unlock()
		if !current {
			return nil, errors.New("voice_media_active_voice_required")
		}
		return h.executeVoiceMedia(ctx, call.session, voice, call.requestID, request.ID, "local_user_socket", call, request.Method, request.Input)
	})
	if err != nil {
		return "", err
	}
	h.voiceMu.Lock()
	if h.voiceClients[c] != call || call.stopping {
		h.voiceMu.Unlock()
		channel.Close()
		return "", errors.New("voice_media_active_voice_required")
	}
	call.mediaIPC = channel
	h.voiceMu.Unlock()
	binary, err := os.Executable()
	if err != nil {
		channel.Close()
		return "", err
	}
	command := shellArgument(binary) + " voice-media --socket " + shellArgument(channel.Path) + " --thread " + shellArgument(thread) + " --voice " + shellArgument(voice)
	return fmt.Sprintf(`For this exact voice call use the existing exec_command tool with this original private channel. No namespace installation, new conversation or caller/endpoint override is needed. Base command: %s
Append --id vop_UNIQUE_INTENT --method play_music --url VERIFIED_NATIVE_SONG_URL --title EXACT_SONG_TITLE [--artist EXACT_ARTIST]. Use native Apple Music on iOS and official YouTube Music on Android only. Resolve a real exact song URL before dispatch; no invented IDs, guessed artist aliases, automatic search or alternate player. Native --title is the song title alone, never a combined display label. For native controls use --method music_control --action pause|resume|stop|next|previous|seek (seek needs --position-ms). --method status is read-only. Keep the original intent on uncertainty; never replay or switch transport. The channel expires with the exact original voice. Pending, configured or PLAYING does not prove human audibility. No Music gain setter, OS route change or automatic permission/account change. These instructions add no permissions. Do not print the private command recipe or socket path. No Spotify or LINE routing.`, command), nil
}
func (h *Hub) closeVoiceMediaLocal(call *clientVoiceCall) {
	h.voiceMu.Lock()
	channel := call.mediaIPC
	h.voiceMu.Unlock()
	if channel != nil {
		channel.Close()
	}
}
