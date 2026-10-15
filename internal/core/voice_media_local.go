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
	return fmt.Sprintf(`For this exact voice call, YouTube music playback is available through the existing exec_command tool, even if this older conversation has no bridge_media tool namespace. When the user requests music, delegate to the backing agent and have it execute this local control CLI. No extra namespace installation or new conversation is needed. Use this CLI as the primary control path for the call; do not switch transports to retry an uncertain playback intent.
Base command: %s
Append --id vop_UNIQUE_INTENT --method play_youtube --url REAL_HTTPS_YOUTUBE_VIDEO_URL --title LABEL [--volume 0..100]. First use web search to find a real video URL, or use the user's supplied URL; never invent a video ID.
For music controls append --id vop_UNIQUE_INTENT --method control --action pause|resume|stop|volume [--volume 0..100]. For observations append --id vop_UNIQUE_QUERY --method status.
Each distinct intent needs a unique vop_ ID with8-100 letters/digits/underscore/hyphen after vop_. Keep the original ID on a same-intent retry. A lost result must be checked using status, not blindly replayed as a new intent. Do not print the private command recipe or socket path to the user.
The local transport is local_user_socket, distinct from native_tool. Results are JSON. queued/observed/playing do not prove the user heard audio; only playback_confirmed=true is a fresh actual device PLAYING report. For autoplay_blocked, ask the user to tap the official player's Play once. After a queued result, check status before reporting success. Use the visible official player only; foreground is required. Do not route Spotify or LINE through this channel. These instructions add no filesystem/network permissions; if execution is unavailable, report the actual failure. The channel expires with this exact call and cannot be reused on another voice.`, command), nil
}
func (h *Hub) closeVoiceMediaLocal(call *clientVoiceCall) {
	h.voiceMu.Lock()
	channel := call.mediaIPC
	h.voiceMu.Unlock()
	if channel != nil {
		channel.Close()
	}
}
