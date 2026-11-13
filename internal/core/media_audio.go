package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func (h *Hub) supportsAudioInput(s *session.Session) bool {
	p, ok := h.exec.(backend.AudioInputSupport)
	return ok && s.Backend() == backend.Codex && p.SupportsAudioInput(context.Background(), s)
}

type audioProbeOutput struct{ bytes.Buffer }

func (b *audioProbeOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 64*1024 {
		return 0, errors.New("audio probe output exceeded limit")
	}
	return b.Buffer.Write(p)
}

// Reject disguised video, invalid containers and unbounded recordings before
// committing an audio manifest. No transcription or remote model call occurs.
func probeUploadedAudio(path, mediaType string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries", "format=duration,format_name:stream=codec_type", "-of", "json", path)
	var out audioProbeOutput
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return 0, errors.New("錄音格式無法驗證，原附件保留")
	}
	var v struct {
		Streams []struct {
			CodecType string `json:"codec_type"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
			Name     string `json:"format_name"`
		} `json:"format"`
	}
	if json.Unmarshal(out.Bytes(), &v) != nil || len(v.Streams) == 0 || len(v.Streams) > 8 {
		return 0, errors.New("invalid audio streams")
	}
	for _, stream := range v.Streams {
		if stream.CodecType != "audio" {
			return 0, errors.New("錄音附件不可包含影片或其他串流")
		}
	}
	format := map[string]string{
		".wav": "wav", ".mp3": "mp3", ".m4a": "mov", ".webm": "webm", ".ogg": "ogg", ".flac": "flac",
	}[backend.AudioExtension(mediaType)]
	if format == "" || !strings.Contains(","+v.Format.Name+",", ","+format+",") {
		return 0, errors.New("錄音容器與宣告格式不符，原附件保留")
	}
	seconds, err := strconv.ParseFloat(v.Format.Duration, 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds <= 0 || seconds > 600 {
		return 0, errors.New("錄音附件須介於0至10分鐘")
	}
	return int64(math.Ceil(seconds * 1000)), nil
}
