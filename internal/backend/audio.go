package backend

import "strings"

// Bridge engineering bounds, independent of upstream published limits.
const MediaAudioMaxBytes int64 = 10 * 1024 * 1024

func IsAudioType(mime string) bool { return strings.HasPrefix(strings.ToLower(mime), "audio/") }

func AudioExtension(mime string) string {
	switch strings.ToLower(strings.Split(mime, ";")[0]) {
	case "audio/wav", "audio/x-wav", "audio/wave":
		return ".wav"
	case "audio/mpeg", "audio/mp3":
		return ".mp3"
	case "audio/mp4", "audio/m4a", "audio/x-m4a":
		return ".m4a"
	case "audio/webm":
		return ".webm"
	case "audio/ogg":
		return ".ogg"
	case "audio/flac", "audio/x-flac":
		return ".flac"
	default:
		return ""
	}
}
