package core

import (
	"everything-go/internal/musicidentity"
	"everything-go/internal/protocol"
	"slices"
	"strings"
)

func validNativeMetadataEvidence(x *protocol.MediaSessionState) bool {
	e := x.NativeMatch
	if e == nil || e.Version != 1 || e.Basis != "metadata_v1" || x.NativeProvider != "ytmusic_android" || x.Kind != "receipt" || x.Stage != "confirmed" || !x.Dispatched || !x.ActionConfirmed || x.Error != "" || x.MediaID != "" || x.BindingID == "" || x.Title == "" || x.Artist == "" || x.ObservationAgeMS < 0 || x.ObservationAgeMS > 5000 {
		return false
	}
	if !slices.Contains([]string{"play_uri", "resume", "next", "previous", "seek"}, e.Action) {
		return false
	}
	for _, s := range []string{e.BeforeTitle, e.BeforeArtist} {
		if !safeSessionLabel(s, 200) {
			return false
		}
	}
	for _, s := range []string{e.BeforeBindingID, e.BeforeMediaID, e.ExpectedMediaID} {
		if len(s) > 512 || strings.ContainsAny(s, "\r\n\x00") {
			return false
		}
	}
	return true
}

func nativeMetadataCausalMatches(r *callMediaReceipt, x *protocol.MediaSessionState) bool {
	if !validNativeMetadataEvidence(x) {
		return false
	}
	cmd, e := r.command.MediaSession, x.NativeMatch
	if cmd == nil || cmd.Target != "native_music" || x.ActiveSource != "native_music" {
		return false
	}
	action := cmd.Action
	if action == "play" {
		action = "play_uri"
	}
	if e.Action != action {
		return false
	}
	if action == "play_uri" {
		return x.Phase == "playing" && x.PlaybackConfirmed && youtubeVideoID.MatchString(cmd.MediaID) && e.ExpectedMediaID == cmd.MediaID && musicidentity.MatchMetadata(cmd.Title, cmd.Artist, x.Title, x.Artist)
	}
	// Controls cannot borrow evidence from an earlier play or a later snapshot.
	b := r.before
	if b == nil || b.NativeProvider != "ytmusic_android" || b.Error != "" || !slices.Contains([]string{"playing", "paused"}, b.Phase) || b.Stage != "confirmed" || b.ActiveSource != "native_music" || b.BindingID == "" || b.BindingID != x.BindingID || e.BeforeBindingID != b.BindingID || e.BeforeMediaID != b.MediaID || e.BeforeTitle != b.Title || e.BeforeArtist != b.Artist || e.ExpectedMediaID != "" {
		return false
	}
	switch action {
	case "next", "previous":
		return (x.Phase == "playing" && x.PlaybackConfirmed || x.Phase == "paused" && !x.PlaybackConfirmed) && musicidentity.MetadataChanged(b.Title, b.Artist, x.Title, x.Artist)
	case "resume":
		return x.Phase == "playing" && x.PlaybackConfirmed && musicidentity.MatchMetadata(b.Title, b.Artist, x.Title, x.Artist)
	case "seek":
		return x.Phase == "playing" && x.PlaybackConfirmed && x.PositionMS != nil && cmd.PositionMS != nil && absMediaPosition(*x.PositionMS-*cmd.PositionMS) <= 3000 && musicidentity.MatchMetadata(b.Title, b.Artist, x.Title, x.Artist)
	}
	return false
}
