package mediaservice

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"time"
)

func RunCLI(args []string, out, stderr io.Writer) int {
	f := flag.NewFlagSet("bridge-media", flag.ContinueOnError)
	f.SetOutput(stderr)
	manifest := f.String("manifest", DefaultManifest(), "Installed media service manifest")
	id := f.String("id", "", "Unique vop_ intent or query ID")
	method := f.String("method", "status", "play_youtube, play_music, control, music_control, voice_volume, media_volume or status")
	source := f.String("source", "", "Exact youtube or native_music source")
	revision := f.Uint64("expected-revision", 0, "Observed native media-session revision")
	adopt := f.Bool("adopt-current", false, "Explicit user consent to pause the fresh identified foreign item")
	expected := f.String("expected-media-id", "", "Exact fresh foreign item ID; requires explicit adoption")
	operation := f.String("operation", "", "Original operation for status")
	url := f.String("url", "", "Verified HTTPS video or selected Music service song URL")
	title := f.String("title", "", "Exact native Music song title only, without artist prefix; YouTube retains its title label")
	label := f.String("label", "", "Display label only; never used for native title/artist/item matching")
	query := f.String("query", "", "Official Music search query")
	artist := f.String("artist", "", "Exact native Music artist; do not guess localized aliases")
	position := f.Int64("position-ms", -1, "Music seek position")
	action := f.String("action", "", "pause, resume, stop, next, previous, seek or volume (provider-dependent)")
	volume := f.Int("volume", -1, "Player, assistant or explicitly requested phone media volume 0..100")
	delta := f.Int("delta", 0, "Assistant gain or explicit phone media volume change in percentage points")
	failure := func(e error) int { json.NewEncoder(out).Encode(Reply{Error: e.Error()}); return 1 }
	if f.Parse(args) != nil || f.NArg() != 0 {
		return 2
	}
	thread := os.Getenv("CODEX_THREAD_ID")
	session := os.Getenv("CODEX_SESSION_ID")
	if thread == "" || session != "" && session != thread {
		return failure(errors.New("origin_context_unavailable"))
	}
	q := Request{ID: *id, Thread: thread, Method: *method, Operation: *operation}
	q.Input.Source, q.Input.AdoptCurrent, q.Input.ExpectedMediaID = *source, *adopt, *expected
	q.Input.URL = *url
	q.Input.Title = *title
	q.Input.DisplayLabel = *label
	q.Input.Action = *action
	q.Input.Query = *query
	q.Input.Artist = *artist
	f.Visit(func(v *flag.Flag) {
		if v.Name == "expected-revision" {
			q.Input.ExpectedRevision = revision
		}
		if v.Name == "delta" {
			q.Input.Delta = delta
		}
		if v.Name == "position-ms" {
			q.Input.PositionMS = position
		}
	})
	provided := false
	f.Visit(func(v *flag.Flag) {
		if v.Name == "volume" {
			provided = true
		}
	})
	if provided {
		if *volume < 0 || *volume > 100 {
			return failure(errors.New("media_volume_invalid"))
		}
		q.Input.Volume = volume
	}
	if !IntentID.MatchString(q.ID) || q.Operation != "" && !IntentID.MatchString(q.Operation) {
		return failure(errors.New("media_intent_invalid"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	r, e := Send(ctx, *manifest, q)
	if e != nil {
		return failure(e)
	}
	json.NewEncoder(out).Encode(r)
	return 0
}
