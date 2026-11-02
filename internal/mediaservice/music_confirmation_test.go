package mediaservice

import (
	"bytes"
	"context"
	"everything-go/internal/backend"
	"path/filepath"
	"strings"
	"testing"
)

func TestMusicConfirmationCLIExactTitleArtistAndSeparateDisplayLabel(t *testing.T) {
	t.Setenv("CODEX_THREAD_ID", "synthetic-thread")
	t.Setenv("CODEX_SESSION_ID", "synthetic-thread")
	manifest := filepath.Join(t.TempDir(), "private", "manifest.json")
	seen := make(chan Request, 1)
	server, e := OpenServer(t.TempDir(), "synthetic-instance", manifest, func(ctx context.Context, q Request) (Result, error) {
		seen <- q
		return decorate(backend.VoiceMediaResult{State: "queued"}, q.ID, true, true), nil
	})
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close()
	var out, err bytes.Buffer
	if code := RunCLI([]string{"--manifest", manifest, "--id", "vop_synthetic_yellow", "--method", "play_music", "--url", "https://music.youtube.com/watch?v=yKNxeF4KMsY", "--title", "Yellow", "--artist", "Coldplay", "--label", "Coldplay - Yellow"}, &out, &err); code != 0 {
		t.Fatal(code, out.String(), err.String())
	}
	q := <-seen
	if q.Input.Title != "Yellow" || q.Input.Artist != "Coldplay" || q.Input.DisplayLabel != "Coldplay - Yellow" {
		t.Fatal(q)
	}
	out.Reset()
	err.Reset()
	RunCLI([]string{"--help"}, &out, &err)
	if !strings.Contains(err.String(), "Exact native Music song title only") || !strings.Contains(err.String(), "Display label only; never used for native") {
		t.Fatal("ambiguous CLI help", err.String())
	}
}
