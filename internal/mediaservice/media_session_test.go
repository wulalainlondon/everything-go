package mediaservice

import (
	"bytes"
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestMediaSessionHistoricalNestedFlagsAndOriginalObserver(t *testing.T) {
	root := t.TempDir()
	s, e := Open(root)
	if e != nil {
		t.Fatal(e)
	}
	snapshot := &protocol.MediaSessionState{Kind: "receipt", Revision: 1, Version: 1, ObservationAgeMS: 0, ActionConfirmed: true, PlaybackConfirmed: true}
	old, e := s.Execute(context.Background(), testOrigin(), testRequest(), func() (backend.VoiceMediaResult, error) {
		return backend.VoiceMediaResult{CommandID: "original-youtube", Provider: "call_media_session", State: "playing", PlaybackConfirmed: true, MediaSession: snapshot, MediaSessionReceipt: snapshot}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	var wrong atomic.Int32
	q := testRequest()
	q.ID = "vop_new_query"
	q.Method = "status"
	q.Operation = testRequest().ID
	result, e := s.Execute(context.Background(), testOrigin(), q, func() (backend.VoiceMediaResult, error) { wrong.Add(1); return backend.VoiceMediaResult{}, nil }, func(original backend.VoiceMediaResult) (backend.VoiceMediaResult, error) {
		if original.CommandID != old.CommandID {
			t.Fatal(original)
		}
		return backend.VoiceMediaResult{CommandID: original.CommandID, Provider: original.Provider, State: "stopped"}, nil
	})
	if e != nil || wrong.Load() != 0 || result.CommandID != old.CommandID || result.State != "stopped" {
		t.Fatal(result, e, wrong.Load())
	}
	s.Close()
	s, e = Open(root)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	again, e := s.Execute(context.Background(), testOrigin(), testRequest(), func() (backend.VoiceMediaResult, error) {
		t.Fatal("restart replay")
		return backend.VoiceMediaResult{}, nil
	})
	if e != nil || !again.Historical || again.PlaybackConfirmed {
		t.Fatal(again, e)
	}
	h := historical(old)
	if h.MediaSession.ActionConfirmed || h.MediaSession.PlaybackConfirmed || h.MediaSessionReceipt.ActionConfirmed || h.MediaSessionReceipt.ObservationAgeMS != -1 || !snapshot.ActionConfirmed {
		t.Fatal("stale nested flags or aliased mutation", h)
	}
}
func TestMediaSessionCLIParsesBoundSourceSeekAndExplicitAdoption(t *testing.T) {
	t.Setenv("CODEX_THREAD_ID", "test-thread")
	t.Setenv("CODEX_SESSION_ID", "test-thread")
	manifest := filepath.Join(t.TempDir(), "private", "manifest.json")
	seen := make(chan Request, 1)
	server, err := OpenServer(t.TempDir(), "instance-original", manifest, func(ctx context.Context, q Request) (Result, error) {
		seen <- q
		return decorate(backend.VoiceMediaResult{State: "queued"}, q.ID, true, true), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	var out, stderr bytes.Buffer
	code := RunCLI([]string{"--manifest", manifest, "--id", "vop_original_seek", "--method", "control", "--source", "native_music", "--action", "seek", "--position-ms", "1234", "--expected-revision", "0", "--adopt-current=true", "--expected-media-id", "explicit-foreign"}, &out, &stderr)
	if code != 0 {
		t.Fatal(code, out.String(), stderr.String())
	}
	q := <-seen
	if q.Thread != "test-thread" || q.Input.Source != "native_music" || q.Input.PositionMS == nil || *q.Input.PositionMS != 1234 || q.Input.ExpectedRevision == nil || *q.Input.ExpectedRevision != 0 || !q.Input.AdoptCurrent || q.Input.ExpectedMediaID != "explicit-foreign" {
		t.Fatal(q)
	}
}
