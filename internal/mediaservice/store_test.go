package mediaservice

import (
	"context"
	"errors"
	"everything-go/internal/backend"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testOrigin() backend.MediaOrigin {
	return backend.MediaOrigin{SessionID: "s", ThreadID: "t", TurnID: "turn", VoiceID: "voice", Generation: "gen"}
}
func testRequest() Request {
	return Request{ID: "vop_one_operation", Thread: "t", Method: "play_youtube", Input: backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}}
}
func TestReceiptSurvivesRestartAndDoesNotReplayUnknown(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	var called atomic.Int32
	r, e := s.Execute(context.Background(), testOrigin(), testRequest(), func() (backend.VoiceMediaResult, error) {
		called.Add(1)
		return backend.VoiceMediaResult{}, errors.New("lost_after_send")
	})
	if e == nil || r.State != "unknown" || r.Dispatched != nil {
		t.Fatal(r, e)
	}
	s.Close()
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	r, e = s.Execute(context.Background(), testOrigin(), testRequest(), func() (backend.VoiceMediaResult, error) {
		called.Add(1)
		return backend.VoiceMediaResult{State: "playing"}, nil
	})
	if e != nil || r.State != "unknown" || called.Load() != 1 {
		t.Fatal(r, e, called.Load())
	}
}
func TestConcurrentDuplicateIntentHasOneEffectAndConflictFails(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var count atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := s.Execute(context.Background(), testOrigin(), testRequest(), func() (backend.VoiceMediaResult, error) {
				count.Add(1)
				return backend.VoiceMediaResult{CommandID: "c", State: "queued"}, nil
			})
			if e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if count.Load() != 1 {
		t.Fatal(count.Load())
	}
	q := testRequest()
	q.Input.URL = "https://youtu.be/abcdefghijk"
	if _, e = s.Execute(context.Background(), testOrigin(), q, func() (backend.VoiceMediaResult, error) {
		t.Fatal("conflict effect")
		return backend.VoiceMediaResult{}, nil
	}); e == nil {
		t.Fatal("no conflict")
	}
}
func TestOperationStatusCannotRefreshFromAnotherCommandOrVoice(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	o := testOrigin()
	q := testRequest()
	_, e = s.Execute(context.Background(), o, q, func() (backend.VoiceMediaResult, error) {
		return backend.VoiceMediaResult{CommandID: "original", State: "playing", Observed: true, PlaybackConfirmed: true, LastObservedMS: time.Now().UnixMilli()}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	q.Method = "status"
	q.Operation = q.ID
	q.ID = "vop_status_query"
	q.Input = backend.VoiceMediaRequest{}
	r, e := s.Execute(context.Background(), o, q, func() (backend.VoiceMediaResult, error) {
		return backend.VoiceMediaResult{CommandID: "newer", State: "playing"}, nil
	})
	if e != nil || r.CommandID != "original" {
		t.Fatal(r, e)
	}
	o.Generation = "other-generation"
	r, e = s.Execute(context.Background(), o, q, func() (backend.VoiceMediaResult, error) {
		t.Fatal("old operation targeted new voice")
		return backend.VoiceMediaResult{}, nil
	})
	if e != nil || r.State != "expired" || r.PlaybackConfirmed {
		t.Fatal(r, e)
	}
}
