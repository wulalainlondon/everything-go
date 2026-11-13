package core

import (
	"context"
	"encoding/base64"
	"everything-go/internal/sessionmedia"
	"image"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestQAR2LastSubscriberCancellationMustStopNextCaptureWithinSameNativeRow(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	image.RegisterFormat("png", "QAR2HOLDER", func(io.Reader) (image.Image, error) {
		close(started)
		<-finish
		return image.NewRGBA(image.Rect(0, 0, 1, 1)), nil
	}, func(io.Reader) (image.Config, error) { return image.Config{Width: 1, Height: 1}, nil })
	var decodes atomic.Int32
	image.RegisterFormat("png", "QAR2COUNT", func(io.Reader) (image.Image, error) {
		decodes.Add(1)
		return image.NewRGBA(image.Rect(0, 0, 1, 1)), nil
	}, func(io.Reader) (image.Config, error) { return image.Config{Width: 1, Height: 1}, nil })
	holder := sessionmedia.New(t.TempDir())
	defer holder.Close()
	heldDone := make(chan struct{})
	go func() { defer close(heldDone); holder.Put([]byte("QAR2HOLDER")) }()
	<-started
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	s := h.registry.Create("qa-r2-cancel", "Fixture", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	row := scopedHistoryRow("physical", 1)
	row["images"] = []map[string]string{{"data": base64.StdEncoding.EncodeToString([]byte("QAR2COUNT1")), "media_type": "image/png"}, {"data": base64.StdEncoding.EncodeToString([]byte("QAR2COUNT2")), "media_type": "image/png"}}
	rowReady := make(chan struct{})
	provider := &historyRangeFixtureProvider{afterLoad: func() { close(rowReady) }, byID: map[string][]map[string]any{"physical": {row}}, generation: map[string]string{"physical": "g"}}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: provider})
	binding, e := h.galleryBinding(s)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- h.joinGalleryAdvance(ctx, s, binding, func() bool { return ctx.Err() == nil }) }()
	<-rowReady
	g := h.sessionGallery()
	var job *galleryScanJob
	g.jobsMu.Lock()
	for _, v := range g.jobs {
		job = v
	}
	g.jobsMu.Unlock()
	if job == nil {
		close(finish)
		<-heldDone
		t.Fatal("worker not ready")
	}
	// Provider has returned its single row; the first capture is waiting on the held shared lane.
	time.Sleep(30 * time.Millisecond)
	cancel()
	if e = <-result; e == nil {
		t.Fatal("fixture cancellation failed")
	}
	close(finish)
	<-heldDone
	select {
	case <-job.done:
	case <-time.After(time.Second):
		t.Fatal("retired worker did not finish")
	}
	if n := decodes.Load(); n > 1 {
		t.Fatalf("last subscriber gone but %d source images decoded; second same-row capture started after cancellation", n)
	}
}
