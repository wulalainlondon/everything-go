package core

import (
	"context"
	"encoding/base64"
	"errors"
	"everything-go/internal/sessionmedia"
	"image"
	"io"
	"sync"
	"testing"
	"time"
)

func qaR3Hold(t *testing.T, magic string) func() {
	t.Helper()
	started, finish := make(chan struct{}), make(chan struct{})
	image.RegisterFormat("png", magic, func(io.Reader) (image.Image, error) {
		close(started)
		<-finish
		return image.NewRGBA(image.Rect(0, 0, 1, 1)), nil
	}, func(io.Reader) (image.Config, error) { return image.Config{Width: 1, Height: 1}, nil })
	holder := sessionmedia.New(t.TempDir())
	done := make(chan struct{})
	go func() { defer close(done); holder.Put([]byte(magic)) }()
	<-started
	var once sync.Once
	release := func() { once.Do(func() { close(finish); <-done; holder.Close() }) }
	t.Cleanup(release)
	return release
}
func TestQAR3BusyRecoversSameCheckpointWithoutLosingImage(t *testing.T) {
	release := qaR3Hold(t, "QA3BLOCKRECOVERY")
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	s := h.registry.Create("qa-r3-recovery", "Fixture", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	png := mediaPNG(t)
	row := scopedHistoryRow("physical", 1)
	row["images"] = []map[string]string{{"data": base64.StdEncoding.EncodeToString(png), "media_type": "image/png"}}
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"physical": {row}}, generation: map[string]string{"physical": "g"}}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	b, e := h.galleryBinding(s)
	if e != nil {
		t.Fatal(e)
	}
	before, _ := h.sessionGallery().store.Load(s.ID, b.key)
	e = h.joinGalleryAdvance(context.Background(), s, b, func() bool { return true })
	if !errors.Is(e, sessionmedia.ErrResourceBusy) {
		t.Fatalf("expected typed busy, got %v", e)
	}
	after, items, _, _, e := h.sessionGallery().store.View(s.ID, b.key, "all", "", 60)
	if e != nil || after.Epoch != before.Epoch || after.Revision != before.Revision || after.Started || len(items) != 0 {
		t.Fatalf("busy moved checkpoint %+v %+v %v", after, items, e)
	}
	release()
	if e = h.joinGalleryAdvance(context.Background(), s, b, func() bool { return true }); e != nil {
		t.Fatal("original checkpoint recovery failed", e)
	}
	after, items, _, _, e = h.sessionGallery().store.View(s.ID, b.key, "all", "", 60)
	if e != nil || after.Epoch != before.Epoch || !after.Complete || len(items) != 1 || items[0].Availability != "available" || items[0].SHA256 != sessionmedia.IDBytes(png) {
		t.Fatalf("image not recovered %+v %+v %v", after, items, e)
	}
}
func TestQAR3CancelFirstWhileDecodeQueuedKeepsSecondLegalSubscriber(t *testing.T) {
	release := qaR3Hold(t, "QA3BLOCKSURVIVOR")
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	s := h.registry.Create("qa-r3-survivor", "Fixture", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	row := scopedHistoryRow("physical", 1)
	row["images"] = []map[string]string{{"data": base64.StdEncoding.EncodeToString(mediaPNG(t)), "media_type": "image/png"}}
	rowReady := make(chan struct{})
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"physical": {row}}, generation: map[string]string{"physical": "g"}, afterLoad: func() { close(rowReady) }}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	b, e := h.galleryBinding(s)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	one, two := make(chan error, 1), make(chan error, 1)
	go func() { one <- h.joinGalleryAdvance(ctx, s, b, func() bool { return ctx.Err() == nil }) }()
	<-rowReady
	go func() { two <- h.joinGalleryAdvance(context.Background(), s, b, func() bool { return true }) }()
	g := h.sessionGallery()
	deadline := time.Now().Add(time.Second)
	for {
		g.jobsMu.Lock()
		n := g.subscribers
		g.jobsMu.Unlock()
		if n == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second not registered")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if e = <-one; e == nil {
		t.Fatal("first did not cancel")
	}
	release()
	select {
	case e = <-two:
		if e != nil {
			t.Fatal("surviving legal subscriber killed", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("store/job guard lock stalled")
	}
	_, items, _, _, e := g.store.View(s.ID, b.key, "all", "", 60)
	if e != nil || len(items) != 1 || items[0].Availability != "available" {
		t.Fatal("survivor did not publish lawful image", items, e)
	}
}
