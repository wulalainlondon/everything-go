package core

import (
	"context"
	"encoding/base64"
	"everything-go/internal/sessionmedia"
	"image"
	"io"
	"testing"
)

func TestQAR2TransientDecodeBusyMustNotCommitUnrepairableCatalogEntry(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	image.RegisterFormat("png", "QAR2BLOCK", func(io.Reader) (image.Image, error) {
		close(started)
		<-finish
		return image.NewRGBA(image.Rect(0, 0, 1, 1)), nil
	}, func(io.Reader) (image.Config, error) { return image.Config{Width: 1, Height: 1}, nil })
	holder := sessionmedia.New(t.TempDir())
	defer holder.Close()
	done := make(chan struct{})
	go func() { defer close(done); holder.Put([]byte("QAR2BLOCK")) }()
	defer func() { close(finish); <-done }()
	<-started
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	s := h.registry.Create("qa-r2-busy", "Fixture", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	row := scopedHistoryRow("physical", 1)
	row["images"] = []map[string]string{{"data": base64.StdEncoding.EncodeToString(mediaPNG(t)), "media_type": "image/png"}}
	provider := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"physical": {row}}, generation: map[string]string{"physical": "g"}}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: provider})
	binding, e := h.galleryBinding(s)
	if e != nil {
		t.Fatal(e)
	}
	err := h.joinGalleryAdvance(context.Background(), s, binding, func() bool { return true })
	c, items, _, _, e := h.sessionGallery().store.View(s.ID, binding.key, "all", "", 60)
	if e != nil {
		t.Fatal(e)
	}
	if err == nil && c.Started && c.Complete && !c.MoreAfter && len(items) == 1 && items[0].Availability != "available" {
		t.Fatalf("transient decoder busy consumed source checkpoint and advertised complete, entry availability=%s, dirty witness committed=%t", items[0].Availability, c.HistoryRevision == binding.revision)
	}
}
