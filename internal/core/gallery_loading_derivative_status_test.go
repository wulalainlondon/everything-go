package core

import (
	"context"
	"encoding/base64"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestQAR3DerivativeRetirementMustReturnAuthorityStatus(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	h.pairing.Claim("qa-r3-token", "qa-r3-device")
	s := h.registry.Create("qa-r3-derivative", "Fixture", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	row := scopedHistoryRow("physical", 1)
	row["images"] = []map[string]string{{"data": base64.StdEncoding.EncodeToString(mediaPNG(t)), "media_type": "image/png"}}
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"physical": {row}}, generation: map[string]string{"physical": "g"}}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	b, _ := h.galleryBinding(s)
	if e := h.joinGalleryAdvance(context.Background(), s, b, func() bool { return true }); e != nil {
		t.Fatal(e)
	}
	g := h.sessionGallery()
	c, items, _, _, e := g.store.View(s.ID, b.key, "all", "", 1)
	if e != nil || len(items) != 1 {
		t.Fatal(e, items)
	}
	release := qaR3Hold(t, "QA3BLOCKRETIRE")
	defer release()
	q := url.Values{"session_id": {s.ID}, "snapshot": {c.Epoch}, "id": {items[0].ID}}
	r := httptest.NewRequest("GET", "/api/jarvis/v1/media/gallery/preview?"+q.Encode(), nil)
	r.Header.Set("Authorization", "Bearer qa-r3-token")
	r.Header.Set("X-Bridge-Device-ID", "qa-r3-device")
	r.Header.Set("Origin", "capacitor://localhost")
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { h.ServeJarvisAPI(w, r); close(done) }()
	deadline := time.Now().Add(time.Second)
	for len(g.decode) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("preview did not reach decoder")
		}
		time.Sleep(time.Millisecond)
	}
	yes := true
	s.SetMeta(nil, &yes)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retirement not observed")
	}
	if w.Code != 403 && w.Code != 409 {
		t.Fatalf("known source retirement lost authority classification: HTTP%d body=%s", w.Code, w.Body.String())
	}
}

func TestGalleryDerivativeBusyMustRemainRetryable(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	h.pairing.Claim("qa-r3-token", "qa-r3-device")
	s := h.registry.Create("qa-r3-derivative", "Fixture", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	row := scopedHistoryRow("physical", 1)
	row["images"] = []map[string]string{{"data": base64.StdEncoding.EncodeToString(mediaPNG(t)), "media_type": "image/png"}}
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"physical": {row}}, generation: map[string]string{"physical": "g"}}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	b, _ := h.galleryBinding(s)
	if e := h.joinGalleryAdvance(context.Background(), s, b, func() bool { return true }); e != nil {
		t.Fatal(e)
	}
	g := h.sessionGallery()
	c, items, _, _, e := g.store.View(s.ID, b.key, "all", "", 1)
	if e != nil || len(items) != 1 {
		t.Fatal(e, items)
	}
	release := qaR3Hold(t, "QA3BLOCKBUSY")
	defer release()
	q := url.Values{"session_id": {s.ID}, "snapshot": {c.Epoch}, "id": {items[0].ID}}
	r := httptest.NewRequest("GET", "/api/jarvis/v1/media/gallery/preview?"+q.Encode(), nil)
	r.Header.Set("Authorization", "Bearer qa-r3-token")
	r.Header.Set("X-Bridge-Device-ID", "qa-r3-device")
	r.Header.Set("Origin", "capacitor://localhost")
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { h.ServeJarvisAPI(w, r); close(done) }()
	deadline := time.Now().Add(time.Second)
	for len(g.decode) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("preview did not reach decoder")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("resource busy was not bounded")
	}
	if w.Code != 503 || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("resource pressure lost retryable classification: HTTP%d body=%s", w.Code, w.Body.String())
	}
}
