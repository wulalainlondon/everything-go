package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"everything-go/internal/sessionmedia"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestQAHiddenABACannotCommitOldWorker(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	s := h.registry.Create("qa-aba", "Fixture", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	started, finish := make(chan struct{}, 1), make(chan struct{})
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"physical": {scopedHistoryRow("physical", 1)}}, generation: map[string]string{"physical": "g"}, afterLoad: func() { started <- struct{}{}; <-finish }}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	b, e := h.galleryBinding(s)
	if e != nil {
		t.Fatal(e)
	}
	valid := func() bool {
		snap := s.Snapshot()
		now, e := h.galleryBinding(s)
		return e == nil && !snap.Hidden && now.key == b.key
	}
	done := make(chan error, 1)
	go func() { done <- h.joinGalleryAdvance(context.Background(), s, b, valid) }()
	<-started
	before := s.Snapshot().SourceRevision
	yes, no := true, false
	s.SetMeta(nil, &yes)
	s.SetMeta(nil, &no)
	if s.Snapshot().SourceRevision == before {
		t.Fatal("fixture failed source ABA")
	}
	close(finish)
	if e = <-done; e == nil {
		t.Fatal("old worker committed after hidden A-to-B-to-A with newer source revision")
	}
}
func TestQAQueuedStaleImageDoesNotResetNewCatalog(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	h.pairing.Claim("qa-token", "qa-device")
	s := h.registry.Create("qa-queued", "Fixture", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"physical": {scopedHistoryRow("physical", 1)}}, generation: map[string]string{"physical": "g"}}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	a, _ := h.galleryBinding(s)
	g := h.sessionGallery()
	ca, _ := g.store.Load(s.ID, a.key)
	ca.Started = true
	g.store.Commit(s.ID, ca, nil)
	release, e := g.io.acquire(context.Background(), "holder", 1)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	query := url.Values{"session_id": {s.ID}, "snapshot": {ca.Epoch}, "id": {strings.Repeat("a", 64)}}
	r := httptest.NewRequest("GET", "/api/jarvis/v1/media/gallery/thumbnail?"+query.Encode(), nil)
	r.Header.Set("Authorization", "Bearer qa-token")
	r.Header.Set("X-Bridge-Device-ID", "qa-device")
	r.Header.Set("Origin", "capacitor://localhost")
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { h.ServeJarvisAPI(w, r); close(done) }()
	deadline := time.Now().Add(time.Second)
	for {
		g.io.mu.Lock()
		n := len(g.io.queue)
		g.io.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not queued")
		}
		time.Sleep(time.Millisecond)
	}
	p.generation["physical"] = "g2"
	b, _ := h.galleryBinding(s)
	cb, _ := g.store.Load(s.ID, b.key)
	cb.Started = true
	g.store.Commit(s.ID, cb, nil)
	release()
	<-done
	after, _, _, _, e := g.store.View(s.ID, b.key, "all", "", 1)
	if e != nil {
		t.Fatal(e)
	}
	if after.Epoch != cb.Epoch {
		t.Fatalf("rejected stale request reset lawful catalog: HTTP=%d oldEpoch=%s newEpoch=%s", w.Code, cb.Epoch, after.Epoch)
	}
}
func TestQACompleteStatusMustDiscoverSameGenerationAppend(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	h.pairing.Claim("qa-token", "qa-device")
	s := h.registry.Create("qa-append", "Fixture", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	row := scopedHistoryRow("physical", 1)
	row["images"] = []map[string]string{{"data": base64.StdEncoding.EncodeToString(mediaPNG(t)), "media_type": "image/png"}}
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"physical": {row}}, generation: map[string]string{"physical": "g"}}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	b, _ := h.galleryBinding(s)
	if e := h.joinGalleryAdvance(context.Background(), s, b, func() bool { return true }); e != nil {
		t.Fatal(e)
	}
	row2 := scopedHistoryRow("physical", 2)
	row2["images"] = row["images"]
	p.byID["physical"] = append(p.byID["physical"], row2)
	r := httptest.NewRequest("GET", "/api/jarvis/v1/media/gallery/status?session_id="+s.ID, nil)
	r.Header.Set("Authorization", "Bearer qa-token")
	r.Header.Set("X-Bridge-Device-ID", "qa-device")
	r.Header.Set("Origin", "capacitor://localhost")
	w := httptest.NewRecorder()
	h.ServeJarvisAPI(w, r)
	var result struct {
		Count    int    `json:"count"`
		Dirty    bool   `json:"dirty"`
		Coverage string `json:"coverage"`
	}
	json.Unmarshal(w.Body.Bytes(), &result)
	if w.Code == 200 && result.Count == 1 && result.Coverage == "complete" && !result.Dirty {
		t.Fatal("new append invisible: status remains complete count=1 dirty=false; controller never advances")
	}
}
func TestQAAdmissionAgedBackgroundMustGetNonReservedSlot(t *testing.T) {
	a := galleryIOAdmission{active: 1, queue: []*galleryIOWaiter{{binding: "other", priority: 2, at: time.Now().Add(-3 * time.Second), ready: make(chan struct{})}, {binding: "foreground", priority: 0, at: time.Now(), ready: make(chan struct{})}}}
	aged := a.queue[0]
	a.dispatch()
	if !aged.running {
		t.Fatal("aged background starved behind a fresh foreground even with second nonreserved slot")
	}
}

var _ = sessionmedia.ID
