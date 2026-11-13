package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"everything-go/internal/sessionmedia"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestGalleryIndexedListStatusAdvanceAndFreshValidation(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	if e := h.pairing.Claim("gallery-loading-fixture", "gallery-loading-device"); e != nil {
		t.Fatal(e)
	}
	s := h.registry.Create("gallery-loading", "Fixture", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	png := mediaPNG(t)
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{}, generation: map[string]string{"physical": "g1"}}
	for i := 1; i <= 250; i++ {
		row := scopedHistoryRow("physical", i)
		if i == 5 {
			row["images"] = []map[string]string{{"data": base64.StdEncoding.EncodeToString(png), "media_type": "image/png"}}
		}
		p.byID["physical"] = append(p.byID["physical"], row)
	}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	req := func(op string, q url.Values, body string) *httptest.ResponseRecorder {
		q.Set("session_id", s.ID)
		method := "GET"
		if body != "" {
			method = "POST"
		}
		r := httptest.NewRequest(method, "/api/jarvis/v1/media/gallery/"+op+"?"+q.Encode(), strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer gallery-loading-fixture")
		r.Header.Set("X-Bridge-Device-ID", "gallery-loading-device")
		r.Header.Set("Origin", "capacitor://localhost")
		w := httptest.NewRecorder()
		h.ServeJarvisAPI(w, r)
		return w
	}
	var page struct {
		Items    []sessionmedia.Entry `json:"items"`
		Snapshot string               `json:"snapshot"`
		Coverage string               `json:"coverage"`
		Count    int                  `json:"count"`
	}
	read := func() *httptest.ResponseRecorder {
		return req("list", url.Values{"mode": {"indexed"}, "filter": {"all"}}, "")
	}
	w := read()
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	_ = json.Unmarshal(w.Body.Bytes(), &page)
	if p.loads != 0 || page.Coverage != "indexing" || page.Count != 0 {
		t.Fatalf("pure query scanned %+v loads%d", page, p.loads)
	}
	initial := page.Snapshot
	for i := 0; i < 5; i++ {
		w = req("advance", url.Values{}, "{}")
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		w = read()
		if e := json.Unmarshal(w.Body.Bytes(), &page); e != nil {
			t.Fatal(e)
		}
		if page.Snapshot != initial {
			t.Fatal("normal request end retired catalog epoch")
		}
		if page.Coverage == "complete" {
			break
		}
	}
	if len(page.Items) != 1 || page.Count != 1 || page.Coverage != "complete" {
		t.Fatalf("backfill failed %+v", page)
	}
	before := p.loads
	for i := 0; i < 20; i++ {
		filter := []string{"all", "user", "assistant"}[i%3]
		w = req("list", url.Values{"mode": {"indexed"}, "filter": {filter}}, "")
		if w.Code != 200 {
			t.Fatal("rapid filter", w.Code, w.Body.String())
		}
		w = req("status", url.Values{}, "")
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	if p.loads != before {
		t.Fatal("status/filter advanced history")
	}
	entry := page.Items[0]
	q := url.Values{"snapshot": {page.Snapshot}, "id": {entry.ID}, "sha256": {entry.SHA256}, "purpose": {"download"}}
	w = req("validate", q, "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	body, _ := json.Marshal(map[string]any{"items": []map[string]string{{"id": entry.ID, "sha256": entry.SHA256, "purpose": "thumbnail_display"}}})
	w = req("validate", url.Values{"snapshot": {page.Snapshot}}, string(body))
	if w.Code != 200 {
		t.Fatal("batch", w.Code, w.Body.String())
	}
	q.Set("sha256", strings.Repeat("f", 64))
	if w = req("validate", q, ""); w.Code != 409 {
		t.Fatal("cached revision accepted", w.Code)
	}
	q.Set("sha256", entry.SHA256)
	for _, op := range []string{"preview", "thumbnail", "original"} {
		w = req(op, q, "")
		if w.Code != 200 {
			t.Fatal(op, w.Code, w.Body.String())
		}
		if op == "original" && !bytes.Equal(w.Body.Bytes(), png) {
			t.Fatal("derivative substituted for original")
		}
	}
	p.generation["physical"] = "g2"
	if w = req("validate", q, ""); w.Code != 409 {
		t.Fatal("old snapshot accepted", w.Code)
	}
	if len(h.attachments.ListSession(s.ID, 50)) != 0 {
		t.Fatal("reading emitted media events")
	}
}

func TestGalleryAdvanceCoalescesAndFirstCancellationDoesNotBorrowOrKill(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	s := h.registry.Create("coalesce", "Fixture", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	started, finish := make(chan struct{}, 1), make(chan struct{})
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"physical": {scopedHistoryRow("physical", 1)}}, generation: map[string]string{"physical": "g"}, afterLoad: func() { started <- struct{}{}; <-finish }}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	b, e := h.galleryBinding(s)
	if e != nil {
		t.Fatal(e)
	}
	first, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, bresult := make(chan error, 1), make(chan error, 1)
	go func() { a <- h.joinGalleryAdvance(first, s, b, func() bool { return first.Err() == nil }) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start")
	}
	go func() { bresult <- h.joinGalleryAdvance(context.Background(), s, b, func() bool { return true }) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		g := h.sessionGallery()
		g.jobsMu.Lock()
		n := g.subscribers
		g.jobsMu.Unlock()
		if n == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second subscriber not registered")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if e = <-a; e == nil {
		t.Fatal("cancelled caller accepted result")
	}
	close(finish)
	select {
	case e = <-bresult:
		if e != nil {
			t.Fatal("first caller cancellation killed legal subscriber", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not finish")
	}
	c, e := h.sessionGallery().store.Load(s.ID, b.key)
	if e != nil || !c.Started || p.loads != 1 {
		t.Fatal("coalesced commit/read snapshot failed", c, p.loads, e)
	}
}

func (p *historyRangeFixtureProvider) HistorySourceRevision(id string) (string, error) {
	rows := p.byID[id]
	last := ""
	if len(rows) > 0 {
		last, _ = rows[len(rows)-1]["source_message_id"].(string)
	}
	return sessionmedia.ID(p.generation[id], last, strconv.Itoa(len(rows))), nil
}
