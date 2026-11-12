package core

import (
	"encoding/base64"
	"encoding/json"
	"everything-go/internal/filetransfer"
	"everything-go/internal/media"
	"everything-go/internal/sessionmedia"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func TestSessionGalleryRegressionLiteralCodeNotAttachment(t *testing.T) {
	for name, content := range map[string]string{"blockquoteFence": "> ```markdown\n> ![example](/private/example.png)\n> ```", "htmlComment": "<!-- ![example](/private/example.png) -->", "tabCode": "\t![example](/private/example.png)"} {
		t.Run(name, func(t *testing.T) {
			if got := galleryMarkdownPaths(content, ""); len(got) != 0 {
				t.Fatalf("literal content admitted as image authority: %q", got)
			}
		})
	}
}
func TestSessionGalleryRegressionPrivateIndexRejectsLegacyRoute(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	c, e := h.sessionGallery().store.Load("private-session", "binding")
	if e != nil {
		t.Fatal(e)
	}
	if e = h.sessionGallery().store.Commit("private-session", c, nil); e != nil {
		t.Fatal(e)
	}
	index := filepath.Join(h.cfg.DataDir, "session-media", "index.sqlite")
	r := httptest.NewRequest("GET", "/media"+index, nil)
	w := httptest.NewRecorder()
	media.Handler(h.TypedMediaPathPrivate).ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("private index exposed via original unauthenticated legacy handler: status=%d bytes=%d", w.Code, w.Body.Len())
	}
}
func TestSessionGalleryRegressionDistinctMessageOccurrences(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	s := h.registry.Create("same-session", "Synthetic", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	path := filepath.Join(t.TempDir(), "image.png")
	if e := os.WriteFile(path, mediaPNG(t), 0600); e != nil {
		t.Fatal(e)
	}
	a := scopedHistoryRow("physical", 1)
	a["role"] = "assistant"
	a["request_id"] = "same-original-request"
	a["content"] = "![first](" + path + ")"
	b := scopedHistoryRow("physical", 2)
	b["role"] = "assistant"
	b["request_id"] = "same-original-request"
	b["content"] = "![again](" + path + ")"
	one := h.galleryRecords(s, []string{"physical"}, a)
	two := h.galleryRecords(s, []string{"physical"}, b)
	if len(one) != 1 || len(two) != 1 {
		t.Fatalf("fixture missing: %v %v", one, two)
	}
	if one[0].Entry.ID == two[0].Entry.ID {
		t.Fatal("two source messages collapse to one occurrence ID")
	}
}
func TestSessionGalleryRegressionAppendDuringBackfillNotComplete(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	if e := h.pairing.Claim("synthetic-qa-token", "synthetic-qa-phone"); e != nil {
		t.Fatal(e)
	}
	s := h.registry.Create("qa-session", "Synthetic", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{}, generation: map[string]string{"physical": "g1"}}
	for i := 1; i <= 200; i++ {
		p.byID["physical"] = append(p.byID["physical"], scopedHistoryRow("physical", i))
	}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	get := func(cursor string) struct {
		Coverage string `json:"coverage"`
		Count    int    `json:"count"`
		Cursor   string `json:"next_cursor"`
	} { q := url.Values{"session_id": {s.ID}, "filter": {"all"}, "cursor": {cursor}}; r := httptest.NewRequest("GET", "/api/jarvis/v1/media/gallery/list?"+q.Encode(), nil); r.Header.Set("Authorization", "Bearer synthetic-qa-token"); r.Header.Set("X-Bridge-Device-ID", "synthetic-qa-phone"); r.Header.Set("Origin", "capacitor://localhost"); w := httptest.NewRecorder(); h.ServeJarvisAPI(w, r); if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}; var v struct {
		Coverage string `json:"coverage"`
		Count    int    `json:"count"`
		Cursor   string `json:"next_cursor"`
	}; if e := json.Unmarshal(w.Body.Bytes(), &v); e != nil {
		t.Fatal(e)
	}; return v }
	first := get("")
	if first.Cursor == "" {
		t.Fatal("fixture did not need backfill")
	}
	appendRow := scopedHistoryRow("physical", 201)
	appendRow["images"] = []map[string]string{{"data": base64.StdEncoding.EncodeToString(mediaPNG(t)), "media_type": "image/png"}}
	p.byID["physical"] = append(p.byID["physical"], appendRow)
	last := get(first.Cursor)
	if last.Coverage == "complete" && last.Cursor == "" && last.Count == 0 {
		t.Fatal("gallery falsely complete and empty despite new historical image appended during backfill")
	}
	_ = sessionmedia.MaxImageBytes
}

func TestSessionGalleryRegressionPrivateIndexRejectsFilesRoute(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	c, e := h.sessionGallery().store.Load("private-session", "binding")
	if e != nil {
		t.Fatal(e)
	}
	if e = h.sessionGallery().store.Commit("private-session", c, nil); e != nil {
		t.Fatal(e)
	}
	service, e := filetransfer.NewService(h.cfg.DataDir, t.TempDir(), func(*http.Request) bool { return true })
	if e != nil {
		t.Fatal(e)
	}
	index := filepath.Join(h.cfg.DataDir, "session-media", "index.sqlite")
	r := httptest.NewRequest("GET", "/files?"+url.Values{"path": {index}}.Encode(), nil)
	w := httptest.NewRecorder()
	service.DownloadHandler().ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("private index bypassed session auth through original files handler with transport auth only: status=%d bytes=%d", w.Code, w.Body.Len())
	}
}
