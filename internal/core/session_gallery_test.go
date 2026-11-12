package core

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"everything-go/internal/sessionmedia"
)

func TestSessionGalleryFullSparseHistoryAndRead(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	t.Cleanup(func() { h.sessionGallery().store.Close() })
	if e := h.pairing.Claim("gallery-fixture-token", "gallery-phone"); e != nil {
		t.Fatal(e)
	}
	s := h.registry.Create("gallery-session", "Synthetic gallery", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	png := mediaPNG(t)
	path := filepath.Join(t.TempDir(), ".codex", "generated_images", "physical", "original.png")
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(path, png, 0600); e != nil {
		t.Fatal(e)
	}
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{}, generation: map[string]string{"physical": "g1"}}
	for i := 1; i <= 355; i++ {
		row := scopedHistoryRow("physical", i)
		row["timestamp"] = int64(1000 + i)
		if i == 2 || i == 3 {
			row["images"] = []map[string]string{{"data": base64.StdEncoding.EncodeToString(png), "media_type": "image/png"}}
		}
		if i == 350 {
			row["role"] = "assistant"
			row["request_id"] = "original-request"
			row["generated_image_paths"] = []string{path}
		}
		p.byID["physical"] = append(p.byID["physical"], row)
	}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	get := func(operation string, q url.Values) *httptest.ResponseRecorder {
		q.Set("session_id", s.ID)
		r := httptest.NewRequest("GET", "/api/jarvis/v1/media/gallery/"+operation+"?"+q.Encode(), nil)
		r.Header.Set("Authorization", "Bearer gallery-fixture-token")
		r.Header.Set("X-Bridge-Device-ID", "gallery-phone")
		r.Header.Set("Origin", "capacitor://localhost")
		out := httptest.NewRecorder()
		h.ServeJarvisAPI(out, r)
		return out
	}
	type page struct {
		Items    []sessionmedia.Entry `json:"items"`
		Cursor   string               `json:"next_cursor"`
		Snapshot string               `json:"snapshot"`
		Coverage string               `json:"coverage"`
		Count    int                  `json:"count"`
	}
	cursor := ""
	found := []sessionmedia.Entry{}
	snapshot := ""
	for i := 0; i < 10; i++ {
		out := get("list", url.Values{"cursor": {cursor}, "filter": {"all"}})
		if out.Code != 200 {
			t.Fatalf("list %d %s", out.Code, out.Body.String())
		}
		var page page
		if e := json.Unmarshal(out.Body.Bytes(), &page); e != nil {
			t.Fatal(e)
		}
		found = append(found, page.Items...)
		cursor = page.Cursor
		snapshot = page.Snapshot
		if cursor == "" {
			if page.Coverage != "complete" || page.Count != 3 {
				t.Fatalf("coverage %+v", page)
			}
			break
		}
	}
	if len(found) != 3 || cursor != "" || found[0].Provenance != "generated" || found[1].Message == found[2].Message {
		t.Fatalf("lost/repeated sparse historical images %+v", found)
	}
	if got := h.attachments.ListSession(s.ID, 50); len(got) != 0 {
		t.Fatal("read emitted media events")
	}
	// Delete the source after indexing: the captured exact original survives.
	_ = os.Remove(path)
	for _, entry := range found {
		for _, op := range []string{"thumbnail", "original", "message"} {
			out := get(op, url.Values{"id": {entry.ID}, "snapshot": {snapshot}})
			if out.Code != 200 {
				t.Fatalf("%s %d %s", op, out.Code, out.Body.String())
			}
			if op == "original" && !bytes.Equal(out.Body.Bytes(), png) {
				t.Fatal("wrong original bytes")
			}
			if op == "thumbnail" && out.Header().Get("Access-Control-Expose-Headers") == "" {
				t.Fatal("native CORS hash header hidden")
			}
		}
	}
	out := get("original", url.Values{"id": {found[0].ID}, "snapshot": {"foreign"}})
	if out.Code != 409 {
		t.Fatal("foreign snapshot accepted")
	}
	// Replace the physical source generation; neither old bytes nor cursor can leak.
	p.generation["physical"] = "g2"
	out = get("original", url.Values{"id": {found[0].ID}, "snapshot": {snapshot}})
	if out.Code != 409 {
		t.Fatal("replaced source retained original grant")
	}
}

func TestSessionGalleryRejectsForeignPathsAndRequestlessGeneration(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	defer h.sessionGallery().store.Close()
	s := h.registry.Create("a", "Fixture", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("physical")
	for _, row := range []map[string]any{
		{"role": "assistant", "source_message_id": "codex:physical:line:1", "source_session_id": "physical", "generated_image_paths": []string{"/tmp/.codex/generated_images/physical/image.png"}},
		{"role": "assistant", "source_message_id": "codex:physical:line:1", "source_session_id": "physical", "request_id": "r1", "generated_image_paths": []string{"/tmp/.codex/generated_images/foreign/image.png"}},
		{"role": "user", "source_message_id": "codex:foreign:line:1", "source_session_id": "foreign", "images": []map[string]string{{"data": base64.StdEncoding.EncodeToString(mediaPNG(t))}}},
	} {
		if entries := h.galleryRecords(s, []string{"physical"}, row); len(entries) > 0 {
			t.Fatalf("foreign/unbound evidence admitted: %+v", entries)
		}
	}
	if _, e := h.galleryBinding(s); e == nil {
		t.Fatal("unsupported provider silently complete")
	}
}

func TestSessionGalleryMarkdownCodeAndOriginalCwd(t *testing.T) {
	content := "```markdown\n![example](/secret/example.png)\n```\n`![inline](/secret/inline.png)`\n    ![indented](/secret/indent.png)\n![actual](/original/a%20b.png)\n![remote](https://foreign/image.png)\n![relative](images/picture.png)"
	paths := galleryMarkdownPaths(content, "")
	if len(paths) != 2 || paths[0] != "/original/a b.png" || paths[1] != "images/picture.png" {
		t.Fatalf("literal code or current cwd became source authority: %+v", paths)
	}
	paths = galleryMarkdownPaths("![relative](images/picture.png)", "/original-cwd")
	if len(paths) != 1 || paths[0] != "/original-cwd/images/picture.png" {
		t.Fatal("original cwd not honored", paths)
	}
}
