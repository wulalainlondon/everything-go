package core

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Explicit opt-in loopback fixture, using only generated test data and a fake
// executor. It cannot send to a model, read live profiles or run native work.
func TestSessionGalleryBrowserFixture(t *testing.T) {
	info := os.Getenv("BRIDGE_GALLERY_BROWSER_FIXTURE")
	if info == "" {
		t.Skip("opt-in browser fixture")
	}
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	t.Cleanup(func() { h.sessionGallery().store.Close() })
	if e := h.pairing.Claim("synthetic-gallery-browser-token", "gallery-browser-phone"); e != nil {
		t.Fatal(e)
	}
	s := h.registry.Create("gallery-browser-session", "Synthetic Media", t.TempDir(), "codex", "", "read-only", "")
	s.SetResumeID("browser-thread")
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{}, generation: map[string]string{"browser-thread": "browser-generation"}}
	palette := []color.RGBA{{183, 104, 84, 255}, {81, 131, 145, 255}, {161, 147, 97, 255}, {121, 116, 158, 255}}
	for i := 1; i <= 280; i++ {
		row := scopedHistoryRow("browser-thread", i)
		row["content"] = "Original synthetic conversation context"
		row["timestamp"] = int64(1794438000000 + i*900000)
		if i%3 == 0 {
			img := image.NewRGBA(image.Rect(0, 0, 480, 640))
			c := palette[(i/3)%len(palette)]
			for y := 0; y < 640; y++ {
				for x := 0; x < 480; x++ {
					shade := c
					if x > 60 && x < 420 && y > 130 && y < 510 {
						shade = color.RGBA{c.R / 2, c.G / 2, c.B / 2, 255}
					}
					img.SetRGBA(x, y, shade)
				}
			}
			var b bytes.Buffer
			if e := png.Encode(&b, img); e != nil {
				t.Fatal(e)
			}
			if i%6 == 0 {
				path := filepath.Join(h.cfg.DataDir, ".codex", "generated_images", "browser-thread", strconv.Itoa(i)+".png")
				if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(path, b.Bytes(), 0600); e != nil {
					t.Fatal(e)
				}
				row["role"] = "assistant"
				row["request_id"] = "synthetic-generated-" + strconv.Itoa(i)
				row["generated_image_paths"] = []string{path}
			} else {
				row["images"] = []map[string]string{{"data": base64.StdEncoding.EncodeToString(b.Bytes()), "media_type": "image/png"}}
			}
		}
		p.byID["browser-thread"] = append(p.byID["browser-thread"], row)
	}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	closed := make(chan struct{}, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/jarvis/v1/", h.ServeJarvisAPI)
	mux.HandleFunc("/ws", h.ServeWS)
	mux.HandleFunc("/fixture/close", func(w http.ResponseWriter, r *http.Request) {
		select {
		case closed <- struct{}{}:
		default:
		}
		w.WriteHeader(204)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	raw, _ := json.Marshal(map[string]string{"url": server.URL, "authority": "i1", "session": s.ID, "thread": "browser-thread"})
	if e := os.WriteFile(info, raw, 0600); e != nil {
		t.Fatal(e)
	}
	select {
	case <-closed:
	case <-time.After(15 * time.Minute):
		t.Fatal("fixture was not closed")
	}
}
