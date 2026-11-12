package sessionmedia

import (
	"bytes"
	"image"
	"image/color"
	"image/gif"
	"os"
	"path/filepath"
	"testing"
)

func TestCaptureIncompleteImageDoesNotPinOriginal(t *testing.T) {
	s := New(t.TempDir())
	defer s.Close()
	full := testPNG(t, color.White)
	partial := full[:33]
	path := filepath.Join(t.TempDir(), "in-progress.png")
	if e := os.WriteFile(path, partial, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Capture("session", "request", path); e == nil {
		t.Fatal("incomplete header pinned")
	}
	if e := os.WriteFile(path, full, 0600); e != nil {
		t.Fatal(e)
	}
	hash, e := s.Capture("session", "request", path)
	if e != nil || hash != IDBytes(full) {
		t.Fatal("completed original cannot recover", e)
	}
	got, e := s.Read(hash)
	if e != nil || !bytes.Equal(got, full) {
		t.Fatal("wrong saved original", e)
	}
}
func TestStoreAnimatedGIFCompleteFrameBudget(t *testing.T) {
	s := New(t.TempDir())
	defer s.Close()
	frame := image.NewPaletted(image.Rect(0, 0, 2, 2), color.Palette{color.Black, color.White})
	var b bytes.Buffer
	if e := gif.EncodeAll(&b, &gif.GIF{Image: []*image.Paletted{frame, frame}, Delay: []int{2, 3}}); e != nil {
		t.Fatal(e)
	}
	full := b.Bytes()
	hash, e := s.Put(full)
	if e != nil {
		t.Fatal(e)
	}
	got, e := s.Read(hash)
	if e != nil || !bytes.Equal(got, full) {
		t.Fatal("animation bytes changed")
	}
	if _, e = s.Put(full[:len(full)-1]); e == nil {
		t.Fatal("incomplete GIF admitted")
	}
	bomb := append([]byte(nil), full...)
	for at := 13; at+9 < len(bomb); at++ {
		if bomb[at] == 0x2c {
			bomb[at+5] = 255
			bomb[at+6] = 255
			bomb[at+7] = 255
			bomb[at+8] = 255
			break
		}
	}
	if boundedGIF(bomb) {
		t.Fatal("frame allocation not bounded")
	}
}
