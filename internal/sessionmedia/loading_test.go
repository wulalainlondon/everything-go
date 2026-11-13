package sessionmedia

import (
	"image"
	"image/color"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadingCASReadViewAndRetiredABA(t *testing.T) {
	s := New(t.TempDir())
	defer s.Close()
	old, e := s.Load("session", "A")
	if e != nil {
		t.Fatal(e)
	}
	next := old
	next.Started = true
	next.Complete = true
	if e = s.CommitCAS("session", old, next, []Record{{Entry: Entry{ID: ID("one"), Author: "user", Sort: "1"}}}); e != nil {
		t.Fatal(e)
	}
	c, items, _, count, e := s.View("session", "A", "all", "", 60)
	if e != nil || c.Revision != 1 || count != 1 || len(items) != count || !c.Complete {
		t.Fatalf("inconsistent view %+v %d %d %v", c, len(items), count, e)
	}
	if e = s.CommitCAS("session", old, next, nil); e == nil {
		t.Fatal("stale checkpoint overwrite")
	}
	if _, e = s.Load("session", "B"); e != nil {
		t.Fatal(e)
	}
	renew, e := s.Load("session", "A")
	if e != nil || renew.Epoch == old.Epoch {
		t.Fatal("ABA revived epoch")
	}
	if e = s.CommitCAS("session", c, next, nil); e == nil {
		t.Fatal("retired worker committed into new catalog")
	}
}
func TestLoadingDerivativeManifestOriginalGuardAndTamper(t *testing.T) {
	s := New(t.TempDir())
	defer s.Close()
	s.free = func(string) (uint64, error) { return 4 << 30, nil }
	original := testPNG(t, color.White)
	hash, e := s.Put(original)
	if e != nil {
		t.Fatal(e)
	}
	for _, variant := range []string{"thumbnail", "preview"} {
		b, e := s.Derivative(hash, original, variant)
		if e != nil {
			t.Fatal(e)
		}
		cached, _, hit, e := s.VerifiedDerivative(hash, variant)
		if e != nil || !hit || IDBytes(cached) != IDBytes(b) {
			t.Fatal("manifest hit", variant, hit, e)
		}
		path := derivativePath(s.root, hash, variant)
		if e = os.WriteFile(path, []byte("tampered"), 0600); e != nil {
			t.Fatal(e)
		}
		if _, _, hit, e = s.VerifiedDerivative(hash, variant); e != nil || hit {
			t.Fatal("tampered derivative trusted", hit, e)
		}
	}
	path := filepath.Join(s.root, "blobs", hash)
	stat, e := os.Stat(path)
	if e != nil {
		t.Fatal(e)
	}
	changed := append([]byte(nil), original...)
	changed[len(changed)-1] ^= 1
	if e = os.WriteFile(path, changed, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.Chtimes(path, stat.ModTime(), stat.ModTime()); e != nil {
		t.Fatal(e)
	}
	if _, _, _, _, e = s.VerifyOriginal(hash); e == nil {
		t.Fatal("same size/time mutation skipped full digest")
	}
	if _, _, _, e = s.VerifiedDerivative(hash, "preview"); e == nil {
		t.Fatal("derivative bypassed source digest")
	}
	if e = os.Remove(path); e != nil {
		t.Fatal(e)
	}
	if _, _, _, e = s.VerifiedDerivative(hash, "thumbnail"); e == nil {
		t.Fatal("missing original served from derivative")
	}
}

func TestLoadingOrientedViewPreservesCameraTransforms(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 3, 2))
	src.Set(0, 0, color.White)
	src.Set(2, 1, color.RGBA{R: 255, A: 255})
	for _, test := range []struct {
		o, w, h, x, y int
		want          color.Color
	}{{1, 3, 2, 0, 0, color.White}, {6, 2, 3, 1, 0, color.White}, {8, 2, 3, 0, 2, color.White}, {3, 3, 2, 2, 1, color.White}, {5, 2, 3, 0, 0, color.White}, {7, 2, 3, 1, 2, color.White}} {
		v := orientedImage{src, test.o}
		if v.Bounds().Dx() != test.w || v.Bounds().Dy() != test.h {
			t.Fatal("orientation dimensions", test.o)
		}
		r, g, b, a := v.At(test.x, test.y).RGBA()
		wr, wg, wb, wa := test.want.RGBA()
		if r != wr || g != wg || b != wb || a != wa {
			t.Fatal("wrong orientation", test.o)
		}
	}
}

func TestLoadingDecodeAdmissionAndCrashOrphanQuota(t *testing.T) {
	b := testPNG(t, color.White)
	release, e := acquireImageDecode(b, 1, 1, false, false)
	if e != nil {
		t.Fatal(e)
	}
	imageDecodes.Lock()
	active := imageDecodes.active
	imageDecodes.Unlock()
	if !active {
		t.Fatal("decode reservation not held")
	}
	release()
	release()
	if free, e := acquireImageDecode(b, 8000, 8000, false, true); e == nil {
		free()
		t.Fatal("unqualified64MP allocation admitted")
	}
	s := New(t.TempDir())
	defer s.Close()
	if _, e = s.Put(b); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(s.root, "blobs", ID("crash-orphan")+".preview.v1.jpg")
	if e = os.WriteFile(path, []byte("orphan-owned-by-fixture"), 0600); e != nil {
		t.Fatal(e)
	}
	n, e := s.previewBytes("")
	if e != nil || n != int64(len("orphan-owned-by-fixture")) {
		t.Fatal("crash orphan unaccounted", n, e)
	}
}
