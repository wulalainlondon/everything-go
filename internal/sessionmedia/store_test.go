package sessionmedia

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"testing"
)

func testPNG(t *testing.T, c color.Color) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 640, 480))
	img.Set(1, 1, c)
	var b bytes.Buffer
	if e := png.Encode(&b, img); e != nil {
		t.Fatal(e)
	}
	return b.Bytes()
}
func TestStoreCheckpointCursorAndReopen(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	c, e := s.Load("a", "binding")
	if e != nil {
		t.Fatal(e)
	}
	c.Started = true
	c.Complete = true
	records := []Record{}
	for i, key := range []string{"0001", "0002", "0003"} {
		records = append(records, Record{Entry: Entry{ID: ID(key), Message: key, Author: []string{"user", "assistant", "user"}[i], Sort: key}})
	}
	if e = s.Commit("a", c, records); e != nil {
		t.Fatal(e)
	}
	first, next, count, e := s.List("a", c, "all", "", 2)
	if e != nil || count != 3 || len(first) != 2 || first[0].Message != "0003" || next == "" {
		t.Fatalf("first=%+v count=%d next=%s err=%v", first, count, next, e)
	}
	if _, _, _, e = s.List("foreign", c, "all", next, 2); e == nil {
		t.Fatal("cross-session cursor accepted")
	}
	if _, _, _, e = s.List("a", c, "user", next, 2); e == nil {
		t.Fatal("cross-filter cursor accepted")
	}
	if _, _, _, e = s.List("a", c, "all", next+"x", 2); e == nil {
		t.Fatal("tampered cursor accepted")
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s = New(root)
	defer s.Close()
	reopened, e := s.Load("a", "binding")
	if e != nil || reopened.Epoch != c.Epoch {
		t.Fatal("checkpoint lost", e)
	}
	last, next, count, e := s.List("a", reopened, "all", next, 2)
	if e != nil || len(last) != 1 || last[0].Message != "0001" || next != "" || count != 3 {
		t.Fatal("pagination after reopen", last, next, e)
	}
	reset, e := s.Load("a", "changed")
	if e != nil || reset.Started || reset.Epoch == c.Epoch {
		t.Fatal("new source inherited checkpoint")
	}
	if e = s.Commit("a", reset, nil); e != nil {
		t.Fatal(e)
	}
	items, _, count, e := s.List("a", reset, "all", "", 60)
	if e != nil || count != 0 || len(items) != 0 {
		t.Fatal("old scope exposed", e)
	}
}
func TestCapturePinsFirstVersionAndThumb(t *testing.T) {
	s := New(t.TempDir())
	defer s.Close()
	p := filepath.Join(t.TempDir(), "image.png")
	original := testPNG(t, color.White)
	if e := os.WriteFile(p, original, 0600); e != nil {
		t.Fatal(e)
	}
	hash, e := s.Capture("a", "turn1", p)
	if e != nil {
		t.Fatal(e)
	}
	replacement := testPNG(t, color.Black)
	_ = os.WriteFile(p, replacement, 0600)
	repeated, e := s.Capture("a", "turn1", p)
	if e != nil || repeated != hash {
		t.Fatal("replay adopted changed path")
	}
	second, e := s.Capture("a", "turn2", p)
	if e != nil || second == hash {
		t.Fatal("new turn lost distinct asset")
	}
	_ = os.Remove(p)
	b, e := s.Read(hash)
	if e != nil || !bytes.Equal(b, original) {
		t.Fatal("original not retained", e)
	}
	thumb, e := Thumbnail(b)
	if e != nil || len(thumb) > 80<<10 {
		t.Fatal(e)
	}
	mime, w, h, e := ImageInfo(thumb)
	if e != nil || mime != "image/jpeg" || w != 320 || h != 240 {
		t.Fatalf("thumbnail %s %dx%d %v", mime, w, h, e)
	}
	if _, e = s.Read("../index.sqlite"); e == nil {
		t.Fatal("path traversal accepted")
	}
	if _, e = s.Put([]byte("not an image")); e == nil {
		t.Fatal("invalid image admitted")
	}
}
func TestBackfillCursorProgressWithSparsePages(t *testing.T) {
	s := New(t.TempDir())
	defer s.Close()
	c, e := s.Load("a", "binding")
	if e != nil {
		t.Fatal(e)
	}
	c.Started = true
	if e = s.Commit("a", c, nil); e != nil {
		t.Fatal(e)
	}
	items, cursor, _, e := s.List("a", c, "all", "", 60)
	if e != nil || len(items) != 0 || cursor == "" {
		t.Fatal("sparse scan falsely complete")
	}
	if e = s.Commit("a", c, []Record{{Entry: Entry{ID: ID("old"), Message: "old", Author: "user", Sort: "0001"}}}); e != nil {
		t.Fatal(e)
	}
	c.Complete = true
	items, next, _, e := s.List("a", c, "all", cursor, 60)
	if e != nil || len(items) != 1 || next != "" {
		t.Fatal("old image lost after sparse page", e)
	}
}

func TestStoreDiskFloorAndInterruptedAssetRecovery(t *testing.T) {
	s := New(t.TempDir())
	defer s.Close()
	data := testPNG(t, color.White)
	s.free = func(string) (uint64, error) { return 512 << 20, nil }
	if _, e := s.Put(data); e == nil {
		t.Fatal("disk floor admission accepted")
	}
	s.free = func(string) (uint64, error) { return 4 << 30, nil }
	hash, e := s.Put(data)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(filepath.Join(s.root, "blobs", hash)); e != nil {
		t.Fatal(e)
	}
	if recovered, e := s.Put(data); e != nil || recovered != hash {
		t.Fatal("interrupted original could not be restored from exact same bytes", e)
	}
	if _, e = s.Read(hash); e != nil {
		t.Fatal(e)
	}
	c, e := s.Load("scope", "binding")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Commit("scope", c, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = s.db.Exec("UPDATE checkpoints SET value=? WHERE session=?", `{"binding":"binding"}`, "scope"); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Load("scope", "binding"); e == nil {
		t.Fatal("partial checkpoint accepted")
	}
}
