package artifacts

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestScanClassifiesSortsAndSkipsHiddenTrees(t *testing.T) {
	root := t.TempDir()
	oldFile := filepath.Join(root, "summary.md")
	newFile := filepath.Join(root, "frame.png")
	ignored := filepath.Join(root, ".git", "secret.png")
	if err := os.MkdirAll(filepath.Dir(ignored), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{oldFile, newFile, ignored, filepath.Join(root, "ignore.exe")} {
		if err := os.WriteFile(name, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(oldFile, old, old); err != nil {
		t.Fatal(err)
	}

	got := Scan([]string{root}, 100, func(name string) string { return "https://current.example/media/" + filepath.Base(name) })
	if len(got) != 2 {
		t.Fatalf("Scan returned %d artifacts: %+v", len(got), got)
	}
	if got[0].Kind != "image" || got[0].Title != "frame.png" {
		t.Fatalf("newest artifact = %+v", got[0])
	}
	if got[1].Kind != "summary" {
		t.Fatalf("summary kind = %q", got[1].Kind)
	}
	if got[0].URL != "https://current.example/media/frame.png" {
		t.Fatalf("URL = %q", got[0].URL)
	}
}

func TestScanHonorsLimit(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a.md", "b.md", "c.md"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if got := Scan([]string{root}, 2, nil); len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
}

func TestDownloadYouTubeRejectsEmptyURLBeforeLookup(t *testing.T) {
	_, err := DownloadYouTube("  ", "s1", "yt_test", nil)
	if err == nil || err.Error() != "YouTube URL is required" {
		t.Fatalf("err = %v", err)
	}
}
