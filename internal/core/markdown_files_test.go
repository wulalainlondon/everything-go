package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func quoteJSON(value string) string {
	b, _ := json.Marshal(value)
	return string(b)
}

func TestScanMarkdownFiles(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs", "README.md"), []byte("# Readme"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs", "notes.markdown"), []byte("Notes"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs", "skip.txt"), []byte("No"), 0o644); err != nil {
		t.Fatal(err)
	}

	route(h, c, `{"type":"scan_markdown_files","paths":[`+quoteJSON(root)+`]}`)
	ev := waitForType(t, c, "markdown_files_listing")
	files, ok := ev["files"].([]any)
	if !ok || len(files) != 2 {
		t.Fatalf("files = %#v, want 2 markdown files", ev["files"])
	}
}

func TestSaveFileUpdatesMarkdown(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	path := filepath.Join(t.TempDir(), "README.md")
	if err := os.WriteFile(path, []byte("# Old"), 0o644); err != nil {
		t.Fatal(err)
	}

	route(h, c, `{"type":"save_file","path":`+quoteJSON(path)+`,"content":"# New"}`)
	ev := waitForType(t, c, "file_saved")
	if ev["error"] != nil {
		t.Fatalf("unexpected save error: %v", ev["error"])
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "# New" {
		t.Fatalf("file content = %q", string(got))
	}
}

func TestSaveFileRejectsConflictingModified(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	path := filepath.Join(t.TempDir(), "README.md")
	if err := os.WriteFile(path, []byte("# Current"), 0o644); err != nil {
		t.Fatal(err)
	}

	route(h, c, `{"type":"save_file","path":`+quoteJSON(path)+`,"content":"# Stale","expected_modified":1}`)
	ev := waitForType(t, c, "file_saved")
	if ev["error"] != "file changed on disk; reopen before saving" {
		t.Fatalf("error = %v", ev["error"])
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "# Current" {
		t.Fatalf("conflicting save changed file: %q", string(got))
	}
}
