package runtime

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitDiffNoCwd(t *testing.T) {
	if r := GitDiff(""); r.Error != "no_cwd" {
		t.Fatalf("empty cwd → error %q, want no_cwd", r.Error)
	}
	if r := GitDiff(filepath.Join(t.TempDir(), "does-not-exist")); r.Error != "no_cwd" {
		t.Fatalf("missing dir → error %q, want no_cwd", r.Error)
	}
}

// TestGitDiffInitThenDiff exercises the full path: a fresh dir auto-inits a
// baseline repo (initialized=true, no diff yet), and a subsequent edit shows up
// in the diff against HEAD (initialized=false).
func TestGitDiffInitThenDiff(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	cwd := t.TempDir()
	if err := os.WriteFile(filepath.Join(cwd, "hello.txt"), []byte("first\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// First call: no .git → baseline init, everything committed, clean diff.
	r := GitDiff(cwd)
	if r.Error != "" {
		t.Fatalf("baseline init errored: %q", r.Error)
	}
	if !r.Initialized {
		t.Fatal("first call should report initialized=true")
	}
	if strings.TrimSpace(r.Diff) != "" {
		t.Fatalf("baseline diff should be empty, got %q", r.Diff)
	}
	if !isDir(filepath.Join(cwd, ".git")) {
		t.Fatal(".git was not created")
	}
	if _, err := os.Stat(filepath.Join(cwd, ".gitignore")); err != nil {
		t.Fatal(".gitignore was not written")
	}

	// Edit the file → second call must show the change, no re-init.
	if err := os.WriteFile(filepath.Join(cwd, "hello.txt"), []byte("first\nsecond\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r2 := GitDiff(cwd)
	if r2.Error != "" {
		t.Fatalf("second diff errored: %q", r2.Error)
	}
	if r2.Initialized {
		t.Fatal("second call should report initialized=false (repo already exists)")
	}
	if !strings.Contains(r2.Diff, "+second") {
		t.Fatalf("diff should contain the added line, got:\n%s", r2.Diff)
	}
}
