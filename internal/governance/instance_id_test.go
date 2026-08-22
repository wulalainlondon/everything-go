package governance

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestLoadOrCreateInstanceIDIsStableAndPrivate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime", "instance_id")
	first, err := LoadOrCreateInstanceID(path)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^eg_[0-9a-f]{32}$`).MatchString(first) {
		t.Fatalf("unexpected instance id: %q", first)
	}
	second, err := LoadOrCreateInstanceID(path)
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatalf("instance id changed: %q != %q", second, first)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("instance id mode = %o, want 600", info.Mode().Perm())
	}
}

func TestLoadOrCreateInstanceIDPreservesExistingValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "instance_id")
	if err := os.WriteFile(path, []byte("existing-id\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	id, err := LoadOrCreateInstanceID(path)
	if err != nil {
		t.Fatal(err)
	}
	if id != "existing-id" {
		t.Fatalf("id = %q", id)
	}
}
