package mediaservice

import (
	"path/filepath"
	"testing"
)

func TestPathsKeepDefaultAndIsolateDedicatedInstance(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	m, l, err := Paths("")
	if err != nil || m != filepath.Join(home, ".everything-go-runtime", "media-control", "current.json") || l != filepath.Join(home, ".local", "bin", "bridge-media") {
		t.Fatal(m, l, err)
	}
	dir := filepath.Join(home, "dedicated instance", "media-control")
	m, l, err = Paths(dir)
	if err != nil || m != filepath.Join(dir, "current.json") || l != filepath.Join(dir, "bridge-media") {
		t.Fatal(m, l, err)
	}
	for _, invalid := range []string{"relative", dir + "/", dir + "/../elsewhere"} {
		if _, _, err := Paths(invalid); err == nil {
			t.Fatalf("accepted noncanonical directory %q", invalid)
		}
	}
}
