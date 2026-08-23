package instances

import (
	"os"
	"path/filepath"
	"testing"
)

func TestUpsertPersistsAndDelete(t *testing.T) {
	dataDir := t.TempDir()
	root := t.TempDir()
	m := New(dataDir, "/missing/bridge")
	status, code := m.Upsert(Instance{Name: "studio", Port: 9453, RootDir: root, DataDir: filepath.Join(dataDir, "studio"), Backend: "codex"})
	if code != "" {
		t.Fatalf("upsert code = %q", code)
	}
	if status.State != "stopped" || status.Backend != "codex" {
		t.Fatalf("status = %+v", status)
	}

	reloaded := New(dataDir, "/missing/bridge").List()
	if len(reloaded) != 1 || reloaded[0].Name != "studio" {
		t.Fatalf("reloaded = %+v", reloaded)
	}
	if code := m.Delete("studio"); code != "" {
		t.Fatalf("delete code = %q", code)
	}
	if got := m.List(); len(got) != 0 {
		t.Fatalf("after delete = %+v", got)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "instances.json")); err != nil {
		t.Fatalf("store missing: %v", err)
	}
}

func TestValidationRejectsUnsafeConfiguration(t *testing.T) {
	m := New(t.TempDir(), "")
	cases := []struct {
		item Instance
		want string
	}{
		{Instance{Name: "../escape", Port: 9000}, "name_invalid"},
		{Instance{Name: "low", Port: 80}, "port_invalid"},
		{Instance{Name: "root", Port: 9001, RootDir: "/"}, "root_dir_sensitive"},
		{Instance{Name: "backend", Port: 9002, Backend: "unknown"}, "backend_invalid"},
	}
	for _, tc := range cases {
		if _, code := m.Upsert(tc.item); code != tc.want {
			t.Errorf("Upsert(%+v) code = %q, want %q", tc.item, code, tc.want)
		}
	}
}

func TestPortCollisionAndDefaultProtection(t *testing.T) {
	m := New(t.TempDir(), "")
	if _, code := m.Upsert(Instance{Name: "one", Port: 9000}); code != "" {
		t.Fatal(code)
	}
	if _, code := m.Upsert(Instance{Name: "two", Port: 9000}); code != "port_in_use" {
		t.Fatalf("collision code = %q", code)
	}
	if code := m.Stop("default"); code != "default_immutable" {
		t.Fatalf("stop default = %q", code)
	}
	if code := m.Delete("default"); code != "default_immutable" {
		t.Fatalf("delete default = %q", code)
	}
}
