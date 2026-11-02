package lifecycle

import (
	"os"
	"path/filepath"
	"testing"
)

func TestQALifecyclePrivateJournalRejectsForeignSymlink(t *testing.T) {
	dir := t.TempDir()
	foreign := filepath.Join(t.TempDir(), "foreign.json")
	if err := os.WriteFile(foreign, []byte(`[]`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(foreign, filepath.Join(dir, "native-lifecycle-v1.json")); err != nil {
		t.Fatal(err)
	}
	if err := New(dir).Add(Event{Kind: "transport_connected", Owner: "provider_connection", Reason: "connected"}); err == nil {
		t.Fatal("REPRO: lifecycle follows/imports a foreign journal symlink instead of rejecting it")
	}
}
