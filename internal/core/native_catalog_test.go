package core

import (
	"everything-go/internal/messagequeue"
	"everything-go/internal/nativewatch"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeCatalogDeletionNeedsTwoScansAndProtectsDurableWork(t *testing.T) {
	h, _ := newTestHub(t)
	root := t.TempDir()
	opts := nativewatch.Options{ClaudeProjectsDir: root}
	ids := []string{"12345678-1234-1234-1234-123456789abc", "22345678-1234-1234-1234-123456789abc", "32345678-1234-1234-1234-123456789abc"}
	for _, id := range ids {
		h.registry.Create("jl_c_"+id[:12], "fixture", "/", "claude", "", "", id)
	}
	h.registry.Create("bridge-created", "retained", "/", "claude", "", "", ids[0])
	live := filepath.Join(root, ids[1]+".jsonl")
	if err := os.WriteFile(live, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	queuedID := "jl_c_" + ids[2][:12]
	if _, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: queuedID, RequestID: "r_protected", Content: "keep", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	previous, n := h.reconcileNativeCatalog(opts, nil)
	if n != 0 {
		t.Fatal("first absence removed row")
	}
	_, n = h.reconcileNativeCatalog(opts, previous)
	if n != 1 {
		t.Fatalf("removed %d want 1", n)
	}
	for _, id := range []string{"bridge-created", "jl_c_" + ids[1][:12], queuedID} {
		if _, ok := h.registry.Get(id); !ok {
			t.Fatalf("protected %s removed", id)
		}
	}
	if _, ok := h.registry.Get("jl_c_" + ids[0][:12]); ok {
		t.Fatal("deleted imported row retained")
	}
}
