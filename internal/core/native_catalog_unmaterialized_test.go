package core

import (
	"everything-go/internal/nativewatch"
	"os"
	"path/filepath"
	"testing"
)

// Opening a new ordinary conversation may bind a thread via GetGoal before a
// native user turn materializes a rollout. A healthy empty catalog proves no
// file exists, not that this never-materialized conversation was deleted.
func TestNativeCatalogKeepsNewBridgeThreadWithoutEverObservedRollout(t *testing.T) {
	h, _ := newTestHub(t)
	root := t.TempDir()
	opts := nativewatch.Options{CodexSessionsDir: root}
	s := h.registry.Create("s_new_empty", "new controlled", root, "codex", "", "read-only", "")
	s.SetResumeID("01a18e76-66d8-7e70-afbc-74c93671eb68")
	var previous map[string]bool
	for scan := 0; scan < 4; scan++ {
		var removed int
		previous, removed = h.reconcileNativeCatalog(opts, previous)
		if removed != 0 {
			t.Fatalf("scan%d removed newly bound thread before any rollout was observed", scan+1)
		}
		if _, ok := h.registry.Get(s.ID); !ok {
			t.Fatalf("scan%d lost newly bound session", scan+1)
		}
	}
}

func TestNativeCatalogBridgeRetiresOnlyAfterObservedPresentThenTwoAbsences(t *testing.T) {
	h, _ := newTestHub(t)
	root := t.TempDir()
	opts := nativewatch.Options{CodexSessionsDir: root}
	id := "11a18e76-66d8-7e70-afbc-74c93671eb68"
	s := h.registry.Create("s_materialized", "original", root, "codex", "", "read-only", id)
	file := filepath.Join(root, "rollout-2026-10-31-"+id+".jsonl")
	if err := os.WriteFile(file, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	previous, n := h.reconcileNativeCatalog(opts, nil)
	if n != 0 {
		t.Fatal("present row retired")
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	previous, n = h.reconcileNativeCatalog(opts, previous)
	if n != 0 {
		t.Fatal("first absence retired")
	}
	_, n = h.reconcileNativeCatalog(opts, previous)
	if n != 1 {
		t.Fatalf("confirmed observed deletion removed=%d want1", n)
	}
	if _, ok := h.registry.Get(s.ID); ok {
		t.Fatal("confirmed old deletion retained")
	}
}

func TestNativeCatalogObservationCannotCrossNativeGeneration(t *testing.T) {
	h, _ := newTestHub(t)
	root := t.TempDir()
	opts := nativewatch.Options{CodexSessionsDir: root}
	old := "21a18e76-66d8-7e70-afbc-74c93671eb68"
	next := "31a18e76-66d8-7e70-afbc-74c93671eb68"
	s := h.registry.Create("s_next_empty", "original", root, "codex", "", "read-only", old)
	file := filepath.Join(root, "rollout-2026-10-31-"+old+".jsonl")
	if err := os.WriteFile(file, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	previous, _ := h.reconcileNativeCatalog(opts, nil)
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	s.SetResumeID(next)
	for scan := 0; scan < 3; scan++ {
		var n int
		previous, n = h.reconcileNativeCatalog(opts, previous)
		if n != 0 {
			t.Fatal("older physical thread observation retired new never-materialized generation")
		}
	}
}

func TestNativeCatalogBridgeAbsenceAfterRestartHasNoDeletionProof(t *testing.T) {
	h, _ := newTestHub(t)
	root := t.TempDir()
	opts := nativewatch.Options{CodexSessionsDir: root}
	id := "41a18e76-66d8-7e70-afbc-74c93671eb68"
	h.registry.Create("s_restored", "restored", root, "codex", "", "read-only", id)
	// A fresh watcher after restart has no complete positive observation. The
	// persisted native ID alone does not distinguish start-before-rollout.
	previous, n := h.reconcileNativeCatalog(opts, nil)
	if n != 0 {
		t.Fatal("startup absence retired row")
	}
	_, n = h.reconcileNativeCatalog(opts, previous)
	if n != 0 {
		t.Fatal("post-startup absence retired without presence evidence")
	}
}

func TestNativeCatalogIncompleteScanBreaksConsecutiveAbsence(t *testing.T) {
	h, _ := newTestHub(t)
	root := t.TempDir()
	opts := nativewatch.Options{CodexSessionsDir: root}
	id := "51a18e76-66d8-7e70-afbc-74c93671eb68"
	h.registry.Create("s_incomplete", "original", root, "codex", "", "read-only", id)
	file := filepath.Join(root, "rollout-2026-10-31-"+id+".jsonl.gz")
	if err := os.WriteFile(file, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	previous, _ := h.reconcileNativeCatalog(opts, nil)
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	previous, n := h.reconcileNativeCatalog(opts, previous)
	if n != 0 {
		t.Fatal("first absence retired")
	}
	link := filepath.Join(root, "unreadable-link")
	if err := os.Symlink(t.TempDir(), link); err != nil {
		t.Fatal(err)
	}
	previous, n = h.reconcileNativeCatalog(opts, previous)
	if n != 0 {
		t.Fatal("incomplete scan retired")
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	previous, n = h.reconcileNativeCatalog(opts, previous)
	if n != 0 {
		t.Fatal("absence after incomplete scan retired too early")
	}
	_, n = h.reconcileNativeCatalog(opts, previous)
	if n != 1 {
		t.Fatalf("confirmed deletion after two fresh complete absences=%d", n)
	}
}
