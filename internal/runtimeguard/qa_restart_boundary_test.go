package runtimeguard

import (
	"os"
	"path/filepath"
	"testing"
)

func TestQARuntimeLockReplacementMustNotAdmitSecondWriter(t *testing.T) {
	dir := t.TempDir()
	release, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err = os.Rename(filepath.Join(dir, ".go-service-owner.lock"), filepath.Join(dir, ".retired-owner-lock")); err != nil {
		t.Fatal(err)
	}
	next, err := Acquire(dir)
	if err == nil {
		defer next()
		t.Fatal("REPRO: same DataDir has two admitted runtime writers after lock-path replacement")
	}
}
