//go:build !windows

package runtimeguard

import (
	"bufio"
	"os"
	"os/exec"
	"testing"
)

func TestRuntimeLockChildCrashReleasesOnlyItsKernelLease(t *testing.T) {
	if os.Getenv("BRIDGE_LOCK_TEST_CHILD") != "" {
		release, err := Acquire(os.Getenv("BRIDGE_LOCK_TEST_DIR"))
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		println("LOCK_READY")
		select {}
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRuntimeLockChildCrashReleasesOnlyItsKernelLease$")
	cmd.Env = append(os.Environ(), "BRIDGE_LOCK_TEST_CHILD=1", "BRIDGE_LOCK_TEST_DIR="+dir)
	out, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	reader := bufio.NewScanner(out)
	if !reader.Scan() || reader.Text() != "LOCK_READY" {
		t.Fatal("child lock witness missing")
	}
	if release, err := Acquire(dir); err == nil {
		release()
		t.Fatal("concurrent process acquired live lock")
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	release, err := Acquire(dir)
	if err != nil {
		t.Fatal("crashed owner left a false durable lock", err)
	}
	release()
}

func TestSameDataDirectoryRejectsSecondWriterAndReleases(t *testing.T) {
	dir := t.TempDir()
	release, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	if next, err := Acquire(dir); err == nil {
		next()
		t.Fatal("second service writer admitted")
	}
	release()
	next, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	next()
}

func TestRuntimeLockIsNotInheritedByProtectedChild(t *testing.T) {
	dir := t.TempDir()
	release, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sleep", "10")
	if err = cmd.Start(); err != nil {
		release()
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	release()
	next, err := Acquire(dir)
	if err != nil {
		t.Fatal("child inherited writer lock", err)
	}
	next()
}

func TestRuntimeLockRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := t.TempDir() + "/target"
	if err := os.WriteFile(target, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, dir+"/.go-service-owner.lock"); err != nil {
		t.Fatal(err)
	}
	if release, err := Acquire(dir); err == nil {
		release()
		t.Fatal("followed substituted lock")
	}
}
