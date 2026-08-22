package governance

import "testing"

func TestSessionControlPersistsDesktopLeaseAndReclaim(t *testing.T) {
	dir := t.TempDir()
	s := NewSessionControlStore(dir)
	if !s.MobileMayWrite("s1") {
		t.Fatal("new session should default to mobile control")
	}
	pending, err := s.BeginDesktopHandoff("s1", "thread-1")
	if err != nil || pending.State != ControlPending || s.MobileMayWrite("s1") {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	desktop := s.CompleteDesktopHandoff("s1")
	if desktop.Owner != ControlDesktop || s.MobileMayWrite("s1") {
		t.Fatalf("desktop=%+v", desktop)
	}

	reloaded := NewSessionControlStore(dir)
	if entry := reloaded.Get("s1"); entry.Owner != ControlDesktop || entry.ThreadID != "thread-1" {
		t.Fatalf("restart lost desktop lease: %+v", entry)
	}
	mobile := reloaded.Reclaim("s1", "thread-1")
	if mobile.Owner != ControlMobile || !reloaded.MobileMayWrite("s1") {
		t.Fatalf("reclaim=%+v", mobile)
	}
}

func TestMarkDesktopOwnerIsIdempotent(t *testing.T) {
	s := NewSessionControlStore(t.TempDir())
	first, changed := s.MarkDesktopOwner("s1", "thread-1")
	if !changed || first.Owner != ControlDesktop || first.State != ControlActive || s.MobileMayWrite("s1") {
		t.Fatalf("first external owner=%+v changed=%v", first, changed)
	}
	second, changed := s.MarkDesktopOwner("s1", "thread-1")
	if changed || second.UpdatedAt != first.UpdatedAt {
		t.Fatalf("idempotent mark changed state: first=%+v second=%+v changed=%v", first, second, changed)
	}
}
