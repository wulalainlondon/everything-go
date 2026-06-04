package governance

import (
	"path/filepath"
	"testing"
)

func newTestPairing(t *testing.T) (*Pairing, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pairing.json")
	return NewPairing(path), path
}

func TestPairingStartsUnlocked(t *testing.T) {
	p, _ := newTestPairing(t)
	if p.IsLocked() {
		t.Fatal("fresh pairing should be unlocked")
	}
	if p.LockedTo("anything") {
		t.Fatal("unlocked pairing should not be locked to any token")
	}
}

func TestPairingClaimLocks(t *testing.T) {
	p, _ := newTestPairing(t)
	if err := p.Claim("tok-A", "dev1"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !p.IsLocked() {
		t.Fatal("should be locked after claim")
	}
	if !p.LockedTo("tok-A") {
		t.Fatal("should be locked to the claiming token")
	}
	if p.LockedTo("tok-B") {
		t.Fatal("must not be locked to a different token")
	}
}

func TestPairingClaimSameTokenIdempotent(t *testing.T) {
	p, _ := newTestPairing(t)
	if err := p.Claim("tok-A", "dev1"); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := p.Claim("tok-A", "dev1-again"); err != nil {
		t.Fatalf("re-claim with same token must succeed (idempotent): %v", err)
	}
}

func TestPairingClaimDifferentTokenRejected(t *testing.T) {
	p, _ := newTestPairing(t)
	_ = p.Claim("tok-A", "dev1")
	if err := p.Claim("tok-B", "dev2"); err != ErrClaimedByAnother {
		t.Fatalf("expected ErrClaimedByAnother, got %v", err)
	}
	if !p.LockedTo("tok-A") {
		t.Fatal("rejected claim must not change the existing lock")
	}
}

func TestPairingUnclaimWrongTokenRejected(t *testing.T) {
	p, _ := newTestPairing(t)
	_ = p.Claim("tok-A", "dev1")
	if err := p.Unclaim("tok-B"); err != ErrTokenMismatch {
		t.Fatalf("expected ErrTokenMismatch, got %v", err)
	}
	if !p.IsLocked() {
		t.Fatal("failed unclaim must leave the lock intact")
	}
}

func TestPairingUnclaimReleases(t *testing.T) {
	p, _ := newTestPairing(t)
	_ = p.Claim("tok-A", "dev1")
	if err := p.Unclaim("tok-A"); err != nil {
		t.Fatalf("unclaim: %v", err)
	}
	if p.IsLocked() {
		t.Fatal("should be unlocked after unclaim")
	}
}

func TestPairingPersistsAcrossReload(t *testing.T) {
	p, path := newTestPairing(t)
	if err := p.Claim("tok-A", "dev1"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// A fresh Pairing reading the same file must see the lock.
	reloaded := NewPairing(path)
	if !reloaded.IsLocked() || !reloaded.LockedTo("tok-A") {
		t.Fatal("pairing did not persist across reload")
	}
	// Unclaim removes the file, so a further reload is unlocked.
	if err := reloaded.Unclaim("tok-A"); err != nil {
		t.Fatalf("unclaim: %v", err)
	}
	if again := NewPairing(path); again.IsLocked() {
		t.Fatal("unclaim should not persist a lock")
	}
}
