package governance

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestPairingWindowAddsExactlyOneDevice(t *testing.T) {
	p, _ := newTestPairing(t)
	if err := p.Claim("tok-A", "dev1"); err != nil {
		t.Fatal(err)
	}
	p.OpenEnrollment(time.Minute)
	if !p.EnrollmentOpen() {
		t.Fatal("pairing window should be open")
	}
	if err := p.Claim("tok-B", "dev2"); err != nil {
		t.Fatalf("second device claim: %v", err)
	}
	if !p.LockedTo("tok-A") || !p.LockedTo("tok-B") {
		t.Fatal("both device credentials should remain trusted")
	}
	if p.EnrollmentOpen() {
		t.Fatal("window should close after one new device")
	}
	if err := p.Claim("tok-C", "dev3"); err != ErrClaimedByAnother {
		t.Fatalf("third device should need a new window, got %v", err)
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

func TestPairingUnclaimRevokesOnlyOneDevice(t *testing.T) {
	p, _ := newTestPairing(t)
	_ = p.Claim("tok-A", "dev1")
	p.OpenEnrollment(time.Minute)
	_ = p.Claim("tok-B", "dev2")
	if err := p.Unclaim("tok-A"); err != nil {
		t.Fatal(err)
	}
	if p.LockedTo("tok-A") || !p.LockedTo("tok-B") || !p.IsLocked() {
		t.Fatal("revoking one device must preserve the other")
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

func TestPairingMigratesLegacyFileAndTightensPermissions(t *testing.T) {
	p, path := newTestPairing(t)
	_ = p
	legacy := []byte(`{"paired_token":"old-token","paired_device_id":"old-device","paired_at":123}`)
	if err := os.WriteFile(path, legacy, 0o644); err != nil {
		t.Fatal(err)
	}
	reloaded := NewPairing(path)
	if !reloaded.LockedTo("old-token") {
		t.Fatal("legacy credential was not migrated in memory")
	}
	if err := reloaded.Claim("old-token", "old-device"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("pairing file mode = %o, want 600", info.Mode().Perm())
	}
}
