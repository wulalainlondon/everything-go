// Package governance implements connection-governance: bridge pairing
// (claim/unclaim single-owner lock) and the offline event buffer that lets a
// reconnecting client recover events emitted while it was disconnected.
//
// Fidelity reference: bridge/pairing.py, bridge/offline_replay.py, and the
// claim/unclaim + reconnect logic in bridge/handlers/connection.py.
package governance

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	// ErrClaimedByAnother is returned when claim_bridge targets a bridge already
	// locked to a different auth token.
	ErrClaimedByAnother = errors.New("Bridge already claimed by another device")
	// ErrTokenMismatch is returned when unclaim_bridge presents the wrong token.
	ErrTokenMismatch = errors.New("Unauthorized: token mismatch")
)

// Pairing is the single-owner lock state, persisted to pairing.json.
type Pairing struct {
	mu   sync.Mutex
	path string

	token    string
	deviceID string
	pairedAt int64
}

type pairingFile struct {
	PairedToken    string `json:"paired_token"`
	PairedDeviceID string `json:"paired_device_id"`
	PairedAt       int64  `json:"paired_at"`
}

// NewPairing loads pairing state from path (absent file → unpaired).
func NewPairing(path string) *Pairing {
	p := &Pairing{path: path}
	if data, err := os.ReadFile(path); err == nil {
		var f pairingFile
		if json.Unmarshal(data, &f) == nil {
			p.token = f.PairedToken
			p.deviceID = f.PairedDeviceID
			p.pairedAt = f.PairedAt
		}
	}
	return p
}

// IsLocked reports whether the bridge is claimed by some device.
func (p *Pairing) IsLocked() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.token != ""
}

// LockedTo reports whether the bridge is claimed by exactly this token.
func (p *Pairing) LockedTo(token string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.token != "" && p.token == token
}

// Claim locks the bridge to token/deviceID. Idempotent for the same token;
// rejects a different token while already locked.
func (p *Pairing) Claim(token, deviceID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" && p.token != token {
		return ErrClaimedByAnother
	}
	p.token = token
	p.deviceID = deviceID
	p.pairedAt = time.Now().Unix()
	return p.saveLocked()
}

// Unclaim releases the lock. Requires the matching token (no-op if unpaired).
func (p *Pairing) Unclaim(token string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" && p.token != token {
		return ErrTokenMismatch
	}
	p.token = ""
	p.deviceID = ""
	p.pairedAt = 0
	_ = os.Remove(p.path)
	return nil
}

// saveLocked atomically writes pairing.json (.tmp + rename). Caller holds mu.
func (p *Pairing) saveLocked() error {
	if p.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p.path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(pairingFile{
		PairedToken: p.token, PairedDeviceID: p.deviceID, PairedAt: p.pairedAt,
	})
	if err != nil {
		return err
	}
	tmp := p.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p.path)
}
