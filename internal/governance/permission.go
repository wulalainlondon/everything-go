package governance

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"everything-go/internal/protocol"
)

// PermissionManager gates high-risk operations (kill_process, shell_input)
// behind a human approval round-trip, mirroring bridge/permission_manager.py.
// Request broadcasts a permission_request, blocks until the user answers via
// permission_response (→ Resolve) or the TTL expires (→ deny), then returns the
// decision. Modes: "off" (always allow), "warn" (auto-allow + notify),
// "enforce" (real prompt; the default, matching Python).
//
// IMPORTANT: Request blocks. It must NOT be called on the connection read loop
// (the answer arrives on that same loop), so callers run it in a goroutine.
type PermissionManager struct {
	emit func(any)
	mode string
	ttl  time.Duration

	mu        sync.Mutex
	waiters   map[string]chan bool
	requester map[string]string // request_id -> requester device_id (binds the answer)
}

// NewPermissionManager builds a manager. emit broadcasts events to all clients.
// mode defaults to "enforce" when empty.
func NewPermissionManager(emit func(any), mode string) *PermissionManager {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "enforce"
	}
	return &PermissionManager{
		emit: emit, mode: mode, ttl: 60 * time.Second,
		waiters: map[string]chan bool{}, requester: map[string]string{},
	}
}

func (m *PermissionManager) Mode() string { return m.mode }

// Request asks the user to approve an action and blocks until decided/expired.
func (m *PermissionManager) Request(deviceID, action, title, justification, preview, risk, sessionID string) bool {
	switch m.mode {
	case "off":
		return true
	case "warn":
		m.emit(protocol.NewPermissionResult("", sessionID, action, "warn_auto_approved", "permission mode=warn"))
		return true
	}

	if len(preview) > 500 {
		preview = preview[:500]
	}
	rid := "perm_" + permRandHex(12)
	ch := make(chan bool, 1)
	expires := time.Now().Add(m.ttl).UnixMilli()

	m.mu.Lock()
	m.waiters[rid] = ch
	m.requester[rid] = deviceID
	m.mu.Unlock()

	m.emit(protocol.NewPermissionRequest(rid, sessionID, action, title, justification, preview, risk, expires))

	var approved bool
	select {
	case approved = <-ch:
	case <-time.After(m.ttl):
		approved = false
		m.emit(protocol.NewPermissionResult(rid, sessionID, action, "expired", ""))
	}

	m.mu.Lock()
	delete(m.waiters, rid)
	delete(m.requester, rid)
	m.mu.Unlock()
	return approved
}

// Resolve applies the user's decision. The responder device must match the
// requester (a different connected client cannot approve someone's high-risk op).
func (m *PermissionManager) Resolve(requestID, decision, responderDeviceID string) {
	m.mu.Lock()
	ch := m.waiters[requestID]
	requester := m.requester[requestID]
	m.mu.Unlock()
	if ch == nil {
		return // unknown or already resolved/expired
	}
	if responderDeviceID != "" && requester != "" && responderDeviceID != requester {
		m.emit(protocol.NewPermissionResult(requestID, "", "", "denied", "response device mismatch"))
		return
	}
	approved := decision == "approve"
	select {
	case ch <- approved:
	default:
	}
	m.emit(protocol.NewPermissionResult(requestID, "", "", decision, ""))
}

func permRandHex(n int) string {
	b := make([]byte, (n+1)/2)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)[:n]
}
