package governance

import (
	"sync"
	"testing"
	"time"

	"everything-go/internal/protocol"
)

type capEmit struct {
	mu sync.Mutex
	ev []any
}

func (c *capEmit) emit(e any) { c.mu.Lock(); c.ev = append(c.ev, e); c.mu.Unlock() }

func (c *capEmit) waitRequestID(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for _, e := range c.ev {
			if r, ok := e.(protocol.PermissionRequest); ok {
				c.mu.Unlock()
				return r.RequestID
			}
		}
		c.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("no permission_request emitted")
	return ""
}

func (c *capEmit) count(match func(any) bool) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.ev {
		if match(e) {
			n++
		}
	}
	return n
}

func TestPermissionOffAlwaysAllows(t *testing.T) {
	c := &capEmit{}
	m := NewPermissionManager(c.emit, "off")
	if !m.Request("d1", "kill_process", "t", "j", "p", "high", "") {
		t.Fatal("off mode must allow")
	}
	if c.count(func(e any) bool { _, ok := e.(protocol.PermissionRequest); return ok }) != 0 {
		t.Fatal("off mode must not prompt")
	}
}

func TestPermissionWarnAutoApproves(t *testing.T) {
	c := &capEmit{}
	m := NewPermissionManager(c.emit, "warn")
	if !m.Request("d1", "a", "t", "j", "p", "high", "") {
		t.Fatal("warn mode must allow")
	}
	if c.count(func(e any) bool {
		r, ok := e.(protocol.PermissionResult)
		return ok && r.Decision == "warn_auto_approved"
	}) != 1 {
		t.Fatal("warn mode must emit warn_auto_approved result")
	}
}

func TestPermissionEnforceApproveDeny(t *testing.T) {
	for _, tc := range []struct {
		decision string
		want     bool
	}{{"approve", true}, {"deny", false}} {
		c := &capEmit{}
		m := NewPermissionManager(c.emit, "enforce")
		res := make(chan bool, 1)
		go func() { res <- m.Request("d1", "a", "t", "j", "p", "high", "") }()
		rid := c.waitRequestID(t)
		m.Resolve(rid, tc.decision, "d1")
		select {
		case got := <-res:
			if got != tc.want {
				t.Fatalf("decision %q → got %v, want %v", tc.decision, got, tc.want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Request did not return after %q", tc.decision)
		}
	}
}

func TestPermissionDeviceMismatchIgnored(t *testing.T) {
	c := &capEmit{}
	m := NewPermissionManager(c.emit, "enforce")
	res := make(chan bool, 1)
	go func() { res <- m.Request("owner", "a", "t", "j", "p", "high", "") }()
	rid := c.waitRequestID(t)

	// A different device must NOT be able to resolve it.
	m.Resolve(rid, "approve", "attacker")
	select {
	case <-res:
		t.Fatal("mismatched-device approval must not resolve the request")
	case <-time.After(150 * time.Millisecond):
	}
	// The real owner can.
	m.Resolve(rid, "approve", "owner")
	select {
	case got := <-res:
		if !got {
			t.Fatal("owner approval should approve")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("owner approval did not resolve")
	}
}

func TestPermissionTimeoutDenies(t *testing.T) {
	c := &capEmit{}
	m := NewPermissionManager(c.emit, "enforce")
	m.ttl = 40 * time.Millisecond
	if m.Request("d1", "a", "t", "j", "p", "high", "") {
		t.Fatal("timeout must deny")
	}
	if c.count(func(e any) bool {
		r, ok := e.(protocol.PermissionResult)
		return ok && r.Decision == "expired"
	}) != 1 {
		t.Fatal("timeout must emit an expired result")
	}
}
