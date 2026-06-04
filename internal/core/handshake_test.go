package core

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func (h *Hub) clientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func TestAuthValidUnlockedAcceptsAll(t *testing.T) {
	h, _ := newTestHub(t)
	if !h.authValid("") || !h.authValid("anything") {
		t.Fatal("an unlocked bridge must accept any token")
	}
}

func TestAuthValidLockedEnforcesToken(t *testing.T) {
	h, _ := newTestHub(t)
	if err := h.pairing.Claim("tok", "dev1"); err != nil {
		t.Fatal(err)
	}
	if h.authValid("") {
		t.Fatal("locked bridge must reject an empty token")
	}
	if h.authValid("wrong") {
		t.Fatal("locked bridge must reject a mismatched token")
	}
	if !h.authValid("tok") {
		t.Fatal("locked bridge must accept the paired token")
	}
}

func TestAuthValidEnvOverrideWins(t *testing.T) {
	h, _ := newTestHub(t)
	t.Setenv("BRIDGE_AUTH_TOKEN", "secret")
	// Even when also paired to a different token, the env override decides.
	_ = h.pairing.Claim("paired", "dev1")
	if !h.authValid("secret") {
		t.Fatal("env token must be accepted")
	}
	if h.authValid("paired") || h.authValid("") {
		t.Fatal("with BRIDGE_AUTH_TOKEN set, only that token is valid")
	}
}

// --- handshake integration (real WS over httptest) -------------------------

func dialWS(t *testing.T, h *Hub) (*websocket.Conn, context.Context, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		srv.Close()
		cancel()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		conn.Close(websocket.StatusNormalClosure, "")
		srv.Close()
		cancel()
	}
	return conn, ctx, cleanup
}

func readEvent(t *testing.T, ctx context.Context, conn *websocket.Conn) map[string]any {
	t.Helper()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	return m
}

func TestHandshakeRejectsNonHelloFirstFrame(t *testing.T) {
	h, _ := newTestHub(t)
	conn, ctx, cleanup := dialWS(t, h)
	defer cleanup()

	_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"ping"}`))
	m := readEvent(t, ctx, conn)
	if m["type"] != "error" || !strings.Contains(m["message"].(string), "first message must be hello") {
		t.Fatalf("expected first-message-must-be-hello error, got %v", m)
	}
	// The connection must not have been registered.
	if h.clientCount() != 0 {
		t.Fatal("a rejected handshake must not register a client")
	}
}

func TestHandshakeRejectsWrongTokenWhenLocked(t *testing.T) {
	h, _ := newTestHub(t)
	if err := h.pairing.Claim("right", "dev1"); err != nil {
		t.Fatal(err)
	}
	conn, ctx, cleanup := dialWS(t, h)
	defer cleanup()

	_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","device_id":"d1","auth_token":"wrong"}`))
	m := readEvent(t, ctx, conn)
	if m["type"] != "error" || !strings.Contains(m["message"].(string), "Unauthorized") {
		t.Fatalf("expected Unauthorized error, got %v", m)
	}
	if h.clientCount() != 0 {
		t.Fatal("an unauthorized handshake must not register a client")
	}
}

func TestHandshakeAcceptsValidHello(t *testing.T) {
	h, _ := newTestHub(t)
	conn, ctx, cleanup := dialWS(t, h)
	defer cleanup()

	_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","device_id":"d1"}`))
	m := readEvent(t, ctx, conn)
	if m["type"] != "hello_ack" {
		t.Fatalf("valid hello should get hello_ack, got %v", m)
	}
	// hello_ack is followed by the proactive sessions_list.
	m2 := readEvent(t, ctx, conn)
	if m2["type"] != "sessions_list" {
		t.Fatalf("expected sessions_list after hello_ack, got %v", m2)
	}
}

func TestHandshakeAcceptsPairedTokenWhenLocked(t *testing.T) {
	h, _ := newTestHub(t)
	if err := h.pairing.Claim("right", "dev1"); err != nil {
		t.Fatal(err)
	}
	conn, ctx, cleanup := dialWS(t, h)
	defer cleanup()

	_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","device_id":"d1","auth_token":"right"}`))
	m := readEvent(t, ctx, conn)
	if m["type"] != "hello_ack" {
		t.Fatalf("paired token should pass handshake, got %v", m)
	}
	if m["locked_to_me"] != true {
		t.Fatalf("hello_ack should report locked_to_me=true for the owner, got %v", m)
	}
}

func TestHandshakeRespectsContextTimeout(t *testing.T) {
	h, _ := newTestHub(t)
	c := &Client{hub: h, conn: nopConn{}, send: make(chan []byte, 1), quit: make(chan struct{}), clientID: "timeout"}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, ok := c.handshake(ctx); ok {
		t.Fatal("handshake with no first frame must fail")
	}
	if time.Since(start) > time.Second {
		t.Fatal("handshake ignored context timeout")
	}
}
