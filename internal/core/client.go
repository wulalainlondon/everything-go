package core

import (
	"context"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
)

// sendQueue bounds per-client outbound backpressure. If a client falls this far
// behind, it is dropped rather than letting the buffer grow unbounded.
const sendQueue = 1024

// Server-side liveness probe (#11). The app drives its own heartbeat, but a
// half-dead socket (TCP still open, app backgrounded/gone) never sends a fresh
// hello, so latest-device-wins can't evict it. An unanswered ping detects the
// zombie and drops it. pingInterval is well under any NAT/idle timeout; a single
// missed pong window (pingTimeout) is enough to declare the peer gone.
const (
	handshakeTimeout = 10 * time.Second
	pingInterval     = 30 * time.Second
	pingTimeout      = 10 * time.Second
)

// wireConn is the minimal transport contract the Client runs over: read one
// frame, write one frame, close. Both the WebSocket (wsConn) and a promoted
// WebRTC DataChannel (dcConn) satisfy it, so the same handshake + route loop
// drives a client regardless of whether traffic arrives over the LAN/tunnel WS
// or a P2P DataChannel.
type wireConn interface {
	Read(ctx context.Context) ([]byte, error)
	Write(ctx context.Context, data []byte) error
	Close(reason string)
	Kind() string // "ws" | "webrtc" — for logging only
}

// pinger is the optional contract a transport implements when it supports an
// application-level liveness probe (WS ping/pong). serveConn starts a ping loop
// only for transports that satisfy it — a WebRTC DataChannel (dcConn) does not,
// so it is left to Pion's own ICE keepalive.
type pinger interface {
	Ping(ctx context.Context) error
}

// addressable is the optional contract a transport implements when it can report
// a remote peer address, surfaced in connection logs for observability.
type addressable interface {
	RemoteAddr() string
}

// wsConn adapts coder/websocket to wireConn. Frames are always text (the wire
// protocol is JSON); the read/write message type is fixed.
type wsConn struct {
	c    *websocket.Conn
	addr string // r.RemoteAddr captured at accept time, for logging
}

func (w wsConn) Read(ctx context.Context) ([]byte, error) {
	_, data, err := w.c.Read(ctx)
	return data, err
}

func (w wsConn) Write(ctx context.Context, data []byte) error {
	return w.c.Write(ctx, websocket.MessageText, data)
}

func (w wsConn) Close(reason string) { w.c.Close(websocket.StatusNormalClosure, reason) }

func (w wsConn) Kind() string { return "ws" }

// Ping sends a WS ping and blocks until the matching pong arrives or ctx fires;
// coder/websocket reads the pong on the same conn the route loop is draining.
func (w wsConn) Ping(ctx context.Context) error { return w.c.Ping(ctx) }

func (w wsConn) RemoteAddr() string { return w.addr }

// Client is one logical connection (WS or WebRTC DataChannel). A single write
// pump goroutine drains the send channel so conn writes are never concurrent.
type Client struct {
	hub  *Hub
	conn wireConn
	send chan []byte
	// quit is closed exactly once when the client is torn down. The send channel
	// is deliberately NEVER closed: background goroutines (sendHistory, sendUsage,
	// …) outlive the read loop and may call enqueue after disconnect, so closing
	// send would risk a send-on-closed-channel panic that crashes the process.
	// They observe quit instead and drop silently. Mirrors the mailbox fix.
	quit      chan struct{}
	closeOnce sync.Once

	// ctx is cancelled when the client is torn down (disconnect, or replaced by a
	// newer client from the same device). Heavy background handlers gate on it so
	// a stale client's work is abandoned rather than computed and dropped.
	ctx    context.Context
	cancel context.CancelFunc

	clientID string
	deviceID string

	// rtc holds the answering peer connection negotiated over this client's
	// signaling channel, if any. Set on webrtc_offer; consulted by webrtc_ice
	// and torn down on disconnect (unless the DataChannel was promoted, in
	// which case the DC's own lifecycle owns the PC).
	rtc *webrtcPeer
}

func (c *Client) enqueue(data []byte) {
	select {
	case c.send <- data:
	case <-c.quit:
		// Client already torn down: drop silently. Never a send-on-closed panic
		// because send is never closed.
	default:
		// Slow client: drop the connection instead of blocking the hub. Log
		// enough to spot a storm (which device/transport, how deep the queue).
		log.Printf("[storm] send buffer full → dropping client=%s device=%s kind=%s queued=%d/%d",
			c.clientID, c.deviceID, c.conn.Kind(), len(c.send), cap(c.send))
		c.conn.Close("send buffer overflow")
		c.shutdown()
	}
}

// shutdown signals teardown to the write pump and any background enqueuers, and
// cancels the client context so heavy handlers abandon. Safe to call repeatedly
// and from multiple goroutines.
func (c *Client) shutdown() {
	c.closeOnce.Do(func() {
		close(c.quit)
		if c.cancel != nil {
			c.cancel()
		}
	})
}

// live reports whether this client should still receive results: not torn down
// AND still the current (latest) client for its device. Heavy handlers check it
// before enqueueing so a replaced/stale client's work is dropped at the boundary.
func (c *Client) live() bool {
	select {
	case <-c.quit:
		return false
	default:
	}
	return c.hub.isCurrent(c)
}

// remoteAddr returns the transport's peer address when known, else "" — used
// only in connection logs.
func (c *Client) remoteAddr() string {
	if a, ok := c.conn.(addressable); ok {
		return a.RemoteAddr()
	}
	return ""
}

// pingLoop probes the transport's liveness on an interval and tears the client
// down if a pong does not arrive within pingTimeout. It is a no-op for transports
// without an application-level ping (e.g. a WebRTC DataChannel). Exits on
// teardown (quit) or context cancellation.
func (c *Client) pingLoop(ctx context.Context) {
	c.pingLoopEvery(ctx, pingInterval, pingTimeout)
}

// pingLoopEvery is pingLoop parameterized on cadence so tests can drive it fast.
func (c *Client) pingLoopEvery(ctx context.Context, interval, timeout time.Duration) {
	p, ok := c.conn.(pinger)
	if !ok {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.quit:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, timeout)
			err := p.Ping(pctx)
			cancel()
			if err != nil {
				select {
				case <-c.quit: // already being torn down; not a zombie
					return
				default:
				}
				log.Printf("[conn] ping timeout client=%s device=%s kind=%s addr=%s → dropping zombie",
					c.clientID, c.deviceID, c.conn.Kind(), c.remoteAddr())
				c.conn.Close("ping timeout")
				c.shutdown()
				return
			}
		}
	}
}

// ServeWS upgrades an HTTP request to a WebSocket and runs the client until the
// connection closes.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // app connects from arbitrary LAN origins
	})
	if err != nil {
		log.Printf("ws accept error: %v", err)
		return
	}
	conn.SetReadLimit(32 * 1024 * 1024)
	h.serveConn(context.Background(), wsConn{c: conn, addr: r.RemoteAddr})
}

// serveConn runs the full client lifecycle over an arbitrary transport: the
// handshake gate, the write pump, the initial hello route, then the read loop.
// On exit it deregisters the client, tears down any non-promoted WebRTC peer,
// and closes the transport. Shared by the WS path and the WebRTC DataChannel
// takeover path.
func (h *Hub) serveConn(ctx context.Context, conn wireConn) {
	cctx, cancel := context.WithCancel(ctx)
	c := &Client{
		hub:      h,
		conn:     conn,
		send:     make(chan []byte, sendQueue),
		quit:     make(chan struct{}),
		ctx:      cctx,
		cancel:   cancel,
		clientID: randomID(),
	}

	// Governance boundary: the first frame MUST be a valid hello carrying an
	// accepted auth token (when the bridge is locked or BRIDGE_AUTH_TOKEN is
	// set). A connection that fails the handshake never reaches the router and
	// is never registered, so no command can run on it. Mirrors the Python
	// bridge's handshake in handlers/connection.py.
	handshakeCtx, cancelHandshake := context.WithTimeout(ctx, handshakeTimeout)
	hello, ok := c.handshake(handshakeCtx)
	cancelHandshake()
	if !ok {
		// Close with normal closure to match the Python bridge, which rejects a
		// bad handshake by sending the error frame and returning. Parity matters:
		// the app surfaces the close code in its connection test, so an identical
		// code keeps the A/B experience identical.
		conn.Close("handshake rejected")
		return
	}

	h.addClient(c)
	log.Printf("[conn] connected client=%s kind=%s device=%s addr=%s", c.clientID, conn.Kind(), hello.DeviceID, c.remoteAddr())

	go c.writePump(ctx)
	go c.pingLoop(ctx)     // server-side liveness probe; no-op for non-pingable transports
	h.route(ctx, c, hello) // process the validated hello → hello_ack + sessions_list + replay
	closeReason := c.readLoop(ctx)

	h.removeClient(c)
	h.cleanupWebRTC(c) // drop the answering PC unless its DataChannel was promoted
	c.shutdown()       // stop the write pump; background enqueuers now drop silently
	conn.Close("")
	log.Printf("[conn] disconnected client=%s kind=%s device=%s addr=%s reason=%v", c.clientID, conn.Kind(), c.deviceID, c.remoteAddr(), closeReason)
}

// handshake reads and validates the first frame. It must be a well-formed hello
// and, when the bridge is locked or BRIDGE_AUTH_TOKEN is set, carry a matching
// auth token. On success it returns the parsed hello for the caller to route;
// on failure it writes a protocol error directly (the write pump is not running
// yet) and returns false.
func (c *Client) handshake(ctx context.Context) (clientproto.Command, bool) {
	data, err := c.conn.Read(ctx)
	if err != nil {
		return clientproto.Command{}, false
	}
	in, err := protocol.ParseInbound(data)
	if err != nil || in.Type != "hello" {
		c.writeNow(ctx, protocol.NewError("", "", "Protocol error: first message must be hello"))
		return clientproto.Command{}, false
	}
	if !c.hub.authValid(strings.TrimSpace(in.AuthToken)) {
		c.writeNow(ctx, protocol.NewError("", "", "Unauthorized: invalid auth token"))
		return clientproto.Command{}, false
	}
	logInbound(in.Type, in.SessionID)
	return c.hub.client.ParseCommand(in), true
}

// writeNow sends a single event synchronously, used during the handshake before
// the write pump owns the connection. Safe because no other goroutine writes the
// conn at this point.
func (c *Client) writeNow(ctx context.Context, event any) {
	logOutbound(event)
	data, err := marshalEvent(event)
	if err != nil {
		return
	}
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = c.conn.Write(wctx, data)
}

func (c *Client) writePump(ctx context.Context) {
	for {
		select {
		case <-c.quit:
			return
		case data := <-c.send:
			wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := c.conn.Write(wctx, data)
			cancel()
			if err != nil {
				c.shutdown() // unblock enqueuers waiting on a dead socket
				return
			}
		}
	}
}

// readLoop consumes inbound frames until the transport errors, returning that
// error as the close reason (for observability).
func (c *Client) readLoop(ctx context.Context) error {
	for {
		data, err := c.conn.Read(ctx)
		if err != nil {
			return err
		}
		in, err := protocol.ParseInbound(data)
		if err != nil {
			log.Printf("client %s: bad frame: %v", c.clientID, err)
			continue
		}
		logInbound(in.Type, in.SessionID)
		c.hub.route(ctx, c, c.hub.client.ParseCommand(in))
	}
}
