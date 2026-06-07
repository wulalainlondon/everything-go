package core

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// nopConn is a wireConn that never delivers frames (Read blocks until ctx done)
// and swallows writes — for manually-built test clients.
type nopConn struct{}

func (nopConn) Read(ctx context.Context) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }
func (nopConn) Write(context.Context, []byte) error      { return nil }
func (nopConn) Close(string)                             {}
func (nopConn) Kind() string                             { return "test" }

// pingConn is a wireConn that also satisfies pinger/addressable, with a
// switchable ping outcome so the liveness loop (#11) can be exercised.
type pingConn struct {
	nopConn
	failPing  atomic.Bool
	pingCalls int64
}

func (p *pingConn) Ping(context.Context) error {
	atomic.AddInt64(&p.pingCalls, 1)
	if p.failPing.Load() {
		return context.DeadlineExceeded // simulate an unanswered pong (zombie)
	}
	return nil
}

func (p *pingConn) RemoteAddr() string { return "203.0.113.7:54321" }

// countingProvider counts heavy calls so we can prove coalescing.
type countingProvider struct {
	loadHist  int64
	resumable int64
}

func (p *countingProvider) LoadHistory(string, history.Opts) (*history.Result, error) {
	atomic.AddInt64(&p.loadHist, 1)
	time.Sleep(30 * time.Millisecond) // widen the concurrency window
	return &history.Result{Kind: "snapshot", Messages: []map[string]any{{"x": 1}}, SourceCount: 1, KnownIDFound: true}, nil
}

func (p *countingProvider) ResumableSessions(int) ([]history.ResumableSession, error) {
	atomic.AddInt64(&p.resumable, 1)
	time.Sleep(30 * time.Millisecond)
	return []history.ResumableSession{{ID: "r1", Name: "R"}}, nil
}

// histExec is a fakeExec that also serves history (implements historyRouter).
type histExec struct {
	fakeExec
	prov *countingProvider
}

func (e *histExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.prov, true
}
func (e *histExec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.prov}
}

func newDeviceClient(h *Hub, device string, buf int) *Client {
	return &Client{
		hub: h, conn: nopConn{}, send: make(chan []byte, buf),
		quit: make(chan struct{}), deviceID: device, clientID: randomID(),
	}
}

// #1/#15: five clients from the same device → only the newest is current, and
// the older four are shut down.
func TestLatestDeviceWins(t *testing.T) {
	h, _ := newTestHub(t)
	var clients []*Client
	for i := 0; i < 5; i++ {
		c := newDeviceClient(h, "dev", 16)
		clients = append(clients, c)
		h.registerLatest(c)
	}
	for i, c := range clients {
		if got, want := h.isCurrent(c), i == 4; got != want {
			t.Errorf("client %d isCurrent=%v, want %v", i, got, want)
		}
	}
	for i := 0; i < 4; i++ {
		select {
		case <-clients[i].quit: // good: evicted client was shut down
		case <-time.After(time.Second):
			t.Errorf("client %d should have been shut down on eviction", i)
		}
	}
}

// #3/#15: a stale (replaced) client's heavy handler returns nothing.
func TestStaleClientDropsResults(t *testing.T) {
	h, _ := newTestHub(t)
	h.SetExecutor(&histExec{fakeExec: fakeExec{sink: h}, prov: &countingProvider{}})

	old := newDeviceClient(h, "dev", 16)
	h.registerLatest(old)
	newer := newDeviceClient(h, "dev", 16)
	h.registerLatest(newer) // evicts old

	h.sendResumable(old, 100) // stale → must not enqueue
	select {
	case <-old.send:
		t.Fatal("stale client must not receive results")
	default:
	}
	// the current client still works
	h.sendResumable(newer, 100)
	select {
	case <-newer.send:
	case <-time.After(2 * time.Second):
		t.Fatal("current client should receive resumable_sessions")
	}
}

// #4/#15: 100 concurrent identical request_history → LoadHistory runs once.
func TestHistoryCoalesced(t *testing.T) {
	h, _ := newTestHub(t)
	prov := &countingProvider{}
	h.SetExecutor(&histExec{fakeExec: fakeExec{sink: h}, prov: prov})
	s := h.registry.Create("s1", "n", "/tmp", "claude", "", "", "")
	s.SetResumeID("resume-uuid-1")

	c := newDeviceClient(h, "dev", 256)
	h.registerLatest(c)
	in := protocol.Inbound{Type: "request_history", SessionID: "s1", Mode: "snapshot"}
	cmd := clientproto.NewAppV1().ParseCommand(in)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); h.sendHistory(c, s, cmd) }()
	}
	wg.Wait()
	if n := atomic.LoadInt64(&prov.loadHist); n != 1 {
		t.Fatalf("LoadHistory called %d times for 100 identical requests, want 1", n)
	}
}

// #6/#15: 100 concurrent get_resumable_sessions → provider scan runs once.
func TestResumableCoalesced(t *testing.T) {
	h, _ := newTestHub(t)
	prov := &countingProvider{}
	h.SetExecutor(&histExec{fakeExec: fakeExec{sink: h}, prov: prov})
	c := newDeviceClient(h, "dev", 256)
	h.registerLatest(c)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); h.sendResumable(c, 100) }()
	}
	wg.Wait()
	if n := atomic.LoadInt64(&prov.resumable); n != 1 {
		t.Fatalf("ResumableSessions scanned %d times for 100 requests, want 1", n)
	}
}

// #10/#15: a client whose send buffer overflows is dropped (quit closed), and
// the hub stays healthy for other clients.
func TestSendOverflowDropsClient(t *testing.T) {
	h, _ := newTestHub(t)
	c := newDeviceClient(h, "slow", 2) // tiny buffer, no write pump draining it
	h.addClient(c)
	for i := 0; i < 10; i++ {
		c.enqueue([]byte("x"))
	}
	select {
	case <-c.quit:
	case <-time.After(time.Second):
		t.Fatal("overflowing client should be dropped (quit closed)")
	}
	// hub unaffected: a fresh client can still be registered + enqueued.
	ok := newDeviceClient(h, "fast", 16)
	h.addClient(ok)
	ok.enqueue([]byte("y"))
	select {
	case <-ok.send:
	case <-time.After(time.Second):
		t.Fatal("healthy client should still receive after another was dropped")
	}
}

// #11: a zombie socket (ping unanswered) is detected and dropped even though it
// never sends a fresh hello.
func TestPingZombieDropped(t *testing.T) {
	h, _ := newTestHub(t)
	pc := &pingConn{}
	pc.failPing.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Client{
		hub: h, conn: pc, send: make(chan []byte, 4),
		quit: make(chan struct{}), ctx: ctx, cancel: cancel,
		deviceID: "dev", clientID: randomID(),
	}
	go c.pingLoopEvery(ctx, 5*time.Millisecond, 5*time.Millisecond)
	select {
	case <-c.quit: // good: zombie torn down
	case <-time.After(2 * time.Second):
		t.Fatal("zombie client (failed ping) should have been dropped")
	}
}

// #11: a responsive client is never dropped by the ping loop, and the loop is a
// no-op for a transport without ping (nopConn).
func TestPingHealthyAndNoop(t *testing.T) {
	h, _ := newTestHub(t)

	// healthy: pings succeed → stays alive across several intervals.
	pc := &pingConn{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Client{
		hub: h, conn: pc, send: make(chan []byte, 4),
		quit: make(chan struct{}), ctx: ctx, cancel: cancel,
		deviceID: "dev", clientID: randomID(),
	}
	go c.pingLoopEvery(ctx, 5*time.Millisecond, time.Second)
	time.Sleep(60 * time.Millisecond)
	select {
	case <-c.quit:
		t.Fatal("healthy client must not be dropped by the ping loop")
	default:
	}
	if n := atomic.LoadInt64(&pc.pingCalls); n == 0 {
		t.Fatal("ping loop should have probed at least once")
	}

	// no-op: a transport without Ping returns immediately (no goroutine leak,
	// client stays alive).
	nc := newDeviceClient(h, "dev2", 4)
	done := make(chan struct{})
	go func() { nc.pingLoopEvery(context.Background(), time.Millisecond, time.Millisecond); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pingLoop on a non-pingable transport should return immediately")
	}
}
