// Package core is the Go connection layer: WebSocket termination, the client
// registry, envelope routing, and session management. It is written against the
// executor.Executor interface and knows nothing about how turns are actually
// run — that is the seam that lets the same core back configs 2 and 3.
package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/executor"
	"everything-go/internal/fcm"
	"everything-go/internal/feed"
	"everything-go/internal/governance"
	"everything-go/internal/inbox"
	"everything-go/internal/media"
	"everything-go/internal/nativewatch"
	"everything-go/internal/protocol"
	"everything-go/internal/runtime"
	"everything-go/internal/search"
	"everything-go/internal/session"
)

// Config carries the connection-identity fields surfaced in hello_ack so the
// app can label the bridge and respect its filesystem jail.
type Config struct {
	InstanceName string
	InstanceID   string
	RootDir      string
	DataDir      string
	LanIP        string
	TailscaleIP  string
	Backends     []backend.Definition
}

// Hub owns the set of connected clients and the session registry, and acts as
// the executor.Sink (Emit broadcasts an event to connected clients, or buffers
// it when none are connected so a reconnecting client can recover it).
type Hub struct {
	registry  *session.Registry
	exec      executor.Executor
	shells    *runtime.ShellManager
	pairing   *governance.Pairing
	perms     *governance.PermissionManager
	offline   *governance.OfflineBuffer
	goals     *governance.GoalStateStore
	search    *search.Index
	fcm       *fcm.Notifier
	feed      *feed.Store
	inbox     *inbox.Store
	mediaScan *media.Scanner
	cfg       Config
	client    clientproto.AppV1
	gen       string // per-boot generation id

	iceServers []webrtc.ICEServer // STUN/TURN for WebRTC answers (default: Google STUN)

	mu      sync.RWMutex
	clients map[*Client]struct{}

	// latestByDevice maps device_id → its newest client. A single device keeps
	// exactly one live client; a new connection from the same device evicts the
	// old one (the mobile half-disconnect "storm" otherwise piles up zombies).
	// This also restores parity with Python's ws_ref-rebind (latest socket wins).
	latestMu       sync.Mutex
	latestByDevice map[string]*Client

	turnMu   sync.Mutex
	turnText map[string]*strings.Builder // session_id -> assistant text this turn

	storm *stormGuards // dedupe/throttle/semaphore for heavy handlers

	// nativeDirty is set by the native-session watcher when an import changes
	// the registry. A single coalescer goroutine drains it on a timer so a
	// startup scan of hundreds of transcripts produces ONE persist + ONE
	// sessions_list broadcast instead of hundreds (which OOM-killed the app).
	nativeDirty atomic.Bool

	// tunnelURL is the current cloudflared public URL, set by NotifyTunnelURL
	// and included in hello_ack so reconnecting clients always get the latest URL
	// even if they missed the FCM push when the tunnel started.
	tunnelURLMu sync.RWMutex
	tunnelURL   string

	// restart, if set, actually restarts the bridge (wired in main to a
	// self-re-exec). nil → restart_bridge answers "not configured", mirroring
	// Python's gate on an unset restart-trigger path.
	restart func()

	replayMu    sync.Mutex
	replayLease *replayLease
}

func NewHub(reg *session.Registry, cfg Config, pairing *governance.Pairing, port int) *Hub {
	h := &Hub{
		registry:       reg,
		pairing:        pairing,
		offline:        governance.NewOfflineBuffer(),
		goals:          governance.NewGoalStateStore(goalSnapshotPath(cfg.DataDir)),
		cfg:            cfg,
		client:         clientproto.NewAppV1(),
		gen:            randomID(),
		clients:        make(map[*Client]struct{}),
		latestByDevice: make(map[string]*Client),
		turnText:       make(map[string]*strings.Builder),
		iceServers:     stunServers,
		storm:          newStormGuards(),
		mediaScan:      media.NewScanner(port),
	}
	if cfg.TailscaleIP != "" {
		h.mediaScan.SetTailscaleIP(cfg.TailscaleIP)
	}
	if cfg.LanIP != "" {
		h.mediaScan.SetLanIP(cfg.LanIP)
	}
	// Shell output is broadcast to connected clients via the Hub sink.
	h.shells = runtime.NewShellManager(h.Emit)
	// Permission gate for high-risk ops (kill_process / shell_input); broadcasts
	// permission_request/result via the hub. Mode from BRIDGE_PERMISSION_MODE
	// (default enforce, mirroring Python prod).
	h.perms = governance.NewPermissionManager(h.Emit, os.Getenv("BRIDGE_PERMISSION_MODE"))
	return h
}

// SetExecutor wires the backend after construction (the executor needs the Hub
// as its Sink, so the Hub is built first).
func (h *Hub) SetExecutor(e executor.Executor) { h.exec = e }

// authValid mirrors bridge_v2.py:_is_auth_token_valid. BRIDGE_AUTH_TOKEN (a
// manual override) takes priority; otherwise, if the bridge is claimed, only the
// paired token is accepted; an unclaimed bridge accepts everyone. provided is
// expected pre-trimmed.
func (h *Hub) authValid(provided string) bool {
	if expected := strings.TrimSpace(os.Getenv("BRIDGE_AUTH_TOKEN")); expected != "" {
		return provided != "" && provided == expected
	}
	if h.pairing.IsLocked() {
		return h.pairing.LockedTo(provided)
	}
	return true
}

// SetSearch wires the search index (nil disables the search command family).
func (h *Hub) SetSearch(s *search.Index) { h.search = s }

// SetFCM wires the push notifier (nil disables push).
func (h *Hub) SetFCM(n *fcm.Notifier) { h.fcm = n }

// SetTunnelURL updates the tunnel base URL used when building media/document
// URLs in scan results. Call this whenever the tunnel address changes.
func (h *Hub) SetTunnelURL(wsURL string) {
	h.mediaScan.SetTunnelURL(wsURL)
}

// NotifyTunnelURL pushes the new tunnel URL to the device via FCM, updates
// the media scanner, and stores it so reconnecting clients get it in hello_ack.
func (h *Hub) NotifyTunnelURL(wsURL string) {
	h.tunnelURLMu.Lock()
	h.tunnelURL = wsURL
	h.tunnelURLMu.Unlock()
	h.mediaScan.SetTunnelURL(wsURL)
	if h.fcm == nil {
		return
	}
	go h.fcm.NotifyTunnelURL(wsURL, h.cfg.InstanceID)
}

// SetFeed wires the feed store (nil → feed commands answer empty/no-op).
func (h *Hub) SetFeed(f *feed.Store) { h.feed = f }

// SetInbox wires the file-push inbox (nil → push_file/get_inbox answer empty/no-op).
func (h *Hub) SetInbox(i *inbox.Store) { h.inbox = i }

// SetRestart wires the bridge-restart action (nil → restart_bridge is a no-op
// that answers "not configured"). main wires this to a self-re-exec.
func (h *Hub) SetRestart(fn func()) { h.restart = fn }

// StartNativeWatcher imports native Claude/Codex CLI JSONL sessions into the
// bridge registry so desktop CLI work appears in the app's normal session list.
func (h *Hub) StartNativeWatcher(ctx context.Context) {
	if os.Getenv("EVERYTHING_GO_NATIVE_WATCH") == "0" {
		log.Printf("[nativewatch] disabled by EVERYTHING_GO_NATIVE_WATCH=0")
		return
	}
	opts := nativewatch.DefaultOptions()
	if v := strings.TrimSpace(os.Getenv("EVERYTHING_GO_NATIVE_POLL_INTERVAL")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			opts.PollInterval = d
		}
	}
	if v := strings.TrimSpace(os.Getenv("EVERYTHING_GO_NATIVE_DEBOUNCE")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			opts.Debounce = d
		}
	}
	if v := strings.TrimSpace(os.Getenv("EVERYTHING_GO_NATIVE_LOOKBACK")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			opts.InitialLookback = d
		}
	}

	// Coalescer: a startup scan imports hundreds of transcripts back-to-back.
	// Broadcasting a full sessions_list per import floods the app (each summary
	// re-queries the search index for previews/last_ts) and OOM-kills it. The
	// watcher callback only flags dirty; this goroutine emits at most once per
	// tick — one persist + one broadcast no matter how many imports landed.
	go func() {
		t := time.NewTicker(1500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if h.nativeDirty.Swap(false) {
					h.registry.Persist()
					h.Emit(h.client.SessionsList(h.sessionSummaries()))
				}
			}
		}
	}()

	go nativewatch.Watch(ctx, opts, func(ns nativewatch.NativeSession) {
		if h.cfg.RootDir != "" && !pathInsideRoot(ns.Cwd, h.cfg.RootDir) {
			return
		}
		_, changed := h.registry.UpsertExternal(ns.ID, ns.Name, ns.Cwd, ns.Backend, ns.ResumeID, ns.LastUsed)
		if !changed {
			return
		}
		log.Printf("[nativewatch] imported %s resume=%s cwd=%q", ns.Backend, ns.ResumeID, ns.Cwd)
		h.nativeDirty.Store(true)
	})
}

func pathInsideRoot(path, root string) bool {
	if path == "" || root == "" {
		return false
	}
	rel, err := filepath.Rel(realpath(root), realpath(path))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// connectedDeviceIDs returns the distinct device ids currently connected, except
// `exclude`. Used to target a file push at every device but the sender.
func (h *Hub) connectedDeviceIDs(exclude string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := map[string]bool{}
	out := []string{}
	for c := range h.clients {
		d := c.deviceID
		if d == "" || d == exclude || seen[d] {
			continue
		}
		seen[d] = true
		out = append(out, d)
	}
	return out
}

// Emit implements executor.Sink. With clients connected it marshals the event
// once and delivers it to every client. With none connected it buffers the
// event for replay on the next reconnect (the offline-recovery path). Safe for
// concurrent use.
func (h *Hub) Emit(event any) {
	logOutbound(event)
	if h.goals.Apply(event) {
		switch e := event.(type) {
		case protocol.GoalUpdate:
			log.Printf("[goal] snapshot session=%s status=%s updated_at=%d", e.SessionID, e.Goal.Status, e.Goal.UpdatedAt)
		case protocol.GoalCleared:
			log.Printf("[goal] snapshot cleared session=%s", e.SessionID)
		}
	}

	// The Hub is the single point every executor event flows through, so it is
	// where session lifecycle state is driven: a terminal event ends the turn
	// (releasing the per-session queue). State is never mutated by the backends.
	h.driveTurnState(event)

	// Accumulate assistant text per turn so a push notification can carry a
	// summary when the turn completes (mirrors the Python notify_fcm payload).
	h.accumulateTurn(event)

	h.mu.RLock()
	n := len(h.clients)
	h.mu.RUnlock()

	if n == 0 {
		h.offline.Append(event)
	} else {
		data, err := json.Marshal(event)
		if err != nil {
			log.Printf("emit marshal error: %v", err)
			return
		}
		h.mu.RLock()
		for c := range h.clients {
			c.enqueue(data)
		}
		h.mu.RUnlock()
	}

	// Persist on the events that change durable session state: a new resume id
	// (session_uuid) or a completed turn (done) — mirrors the Python bridge's
	// persist-on-turn-complete trigger.
	switch event.(type) {
	case protocol.SessionUUID, protocol.Done:
		go h.registry.Persist()
	}
}

// replayOffline flushes buffered events to a single reconnecting client, in
// order. Mirrors bridge/offline_replay.py (called after sessions_list).
func (h *Hub) replayOffline(c *Client) {
	h.startOfflineReplay(c)
}

// driveTurnState advances the session state machine off the executor's terminal
// events. done/stopped/error end the in-flight turn, which releases the
// session's turn worker to run the next queued message. Idempotent per turn.
func (h *Hub) driveTurnState(event any) {
	var sessionID string
	switch e := event.(type) {
	case protocol.Done:
		sessionID = e.SessionID
	case protocol.Stopped:
		sessionID = e.SessionID
	case protocol.Error:
		sessionID = e.SessionID
	default:
		return
	}
	if sessionID == "" {
		return
	}
	if s, ok := h.registry.Get(sessionID); ok {
		s.EndTurn()
	}
}

// accumulateTurn tracks assistant text_chunks per session and, on the turn's
// done event, fires a task-done push with the accumulated summary. stopped
// clears the buffer without notifying.
func (h *Hub) accumulateTurn(event any) {
	switch e := event.(type) {
	case protocol.TextChunk:
		h.turnMu.Lock()
		b := h.turnText[e.SessionID]
		if b == nil {
			b = &strings.Builder{}
			h.turnText[e.SessionID] = b
		}
		b.WriteString(e.Content)
		h.turnMu.Unlock()
	case protocol.Done:
		h.turnMu.Lock()
		b := h.turnText[e.SessionID]
		delete(h.turnText, e.SessionID)
		h.turnMu.Unlock()

		var text string
		if b != nil {
			text = b.String()
		}

		// Scan for media/document paths regardless of FCM being configured.
		h.scanAndEmitMedia(e.SessionID, e.RequestID, text)

		if h.fcm == nil || text == "" {
			return
		}
		name := e.SessionID
		if s, ok := h.registry.Get(e.SessionID); ok {
			if n := s.Name(); n != "" {
				name = n
			}
		}
		go h.fcm.NotifyTaskDone(name, text, e.SessionID)
	case protocol.Stopped:
		h.turnMu.Lock()
		delete(h.turnText, e.SessionID)
		h.turnMu.Unlock()
	}
}

// scanAndEmitMedia scans accumulated turn text for file paths and emits
// protocol.Media / protocol.Document events for any that exist on disk.
// Mirrors Python bridge's scan_for_media, called at the same time as FCM notify.
func (h *Hub) scanAndEmitMedia(sessionID, requestID, text string) {
	if text == "" {
		return
	}
	var cwd string
	if s, ok := h.registry.Get(sessionID); ok {
		cwd = s.Cwd()
	}
	results := h.mediaScan.Scan(text, sessionID, requestID, cwd)
	log.Printf("[media] scan session=%s textLen=%d cwd=%q found=%d", sessionID, len(text), cwd, len(results))
	for _, r := range results {
		switch v := r.(type) {
		case protocol.Media:
			log.Printf("[media] emit type=media url=%s req=%s", v.URL, v.RequestID)
		case protocol.Document:
			log.Printf("[media] emit type=document url=%s req=%s", v.URL, v.RequestID)
		}
		h.Emit(r)
	}
}

func (h *Hub) addClient(c *Client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) removeClient(c *Client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	// Drop the latest-device pointer only if it still points at this client (a
	// newer client may have already replaced it).
	if c.deviceID != "" {
		h.latestMu.Lock()
		if h.latestByDevice[c.deviceID] == c {
			delete(h.latestByDevice, c.deviceID)
		}
		h.latestMu.Unlock()
	}
	h.releaseReplayLease(c)
}

func goalSnapshotPath(dataDir string) string {
	if strings.TrimSpace(dataDir) == "" {
		return ""
	}
	return filepath.Join(dataDir, "goal_snapshots.json")
}

func marshalEvent(event any) ([]byte, error) {
	return json.Marshal(event)
}

func randomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
