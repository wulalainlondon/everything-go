// Command everything-go is a Go re-implementation of the bridge that speaks the
// same external WebSocket protocol as the Python bridge, so the same React app
// can connect to either for A/B/C stability comparison.
//
// The connection core is fixed; --executor selects what runs the AI workload:
//
//	go      pure-Go executor (config 2)
//	python  forward to a Python worker over a socket (config 3) — not yet wired
package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/core"
	"everything-go/internal/eventinbox"
	"everything-go/internal/executor"
	"everything-go/internal/executor/goexec"
	"everything-go/internal/executor/remote"
	"everything-go/internal/fcm"
	"everything-go/internal/feed"
	"everything-go/internal/filetransfer"
	"everything-go/internal/governance"
	"everything-go/internal/inbox"
	"everything-go/internal/media"
	"everything-go/internal/netsvc"
	"everything-go/internal/search"
	"everything-go/internal/session"
	"everything-go/internal/sourcepolicy"
	"everything-go/internal/workitems"
)

//go:embed keys/fcm_service_account.json
var embeddedFCMKey []byte

func main() {
	port := flag.Int("port", 8767, "WebSocket listen port (Python prod uses 8766)")
	execName := flag.String("executor", "go", "AI executor: go | python")
	claudeBin := flag.String("claude-bin", "claude", "path to the claude CLI binary")
	codexBin := flag.String("codex-bin", "codex", "path to the codex CLI binary")
	ollamaHost := flag.String("ollama-host", "http://localhost:11434", "Ollama base URL")
	remoteWSURL := flag.String("remote-ws-url", "", "remote backend WebSocket URL for backend=remote-ws")
	remoteWSToken := flag.String("remote-ws-token", "", "bearer token for remote backend WebSocket")
	dataDir := flag.String("data-dir", ".", "directory for everything-go's own persisted state")
	sessionStore := flag.String("session-store", os.Getenv("EVERYTHING_GO_SESSION_STORE"), "canonical saved_sessions.json path (empty = DATA_DIR/everything_go_sessions.json)")
	instanceName := flag.String("instance-name", "everything-go", "human label shown in the app")
	rootDir := flag.String("root-dir", "", "filesystem jail root (\"\" = no jail)")
	permissionCheck := flag.Bool("permission-check", false, "check filesystem permissions needed by the resident bridge and exit")
	permissionCheckPaths := flag.String("permission-check-paths", "", "additional filesystem paths to check, separated by ':' on Unix or ';' on Windows")
	serviceAccount := flag.String("service-account", "", "path to Firebase serviceAccountKey.json for FCM push (empty = disabled)")
	discovery := flag.Bool("discovery", false, "enable the LAN UDP discovery beacon")
	noDiscovery := flag.Bool("no-discovery", false, "deprecated: discovery is disabled by default")
	discoveryPort := flag.Int("discovery-port", 8767, "UDP port the app's discovery listener binds")
	tunnel := flag.Bool("tunnel", false, "start a cloudflared quick tunnel for remote access")
	mdns := flag.Bool("mdns", false, "enable mDNS (_bridge._tcp) registration")
	mdnsOff := flag.Bool("no-mdns", false, "deprecated: mDNS is disabled by default")
	disableSearch := flag.Bool("disable-search", false, "disable transcript search and background indexing")
	disableNativeWatcher := flag.Bool("disable-native-watcher", false, "disable native session discovery outside bridge-created sessions")
	mode := flag.String("mode", "bridge", "run mode: bridge (resident server) | index (one-shot search ingest, then exit)")
	indexPathsStdin := flag.Bool("index-paths-stdin", false, "index only newline-delimited transcript paths read from stdin")
	flag.Parse()

	// `--mode=index` is the short-lived indexer child: it ingests the search DB
	// to completion and exits, keeping the heap-heavy transcript parse out of the
	// resident bridge. It needs nothing else, so branch before any server setup.
	if *mode == "index" {
		var paths []string
		if *indexPathsStdin {
			var readErr error
			paths, readErr = readIndexPaths(os.Stdin, defaultDirtyPathLimit)
			if readErr != nil {
				log.Printf("[indexer] read dirty paths: %v", readErr)
				os.Exit(2)
			}
		}
		os.Exit(runSearchIndexer(*dataDir, paths, *indexPathsStdin))
	}

	sessionStorePath := *sessionStore
	if sessionStorePath == "" {
		sessionStorePath = filepath.Join(*dataDir, "everything_go_sessions.json")
	}
	if *permissionCheck {
		if err := runPermissionCheck(*dataDir, sessionStorePath, *permissionCheckPaths); err != nil {
			fmt.Fprintf(os.Stderr, "permission check failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stdout, "permission check passed")
		return
	}

	reg := session.NewRegistry()
	reg.AttachStore(session.NewStore(sessionStorePath))
	if removed := reg.PruneCodexSessions(func(cwd, name string) bool {
		return sourcepolicy.IgnoreCodexSession(
			cwd,
			name,
			sourcepolicy.CodexIgnoreCWDGlobs(),
			sourcepolicy.CodexIgnoreNamePrefixes(),
		)
	}); removed > 0 {
		log.Printf("[source-policy] pruned %d persisted Codex session(s)", removed)
	}

	pairing := governance.NewPairing(filepath.Join(*dataDir, "pairing.json"))
	instanceID, err := governance.LoadOrCreateInstanceID(filepath.Join(*dataDir, "instance_id"))
	if err != nil {
		log.Fatalf("load instance id: %v", err)
	}
	cfg := core.Config{
		InstanceName: *instanceName,
		InstanceID:   instanceID,
		RootDir:      *rootDir,
		DataDir:      *dataDir,
		LanIP:        detectLanIP(),
		TailscaleIP:  detectTailscaleIP(),
		Backends:     backend.DefaultRegistry(*remoteWSURL != ""),
		CodexRemote:  codexRemoteEndpoint(),
	}
	hub := core.NewHub(reg, cfg, pairing, *port)
	workService, err := workitems.OpenService(*dataDir, instanceID)
	if err != nil {
		log.Fatalf("open native work items: %v", err)
	}
	defer workService.Close()
	hub.SetWorkItems(workService)
	eventStore, err := eventinbox.Open(*dataDir, instanceID)
	if err != nil {
		log.Fatalf("open external event inbox: %v", err)
	}
	defer eventStore.Close()
	hub.SetEventInbox(eventStore)

	switch *execName {
	case "go":
		terminal := executor.NewTerminalSink(hub)
		claude := goexec.NewClaude(terminal, *claudeBin)
		codex := goexec.NewCodex(terminal, *codexBin)
		codex.SetDataDir(*dataDir)
		ollama := goexec.NewOllama(terminal, *ollamaHost, "")
		backends := map[string]executor.Executor{
			"claude": claude,
			"codex":  codex,
			"ollama": ollama,
		}
		if *remoteWSURL != "" {
			backends["remote-ws"] = remote.NewWS(terminal, *remoteWSURL, *remoteWSToken)
		}
		hub.SetExecutor(executor.NewReliableMux(backends, claude, terminal))
	case "python":
		log.Fatal("--executor=python not yet implemented (config 3 comes after config 2 is proven)")
	default:
		fmt.Fprintf(os.Stderr, "unknown executor %q\n", *execName)
		os.Exit(2)
	}

	// Search index: FTS5 over Claude/Codex JSONL. The resident bridge only READS
	// the WAL DB; ingestion runs in short-lived `--mode=index` child processes so
	// the heap-heavy parse of every transcript lands in a process that exits and
	// returns its memory to the OS (see runSearchIndexerLoop).
	ctx := context.Background()
	hub.StartWorkScheduler(ctx)
	searchDirty := newDirtyPathQueue(defaultDirtyPathLimit)
	nativeWatcherActive := !*disableNativeWatcher && strings.TrimSpace(os.Getenv("EVERYTHING_GO_NATIVE_WATCH")) != "0"
	if !*disableSearch {
		if idx, err := search.New(filepath.Join(*dataDir, "everything_go_search.db")); err != nil {
			log.Printf("search index disabled: %v", err)
		} else {
			hub.SetSearch(idx)
			if exePath, err := os.Executable(); err == nil {
				go runSearchIndexerLoop(ctx, idx, exePath, *dataDir, searchDirty, incrementalSearchEnabled() && nativeWatcherActive)
			} else {
				log.Printf("[search] cannot locate binary for indexer child (%v); serving existing index only", err)
			}
		}
	} else {
		log.Printf("search index disabled by flag")
	}

	// Feed store: HTML/markdown articles pushed from local pipelines, surfaced
	// in the app's feed (feed_push/list/fetch/mark_read/delete).
	hub.SetFeed(feed.New(*dataDir))

	// File-push inbox: desktop→phone file delivery (push_file/file_push_ack/
	// get_inbox), persisted so an offline device recovers it on reconnect.
	hub.SetInbox(inbox.New(*dataDir))

	// restart_bridge: Python touches a trigger file watched by an external
	// launchd restart-agent; the experiment port has no such agent, so we
	// self-re-exec — same binary, args and env, same PID. The short pause lets
	// the restart_ack flush to the socket before the image is replaced.
	if exePath, err := os.Executable(); err == nil {
		hub.SetRestart(func() {
			time.Sleep(200 * time.Millisecond)
			log.Printf("[restart] re-exec %s %v", exePath, os.Args)
			if err := syscall.Exec(exePath, os.Args, os.Environ()); err != nil {
				log.Printf("[restart] exec failed: %v", err)
			}
		})
	}

	// FCM push: explicit --service-account flag overrides the embedded key.
	fcmTokenPath := filepath.Join(*dataDir, "fcm_tokens.json")
	if *serviceAccount != "" {
		if notifier, err := fcm.New(*serviceAccount, fcmTokenPath); err != nil {
			log.Printf("FCM disabled: %v", err)
		} else {
			hub.SetFCM(notifier)
			log.Printf("FCM push enabled (service account: %s)", *serviceAccount)
		}
	} else {
		if notifier, err := fcm.NewFromBytes(embeddedFCMKey, fcmTokenPath); err != nil {
			log.Printf("FCM disabled (embedded key): %v", err)
		} else {
			hub.SetFCM(notifier)
			log.Printf("FCM push enabled (embedded key)")
		}
	}

	// Network presence services (P3 discovery + P4 tunnel). They are opt-in so
	// the fixed-endpoint P2 path stays deterministic and easy to debug.
	if !*disableNativeWatcher {
		hub.StartNativeWatcher(ctx, searchDirty.Add)
	} else {
		log.Printf("native session watcher disabled by flag")
	}
	if *discovery && !*noDiscovery {
		go netsvc.NewBeacon(*port, *discoveryPort, cfg.InstanceID).Run(ctx)
	}
	if *mdns && !*mdnsOff {
		go netsvc.RegisterMDNS(ctx, *port, cfg.InstanceName)
	}
	if *tunnel {
		go func() {
			// Do not expose an unclaimed bridge through a public quick tunnel.
			// LAN discovery remains available and the tunnel starts after pairing.
			for !pairing.IsLocked() {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
			log.Printf("[tunnel] trusted device present; starting remote tunnel")
			netsvc.NewTunnel(*port, hub.NotifyTunnelURL).Run(ctx)
		}()
	}

	transfers, err := filetransfer.NewService(*dataDir, *rootDir, hub.HTTPAuthorized)
	if err != nil {
		log.Fatalf("initialize file transfer: %v", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/media/", media.Handler())
	mux.Handle("/upload", transfers.UploadHandler())
	mux.Handle("/files", transfers.DownloadHandler())
	mux.Handle("/api/drop/v1/uploads", transfers.DropHandler())
	mux.Handle("/api/drop/v1/uploads/", transfers.DropHandler())
	mux.HandleFunc("/api/work/v1/items/", hub.ServeWorkAPI)
	mux.HandleFunc("/api/events/v1/events", hub.ServeEventAPI)
	mux.HandleFunc("/hooks/github", hub.ServeGitHubWebhook)
	mux.HandleFunc("/hooks/apple-app-store", hub.ServeAppStoreWebhook)
	mux.HandleFunc("/hooks/apple-app-store/", hub.ServeAppStoreWebhook)
	mux.HandleFunc("/", hub.ServeWS)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("everything-go listening on %s (executor=%s)", addr, *execName)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func codexRemoteEndpoint() string {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("EVERYTHING_GO_CODEX_APP_SERVER_MODE")))
	if mode == "stdio" || mode == "private" {
		return ""
	}
	if socket := strings.TrimSpace(os.Getenv("EVERYTHING_GO_CODEX_APP_SERVER_SOCKET")); socket != "" {
		return "unix://" + socket
	}
	return "unix://"
}

// runSearchIndexer is the body of `--mode=index`: a one-shot search ingest that
// runs to completion and exits, so the heap-heavy transcript parse happens in a
// throwaway process rather than the resident bridge. A flock prevents two
// indexers from writing the WAL DB at once (e.g. across a bridge restart).
func runSearchIndexer(dataDir string, paths []string, explicitPaths bool) int {
	unlock, err := acquireIndexLock(dataDir)
	if err != nil {
		log.Printf("[indexer] another indexer is running (%v); exiting", err)
		return 0
	}
	defer unlock()

	idx, err := search.New(filepath.Join(dataDir, "everything_go_search.db"))
	if err != nil {
		log.Printf("[indexer] open search db: %v", err)
		return 1
	}
	defer idx.Close()

	t0 := time.Now()
	var metrics search.IngestMetrics
	if explicitPaths {
		metrics = idx.RunPathsMetrics(paths)
	} else {
		metrics = idx.RunOnceMetrics()
	}
	log.Printf("[indexer] mode=%s files_seen=%d files_changed=%d files_queued=%d messages=%d bytes_read=%d db_bytes_delta=%d maintenance_rows=%d wal_before=%d wal_after=%d checkpoint=%d/%d busy=%d duration=%s",
		metrics.Mode, metrics.FilesSeen, metrics.FilesChanged, metrics.FilesQueued, metrics.MessagesAdded,
		metrics.BytesRead, metrics.DBBytesDelta, metrics.MaintenanceRows, metrics.WALBytesBefore, metrics.WALBytesAfter,
		metrics.CheckpointDone, metrics.CheckpointLog, metrics.CheckpointBusy, time.Since(t0).Round(time.Millisecond))
	if data, err := json.Marshal(metrics); err == nil {
		fmt.Printf("%s%s\n", indexResultPrefix, data)
	}
	return 0
}

const indexResultPrefix = "EVERYTHING_GO_INDEX_RESULT="

// runSearchIndexerLoop performs a full stat-based reconciliation at an
// adaptive interval. Native watcher notifications wake it immediately; quiet
// systems back off to maxInterval. Spawn-and-wait serializes runs and each
// child's exit returns its transcript parsing heap to the OS.
func runLegacySearchIndexerLoop(ctx context.Context, idx *search.Index, exePath, dataDir string, dirty *dirtyPathQueue, minInterval, maxInterval time.Duration) {
	idleCycles := 0
	for {
		idx.SetIndexing(true)
		cmd := exec.CommandContext(ctx, exePath, "--mode=index", "--data-dir", dataDir)
		var childOut bytes.Buffer
		cmd.Stdout = io.MultiWriter(os.Stdout, &childOut)
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		idx.SetIndexing(false)
		metrics, metricsOK := parseIndexMetrics(childOut.String())
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[search] indexer child failed: %v", err)
			idleCycles = 0
		} else {
			idx.MarkReady()
			if metricsOK {
				idx.RecordIngestMetrics(metrics)
			}
			if !metricsOK || metrics.FilesChanged > 0 || metrics.MessagesAdded > 0 {
				idleCycles = 0
			} else {
				idleCycles++
			}
		}
		delay := indexBackoff(minInterval, maxInterval, idleCycles)
		log.Printf("[search] next reconciliation in %s (idle_cycles=%d)", delay, idleCycles)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-dirty.Notify():
			stopTimer(timer)
			dirty.Drain()
			idleCycles = 0
			log.Printf("[search] transcript change detected; reconciling now")
		case <-timer.C:
		}
	}
}

func parseIndexMetrics(output string) (search.IngestMetrics, bool) {
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, indexResultPrefix) {
			continue
		}
		var metrics search.IngestMetrics
		if json.Unmarshal([]byte(strings.TrimPrefix(line, indexResultPrefix)), &metrics) == nil {
			return metrics, true
		}
	}
	return search.IngestMetrics{}, false
}

func indexBackoff(minInterval, maxInterval time.Duration, idleCycles int) time.Duration {
	if minInterval <= 0 {
		minInterval = time.Minute
	}
	if maxInterval < minInterval {
		maxInterval = minInterval
	}
	multipliers := [...]int{1, 2, 5, 15}
	// The first completed idle pass still gets the minimum follow-up delay.
	if idleCycles > 0 {
		idleCycles--
	} else {
		idleCycles = 0
	}
	if idleCycles >= len(multipliers) {
		return maxInterval
	}
	delay := time.Duration(multipliers[idleCycles]) * minInterval
	if delay > maxInterval {
		return maxInterval
	}
	return delay
}

func stopTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

// acquireIndexLock takes an exclusive, non-blocking flock so only one indexer
// child writes the search DB at a time. The returned func releases it.
func acquireIndexLock(dataDir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dataDir, "everything_go_indexer.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

func runPermissionCheck(dataDir, sessionStorePath, extraPaths string) error {
	home, _ := os.UserHomeDir()
	paths := []string{
		dataDir,
		filepath.Join(home, ".claude", "projects"),
		sourcepolicy.CodexSessionsDir(),
	}
	if sessionStorePath != "" {
		paths = append(paths, sessionStorePath)
	}
	if extraPaths != "" {
		paths = append(paths, filepath.SplitList(extraPaths)...)
	}

	var failures []string
	seen := map[string]bool{}
	for _, p := range paths {
		p = strings.TrimSpace(os.ExpandEnv(p))
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		if err := checkReadablePath(p); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", p, err))
		}
	}
	if len(failures) > 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}

func checkReadablePath(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, _ = io.CopyN(io.Discard, f, 1)
		return nil
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		child := filepath.Join(path, ent.Name())
		if ent.IsDir() {
			if _, err := os.ReadDir(child); err != nil && !os.IsNotExist(err) {
				return err
			}
			return nil
		}
		f, err := os.Open(child)
		if err != nil {
			return err
		}
		_, _ = io.CopyN(io.Discard, f, 1)
		_ = f.Close()
		return nil
	}
	return nil
}

// detectLanIP returns the first non-loopback, non-Tailscale IPv4 address.
func detectLanIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifaces {
		// Skip Tailscale virtual interfaces — those are handled by detectTailscaleIP.
		if strings.HasPrefix(iface.Name, "utun") || iface.Name == "tailscale0" {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok || ipnet.IP.IsLoopback() {
				continue
			}
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				return ip4.String()
			}
		}
	}
	return ""
}

// detectTailscaleIP returns the Tailscale IP (100.64.0.0/10 CGNAT range) on
// the local machine, or "" if Tailscale is not running. Phones connected via
// Tailscale can reach this IP; LAN-only clients cannot.
func detectTailscaleIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifaces {
		if !strings.HasPrefix(iface.Name, "utun") && iface.Name != "tailscale0" {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok || ipnet.IP.IsLoopback() {
				continue
			}
			ip4 := ipnet.IP.To4()
			if ip4 == nil {
				continue
			}
			// Tailscale uses 100.64.0.0/10 (first octet 100, second 64–127).
			if ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
				return ip4.String()
			}
		}
	}
	return ""
}
