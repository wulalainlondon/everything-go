// Command everything-go is the native Averything bridge service.
package main

import (
	"context"
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
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/core"
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
)

func main() {
	port := flag.Int("port", 8767, "WebSocket listen port")
	legacyExecutor := flag.String("executor", "go", "deprecated compatibility flag; only go is supported")
	claudeBin := flag.String("claude-bin", "claude", "path to the claude CLI binary")
	codexBin := flag.String("codex-bin", "codex", "path to the codex CLI binary")
	geminiBin := flag.String("gemini-bin", "", "path to the Gemini CLI binary (empty = discover gemini in PATH)")
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
	mode := flag.String("mode", "bridge", "run mode: bridge | index | healthcheck | doctor")
	flag.Parse()
	if *legacyExecutor != "go" {
		fmt.Fprintf(os.Stderr, "unsupported executor %q; this release is Go-only\n", *legacyExecutor)
		os.Exit(2)
	}

	switch *mode {
	case "index":
		os.Exit(runSearchIndexer(*dataDir))
	case "healthcheck":
		os.Exit(runHealthcheck(*port))
	case "doctor":
		os.Exit(runDoctor(*port, *dataDir, *claudeBin, *codexBin, *geminiBin))
	case "bridge":
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", *mode)
		os.Exit(2)
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

	terminal := executor.NewTerminalSink(hub)
	claude := goexec.NewClaude(terminal, *claudeBin)
	codex := goexec.NewCodex(terminal, *codexBin)
	codex.SetDataDir(*dataDir)
	ollama := goexec.NewOllama(terminal, *ollamaHost, "")
	gemini := goexec.NewGemini(terminal, *geminiBin)
	backends := map[string]executor.Executor{
		"claude": claude,
		"codex":  codex,
		"ollama": ollama,
		"gemini": gemini,
	}
	if *remoteWSURL != "" {
		backends["remote-ws"] = remote.NewWS(terminal, *remoteWSURL, *remoteWSToken)
	}
	hub.SetExecutor(executor.NewReliableMux(backends, claude, terminal))

	// Search index: FTS5 over Claude/Codex JSONL. The resident bridge only READS
	// the WAL DB; ingestion runs in short-lived `--mode=index` child processes so
	// the heap-heavy parse of every transcript lands in a process that exits and
	// returns its memory to the OS (see runSearchIndexerLoop).
	ctx := context.Background()
	externalTunnelURLFile := strings.TrimSpace(os.Getenv("BRIDGE_TUNNEL_URL_FILE"))
	if externalTunnelURLFile != "" {
		go netsvc.WatchURLFile(ctx, externalTunnelURLFile, time.Second, hub.NotifyTunnelURL)
		log.Printf("[tunnel] watching externally managed URL file %s", externalTunnelURLFile)
	}
	if !*disableSearch {
		if idx, err := search.New(filepath.Join(*dataDir, "everything_go_search.db")); err != nil {
			log.Printf("search index disabled: %v", err)
		} else {
			hub.SetSearch(idx)
			if exePath, err := os.Executable(); err == nil {
				go runSearchIndexerLoop(ctx, idx, exePath, *dataDir, 60*time.Second)
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

	// restart_bridge self-reexecs the same verified binary. The short pause lets
	// the restart acknowledgement flush before the image is replaced.
	if exePath, err := os.Executable(); err == nil {
		hub.SetRestart(func() {
			time.Sleep(200 * time.Millisecond)
			log.Printf("[restart] re-exec %s %v", exePath, os.Args)
			if err := reexecSelf(exePath, os.Args, os.Environ()); err != nil {
				log.Printf("[restart] exec failed: %v", err)
			}
		})
	}

	// FCM push credentials must remain local runtime data. Public release
	// binaries never embed a service-account private key.
	fcmTokenPath := filepath.Join(*dataDir, "fcm_token.txt")
	if *serviceAccount != "" {
		if notifier, err := fcm.New(*serviceAccount, fcmTokenPath); err != nil {
			log.Printf("FCM disabled: %v", err)
		} else {
			hub.SetFCM(notifier)
			log.Printf("FCM push enabled (service account: %s)", *serviceAccount)
		}
	} else {
		log.Printf("FCM disabled: no --service-account configured")
	}

	// Network presence services (P3 discovery + P4 tunnel). They are opt-in so
	// the fixed-endpoint P2 path stays deterministic and easy to debug.
	if !*disableNativeWatcher {
		hub.StartNativeWatcher(ctx)
	} else {
		log.Printf("native session watcher disabled by flag")
	}
	if *discovery && !*noDiscovery {
		go netsvc.NewBeacon(*port, *discoveryPort, cfg.InstanceID).Run(ctx)
	}
	if *mdns && !*mdnsOff {
		go netsvc.RegisterMDNS(ctx, *port, cfg.InstanceName)
	}
	if *tunnel && externalTunnelURLFile == "" {
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

	mux := http.NewServeMux()
	mux.Handle("/media/", media.Handler())
	mux.Handle("/upload", filetransfer.UploadHandler())
	mux.Handle("/files", filetransfer.DownloadHandler())
	mux.HandleFunc("/", hub.ServeWS)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("everything-go listening on %s", addr)
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

func runHealthcheck(port int) int {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprint(port)), 2*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bridge healthcheck failed: %v\n", err)
		return 1
	}
	_ = conn.Close()
	fmt.Printf("bridge healthy on 127.0.0.1:%d\n", port)
	return 0
}

func runDoctor(port int, dataDir, claudeBin, codexBin, geminiBin string) int {
	failures := 0
	backendCount := 0
	checkBinary := func(label, requested string, optional bool) {
		resolved, err := exec.LookPath(requested)
		if strings.TrimSpace(requested) == "" {
			err = errors.New("not configured")
		}
		if err != nil {
			if !optional {
				failures++
			}
			fmt.Printf("%-12s missing (%v)\n", label, err)
			return
		}
		if label != "cloudflared" {
			backendCount++
		}
		fmt.Printf("%-12s %s\n", label, resolved)
	}
	checkBinary("claude", claudeBin, true)
	checkBinary("codex", codexBin, true)
	checkBinary("gemini", geminiBin, true)
	checkBinary("cloudflared", "cloudflared", true)
	if backendCount == 0 {
		failures++
		fmt.Println("backend      no supported AI CLI found")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		failures++
		fmt.Printf("data_dir     error (%v)\n", err)
	} else {
		fmt.Printf("data_dir     %s\n", dataDir)
	}
	if runHealthcheck(port) != 0 {
		failures++
	}
	if failures > 0 {
		return 1
	}
	return 0
}

// runSearchIndexer is the body of `--mode=index`: a one-shot search ingest that
// runs to completion and exits, so the heap-heavy transcript parse happens in a
// throwaway process rather than the resident bridge. A flock prevents two
// indexers from writing the WAL DB at once (e.g. across a bridge restart).
func runSearchIndexer(dataDir string) int {
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
	n := idx.RunOnce()
	log.Printf("[indexer] ingested %d messages in %s", n, time.Since(t0).Round(time.Millisecond))
	return 0
}

// runSearchIndexerLoop keeps the search index fresh by spawning a `--mode=index`
// child, waiting for it to finish, then sleeping. Spawn-and-wait serializes runs
// (never two at once from this bridge) and each child's exit returns its ingest
// memory to the OS. The first run does the full parse; later runs are cheap
// incremental passes (ingest_state skips unchanged files).
func runSearchIndexerLoop(ctx context.Context, idx *search.Index, exePath, dataDir string, interval time.Duration) {
	for {
		idx.SetIndexing(true)
		cmd := exec.CommandContext(ctx, exePath, "--mode=index", "--data-dir", dataDir)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		idx.SetIndexing(false)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[search] indexer child failed: %v", err)
		} else {
			idx.MarkReady()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
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
