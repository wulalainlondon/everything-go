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
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/core"
	"everything-go/internal/executor"
	"everything-go/internal/executor/goexec"
	"everything-go/internal/executor/remote"
	"everything-go/internal/fcm"
	"everything-go/internal/feed"
	"everything-go/internal/governance"
	"everything-go/internal/inbox"
	"everything-go/internal/media"
	"everything-go/internal/netsvc"
	"everything-go/internal/search"
	"everything-go/internal/session"
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
	flag.Parse()

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

	pairing := governance.NewPairing(filepath.Join(*dataDir, "pairing.json"))
	cfg := core.Config{
		InstanceName: *instanceName,
		InstanceID:   "everything-go",
		RootDir:      *rootDir,
		DataDir:      *dataDir,
		LanIP:        detectLanIP(),
		Backends:     backend.DefaultRegistry(*remoteWSURL != ""),
	}
	hub := core.NewHub(reg, cfg, pairing, *port)

	switch *execName {
	case "go":
		terminal := executor.NewTerminalSink(hub)
		claude := goexec.NewClaude(terminal, *claudeBin)
		codex := goexec.NewCodex(terminal, *codexBin)
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

	// Search index: FTS5 over Claude/Codex JSONL, ingested in the background.
	if idx, err := search.New(filepath.Join(*dataDir, "everything_go_search.db")); err != nil {
		log.Printf("search index disabled: %v", err)
	} else {
		idx.Start(30 * time.Second)
		hub.SetSearch(idx)
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
	fcmTokenPath := filepath.Join(*dataDir, "fcm_token.txt")
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
	ctx := context.Background()
	hub.StartNativeWatcher(ctx)
	if *discovery && !*noDiscovery {
		go netsvc.NewBeacon(*port, *discoveryPort, cfg.InstanceID).Run(ctx)
	}
	if *mdns && !*mdnsOff {
		go netsvc.RegisterMDNS(ctx, *port, cfg.InstanceName)
	}
	if *tunnel {
		go netsvc.NewTunnel(*port, hub.NotifyTunnelURL).Run(ctx)
	}

	mux := http.NewServeMux()
	mux.Handle("/media/", media.Handler())
	mux.HandleFunc("/", hub.ServeWS)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("everything-go listening on %s (executor=%s)", addr, *execName)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func runPermissionCheck(dataDir, sessionStorePath, extraPaths string) error {
	home, _ := os.UserHomeDir()
	paths := []string{
		dataDir,
		filepath.Join(home, ".claude", "projects"),
		filepath.Join(home, ".codex", "sessions"),
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

// detectLanIP returns the first non-loopback IPv4 address, surfaced in
// hello_ack.lan_ip so the app can show how to reach this bridge on the LAN.
func detectLanIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		if ip4 := ipnet.IP.To4(); ip4 != nil {
			return ip4.String()
		}
	}
	return ""
}
