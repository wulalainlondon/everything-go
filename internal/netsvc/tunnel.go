package netsvc

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"regexp"
	"sync"
	"time"
)

// trycloudflareURL matches the public hostname cloudflared prints on stdout,
// e.g. https://foo-bar-baz.trycloudflare.com — same pattern as network_services.py.
var trycloudflareURL = regexp.MustCompile(`https://[\w.-]+\.trycloudflare\.com`)

// Tunnel manages a `cloudflared tunnel --url http://localhost:PORT` quick
// tunnel, capturing the public URL it prints. Mirrors network_services.py
// start_cloudflared_tunnel (self-managed mode).
type Tunnel struct {
	port int
	bin  string
	onURL func(string) // called each time a new URL is established (including restarts)

	mu  sync.RWMutex
	url string
	cmd *exec.Cmd
}

// NewTunnel builds a manager for the given WS port. onURL (may be nil) fires
// each time a URL is established — wire it to FCM/log as needed.
func NewTunnel(port int, onURL func(string)) *Tunnel {
	return &Tunnel{port: port, onURL: onURL}
}

// URL returns the current public tunnel URL, or "" if not yet established.
func (t *Tunnel) URL() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.url
}

func (t *Tunnel) setURL(u string) {
	t.mu.Lock()
	changed := t.url != u
	t.url = u
	cb := t.onURL
	t.mu.Unlock()
	if changed && cb != nil {
		cb(u)
	}
}

// Run starts cloudflared and restarts it whenever it exits, until ctx is
// cancelled. Each restart fires onURL with the new URL so FCM is notified.
// A no-op (logged) if cloudflared isn't installed.
func (t *Tunnel) Run(ctx context.Context) {
	bin, err := exec.LookPath("cloudflared")
	if err != nil {
		log.Printf("[tunnel] cloudflared not installed, skipping")
		return
	}
	t.bin = bin

	const (
		initBackoff = 5 * time.Second
		maxBackoff  = 60 * time.Second
	)
	backoff := initBackoff

	for {
		if ctx.Err() != nil {
			return
		}

		hadURL := t.runOnce(ctx, bin)

		if ctx.Err() != nil {
			return
		}

		// Clear URL so the next runOnce fires the callback with the new URL.
		t.mu.Lock()
		t.url = ""
		t.mu.Unlock()

		if hadURL {
			backoff = initBackoff // successful run → reset backoff
		} else {
			if backoff*2 < maxBackoff {
				backoff *= 2
			} else {
				backoff = maxBackoff
			}
		}

		log.Printf("[tunnel] restarting in %s...", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

// runOnce spawns a single cloudflared process and blocks until it exits.
// Returns true if a URL was successfully established during this run.
func (t *Tunnel) runOnce(ctx context.Context, bin string) (hadURL bool) {
	cmd := exec.CommandContext(ctx, bin, "tunnel", "--url", fmt.Sprintf("http://localhost:%d", t.port))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("[tunnel] stdout pipe: %v", err)
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Printf("[tunnel] stderr pipe: %v", err)
		return
	}
	if err := cmd.Start(); err != nil {
		log.Printf("[tunnel] start failed: %v", err)
		return
	}
	t.mu.Lock()
	t.cmd = cmd
	t.mu.Unlock()
	log.Printf("[tunnel] cloudflared started (pid=%d), waiting for URL...", cmd.Process.Pid)

	// cloudflared prints the URL on stderr in recent builds, stdout in older ones.
	go t.scan(stdout)
	go t.scan(stderr)

	_ = cmd.Wait()
	hadURL = t.URL() != ""
	log.Printf("[tunnel] cloudflared exited (hadURL=%v)", hadURL)
	return
}

// scan reads lines from r and records the first trycloudflare URL it sees.
func (t *Tunnel) scan(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		if m := trycloudflareURL.FindString(sc.Text()); m != "" && t.URL() == "" {
			log.Printf("[tunnel] public URL: %s", m)
			t.setURL(m)
		}
	}
}
