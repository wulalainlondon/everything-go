// Package media implements scan_for_media parity with the Python bridge:
// after each turn completes, scan the accumulated assistant text for absolute
// and relative file paths, confirm they exist on disk, classify them as
// image/video/document, and emit the appropriate protocol events.
package media

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"

	"everything-go/internal/protocol"
)

// Regex patterns mirror the Python bridge exactly.
// MEDIA_RE matches absolute paths ending in a known extension.
// MEDIA_RE_REL matches relative paths (./foo or ../foo).
var (
	absRE = regexp.MustCompile(`(/(?:[^\s'"<>]+\.(?:jpg|jpeg|png|gif|webp|mp4|mov|m4v|avi|html|htm|pdf)))`)
	relRE = regexp.MustCompile(`(?:^|[^/\w])(\.\.?/[^\s'"<>]+\.(?:jpg|jpeg|png|gif|webp|mp4|mov|m4v|avi|html|htm|pdf))`)
)

var (
	imageExts = map[string]bool{".jpg": true, ".jpeg": true, ".png": true, ".gif": true, ".webp": true}
	videoExts = map[string]bool{".mp4": true, ".mov": true, ".m4v": true, ".avi": true}
	docExts   = map[string]bool{".html": true, ".htm": true, ".pdf": true}
)

// Scanner scans assistant text for media/document file references.
type Scanner struct {
	port         int
	tunnelURL    atomic.Value // stores string
	tailscaleIP  atomic.Value // stores string — Tailscale (100.x) IP, phone-reachable over VPN
	lanIP        atomic.Value // stores string — LAN IP fallback for same-WiFi clients
}

// NewScanner creates a Scanner bound to the given HTTP port.
func NewScanner(port int) *Scanner {
	s := &Scanner{port: port}
	s.tunnelURL.Store("")
	s.tailscaleIP.Store("")
	s.lanIP.Store("")
	return s
}

// SetTunnelURL updates the tunnel base URL used to build media URLs.
// Pass "" to revert to the LAN IP (or 127.0.0.1) URL.
func (s *Scanner) SetTunnelURL(u string) {
	s.tunnelURL.Store(u)
}

// SetTailscaleIP sets the Tailscale (100.x.x.x) IP used when no tunnel is
// active. Preferred over LAN IP because Tailscale clients can always reach it.
func (s *Scanner) SetTailscaleIP(ip string) {
	s.tailscaleIP.Store(ip)
}

// SetLanIP sets the LAN IP used when no tunnel and no Tailscale IP is available.
func (s *Scanner) SetLanIP(ip string) {
	s.lanIP.Store(ip)
}

// Scan finds all media/document file paths in text, resolves relative paths
// against cwd, confirms files exist, and returns a slice of protocol.Media
// and protocol.Document values (as any).
func (s *Scanner) Scan(text, sessionID, cwd string) []any {
	paths := extractPaths(text, cwd)
	if len(paths) == 0 {
		return nil
	}

	tunnel := s.tunnelURL.Load().(string)
	var results []any
	for _, absPath := range paths {
		ext := strings.ToLower(filepath.Ext(absPath))
		mediaURL := s.buildURL(absPath, tunnel)

		switch {
		case imageExts[ext]:
			results = append(results, protocol.Media{
				Type:      "media",
				SessionID: sessionID,
				MediaType: "image",
				Path:      absPath,
				URL:       mediaURL,
			})
		case videoExts[ext]:
			results = append(results, protocol.Media{
				Type:      "media",
				SessionID: sessionID,
				MediaType: "video",
				Path:      absPath,
				URL:       mediaURL,
			})
		case docExts[ext]:
			docType := "html"
			if ext == ".pdf" {
				docType = "pdf"
			}
			results = append(results, protocol.Document{
				Type:      "document",
				SessionID: sessionID,
				Path:      absPath,
				URL:       mediaURL,
				Title:     filepath.Base(absPath),
				DocType:   docType,
			})
		}
	}
	return results
}

// extractPaths finds all unique, existent absolute paths mentioned in text.
// Relative paths are resolved against cwd.
func extractPaths(text, cwd string) []string {
	seen := map[string]bool{}
	var out []string

	// Absolute paths
	for _, m := range absRE.FindAllStringSubmatch(text, -1) {
		p := m[1]
		if seen[p] {
			continue
		}
		seen[p] = true
		if fileExists(p) {
			out = append(out, p)
		}
	}

	// Relative paths — resolve against cwd
	for _, m := range relRE.FindAllStringSubmatch(text, -1) {
		rel := m[1]
		var abs string
		if cwd != "" {
			abs = filepath.Join(cwd, rel)
		} else {
			abs = rel
		}
		abs = filepath.Clean(abs)
		if seen[abs] {
			continue
		}
		seen[abs] = true
		if fileExists(abs) {
			out = append(out, abs)
		}
	}

	return out
}

// buildURL produces the media URL for the given absolute path.
// Priority: tunnel > Tailscale IP > LAN IP > 127.0.0.1 (loopback, unreachable from phone).
func (s *Scanner) buildURL(absPath, tunnelBase string) string {
	encoded := url.PathEscape(absPath)
	// url.PathEscape escapes '/' as well; we need slashes preserved in the
	// path component. Re-unescape %2F back to /.
	encoded = strings.ReplaceAll(encoded, "%2F", "/")

	if tunnelBase != "" {
		return fmt.Sprintf("%s/media%s", strings.TrimRight(tunnelBase, "/"), encoded)
	}
	if ts := s.tailscaleIP.Load().(string); ts != "" {
		return fmt.Sprintf("http://%s:%d/media%s", ts, s.port, encoded)
	}
	if lan := s.lanIP.Load().(string); lan != "" {
		return fmt.Sprintf("http://%s:%d/media%s", lan, s.port, encoded)
	}
	return fmt.Sprintf("http://127.0.0.1:%d/media%s", s.port, encoded)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
