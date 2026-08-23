package media

import (
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
)

// Handler returns an http.Handler that serves local files at absolute paths.
// The URL scheme is: /media/<absolute-path-to-file>
//
// Example: GET /media/Users/wulala/output/chart.png
//   → serves /Users/wulala/output/chart.png
//
// Security: ".." components are rejected after URL-decoding and filepath.Clean.
// The handler does not impose a root-dir jail — it mirrors Python, which serves
// any path the agent produced. The OS file permissions are the security boundary.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Strip the /media prefix, leaving the absolute path (with leading /).
		raw := strings.TrimPrefix(r.URL.Path, "/media")
		if raw == "" {
			http.Error(w, "missing path", http.StatusBadRequest)
			return
		}

		// URL-decode the path (the client may percent-encode spaces, etc.).
		decoded, err := url.PathUnescape(raw)
		if err != nil {
			http.Error(w, "bad path encoding", http.StatusBadRequest)
			return
		}

		// Clean and verify no ".." traversal remains after cleaning.
		cleaned := filepath.Clean(decoded)
		if strings.Contains(cleaned, "..") {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		// The path must be absolute (it started with / before stripping /media).
		if !filepath.IsAbs(cleaned) {
			http.Error(w, "not an absolute path", http.StatusBadRequest)
			return
		}

		http.ServeFile(w, r, cleaned)
	})
}
