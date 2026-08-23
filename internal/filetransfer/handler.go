package filetransfer

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const maxUploadBytes = 512 * 1024 * 1024 // 512 MB

// UploadHandler serves POST /upload.
// Accepts multipart/form-data with a "file" field.
// Saves to ~/Downloads/bridge-inbox/<filename> and replies with JSON.
func UploadHandler() http.Handler {
	return http.HandlerFunc(handleUpload)
}

// DownloadHandler serves GET /files?path=<absolute-path>.
// Replies with the file as a download attachment.
func DownloadHandler() http.Handler {
	return http.HandlerFunc(handleDownload)
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "parse error: "+err.Error(), http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "missing 'file' field: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	name := filepath.Base(header.Filename)
	if name == "." || name == ".." || strings.ContainsAny(name, "/\\") {
		http.Error(w, "invalid filename", http.StatusBadRequest)
		return
	}

	home, _ := os.UserHomeDir()
	destDir := filepath.Join(home, "Downloads", "bridge-inbox")
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		http.Error(w, "mkdir: "+err.Error(), http.StatusInternalServerError)
		return
	}

	destPath := uniquePath(destDir, name)
	out, err := os.Create(destPath)
	if err != nil {
		http.Error(w, "create: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer out.Close()

	n, err := io.Copy(out, file)
	if err != nil {
		http.Error(w, "write: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[upload] %s → %s (%d bytes)", header.Filename, destPath, n)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"path":     destPath,
		"filename": filepath.Base(destPath),
		"size":     n,
	})
}

func handleDownload(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	raw := r.URL.Query().Get("path")
	if raw == "" {
		http.Error(w, "missing path param", http.StatusBadRequest)
		return
	}

	decoded, err := url.QueryUnescape(raw)
	if err != nil {
		http.Error(w, "bad path encoding", http.StatusBadRequest)
		return
	}

	cleaned := filepath.Clean(decoded)
	if !filepath.IsAbs(cleaned) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	w.Header().Set("Content-Disposition", `attachment; filename="`+filepath.Base(cleaned)+`"`)
	http.ServeFile(w, r, cleaned)
}

func setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

func uniquePath(dir, name string) string {
	p := filepath.Join(dir, name)
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return p
	}
	ext := filepath.Ext(name)
	stem := strings.TrimSuffix(name, ext)
	for i := 1; i < 1000; i++ {
		candidate := filepath.Join(dir, fmt.Sprintf("%s_%d%s", stem, i, ext))
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}
	return p
}
