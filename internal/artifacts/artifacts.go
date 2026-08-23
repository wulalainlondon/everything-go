// Package artifacts discovers locally generated media and runs explicit
// YouTube download jobs for the mobile/desktop artifact library.
package artifacts

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var supported = map[string]string{
	".mp4": "video", ".mov": "video", ".m4v": "video", ".webm": "video", ".mkv": "video",
	".jpg": "image", ".jpeg": "image", ".png": "image", ".gif": "image", ".webp": "image", ".heic": "image", ".avif": "image",
	".md": "document", ".txt": "document", ".pdf": "document", ".html": "document", ".htm": "document", ".json": "document", ".csv": "document",
	".srt": "subtitle", ".vtt": "subtitle", ".ass": "subtitle",
}

// Artifact matches the app's ArtifactSchema.
type Artifact struct {
	ID              string         `json:"id"`
	Kind            string         `json:"kind"`
	Status          string         `json:"status,omitempty"`
	Title           string         `json:"title"`
	Path            string         `json:"path,omitempty"`
	URL             string         `json:"url,omitempty"`
	ThumbnailURL    string         `json:"thumbnail_url,omitempty"`
	Source          string         `json:"source"`
	SourceSessionID string         `json:"source_session_id,omitempty"`
	SourceTaskID    string         `json:"source_task_id,omitempty"`
	CreatedAt       int64          `json:"created_at"`
	UpdatedAt       int64          `json:"updated_at,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

type URLBuilder func(string) string

func ForPath(filePath, source, taskID, sessionID string, buildURL URLBuilder) (Artifact, error) {
	realPath, err := filepath.EvalSymlinks(filePath)
	if err != nil {
		return Artifact{}, err
	}
	info, err := os.Stat(realPath)
	if err != nil {
		return Artifact{}, err
	}
	kind := "folder"
	if !info.IsDir() {
		kind = supported[strings.ToLower(filepath.Ext(realPath))]
		if kind == "" {
			return Artifact{}, errors.New("unsupported artifact type")
		}
		lower := strings.ToLower(filepath.Base(realPath))
		if kind == "document" && strings.Contains(lower, "transcript") {
			kind = "transcript"
		} else if kind == "document" && strings.Contains(lower, "summary") {
			kind = "summary"
		}
	}
	contentType := mime.TypeByExtension(strings.ToLower(filepath.Ext(realPath)))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	a := Artifact{
		ID: "file:" + realPath, Kind: kind, Status: "ready", Title: filepath.Base(realPath),
		Path: realPath, Source: source, SourceTaskID: taskID, SourceSessionID: sessionID,
		CreatedAt: info.ModTime().Unix(), UpdatedAt: info.ModTime().Unix(),
		Metadata: map[string]any{"mime_type": contentType, "file_size": info.Size()},
	}
	if kind != "folder" && buildURL != nil {
		a.URL = buildURL(realPath)
	}
	return a, nil
}

// Scan walks roots without following directory symlinks. Hidden directories,
// dependency trees and virtual environments are deliberately excluded.
func Scan(roots []string, limit int, buildURL URLBuilder) []Artifact {
	if limit < 1 {
		limit = 100
	}
	if limit > 300 {
		limit = 300
	}
	result := make([]Artifact, 0, limit)
	seen := make(map[string]bool)
	for _, root := range roots {
		root = filepath.Clean(root)
		if seen[root] {
			continue
		}
		seen[root] = true
		_ = filepath.WalkDir(root, func(current string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return nil
			}
			if entry.IsDir() {
				name := entry.Name()
				if current != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "venv") {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasPrefix(entry.Name(), ".") || supported[strings.ToLower(filepath.Ext(entry.Name()))] == "" {
				return nil
			}
			if a, err := ForPath(current, "file", "", "", buildURL); err == nil {
				result = append(result, a)
			}
			return nil
		})
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].UpdatedAt > result[j].UpdatedAt })
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

func DefaultRoots(rootDir string) []string {
	home, _ := os.UserHomeDir()
	candidates := []string{filepath.Join(home, "Downloads", "youtube"), filepath.Join(home, "Downloads", "bridge-inbox"), filepath.Join(home, "Downloads")}
	if rootDir != "" {
		candidates = append([]string{rootDir}, candidates...)
	}
	return candidates
}

func NewTask(url, sessionID string) (string, Artifact) {
	buf := make([]byte, 5)
	_, _ = rand.Read(buf)
	id := "yt_" + hex.EncodeToString(buf)
	now := time.Now().Unix()
	return id, Artifact{ID: "task:" + id, Kind: "task", Status: "running", Title: "YouTube Processing", Source: "youtube", SourceTaskID: id, SourceSessionID: sessionID, CreatedAt: now, UpdatedAt: now, Metadata: map[string]any{"original_url": url}}
}

// DownloadYouTube executes yt-dlp without a shell and returns only files tied
// to this job when possible. The caller is responsible for sending lifecycle events.
func DownloadYouTube(url, sessionID, taskID string, buildURL URLBuilder) ([]Artifact, error) {
	if strings.TrimSpace(url) == "" {
		return nil, errors.New("YouTube URL is required")
	}
	bin, err := exec.LookPath("yt-dlp")
	if err != nil {
		return nil, errors.New("yt-dlp not found")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	outDir := filepath.Join(home, "Downloads", "youtube")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	before := directoryFiles(outDir)
	template := filepath.Join(outDir, "%(title).180B [%(id)s].%(ext)s")
	cmd := exec.Command(bin, "--no-playlist", "--write-auto-subs", "--write-subs", "--sub-langs", "zh-TW,zh-Hant,zh,en,ja", "--convert-subs", "srt", "-f", "bv*+ba/b", "-o", template, url)
	cmd.Dir = outDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		message := strings.Join(strings.Fields(string(output)), " ")
		if len(message) > 240 {
			message = message[len(message)-240:]
		}
		if message == "" {
			message = err.Error()
		}
		return nil, fmt.Errorf("yt-dlp: %s", message)
	}
	var result []Artifact
	for name := range directoryFiles(outDir) {
		if before[name] {
			continue
		}
		if a, err := ForPath(filepath.Join(outDir, name), "youtube", taskID, sessionID, buildURL); err == nil {
			result = append(result, a)
		}
	}
	if len(result) == 0 {
		result = Scan([]string{outDir}, 12, buildURL)
		for i := range result {
			result[i].Source = "youtube"
			result[i].SourceTaskID = taskID
			result[i].SourceSessionID = sessionID
		}
	}
	return result, nil
}

func directoryFiles(dir string) map[string]bool {
	result := make(map[string]bool)
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if !entry.IsDir() {
			result[entry.Name()] = true
		}
	}
	return result
}
