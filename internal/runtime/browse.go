package runtime

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"everything-go/internal/protocol"
)

const maxEntries = 500
const MaxMarkdownScanFiles = 300

// skipDirs are never useful to browse in a file picker (mirrors file_ops.py).
var skipDirs = map[string]bool{
	"node_modules": true, ".git": true, ".hg": true, ".svn": true,
	"__pycache__": true, ".pytest_cache": true, ".mypy_cache": true, ".tox": true, ".ruff_cache": true,
	".next": true, ".nuxt": true, ".svelte-kit": true, ".turbo": true,
	"dist": true, "build": true, "out": true, "target": true, ".gradle": true,
	".venv": true, "venv": true, "env": true, ".env": true,
	".idea": true, ".vscode": true,
	"coverage": true, ".nyc_output": true,
}

var markdownExtensions = map[string]bool{
	".md": true, ".markdown": true,
}

// ExpandPath resolves "~" and "~/..." to the user home, like Python's
// os.path.expanduser used before resolve_jailed.
func ExpandPath(p string) string {
	if p == "" {
		p = "~"
	}
	if p == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return home
		}
		return p
	}
	if strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, p[2:])
		}
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return abs
}

// ListEntries returns the visible children of path, dirs-first then name-asc,
// skipping noisy dirs and dot/tilde-prefixed names. Mirrors _list_entries.
func ListEntries(path string) []protocol.DirEntry {
	entries := []protocol.DirEntry{}
	des, err := os.ReadDir(path)
	if err != nil {
		return entries
	}
	for _, de := range des {
		name := de.Name()
		if de.IsDir() && skipDirs[name] {
			continue
		}
		if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "~") {
			continue
		}
		info, err := de.Info()
		if err != nil {
			continue
		}
		// is_dir follows symlinks (Python entry.is_dir(follow_symlinks=True)).
		isDir := de.IsDir()
		if info.Mode()&os.ModeSymlink != 0 {
			if target, err := os.Stat(filepath.Join(path, name)); err == nil {
				isDir = target.IsDir()
			}
		}
		entries = append(entries, protocol.DirEntry{
			Name: name, IsDir: isDir, Size: info.Size(),
			Modified: info.ModTime().Unix(),
		})
		if len(entries) >= maxEntries {
			break
		}
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].IsDir != entries[j].IsDir {
			return entries[i].IsDir // dirs first
		}
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})
	return entries
}

func IsMarkdownPath(path string) bool {
	return markdownExtensions[strings.ToLower(filepath.Ext(path))]
}

func ScanMarkdownFiles(root string, remaining int) ([]protocol.MarkdownFileEntry, error) {
	if remaining <= 0 {
		return []protocol.MarkdownFileEntry{}, nil
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []protocol.MarkdownFileEntry{}, nil
	}
	out := []protocol.MarkdownFileEntry{}
	err = filepath.WalkDir(root, func(path string, de os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		name := de.Name()
		if de.IsDir() {
			if path != root && (skipDirs[name] || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "~")) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "~") || !IsMarkdownPath(path) {
			return nil
		}
		info, err := de.Info()
		if err != nil {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			rel = name
		}
		out = append(out, protocol.MarkdownFileEntry{
			Path: path, Root: root, Name: name, RelativePath: rel,
			Size: info.Size(), Modified: info.ModTime().Unix(),
		})
		if len(out) >= remaining {
			return filepath.SkipAll
		}
		return nil
	})
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Modified != out[j].Modified {
			return out[i].Modified > out[j].Modified
		}
		return strings.ToLower(out[i].RelativePath) < strings.ToLower(out[j].RelativePath)
	})
	return out, err
}

func WriteTextFileAtomic(path, content string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp_bridge_*.md")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.WriteString(content); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	ok = true
	return nil
}

// DirHash is a stable fingerprint of (name, is_dir, modified) tuples used by
// the client to skip re-rendering an unchanged listing. It need only be stable
// within this bridge (the client compares against a hash it got from us), so
// Go-native JSON marshaling is fine — unlike content_hash it has no cross-impl
// parity requirement.
func DirHash(entries []protocol.DirEntry) string {
	fp := make([][]any, 0, len(entries))
	for _, e := range entries {
		fp = append(fp, []any{e.Name, e.IsDir, e.Modified})
	}
	b, _ := json.Marshal(fp)
	sum := sha1.Sum(b)
	return hex.EncodeToString(sum[:])[:16]
}
