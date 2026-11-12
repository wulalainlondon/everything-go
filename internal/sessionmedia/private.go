package sessionmedia

import (
	"path/filepath"
	"strings"
)

// PrivatePath protects the complete vault (including SQLite sidecars) through
// legacy path readers. Resolve aliases at both ends before comparing boundaries.
func PrivatePath(dataDir, path string) bool {
	if dataDir == "" {
		return false
	}
	root, err := filepath.Abs(filepath.Join(dataDir, "session-media"))
	if err != nil {
		return true
	}
	candidate, err := filepath.Abs(path)
	if err != nil {
		return true
	}
	contains := func(root, path string) bool {
		rel, e := filepath.Rel(root, path)
		return e == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
	}
	if contains(root, candidate) {
		return true
	}
	if resolved, e := filepath.EvalSymlinks(root); e == nil {
		root = resolved
	}
	if resolved, e := filepath.EvalSymlinks(candidate); e == nil {
		candidate = resolved
	}
	return contains(root, candidate)
}
