package runtime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// gitignoreDefaults seeds a baseline .gitignore on auto-init, matching
// bridge/handlers/system_ops.py _GITIGNORE_DEFAULTS.
const gitignoreDefaults = `node_modules/
dist/
.next/
build/
out/
.env
.env.*
__pycache__/
*.pyc
.venv/
venv/
*.log
.DS_Store
.gradle/
.idea/
*.class
`

// GitDiffResult is the outcome of a get_git_diff request.
type GitDiffResult struct {
	Diff        string
	Error       string // "" on success; stable token otherwise
	Initialized bool   // true if this call created the baseline repo
}

// GitDiff returns `git diff HEAD` for cwd, auto-initializing a baseline repo
// (.gitignore + git init + add + empty baseline commit) when cwd is not yet a
// git work tree. Mirrors system_ops.py get_git_diff.
func GitDiff(cwd string) GitDiffResult {
	if cwd == "" || !isDir(cwd) {
		return GitDiffResult{Error: "no_cwd"}
	}
	if _, err := exec.LookPath("git"); err != nil {
		return GitDiffResult{Error: "git_not_found"}
	}

	initialized := false
	if !isDir(filepath.Join(cwd, ".git")) {
		if err := gitInitBaseline(cwd); err != nil {
			return GitDiffResult{Error: err.Error()}
		}
		initialized = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, _ := runGit(ctx, cwd, "diff", "HEAD")
	if ctx.Err() == context.DeadlineExceeded {
		return GitDiffResult{Error: "timeout"}
	}
	// Match the Python bridge: the diff step ignores git's exit status. An
	// unborn HEAD (repo with no commits) or other benign failure yields an empty
	// diff with no surfaced error — the app handles "" as "no changes".
	return GitDiffResult{Diff: out, Initialized: initialized}
}

func gitInitBaseline(cwd string) error {
	gitignore := filepath.Join(cwd, ".gitignore")
	if _, err := os.Stat(gitignore); os.IsNotExist(err) {
		_ = os.WriteFile(gitignore, []byte(gitignoreDefaults), 0o644)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	steps := [][]string{
		{"init"},
		{"add", "-A"},
		{"-c", "user.email=bridge@local", "-c", "user.name=claude-bridge",
			"commit", "-m", "baseline (claude-bridge)", "--allow-empty"},
	}
	for _, args := range steps {
		if _, err := runGit(ctx, cwd, args...); err != nil {
			return err
		}
	}
	return nil
}

func runGit(ctx context.Context, cwd string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = cwd
	out, err := cmd.Output()
	return string(out), err
}
