// Package runtime implements the bridge's OS-level operations that are
// independent of any AI backend: interactive shell sessions, task/process
// listing and killing, and directory browsing for the file picker.
//
// Fidelity reference: bridge/handlers/runtime_ops.py and
// bridge/handlers/file_ops.py.
package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"sync"

	"everything-go/internal/protocol"
)

// maxShells caps concurrent interactive shells, mirroring the Python bridge.
const maxShells = 5

// shell is one /bin/bash -s subprocess streaming merged stdout/stderr.
type shell struct {
	id     string
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	cancel context.CancelFunc
	cwd    string
}

// ShellManager owns the live shell subprocesses. Output is pushed back through
// the emit sink (the Hub broadcasts it to connected clients).
type ShellManager struct {
	mu     sync.Mutex
	shells map[string]*shell
	emit   func(any)
}

func NewShellManager(emit func(any)) *ShellManager {
	return &ShellManager{shells: make(map[string]*shell), emit: emit}
}

func shellCommand() (string, []string) {
	if runtime.GOOS == "windows" {
		if pwsh, err := exec.LookPath("pwsh"); err == nil {
			return pwsh, []string{"-NoLogo", "-NoProfile", "-Command", "-"}
		}
		if ps, err := exec.LookPath("powershell"); err == nil {
			return ps, []string{"-NoLogo", "-NoProfile", "-Command", "-"}
		}
		return "cmd.exe", []string{"/Q", "/K"}
	}
	return "/bin/bash", []string{"-s"}
}

// Create spawns a shell rooted at cwd (falling back to $HOME) and returns its
// id, or "" with a human error if the cap is hit / spawn fails.
func (m *ShellManager) Create(cwd string) (string, string) {
	m.mu.Lock()
	if len(m.shells) >= maxShells {
		m.mu.Unlock()
		return "", "Max 5 shell sessions reached"
	}
	m.mu.Unlock()

	if cwd == "" || !isDir(cwd) {
		if home, err := os.UserHomeDir(); err == nil {
			cwd = home
		}
	}

	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	shellID := "sh_" + hex.EncodeToString(rnd[:])

	ctx, cancel := context.WithCancel(context.Background())
	bin, args := shellCommand()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = cwd
	cmd.Env = append(os.Environ(), "TERM=dumb")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return "", "shell stdin failed: " + err.Error()
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return "", "shell stdout failed: " + err.Error()
	}
	// stdout and stderr are read separately and both fanned to shell_output,
	// preserving interleaved program output without the StdoutPipe/Stderr alias.
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return "", "shell stderr failed: " + err.Error()
	}

	sh := &shell{id: shellID, cmd: cmd, stdin: stdin, cancel: cancel, cwd: cwd}

	if err := cmd.Start(); err != nil {
		cancel()
		return "", "shell spawn failed: " + err.Error()
	}

	m.mu.Lock()
	m.shells[shellID] = sh
	m.mu.Unlock()

	log.Printf("[shell %s] spawned pid=%d cwd=%s", shellID, cmd.Process.Pid, cwd)
	go m.readPipe(shellID, stdout)
	go m.readPipe(shellID, stderr)
	go func() {
		_ = cmd.Wait()
		m.mu.Lock()
		if m.shells[shellID] == sh {
			delete(m.shells, shellID)
		}
		m.mu.Unlock()
		m.emit(protocol.NewShellClosed(shellID))
	}()
	return shellID, ""
}

func (m *ShellManager) readPipe(shellID string, r io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			m.emit(protocol.NewShellOutput(shellID, string(buf[:n])))
		}
		if err != nil {
			return
		}
	}
}

// Input writes a line (newline-terminated) to the shell's stdin.
func (m *ShellManager) Input(shellID, data string) {
	m.mu.Lock()
	sh := m.shells[shellID]
	m.mu.Unlock()
	if sh == nil {
		return
	}
	line := data
	if len(line) == 0 || line[len(line)-1] != '\n' {
		line += "\n"
	}
	if _, err := sh.stdin.Write([]byte(line)); err != nil {
		log.Printf("[shell %s] stdin write: %v", shellID, err)
	}
}

// Close terminates a shell and removes it.
func (m *ShellManager) Close(shellID string) {
	m.mu.Lock()
	sh := m.shells[shellID]
	delete(m.shells, shellID)
	m.mu.Unlock()
	if sh != nil {
		sh.cancel()
	}
}

// Tasks returns the shell rows for get_tasks (sessions are added by the caller).
func (m *ShellManager) Tasks() []protocol.Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]protocol.Task, 0, len(m.shells))
	for id, sh := range m.shells {
		var pid *int
		alive := false
		if sh.cmd.Process != nil {
			p := sh.cmd.Process.Pid
			pid = &p
			alive = sh.cmd.ProcessState == nil
		}
		name := "Shell"
		if len(id) >= 4 {
			name = "Shell " + id[len(id)-4:]
		}
		out = append(out, protocol.Task{
			ID: id, Name: name, Type: "shell", PID: pid,
			IsStreaming: alive, Cwd: sh.cwd,
		})
	}
	return out
}

// KillTask kills a shell by id; reports whether it was found+terminated.
func (m *ShellManager) KillTask(id string) bool {
	m.mu.Lock()
	sh := m.shells[id]
	delete(m.shells, id)
	m.mu.Unlock()
	if sh == nil {
		return false
	}
	sh.cancel()
	return true
}

func (m *ShellManager) Has(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.shells[id]
	return ok
}

func isDir(p string) bool {
	info, err := os.Stat(p)
	return err == nil && info.IsDir()
}
