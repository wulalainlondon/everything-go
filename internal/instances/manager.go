// Package instances persists and supervises secondary Go bridge instances.
package instances

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var validName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type Instance struct {
	Name    string `json:"name"`
	Port    int    `json:"port"`
	RootDir string `json:"root_dir"`
	DataDir string `json:"data_dir"`
	Backend string `json:"backend,omitempty"`
	Model   string `json:"model,omitempty"`
}

type Status struct {
	Instance
	State         string `json:"state"`
	SupervisorPID *int   `json:"supervisor_pid"`
	BridgePID     *int   `json:"bridge_pid"`
}

type diskState struct {
	Instances []Instance `json:"instances"`
}

type Manager struct {
	mu      sync.Mutex
	path    string
	exePath string
	started map[string]*os.Process
}

func New(dataDir, exePath string) *Manager {
	return &Manager{path: filepath.Join(dataDir, "instances.json"), exePath: exePath, started: make(map[string]*os.Process)}
}

func (m *Manager) List() []Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	items, _ := m.load()
	result := make([]Status, 0, len(items))
	for _, item := range items {
		result = append(result, m.status(item))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func (m *Manager) Upsert(item Instance) (Status, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items, err := m.load()
	if err != nil {
		return Status{}, "store_read_failed"
	}
	if code := validate(item, items); code != "" {
		return Status{}, code
	}
	found := false
	for i := range items {
		if items[i].Name == item.Name {
			items[i] = item
			found = true
			break
		}
	}
	if !found {
		items = append(items, item)
	}
	if err := m.save(items); err != nil {
		return Status{}, "store_write_failed"
	}
	return m.status(item), ""
}

func (m *Manager) Delete(name string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if name == "default" {
		return "default_immutable"
	}
	items, err := m.load()
	if err != nil {
		return "store_read_failed"
	}
	index := -1
	for i := range items {
		if items[i].Name == name {
			index = i
			break
		}
	}
	if index < 0 {
		return "not_found"
	}
	if _, ok := m.managedProcess(items[index]); ok {
		return "still_running"
	}
	items = append(items[:index], items[index+1:]...)
	if err := m.save(items); err != nil {
		return "store_write_failed"
	}
	return ""
}

func (m *Manager) Start(name string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	item, ok := m.find(name)
	if !ok {
		return "not_found"
	}
	if _, ok := m.managedProcess(item); ok {
		return ""
	}
	if m.exePath == "" {
		return "executable_not_found"
	}
	if err := os.MkdirAll(item.DataDir, 0o700); err != nil {
		return "data_dir_create_failed"
	}
	logFile, err := os.OpenFile(filepath.Join(item.DataDir, "bridge.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return "log_open_failed"
	}
	errFile, err := os.OpenFile(filepath.Join(item.DataDir, "bridge.err"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		_ = logFile.Close()
		return "log_open_failed"
	}
	args := []string{"--port", strconv.Itoa(item.Port), "--data-dir", item.DataDir, "--instance-name", item.Name, "--disable-native-watcher", "--disable-search"}
	if item.RootDir != "" {
		args = append(args, "--root-dir", item.RootDir)
	}
	cmd := exec.Command(m.exePath, args...)
	cmd.Stdout, cmd.Stderr = logFile, errFile
	configureDetached(cmd)
	err = cmd.Start()
	_ = logFile.Close()
	_ = errFile.Close()
	if err != nil {
		return "spawn_failed"
	}
	m.started[name] = cmd.Process
	if err := atomicWrite(filepath.Join(item.DataDir, "bridge.pid"), []byte(strconv.Itoa(cmd.Process.Pid))); err != nil {
		_ = cmd.Process.Kill()
		delete(m.started, name)
		return "pid_write_failed"
	}
	_ = atomicWrite(filepath.Join(item.DataDir, ".bridge_state"), []byte("enabled"))
	go func() { _ = cmd.Wait() }()
	return ""
}

func (m *Manager) Stop(name string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if name == "default" {
		return "default_immutable"
	}
	item, ok := m.find(name)
	if !ok {
		return "not_found"
	}
	proc, running := m.managedProcess(item)
	if !running {
		_ = atomicWrite(filepath.Join(item.DataDir, ".bridge_state"), []byte("disabled"))
		return "not_running"
	}
	if err := terminateProcess(proc); err != nil {
		return "kill_failed"
	}
	delete(m.started, name)
	_ = atomicWrite(filepath.Join(item.DataDir, ".bridge_state"), []byte("disabled"))
	_ = os.Remove(filepath.Join(item.DataDir, "bridge.pid"))
	return ""
}

func (m *Manager) find(name string) (Instance, bool) {
	items, _ := m.load()
	for _, item := range items {
		if item.Name == name {
			return item, true
		}
	}
	return Instance{}, false
}

func (m *Manager) status(item Instance) Status {
	state := "stopped"
	var pid *int
	if proc, ok := m.managedProcess(item); ok {
		state, pid = "running", &proc.Pid
	} else if bytes, err := os.ReadFile(filepath.Join(item.DataDir, ".bridge_state")); err == nil && strings.TrimSpace(string(bytes)) == "enabled" {
		state = "crashed"
	}
	return Status{Instance: item, State: state, SupervisorPID: pid, BridgePID: pid}
}

func (m *Manager) managedProcess(item Instance) (*os.Process, bool) {
	if proc := m.started[item.Name]; proc != nil && processAlive(proc.Pid) {
		return proc, true
	}
	data, err := os.ReadFile(filepath.Join(item.DataDir, "bridge.pid"))
	if err != nil {
		return nil, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 || !processAlive(pid) || !processMatches(pid, m.exePath, item.DataDir) {
		return nil, false
	}
	proc, err := os.FindProcess(pid)
	return proc, err == nil
}

func validate(item Instance, existing []Instance) string {
	if !validName.MatchString(item.Name) {
		if strings.TrimSpace(item.Name) == "" {
			return "name_empty"
		}
		return "name_invalid"
	}
	if item.Port < 1024 || item.Port > 65535 {
		return "port_invalid"
	}
	validBackend := map[string]bool{"": true, "claude": true, "codex": true, "ollama": true, "gemini": true}
	if !validBackend[item.Backend] {
		return "backend_invalid"
	}
	if item.RootDir != "" {
		clean := filepath.Clean(item.RootDir)
		home, _ := os.UserHomeDir()
		if !filepath.IsAbs(item.RootDir) || strings.Contains(item.RootDir, "..") {
			return "root_dir_invalid"
		}
		if clean == string(filepath.Separator) || clean == home {
			return "root_dir_sensitive"
		}
		if info, err := os.Stat(clean); err != nil || !info.IsDir() {
			return "root_dir_missing"
		}
	}
	for _, old := range existing {
		if old.Port == item.Port && old.Name != item.Name {
			return "port_in_use"
		}
		if old.Name == "default" && old.Name == item.Name && old.Port != item.Port {
			return "default_immutable"
		}
	}
	return ""
}

func (m *Manager) load() ([]Instance, error) {
	data, err := os.ReadFile(m.path)
	if errors.Is(err, os.ErrNotExist) {
		return []Instance{}, nil
	}
	if err != nil {
		return nil, err
	}
	var state diskState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return state.Instances, nil
}

func (m *Manager) save(items []Instance) error {
	data, err := json.MarshalIndent(diskState{Instances: items}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(m.path), 0o700); err != nil {
		return err
	}
	return atomicWrite(m.path, append(data, '\n'))
}

func atomicWrite(name string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(name), ".instances-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, name); err != nil {
		return fmt.Errorf("replace store: %w", err)
	}
	return nil
}
