// Package lifecycle records bounded, non-content execution lifecycle evidence.
package lifecycle

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Event struct {
	At        int64  `json:"at"`
	Kind      string `json:"kind"`
	Owner     string `json:"owner"`
	PID       int    `json:"pid,omitempty"`
	ParentPID int    `json:"parent_pid,omitempty"`
	Epoch     uint64 `json:"transport_epoch"`
	Request   string `json:"request,omitempty"`
	Turn      string `json:"turn,omitempty"`
	ExitCode  *int   `json:"exit_code,omitempty"`
	Signal    string `json:"signal,omitempty"`
	Reason    string `json:"reason"`
}

type Journal struct {
	mu   sync.Mutex
	path string
}

func New(dir string) *Journal { return &Journal{path: filepath.Join(dir, "native-lifecycle-v1.json")} }

// Add publishes one atomic private bounded snapshot, including a parent-dir
// sync. This journal is evidence, never an authorization or replay ledger.
func (j *Journal) Add(e Event) error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	var rows []Event
	if b, err := os.ReadFile(j.path); err == nil {
		if len(b) > 1<<20 || json.Unmarshal(b, &rows) != nil {
			return errors.New("lifecycle_journal_invalid")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	e.At = time.Now().UnixMilli()
	// Reject oversize identities rather than truncating them into a different
	// request. The limit also bounds the next reader's complete snapshot.
	entry, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if len(entry) > 1800 {
		return errors.New("lifecycle_event_too_large")
	}
	rows = append(rows, e)
	if len(rows) > 512 {
		rows = rows[len(rows)-512:]
	}
	b, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	if len(b) > 1<<20 {
		return errors.New("lifecycle_journal_too_large")
	}
	if err = os.MkdirAll(filepath.Dir(j.path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(j.path), ".lifecycle-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err = f.Chmod(0600); err != nil {
		return err
	}
	if _, err = f.Write(b); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), j.path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(j.path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
