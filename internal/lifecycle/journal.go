// Package lifecycle records private bounded execution evidence, never authority.
package lifecycle

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const maxBytes = 1 << 20

type ProcessIdentity struct {
	PID         int    `json:"pid"`
	ParentPID   int    `json:"parent_pid"`
	Group       int    `json:"group"`
	Start       string `json:"start"`
	Executable  string `json:"executable"`
	Observation string `json:"observation"`
}

type Event struct {
	At        int64            `json:"at"`
	Kind      string           `json:"kind"`
	Owner     string           `json:"owner"`
	PID       int              `json:"pid,omitempty"`
	ParentPID int              `json:"parent_pid,omitempty"`
	Boot      string           `json:"service_boot,omitempty"`
	Service   ProcessIdentity  `json:"service"`
	Process   *ProcessIdentity `json:"process,omitempty"`
	Epoch     uint64           `json:"transport_epoch"`
	Session   string           `json:"session,omitempty"`
	Thread    string           `json:"thread,omitempty"`
	Request   string           `json:"request,omitempty"`
	Turn      string           `json:"turn,omitempty"`
	ExitCode  *int             `json:"exit_code,omitempty"`
	Signal    string           `json:"signal,omitempty"`
	Reason    string           `json:"reason"`
}

var birth struct {
	sync.Once
	boot    string
	process ProcessIdentity
	err     error
}

type Journal struct {
	mu            sync.Mutex
	path          string
	beforePublish func() // nil in production; own-fixture original-path substitution
}

func New(dir string) *Journal {
	birth.Do(func() {
		var token [16]byte
		_, birth.err = rand.Read(token[:])
		birth.boot = hex.EncodeToString(token[:])
		birth.process = ObserveProcess(os.Getpid())
	})
	return &Journal{path: filepath.Join(dir, "native-lifecycle-v1.json")}
}

func privateRegular(s os.FileInfo) bool {
	return s.Mode().IsRegular() && s.Mode().Perm() == 0600 && privateOwner(s)
}
func sameOriginal(path string, original os.FileInfo) bool {
	now, err := os.Lstat(path)
	if original == nil {
		return os.IsNotExist(err)
	}
	return err == nil && privateRegular(now) && os.SameFile(original, now) && now.Size() == original.Size() && now.ModTime() == original.ModTime()
}
func validEvent(e Event) bool {
	if e.Kind == "" || e.Owner == "" || e.Reason == "" || e.At <= 0 {
		return false
	}
	b, err := json.Marshal(e)
	return err == nil && len(b) <= 1800
}

// Add refuses invalid original evidence. Reads stop at max+1 bytes and never
// follow the final path or wait on a FIFO. Publication rechecks original identity.
func (j *Journal) Add(e Event) error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if birth.err != nil {
		return errors.New("lifecycle_birth_unavailable")
	}
	var rows []Event
	original, err := os.Lstat(j.path)
	if err == nil {
		if !privateRegular(original) || original.Size() <= 0 || original.Size() > maxBytes {
			return errors.New("lifecycle_journal_not_private_bounded_regular")
		}
		f, err := openJournal(j.path)
		if err != nil {
			return err
		}
		opened, statErr := f.Stat()
		if statErr != nil || !privateRegular(opened) || !os.SameFile(original, opened) {
			f.Close()
			return errors.New("lifecycle_journal_changed")
		}
		b, readErr := io.ReadAll(io.LimitReader(f, maxBytes+1))
		f.Close()
		if readErr != nil {
			return readErr
		}
		if len(b) > maxBytes || len(b) != int(original.Size()) || !sameOriginal(j.path, original) {
			return errors.New("lifecycle_journal_changed_or_oversize")
		}
		decoder := json.NewDecoder(bytes.NewReader(b))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&rows) != nil || rows == nil || len(rows) > 512 || decoder.Decode(new(any)) != io.EOF {
			return errors.New("lifecycle_journal_invalid")
		}
		for _, row := range rows {
			if !validEvent(row) {
				return errors.New("lifecycle_journal_invalid_row")
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	} else {
		original = nil
	}
	e.At = time.Now().UnixMilli()
	e.Boot = birth.boot
	e.Service = birth.process
	if !validEvent(e) {
		return errors.New("lifecycle_event_invalid_or_oversize")
	}
	rows = append(rows, e)
	if len(rows) > 512 {
		rows = rows[len(rows)-512:]
	}
	b, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	if len(b) > maxBytes {
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
	if j.beforePublish != nil {
		j.beforePublish()
	}
	if !sameOriginal(j.path, original) {
		return errors.New("lifecycle_original_changed_before_publish")
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
