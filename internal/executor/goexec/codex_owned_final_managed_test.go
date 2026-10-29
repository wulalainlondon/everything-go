package goexec

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"everything-go/internal/recovery"
	"everything-go/internal/session"
)

func managedOwnedFixture(t *testing.T, mode string) (*Codex, *session.Session, *toolTestWriter) {
	t.Helper()
	c, s, w := ownedFinalFixture(t, mode)
	c.connectOnly = false
	c.authPath = c.subjectAuthPath
	t.Setenv("EVERYTHING_GO_CODEX_EXACT_FINAL_SUBJECT_SHA256", "")
	return c, s, w
}
func TestManagedOwnedFinalExistingConnectionPaidResultAndEpochFences(t *testing.T) {
	for _, mode := range []string{"single", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			c, s, w := managedOwnedFixture(t, mode)
			got, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1")
			if mode == "single" && (err != nil || got == nil || len(w.methods) != 5) {
				t.Fatal(got, err, w.methods)
			}
			n := len(w.methods)
			c.rpc.transportEpoch.Add(1)
			if got, err = c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1"); got != nil || err == nil || len(w.methods) != n {
				t.Fatal("reconnected execution replayed", got, err, w.methods)
			}
			// Eviction/restart cannot remove the durable UNKNOWN/success reservation.
			c.ownedFinals = nil
			c.ownedFinalOrder = nil
			if got, err = c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1"); got != nil || err == nil || len(w.methods) != n {
				t.Fatal("lost RAM allowed replay", got, err, w.methods)
			}
		})
	}
}
func TestManagedOwnedFinalNewExecutionAfterReconnect(t *testing.T) {
	c, s, w := managedOwnedFixture(t, "single")
	if _, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1"); err != nil {
		t.Fatal(err)
	}
	c.rpc.transportEpoch.Add(1)
	if err := c.rememberAcceptedTurnRequest("root", "turn-2", "request-2"); err != nil {
		t.Fatal(err)
	}
	original := w.reply
	w.reply = func(method string, raw json.RawMessage) (any, error) {
		// Independent provider oracle advances to the second completed execution.
		translated := json.RawMessage(strings.ReplaceAll(string(raw), "turn-2", "turn-1"))
		value, err := original(method, translated)
		if err != nil {
			return nil, err
		}
		encoded, _ := json.Marshal(value)
		var out any
		json.Unmarshal([]byte(strings.ReplaceAll(string(encoded), "turn-1", "turn-2")), &out)
		return out, nil
	}
	got, err := c.ExactProviderFinalForRequest(s, "request-2", "root", "turn-2")
	if err != nil || got == nil || got.TurnID != "turn-2" || len(w.methods) != 10 {
		t.Fatal(got, err, w.methods)
	}
}
func TestManagedOwnedFinalBoundedRAMRetainsDurableOnce(t *testing.T) {
	c, s, w := managedOwnedFixture(t, "single")
	for i := 0; i < 130; i++ {
		target := session.NewRegistry().Create(fmt.Sprintf("managed-%d", i), "fixture", t.TempDir(), "codex", "", "", "root")
		if i == 0 {
			s = target
		}
		if f, err := c.ExactProviderFinalForRequest(target, "request-1", "root", "turn-1"); err != nil || f == nil {
			t.Fatalf("execution %d: %v", i, err)
		}
	}
	if len(c.ownedFinals) != 128 || len(c.ownedFinalOrder) != 128 || len(w.methods) != 650 {
		t.Fatal(len(c.ownedFinals), len(w.methods))
	}
	n := len(w.methods)
	if got, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1"); got != nil || err == nil || len(w.methods) != n {
		t.Fatal("FIFO erased once", got, err)
	}
	entries, err := os.ReadDir(filepath.Join(c.dataDir, "codex-exact-final-reads"))
	if err != nil || len(entries) != 130 {
		t.Fatal(len(entries), err)
	}
}
func TestManagedOwnedFinalUnsupportedAndLocalGuardsMakeZeroRPC(t *testing.T) {
	for _, mode := range []string{"missing-auth", "api-key", "no-epoch", "relative-data", "symlink-data", "writable-data", "symlink-journal", "existing-empty", "existing-malformed", "journal-file", "sandbox"} {
		t.Run(mode, func(t *testing.T) {
			c, s, w := managedOwnedFixture(t, "single")
			switch mode {
			case "missing-auth":
				os.Remove(c.authPath)
			case "api-key":
				os.WriteFile(c.authPath, []byte(`{"OPENAI_API_KEY":"fixture-no-secret"}`), 0600)
			case "no-epoch":
				c.rpc.transportEpoch.Store(0)
			case "relative-data":
				c.dataDir = "relative-data"
			case "symlink-data":
				p := filepath.Join(t.TempDir(), "link")
				os.Symlink(c.dataDir, p)
				c.dataDir = p
			case "writable-data":
				os.Chmod(c.dataDir, 0777)
			case "symlink-journal":
				os.Symlink(t.TempDir(), filepath.Join(c.dataDir, "codex-exact-final-reads"))
			case "existing-empty", "existing-malformed":
				os.Mkdir(filepath.Join(c.dataDir, "codex-exact-final-reads"), 0700)
				identity, _ := json.Marshal([]string{s.ID, "request-1", "root", "turn-1"})
				content := []byte{}
				if mode == "existing-malformed" {
					content = []byte("broken")
				}
				os.WriteFile(filepath.Join(c.dataDir, "codex-exact-final-reads", recovery.Hash(identity)+".json"), content, 0600)
			case "journal-file":
				os.WriteFile(filepath.Join(c.dataDir, "codex-exact-final-reads"), []byte("not-directory"), 0600)
			case "sandbox":
				c.appSandbox = &codexAppSandbox{}
			}
			got, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1")
			if got != nil || len(w.methods) != 0 {
				t.Fatal(got, err, w.methods)
			}
		})
	}
}
func TestManagedOwnedFinalChangedMarkerOrSubjectRejectsCached(t *testing.T) {
	for _, mode := range []string{"marker", "subject", "source"} {
		t.Run(mode, func(t *testing.T) {
			c, s, w := managedOwnedFixture(t, "single")
			if _, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1"); err != nil {
				t.Fatal(err)
			}
			n := len(w.methods)
			switch mode {
			case "marker":
				entries, _ := os.ReadDir(filepath.Join(c.dataDir, "codex-exact-final-reads"))
				os.WriteFile(filepath.Join(c.dataDir, "codex-exact-final-reads", entries[0].Name()), []byte("truncated"), 0600)
			case "subject":
				os.WriteFile(c.authPath, []byte(`{"tokens":{"account_id":"other"}}`), 0600)
			case "source":
				s.SetModel("other-config")
			}
			if got, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1"); got != nil || err == nil || len(w.methods) != n {
				t.Fatal(got, err, w.methods)
			}
		})
	}
}
func TestManagedOwnedFinalReservationContendsAcrossHosts(t *testing.T) {
	c, s, _ := managedOwnedFixture(t, "single")
	identity, _ := json.Marshal([]string{s.ID, "request-1", "root", "turn-1"})
	key := recovery.Hash(identity)
	// Distinct hosts share only DATA_DIR, not the in-memory mutex/cache.
	other := &Codex{dataDir: c.dataDir}
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for _, host := range []*Codex{c, other} {
		wg.Add(1)
		go func(host *Codex) {
			defer wg.Done()
			if host.reserveOwnedFinal(key, "subject", 1, 0, 0) == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}(host)
	}
	wg.Wait()
	if wins != 1 {
		t.Fatal("exclusive reservation winners", wins)
	}
}

func TestManagedOwnedFinalLegacyAndObservedExecutionNeverBackfilled(t *testing.T) {
	for _, mode := range []string{"legacy", "observed", "repeated-acceptance"} {
		t.Run(mode, func(t *testing.T) {
			c, s, w := managedOwnedFixture(t, "single")
			if err := os.Remove(c.turnRequestsPath("root")); err != nil {
				t.Fatal(err)
			}
			if err := c.rememberTurnRequest("root", "turn-1", "request-1"); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(c.turnRequestsPath("root"))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "repeated-acceptance" {
				if err := c.rememberAcceptedTurnRequest("root", "turn-1", "request-1"); err != nil {
					t.Fatal(err)
				}
			}
			got, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1")
			if got != nil || err != nil || len(w.methods) != 0 {
				t.Fatal("legacy execution acquired fallback", got, err, w.methods)
			}
			after, _ := os.ReadFile(c.turnRequestsPath("root"))
			if string(before) != string(after) {
				t.Fatal("legacy map backfilled")
			}
			if _, err := os.Stat(filepath.Join(c.dataDir, "codex-exact-final-reads")); !os.IsNotExist(err) {
				t.Fatal("legacy read created once journal", err)
			}
		})
	}
}

func TestManagedOwnedFinalNonregularCachedMarkerNeverBlocks(t *testing.T) {
	for _, mode := range []string{"fifo", "socket", "directory", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			c, s, w := managedOwnedFixture(t, "single")
			if _, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1"); err != nil {
				t.Fatal(err)
			}
			n := len(w.methods)
			dir := filepath.Join(c.dataDir, "codex-exact-final-reads")
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 {
				t.Fatal(err)
			}
			path := filepath.Join(dir, entries[0].Name())
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "fifo":
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			case "socket":
				// Darwin limits sockaddr_un path length; bind at a short temporary
				// name, then move the real socket node to the cache marker.
				shortDir, err := os.MkdirTemp("/tmp", "owned-final-socket-")
				if err != nil {
					t.Fatal(err)
				}
				defer os.RemoveAll(shortDir)
				shortPath := filepath.Join(shortDir, "s")
				l, err := net.Listen("unix", shortPath)
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				if err := os.Rename(shortPath, path); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				target := filepath.Join(t.TempDir(), "regular")
				os.WriteFile(target, []byte("metadata"), 0600)
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan bool, 1)
			go func() {
				f, err := c.ExactProviderFinalForRequest(s, "request-1", "root", "turn-1")
				done <- f == nil && err != nil
			}()
			select {
			case rejected := <-done:
				if !rejected || len(w.methods) != n {
					t.Fatal("nonregular marker accepted/repolled")
				}
			case <-time.After(400 * time.Millisecond):
				// Release a regressed FIFO reader before failing; never leave a blocked test.
				if mode == "fifo" {
					f, _ := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
					if f != nil {
						f.Close()
					}
				}
				t.Fatal("nonregular marker blocked under final-reader mutex")
			}
		})
	}
}
