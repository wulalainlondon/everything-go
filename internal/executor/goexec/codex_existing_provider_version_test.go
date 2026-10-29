package goexec

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestExistingProviderVersionUsesOnlyLeadingServerBuild(t *testing.T) {
	cases := []struct{ name, raw, want string }{
		{"actual-shaped", `{"userAgent":"codex_cli_rs/0.162.1 (Mac OS 26.1; arm64) bridge/1.0 (claude-bridge; 1.0)"}`, "0.162.1"},
		{"client-suffix-not-server", `{"userAgent":"codex_cli_rs/9.9.9 (Mac OS; arm64) (claude-bridge; 0.162.1)"}`, "9.9.9"},
		{"space-originator", `{"userAgent":"Codex Desktop/0.153.0 (Mac OS; arm64)"}`, "0.153.0"},
		{"missing", `{}`, "unknown"}, {"wrong-type", `{"userAgent":42}`, "unknown"},
		{"suffix-only", `{"userAgent":"(claude-bridge; 0.162.1)"}`, "unknown"},
		{"embedded-version", `{"userAgent":"garbage 0.162.1"}`, "unknown"},
		{"bad-leading-version", `{"userAgent":"codex_cli_rs/0.162.1evil (Mac OS)"}`, "unknown"},
		{"newline", `{"userAgent":"codex_cli_rs/0.162.1 (Mac OS)\nforged"}`, "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := existingProviderDiagnostics(json.RawMessage(tc.raw), 4, "original.sock")
			if d["running_version"] != tc.want || d["status"] != "externally_managed" || d["version_transport_epoch"] != uint64(4) {
				t.Fatal(d)
			}
			if _, ok := d["userAgent"]; ok {
				t.Fatal("raw user agent exported")
			}
		})
	}
}

func TestExistingProviderInitializeVersionBoundToLiveTransport(t *testing.T) {
	c, auth, marker, _ := existingProviderFixture(t)
	before, _ := os.ReadFile(auth)
	var raw atomic.Value
	raw.Store(`{"userAgent":"codex_cli_rs/0.162.1 (Mac OS 26; arm64) (claude-bridge; 1.0)"}`)
	var initializes atomic.Int64
	socket, _ := healthServer(t, func(method, _ string) (any, bool) {
		if method != "initialize" {
			t.Errorf("unexpected provider method %s", method)
		}
		initializes.Add(1)
		var x any
		if err := json.Unmarshal([]byte(raw.Load().(string)), &x); err != nil {
			t.Error(err)
		}
		return x, true
	})
	c.appServerSocket = socket
	if err := c.ensureServer(); err != nil {
		t.Fatal(err)
	}
	if c.taskDaemonVersion() != "0.162.1" {
		t.Fatal(c.RuntimeDiagnostics())
	}
	c.refreshRuntimeDiagnostics(filepath.Dir(c.sessionsRoot))
	if c.taskDaemonVersion() != "0.162.1" {
		t.Fatal("refresh discarded original handshake")
	}
	first := c.rpc.transportEpoch.Load()
	c.startMu.Lock()
	err := c.stopServerLocked()
	c.startMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if c.taskDaemonVersion() != "unknown" {
		t.Fatal("retired provider still qualified")
	}
	c.refreshRuntimeDiagnostics(filepath.Dir(c.sessionsRoot))
	if c.taskDaemonVersion() != "unknown" {
		t.Fatal("refresh revived retired version")
	}
	raw.Store(`{}`)
	if err := c.ensureServer(); err != nil {
		t.Fatal(err)
	}
	if c.taskDaemonVersion() != "unknown" || c.rpc.transportEpoch.Load() == first {
		t.Fatal("missing new handshake version inherited old capability")
	}
	if initializes.Load() != 2 {
		t.Fatal("unexpected initialization count", initializes.Load())
	}
	after, _ := os.ReadFile(auth)
	if string(after) != string(before) {
		t.Fatal("original subject changed")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("CLI invoked", err)
	}
	if _, err := os.Stat(filepath.Dir(c.sessionsRoot)); !os.IsNotExist(err) {
		t.Fatal("private credentials/config created", err)
	}
}

func TestExistingProviderVersionCannotSurviveWriterEpochChange(t *testing.T) {
	c := NewCodex(&capSink{}, "forbidden")
	c.connectOnly = true
	c.rpc.setWriter(io.Discard)
	epoch := c.rpc.transportEpoch.Load()
	c.runtimeDiagnostics = map[string]any{"codex": existingProviderDiagnostics(json.RawMessage(`{"userAgent":"codex_cli_rs/0.162.1 (OS)"}`), epoch, "socket")}
	if c.taskDaemonVersion() != "0.162.1" {
		t.Fatal("known same epoch refused")
	}
	c.rpc.setWriter(io.Discard)
	if c.taskDaemonVersion() != "unknown" {
		t.Fatal("replaced writer inherited version")
	}
	c.refreshRuntimeDiagnostics("")
	if c.taskDaemonVersion() != "unknown" {
		t.Fatal("refreshed replaced writer inherited version")
	}
}

func TestExistingProviderVersionDoesNotChangeManagedDefault(t *testing.T) {
	c := NewCodex(&capSink{}, "unused")
	c.connectOnly = false
	c.runtimeDiagnostics = map[string]any{"codex": map[string]any{"running_version": "0.153.0"}}
	if c.taskDaemonVersion() != "0.153.0" {
		t.Fatal("managed default changed")
	}
	c.connectOnly = true
	if c.taskDaemonVersion() != "unknown" {
		t.Fatal("unbound managed version borrowed by connect-only")
	}
}
