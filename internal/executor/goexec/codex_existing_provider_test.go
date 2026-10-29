package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/coder/websocket"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"everything-go/internal/recovery"
)

func existingProviderFixture(t *testing.T) (*Codex, string, string, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var methods []string
	socket, _ := healthServer(t, func(method, _ string) (any, bool) {
		mu.Lock()
		methods = append(methods, method)
		mu.Unlock()
		return map[string]any{}, true
	})
	root := t.TempDir()
	auth := filepath.Join(root, "original-provider-auth.json")
	if err := os.WriteFile(auth, []byte(`{"tokens":{"account_id":"original"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(root, "cli-was-invoked")
	cli := filepath.Join(root, "forbidden-cli")
	if err := os.WriteFile(cli, []byte("#!/bin/sh\n: > '"+marker+"'\nexit 99\n"), 0700); err != nil {
		t.Fatal(err)
	}
	c := NewCodex(&capSink{}, cli)
	c.appServerMode = "daemon"
	c.connectOnly = true
	c.appServerSocket = socket
	c.subjectAuthPath = auth
	c.sessionsRoot = filepath.Join(root, "private", "sessions")
	c.authPath = filepath.Join(root, "private", "auth.json")
	c.remoteReconnect = false
	t.Cleanup(func() { c.startMu.Lock(); _ = c.stopServerLocked(); c.startMu.Unlock() })
	return c, auth, marker, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), methods...) }
}

func TestExistingProviderConnectOnlyNeverManagesDaemonOrCopiesCredentials(t *testing.T) {
	c, auth, marker, methods := existingProviderFixture(t)
	before, _ := os.ReadFile(auth)
	for i := 0; i < 2; i++ {
		if err := c.ensureServer(); err != nil {
			t.Fatal(err)
		}
		if _, err := c.rpcCall("thread/goal/get", map[string]any{"threadId": "fixture"}, time.Second); err != nil {
			t.Fatal(err)
		}
		c.refreshRuntimeDiagnostics(filepath.Dir(c.sessionsRoot))
		if c.healthRestartOwner() {
			t.Fatal("attach client became recovery owner")
		}
		if err := c.recoverUnhealthyDaemon(); err == nil {
			t.Fatal("attach client restarted daemon")
		}
		c.startMu.Lock()
		err := c.stopServerLocked()
		c.startMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("CLI was invoked", err)
	}
	if _, err := os.Stat(filepath.Dir(c.sessionsRoot)); !os.IsNotExist(err) {
		t.Fatal("private provider config/credentials created", err)
	}
	after, _ := os.ReadFile(auth)
	if string(before) != string(after) {
		t.Fatal("original subject file changed")
	}
	calls := methods()
	initialized := 0
	for _, m := range calls {
		if m == "initialize" {
			initialized++
		}
		if m != "initialize" && m != "thread/goal/get" {
			t.Fatal("unexpected provider operation", m)
		}
	}
	if initialized != 2 {
		t.Fatal("original and reconnect not independently initialized", calls)
	}
}

func TestExistingProviderRejectsMissingForeignOrSymlinkSourcesBeforeEffects(t *testing.T) {
	for _, kind := range []string{"missing-socket", "relative-socket", "missing-auth", "empty-auth", "relative-auth", "symlink-auth", "stdio"} {
		t.Run(kind, func(t *testing.T) {
			c, auth, marker, methods := existingProviderFixture(t)
			switch kind {
			case "missing-socket":
				c.appServerSocket = filepath.Join(t.TempDir(), "absent.sock")
			case "relative-socket":
				c.appServerSocket = "relative.sock"
			case "missing-auth":
				c.subjectAuthPath = filepath.Join(t.TempDir(), "absent.json")
			case "empty-auth":
				if err := os.WriteFile(auth, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "relative-auth":
				c.subjectAuthPath = "relative.json"
			case "symlink-auth":
				p := filepath.Join(t.TempDir(), "alias.json")
				if err := os.Symlink(auth, p); err != nil {
					t.Fatal(err)
				}
				c.subjectAuthPath = p
			case "stdio":
				c.appServerMode = "stdio"
			}
			if err := c.ensureServer(); err == nil {
				t.Fatal("invalid source accepted")
			}
			if len(methods()) != 0 {
				t.Fatal("invalid source contacted provider")
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatal("invalid source invoked CLI", err)
			}
		})
	}
}

func TestExistingProviderKeepsOriginalLocalSubjectWriterFence(t *testing.T) {
	c, auth, marker, _ := existingProviderFixture(t)
	first, err := c.automaticLocalSubject()
	if err != nil || first != recovery.Hash([]byte("chatgpt:original")) {
		t.Fatal(first, err)
	}
	if err := os.WriteFile(auth, []byte(`{"tokens":{"account_id":"changed"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	second, err := c.automaticLocalSubject()
	if err != nil || second == first {
		t.Fatal("account change escaped original local fence", err)
	}
	if err := os.WriteFile(auth, []byte(`{"tokens":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = c.automaticLocalSubject(); err != recovery.ErrGuard {
		t.Fatal("missing subject accepted", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("subject read invoked CLI", err)
	}
}

func TestExistingProviderConfigurationOptInOnly(t *testing.T) {
	t.Setenv("EVERYTHING_GO_CODEX_CONNECT_ONLY", "true")
	t.Setenv("EVERYTHING_GO_CODEX_APP_SERVER_SOCKET", "/explicit/provider.sock")
	t.Setenv("EVERYTHING_GO_CODEX_PROVIDER_SUBJECT_AUTH_PATH", "/original/auth.json")
	c := NewCodex(&capSink{}, "codex")
	if !c.connectOnly || c.appServerSocket != "/explicit/provider.sock" || c.subjectAuthPath != "/original/auth.json" {
		t.Fatal("explicit configuration lost")
	}
	t.Setenv("EVERYTHING_GO_CODEX_CONNECT_ONLY", "false")
	c = NewCodex(&capSink{}, "codex")
	if c.connectOnly || strings.Contains(c.authPath, "/original/") {
		t.Fatal("original auth/default mode changed")
	}
}

func TestExistingProviderUnknownAccountDoesNotDropRecoveryGuard(t *testing.T) {
	c, a, w, _, _ := automaticExecutionFixture(t)
	s := c.threadToSession["root"]
	st := c.state(s.ID)
	st.turnActive, st.turnStartPending = false, false
	c.appServerMode, c.remoteConn = "daemon", &websocket.Conn{}
	c.connectOnly = true
	w.call = func(method string, _ json.RawMessage) (json.RawMessage, error) {
		switch method {
		case "account/read":
			return autoResult(map[string]any{"account": map[string]any{"type": "chatgpt"}}), nil
		case "account/rateLimits/read":
			return autoResult(map[string]any{"accountId": "original", "ordinaryUsageAllowed": nil}), nil
		default:
			return autoResult(map[string]any{}), nil
		}
	}
	err := c.Send(recovery.WithAutomatic(context.Background(), a), s, a.src.Request, "original objective", nil, nil)
	if !errors.Is(err, recovery.ErrGuard) {
		t.Fatal("unknown provider did not fail closed", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.starts) != 0 {
		t.Fatal("unknown provider downgraded to unguarded initial send")
	}
}
