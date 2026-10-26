package goexec

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/recovery"
	"everything-go/internal/session"
)

// One real provider turn, opt-in only AFTER Root's exact source/canary review.
// Fault recovery is tested separately with fake daemon/clock; this test never
// manufactures a real canonical failure, overloads a provider or retries.
func TestAutomaticNativeCanaryOneReadonlyTurn(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_RUN_NETWORK_NATIVE_CANARY") != "AUTHORIZED_ONCE" {
		t.Skip("Root-only reviewed one-turn native canary; no live work by default")
	}
	dir := os.Getenv("NETWORK_NATIVE_CANARY_DIR")
	subject := os.Getenv("NETWORK_NATIVE_CANARY_SUBJECT_SHA256")
	if !filepath.IsAbs(dir) || len(subject) != 64 {
		t.Fatal("fresh private canary directory and original readonly subject hash required")
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	entries, e := os.ReadDir(dir)
	if e != nil || len(entries) > 0 {
		t.Fatal("fresh empty once directory required; never retry original canary")
	}
	var nonce [16]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		t.Fatal(e)
	}
	request := "r_" + hex.EncodeToString(nonce[:])
	body := "這是一次唯讀、無工具的連線驗收。只回覆 NETWORK188_NATIVE_OK，不讀檔、不使用任何工具、不執行命令、不使用網路、不聯絡任何人、不新增其他工作。"
	intent := map[string]any{"schema": 1, "request_id": request, "model": "gpt-6.1-sol", "effort": "medium", "sandbox": "read-only", "promptSHA256": recovery.Hash([]byte(body)), "providerSubjectSHA256": subject, "maxNativeTurns": 1, "retries": 0, "providerFailureInjection": false, "sharedDaemonChanges": 0}
	raw, _ := json.MarshalIndent(intent, "", "  ")
	file, e := os.OpenFile(filepath.Join(dir, "native-canary.intent.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		t.Fatal("original once intent already exists or cannot be committed; do not replay", e)
	}
	if _, e = file.Write(append(raw, '\n')); e != nil {
		file.Close()
		t.Fatal(e)
	}
	if e = file.Sync(); e != nil {
		file.Close()
		t.Fatal(e)
	}
	if e = file.Close(); e != nil {
		t.Fatal(e)
	}
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	c.appServerMode = "stdio"
	privateHome, e := prepareAutomaticCanaryHome(c.authPath, dir)
	if e != nil {
		t.Fatal("private original-auth copy failed; no native model submit", e)
	}
	c.sessionsRoot = filepath.Join(privateHome, "sessions")
	c.authPath = filepath.Join(privateHome, "auth.json")
	c.dataDir = dir
	if e = c.ensureServer(); e != nil {
		t.Fatal(e)
	}
	submitted, terminal := false, false
	defer func() {
		if submitted && !terminal {
			t.Log("original canary outcome unknown: preserve private process/intent; no interrupt or retry")
			return
		}
		c.startMu.Lock()
		defer c.startMu.Unlock()
		if e := c.stopServerLocked(); e != nil {
			t.Error(e)
		} else if e = os.Remove(c.authPath); e != nil {
			t.Error("private auth cleanup failed")
		}
	}()
	actual, e := c.automaticProviderSubject()
	if e != nil || actual != subject {
		t.Fatal("original provider subject differs; no model turn submitted", e)
	}
	configRaw, e := c.rpcCall("config/read", map[string]any{"includeLayers": false}, 5*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	var config struct {
		Config map[string]json.RawMessage `json:"config"`
	}
	if json.Unmarshal(configRaw, &config) != nil {
		t.Fatal("unverified tool configuration")
	}
	var mcp map[string]json.RawMessage
	if json.Unmarshal(config.Config["mcp_servers"], &mcp) != nil && len(config.Config["mcp_servers"]) > 0 {
		t.Fatal("unverified MCP configuration")
	}
	names := []string{}
	for name := range mcp {
		names = append(names, name)
	}
	cwd := filepath.Join(dir, "empty-readonly-workspace")
	if e = os.Mkdir(cwd, 0700); e != nil {
		t.Fatal(e)
	}
	response, e := c.rpcCall("thread/start", map[string]any{"model": "gpt-6.1-sol", "cwd": cwd, "sandbox": "read-only", "approvalPolicy": "never", "config": codexPMConfig(names), "dynamicTools": []any{}}, 15*time.Second)
	if e != nil {
		t.Fatal("thread creation outcome unknown; do not retry", e)
	}
	var started struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if json.Unmarshal(response, &started) != nil || started.Thread.ID == "" {
		t.Fatal("unknown original thread; do not retry")
	}
	s := session.NewRegistry().Create("network-native-canary188", "single readonly canary", cwd, backend.Codex, "gpt-6.1-sol", "read-only", started.Thread.ID)
	s.SetEffort("medium")
	st := c.state(s.ID)
	st.threadID = started.Thread.ID
	c.mu.Lock()
	c.threadToSession[started.Thread.ID] = s
	c.mu.Unlock()
	threadReceipt, _ := json.MarshalIndent(map[string]any{"request_id": request, "thread_id": started.Thread.ID, "model": "gpt-6.1-sol", "privateStdioAppServer": true, "nativeTurnSubmitted": false}, "", "  ")
	if e = os.WriteFile(filepath.Join(dir, "native-canary.thread.json"), append(threadReceipt, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
	submitted = true
	if e = c.Send(context.Background(), s, request, body, nil, nil); e != nil {
		t.Fatal("once Send returned an error; inspect original receipt, never replay", e)
	}
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		if sink.count(func(v any) bool {
			switch v.(type) {
			case protocol.Done, protocol.Error, backend.NativeSubmissionUncertain:
				return true
			}
			return false
		}) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if sink.count(func(v any) bool { _, ok := v.(protocol.Done); return ok }) != 1 || sink.count(func(v any) bool { _, ok := v.(protocol.Error); return ok }) != 0 {
		t.Fatal("original native result not exactly one completed turn; no retry allowed")
	}
	terminal = true
	turn, e := c.NativeTurnForRequest(s, request)
	if e != nil || turn == "" {
		t.Fatal(e)
	}
	final, found, e := c.FinalAnswerForRequest(s, request)
	if e != nil || !found || final != "NETWORK188_NATIVE_OK" {
		t.Fatal("original exact native final missing or unexpected; no retry", e)
	}
	// The scoped native tools boundary is also checked independently of output.
	full, e := c.rpcCall("thread/turns/list", map[string]any{"threadId": started.Thread.ID, "limit": 1, "sortDirection": "desc", "itemsView": "full"}, 10*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	var page struct {
		Data []struct {
			ID        string            `json:"id"`
			Status    string            `json:"status"`
			ItemsView string            `json:"itemsView"`
			Items     []json.RawMessage `json:"items"`
		} `json:"data"`
	}
	if json.Unmarshal(full, &page) != nil || len(page.Data) != 1 || page.Data[0].ID != turn || page.Data[0].Status != "completed" || page.Data[0].ItemsView != "full" || page.Data[0].Items == nil {
		t.Fatal("canonical complete turn unavailable")
	}
	for _, raw := range page.Data[0].Items {
		var item struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(raw, &item) != nil {
			t.Fatal("malformed canonical item")
		}
		switch item.Type {
		case "userMessage", "agentMessage", "reasoning":
		default:
			t.Fatal("unexpected tool execution; no repeat")
		}
	}
	result := map[string]any{"schema": 1, "request_id": request, "thread_id": started.Thread.ID, "turn_id": turn, "native_status": "completed", "exactCanaryReply": true, "finalSHA256": recovery.Hash([]byte(final)), "providerSubjectSHA256": actual, "modelTurns": 1, "nativeToolCalls": 0, "canonicalTurnSHA256": recovery.Hash(full), "realProviderOverload": "NOT_OBSERVED", "faultRecoveryEvidence": "separate fake-daemon/fake-clock tests; not forged native failure", "genericObjectiveVerified": false, "scope": "normal native interoperability only; no live automatic recovery run", "privateCodexHome": true, "sharedDaemonRestart": 0, "externalMessages": 0, "callOrLINE": 0}
	raw, _ = json.MarshalIndent(result, "", "  ")
	out, e := os.OpenFile(filepath.Join(dir, "native-canary.result.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer out.Close()
	if _, e = out.Write(append(raw, '\n')); e != nil {
		t.Fatal(e)
	}
	if e = out.Sync(); e != nil {
		t.Fatal(e)
	}
}

// This setup is pure local-file preparation. It neither contacts a daemon nor
// reads configuration into reports; the private auth copy is never evidence.
func prepareAutomaticCanaryHome(originalAuth, dir string) (string, error) {
	home := filepath.Join(dir, "private-codex-home")
	if e := os.Mkdir(home, 0700); e != nil {
		return "", e
	}
	original, e := os.Open(originalAuth)
	if e != nil {
		return "", e
	}
	defer original.Close()
	st, e := original.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Size() > 1<<20 {
		return "", errors.New("unverified original auth file")
	}
	raw, e := io.ReadAll(io.LimitReader(original, (1<<20)+1))
	if e != nil || len(raw) > 1<<20 {
		return "", errors.New("original auth read failed")
	}
	f, e := os.OpenFile(filepath.Join(home, "auth.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return "", e
	}
	defer f.Close()
	if _, e = f.Write(raw); e != nil {
		return "", e
	}
	if e = f.Sync(); e != nil {
		return "", e
	}
	return home, nil
}
func TestAutomaticCanaryPrivateHomeIsolationAndExclusiveAuthCopy(t *testing.T) {
	dir := t.TempDir()
	original := filepath.Join(dir, "original-auth.json")
	raw := []byte(`{"tokens":{"account_id":"fixture-only"}}`)
	if e := os.WriteFile(original, raw, 0600); e != nil {
		t.Fatal(e)
	}
	owned := filepath.Join(dir, "owned")
	if e := os.Mkdir(owned, 0700); e != nil {
		t.Fatal(e)
	}
	home, e := prepareAutomaticCanaryHome(original, owned)
	if e != nil {
		t.Fatal(e)
	}
	copied, e := os.ReadFile(filepath.Join(home, "auth.json"))
	if e != nil || string(copied) != string(raw) {
		t.Fatal("not exact original auth copy", e)
	}
	for _, v := range []struct {
		path string
		mode os.FileMode
	}{{home, 0700}, {filepath.Join(home, "auth.json"), 0600}} {
		st, e := os.Stat(v.path)
		if e != nil || st.Mode().Perm() != v.mode {
			t.Fatal("private permissions", v.path, e)
		}
	}
	if _, e = prepareAutomaticCanaryHome(original, owned); e == nil {
		t.Fatal("private identity overwritten/replayed")
	}
	after, e := os.ReadFile(original)
	if e != nil || string(after) != string(raw) {
		t.Fatal("original auth modified")
	}
	if _, e = os.Stat(filepath.Join(dir, "config.toml")); !os.IsNotExist(e) {
		t.Fatal("original config touched")
	}
}
