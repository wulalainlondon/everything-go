package browsercontrol

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBrowserPrivateIPCStrictRoundtripAndCLIWithoutVoice(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CODEX_THREAD_ID", "fixture-thread")
	t.Setenv("CODEX_SESSION_ID", "")
	manifest := filepath.Join(home, "browser-control", "current.json")
	server, e := OpenServer(t.TempDir(), "fixture-instance", manifest, func(ctx context.Context, q Request) (Result, error) {
		return Result{OK: true, Result: map[string]any{"state": "observed", "thread": q.Thread, "method": q.Method, "voiceRequired": false}}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	q := Request{ID: "bop_fixture_readonly", Thread: "fixture-thread", Method: "status", Params: []byte(`{}`)}
	if r, e := Send(ctx, manifest, q); e != nil || !r.OK {
		t.Fatal(r, e)
	}
	var out, stderr bytes.Buffer
	if code := RunCLIWithManifest([]string{"--id", "bop_cli_no_voice", "--method", "capabilities"}, manifest, &out, &stderr); code != 0 || !bytes.Contains(out.Bytes(), []byte(`"voiceRequired":false`)) {
		t.Fatal(code, out.String(), stderr.String())
	}
	stat, e := os.Stat(manifest)
	if e != nil || stat.Mode().Perm() != 0600 {
		t.Fatal("manifest permissions", e)
	}
}
