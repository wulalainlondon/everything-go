package goexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/agentactivity"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func (r *manualHostedRPC) LatestTurnItem(ctx context.Context, proof agentactivity.NativeReadProof, child, turn string) (json.RawMessage, error) {
	return r.read(ctx, "thread/items/list", map[string]any{"threadId": child, "turnId": turn, "limit": 1, "sortDirection": "desc"}, proof)
}

// Explicit original-source contract qualification. No new model work, native
// history discovery or report text export. The one latest item can contain
// provider fields which are discarded by the public-reference decoder.
func TestManualOriginalNativePublicReturn(t *testing.T) {
	if os.Getenv("BRIDGE_HOSTED_CONTRACT_PROBE") != "original-selected-root" {
		t.Skip("explicit original contract qualification required")
	}
	const root = "01a186e0-6bc9-7f71-a39b-07a692674881"
	const path = "/Users/wulala/.codex/sessions/2026/10/29/rollout-2026-10-29T16-56-57-" + root + ".jsonl"
	before, e := os.Lstat(path)
	if e != nil || !before.Mode().IsRegular() || before.Size() > 256<<20 {
		t.Fatal("bounded original source unavailable")
	}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal("original source unavailable")
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !os.SameFile(before, opened) {
		t.Fatal("original source changed")
	}
	hash := sha256.New()
	events := [][]byte{}
	latest := ""
	stats, e := agentactivity.ReadNativePrefix(io.TeeReader(io.NewSectionReader(f, 0, before.Size()), hash), root, func(b []byte) error {
		a, e := agentactivity.DecodeNativeActivity(b, root)
		if e != nil {
			return e
		}
		if len(events) >= 1024 {
			return agentactivity.ErrNativeSource
		}
		events = append(events, b)
		latest = a.ChildThread
		return nil
	})
	if e != nil || latest == "" {
		t.Fatal("original activity provenance unavailable")
	}
	after, e := os.Stat(path)
	if e != nil || !os.SameFile(opened, after) || after.Size() < before.Size() || after.Size() == before.Size() && !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("original captured prefix changed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	socket := "/Users/wulala/.codex/app-server-control/app-server-control.sock"
	stamp, e := os.Stat(socket)
	if e != nil || stamp.Mode()&os.ModeSocket == 0 {
		t.Fatal("existing original daemon unavailable")
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	conn, _, e := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
	if e != nil {
		t.Fatal("original readonly attachment failed")
	}
	defer conn.CloseNow()
	conn.SetReadLimit(65536)
	scope := agentactivity.NativeScope{Authority: "eg_a0ee076ad8a212fa6f9fbc97838f75f1", Session: "s_oWB6ZVAc", RootThread: root, Epoch: fmt.Sprintf("manual-original-config0:%d", stamp.ModTime().UnixNano())}
	rpc := &manualHostedRPC{conn: conn, socket: socket, stamp: stamp, scope: scope}
	if _, e = rpc.read(ctx, "initialize", map[string]any{"clientInfo": map[string]any{"name": "bridge-hosted-contract-qualification", "version": "1"}, "capabilities": map[string]any{"experimentalApi": true}}); e != nil {
		t.Fatal("original readonly initialize refused")
	}
	if e = conn.Write(ctx, websocket.MessageText, []byte(`{"method":"initialized"}`)); e != nil {
		t.Fatal("original readonly initialize failed")
	}
	source := agentactivity.NewNativeSource(scope)
	for _, b := range events {
		a, _ := agentactivity.DecodeNativeActivity(b, root)
		if a.ChildThread == latest {
			if e = source.Observe(scope, b); e != nil {
				t.Fatal("original native source admission failed")
			}
		}
	}
	probe, e := source.Probe(ctx, rpc)
	if e != nil || len(probe.Actors) != 1 {
		t.Fatal("original child/native round proof failed")
	}
	turn := ""
	for _, round := range probe.Actors[0].Rounds {
		if round.Status == "completed" {
			turn = round.ID
			break
		}
	}
	if turn == "" {
		t.Fatal("no completed original native round in bounded page")
	}
	ref, e := source.PublicReturn(ctx, rpc, latest, turn)
	if e != nil || ref.ChildThread != latest || ref.NativeTurn != turn || ref.ItemID == "" {
		t.Fatal("original native public final reference unavailable")
	}
	b, _ := json.Marshal(stats)
	t.Logf("SAFE_NATIVE_PUBLIC_RETURN prefix_bytes=%d prefix_sha256=%s prefix_stats=%s file_grew=%t original_ancestry_and_session_tree=true actual_completed_native_turn=true exact_native_final_item_reference=true text_retained=false text_exported=false canonical_qa=false exact_immediate_directive_mapping=false native_generation=false", before.Size(), hex.EncodeToString(hash.Sum(nil)), b, after.Size() > before.Size())
}
