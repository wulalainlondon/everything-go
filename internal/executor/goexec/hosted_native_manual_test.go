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

// Explicit one-shot investigation of the user-selected original conversation.
// This is not a recovery/polling path. No discovery, model work or subscription.
func TestManualHostedNativeSource(t *testing.T) {
	if os.Getenv("BRIDGE_HOSTED_NATIVE_PROBE") != "original-selected-root" {
		t.Skip("explicit original-source investigation required")
	}
	const root = "01a186e0-6bc9-7f71-a39b-07a692674881"
	const path = "/Users/wulala/.codex/sessions/2026/10/29/rollout-2026-10-29T16-56-57-" + root + ".jsonl"
	before, e := os.Lstat(path)
	if e != nil || !before.Mode().IsRegular() || before.Size() > 256<<20 {
		t.Fatal("bounded original file unavailable")
	}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal("original file unavailable")
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !os.SameFile(before, opened) {
		t.Fatal("original file changed")
	}
	hash := sha256.New()
	events := [][]byte{}
	latestChild := ""
	stats, e := agentactivity.ReadNativePrefix(io.TeeReader(io.NewSectionReader(f, 0, before.Size()), hash), root, func(b []byte) error {
		a, err := agentactivity.DecodeNativeActivity(b, root)
		if err != nil {
			return err
		}
		if len(events) >= 1024 {
			return agentactivity.ErrNativeSource
		}
		events = append(events, b)
		latestChild = a.ChildThread
		return nil
	})
	if e != nil {
		t.Fatal("strict original prefix qualification failed")
	}
	after, e := os.Stat(path)
	if e != nil || !os.SameFile(opened, after) || after.Size() < before.Size() || after.Size() == before.Size() && !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("original prefix changed")
	}
	if latestChild == "" {
		t.Fatal("no original hosted child source")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	provider := &Codex{sessionsRoot: "/Users/wulala/.codex/sessions"}
	socket := provider.daemonSocketPath("/Users/wulala/.codex")
	socketInfo, e := os.Stat(socket)
	if e != nil || socketInfo.Mode()&os.ModeSocket == 0 {
		t.Fatal("existing daemon socket unavailable")
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	conn, _, e := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
	if e != nil {
		t.Fatal("original daemon readonly attachment unavailable")
	}
	defer conn.CloseNow()
	conn.SetReadLimit(65536)
	scope := agentactivity.NativeScope{Authority: "eg_a0ee076ad8a212fa6f9fbc97838f75f1", Session: "s_oWB6ZVAc", RootThread: root, Epoch: fmt.Sprintf("manual-original-config0:%d", socketInfo.ModTime().UnixNano())}
	rpc := &manualHostedRPC{conn: conn, socket: socket, stamp: socketInfo, scope: scope}
	if _, e = rpc.read(ctx, "initialize", map[string]any{"clientInfo": map[string]any{"name": "bridge-hosted-read-qualification", "version": "1"}, "capabilities": map[string]any{"experimentalApi": true}}); e != nil {
		t.Fatal("original reader initialization refused")
	}
	if e = conn.Write(ctx, websocket.MessageText, []byte(`{"method":"initialized"}`)); e != nil {
		t.Fatal("original reader initialization failed")
	}
	source := agentactivity.NewNativeSource(scope)
	selectedEvents, creationModel, creationEffort := 0, 0, 0
	for _, b := range events {
		a, _ := agentactivity.DecodeNativeActivity(b, root)
		if a.ChildThread != latestChild {
			continue
		}
		if e = source.Observe(scope, b); e != nil {
			t.Fatal("original event admission failed")
		}
		selectedEvents++
		if a.ResolvedCreationModel != nil {
			creationModel++
		}
		if a.ResolvedCreationEffort != nil {
			creationEffort++
		}
	}
	probe, e := source.Probe(ctx, rpc)
	if e != nil {
		t.Fatal("original-owner native probe failed:", e)
	}
	if len(probe.Actors) != 1 || len(probe.Actors[0].Rounds) == 0 {
		t.Fatal("original child rounds unavailable")
	}
	chainMatches := 0
	for _, v := range probe.Actors[0].Rounds {
		if v.ParentAssociation == "native_root_chain" {
			chainMatches++
		}
	}
	safeStats, _ := json.Marshal(stats)
	t.Logf("SAFE_NATIVE_SOURCE_READER prefix_bytes=%d prefix_sha256=%s prefix_stats=%s file_grew=%t native_activity_events=%d selected_child_events=%d creation_model_events=%d creation_effort_events=%d child_rounds=%d native_root_chain_matches=%d more_rounds=%t inventory_complete=%t turn_items_loaded=false private_content_retained=false private_content_exported=false metadata_transport_may_include_preview=true model_generation=false exact_directive_round_mapping=false", before.Size(), hex.EncodeToString(hash.Sum(nil)), safeStats, after.Size() > before.Size(), len(events), selectedEvents, creationModel, creationEffort, len(probe.Actors[0].Rounds), chainMatches, probe.Actors[0].RoundsTruncated, probe.InventoryComplete)
}

// Dedicated bounded read-only transport. It never responds to server requests,
// consumes agent messages, acknowledges work, or invokes recap generation.
// The original selected scope is confined to this opt-in investigation.
// The normal source adapter uses the already-connected owner RPC instead.
type manualHostedRPC struct {
	conn   *websocket.Conn
	socket string
	stamp  os.FileInfo
	scope  agentactivity.NativeScope
	id     int
}

func (r *manualHostedRPC) Scope() agentactivity.NativeScope {
	v, e := os.Stat(r.socket)
	if e != nil || !os.SameFile(r.stamp, v) || !v.ModTime().Equal(r.stamp.ModTime()) {
		return agentactivity.NativeScope{}
	}
	return r.scope
}
func (r *manualHostedRPC) ReadThread(ctx context.Context, proof agentactivity.NativeReadProof, id string) (json.RawMessage, error) {
	return r.read(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": false}, proof)
}
func (r *manualHostedRPC) ListTurns(ctx context.Context, proof agentactivity.NativeReadProof, id string) (json.RawMessage, error) {
	return r.read(ctx, "thread/turns/list", map[string]any{"threadId": id, "limit": 16, "itemsView": "notLoaded", "sortDirection": "desc"}, proof)
}
func (r *manualHostedRPC) read(ctx context.Context, method string, params any, proofs ...agentactivity.NativeReadProof) (json.RawMessage, error) {
	if r.Scope() != r.scope {
		return nil, agentactivity.ErrNativeChanged
	}
	r.id++
	b, _ := json.Marshal(map[string]any{"id": r.id, "method": method, "params": params})
	var release func()
	if len(proofs) > 0 {
		proof := proofs[0]
		if len(proofs) != 1 || proof.Scope != r.Scope() {
			return nil, agentactivity.ErrNativeChanged
		}
		m, ok := params.(map[string]any)
		if !ok {
			return nil, agentactivity.ErrNativeSource
		}
		id, _ := m["threadId"].(string)
		turn, _ := m["turnId"].(string)
		var e error
		release, e = proof.AcquireFor(id, turn)
		if e != nil {
			return nil, e
		}
	}
	writeErr := r.conn.Write(ctx, websocket.MessageText, b)
	if release != nil {
		release()
	}
	if e := writeErr; e != nil {
		return nil, e
	}
	for i := 0; i < 32; i++ {
		_, b, e := r.conn.Read(ctx)
		if e != nil {
			return nil, e
		}
		var reply struct {
			ID            json.RawMessage
			Method        string
			Result, Error json.RawMessage
		}
		if json.Unmarshal(b, &reply) != nil {
			return nil, agentactivity.ErrNativeSource
		}
		if reply.Method != "" {
			if len(reply.ID) > 0 && string(reply.ID) != "null" {
				return nil, agentactivity.ErrNativeSource
			}
			continue
		}
		var id int
		if json.Unmarshal(reply.ID, &id) != nil || id != r.id {
			continue
		}
		if len(reply.Error) > 0 && string(reply.Error) != "null" || len(reply.Result) == 0 {
			return nil, agentactivity.ErrNativeSource
		}
		return reply.Result, nil
	}
	return nil, agentactivity.ErrNativeSource
}
