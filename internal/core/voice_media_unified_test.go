package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/mediaservice"
	"everything-go/internal/protocol"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fixtureMediaOrigin struct{ o backend.MediaOrigin }

func (f fixtureMediaOrigin) ResolveMediaOrigin(string, string) (backend.MediaOrigin, error) {
	return f.o, nil
}

func TestDedicatedUnifiedMediaPreservesPrimaryEntry(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	global := filepath.Join(home, ".local", "bin", "bridge-media")
	if err := os.MkdirAll(filepath.Dir(global), 0700); err != nil {
		t.Fatal(err)
	}
	const sentinel = "primary service must remain unchanged"
	if err := os.WriteFile(global, []byte(sentinel), 0700); err != nil {
		t.Fatal(err)
	}
	h, _, _, _ := controllerFixture(t)
	h.cfg.DataDir = t.TempDir()
	h.cfg.InstanceID = "dedicated-media-fixture"
	h.cfg.MediaControlDir = filepath.Join(home, "dedicated", "media-control")
	manifest, launcher, err := mediaservice.Paths(h.cfg.MediaControlDir)
	if err != nil {
		t.Fatal(err)
	}
	closeMedia, err := h.StartUnifiedVoiceMedia(fixtureMediaOrigin{}, manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeMedia)
	old, err := os.ReadFile(global)
	if err != nil || string(old) != sentinel {
		t.Fatalf("primary entry overwritten: %q %v", old, err)
	}
	script, err := os.ReadFile(launcher)
	if err != nil || !strings.Contains(string(script), " media --manifest "+shellArgument(manifest)) {
		t.Fatalf("wrong dedicated entry: %q %v", script, err)
	}
	if !strings.Contains(h.unifiedMedia.instructions(), shellArgument(launcher)) {
		t.Fatal("call instructions do not advertise dedicated entry")
	}
	for path, mode := range map[string]os.FileMode{launcher: 0700, manifest: 0600, h.cfg.MediaControlDir: 0700} {
		st, err := os.Stat(path)
		if err != nil || st.Mode().Perm() != mode {
			t.Fatalf("private path mode: %s %v", path, err)
		}
	}
}

func TestDedicatedUnifiedMediaRejectsPrimaryManifest(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	h, _, _, _ := controllerFixture(t)
	h.cfg.DataDir = t.TempDir()
	h.cfg.MediaControlDir = filepath.Join(home, "dedicated")
	if _, err := h.StartUnifiedVoiceMedia(fixtureMediaOrigin{}, mediaservice.DefaultManifest()); err == nil {
		t.Fatal("dedicated service accepted primary manifest")
	}
	if _, err := os.Stat(mediaservice.DefaultManifest()); !os.IsNotExist(err) {
		t.Fatal("primary manifest touched", err)
	}
}
func TestUnifiedMediaExactGenerationAndOriginalEventBinding(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var owner *Client
	var call *clientVoiceCall
	for c, v := range h.voiceClients {
		owner = c
		call = v
	}
	call.mediaCapable = true
	call.mediaGeneration = "gen-A"
	store, e := mediaservice.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	o := backend.MediaOrigin{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), TurnID: "turn-A", VoiceID: caller.VoiceID, Generation: "gen-A", RequestID: "native-handoff"}
	u := &unifiedVoiceMedia{store: store, resolver: fixtureMediaOrigin{o}}
	q := mediaservice.Request{ID: "vop_unified_effect", Thread: o.ThreadID, Method: "play_youtube", Input: backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}}
	done := make(chan mediaservice.Result, 1)
	go func() {
		r, e := h.executeUnifiedMedia(context.Background(), u, q)
		if e != nil {
			t.Error(e)
		}
		done <- r
	}()
	event := nextVoiceState(t, owner, "media")
	if event["request_id"] != call.requestID {
		t.Error("changed original mobile request", event["request_id"], call.requestID)
	}
	var command protocol.VoiceMediaCommand
	b, _ := json.Marshal(event["media"])
	json.Unmarshal(b, &command)
	h.handleVoiceMediaState(owner, clientproto.Command{SessionID: o.SessionID, ThreadID: o.ThreadID, VoiceID: o.VoiceID, MediaCommandID: command.CommandID, MediaSequence: command.Sequence, MediaVideoID: command.VideoID, MediaState: "playing"})
	r := <-done
	if !r.PlaybackConfirmed || r.Dispatched == nil || !*r.Dispatched || r.OperationID != q.ID {
		t.Fatal(r)
	}
	if _, e = h.executeUnifiedMedia(context.Background(), u, q); e != nil {
		t.Fatal(e)
	}
	// The canonical intent is shared across adapters; a retry cannot create
	// another effect merely by changing transport.
	cross, e := h.executeMediaWithOrigin(context.Background(), u, o, q, "native_tool")
	if e != nil || cross.Transport != "local_user_socket" || cross.PlaybackConfirmed {
		t.Fatal(cross, e)
	}
	select {
	case <-owner.send:
		t.Fatal("duplicate effect")
	default:
	}
	h.voiceMu.Lock()
	call.mediaGeneration = "gen-B"
	h.voiceMu.Unlock()
	q.ID = "vop_delayed_old_generation"
	if _, e = h.executeUnifiedMedia(context.Background(), u, q); e == nil {
		t.Fatal("old turn targeted new call")
	}
	select {
	case <-owner.send:
		t.Fatal("expired origin effect")
	default:
	}
}

func TestUnifiedMediaUnavailablePlayerPersistsDefinitiveRefusal(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var owner *Client
	for c, call := range h.voiceClients {
		owner = c
		call.mediaCapable = false
		call.mediaGeneration = "gen-A"
	}
	store, err := mediaservice.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	o := backend.MediaOrigin{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), TurnID: "turn-A", VoiceID: caller.VoiceID, Generation: "gen-A"}
	u := &unifiedVoiceMedia{store: store}
	q := mediaservice.Request{ID: "vop_unavailable_player", Thread: o.ThreadID, Method: "play_youtube", Input: backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}}
	r, err := h.executeMediaWithOrigin(context.Background(), u, o, q, "local_user_socket")
	if err != nil || r.State != "failed" || r.ErrorCode != "voice_media_player_not_ready" || r.Dispatched == nil || *r.Dispatched || r.PlaybackConfirmed || r.Endpoint == nil {
		t.Fatalf("refusal lost: %+v, %v", r, err)
	}
	// A later capable endpoint must never execute this refused intent.
	for _, call := range h.voiceClients {
		call.mediaCapable = true
	}
	r, err = h.executeMediaWithOrigin(context.Background(), u, o, q, "native_tool")
	if err != nil || r.State != "failed" || r.ErrorCode != "voice_media_player_not_ready" || r.Dispatched == nil || *r.Dispatched {
		t.Fatalf("duplicate refusal lost: %+v, %v", r, err)
	}
	q.Method, q.Operation, q.ID, q.Input = "status", q.ID, "vop_unavailable_status", backend.VoiceMediaRequest{}
	r, err = h.executeMediaWithOrigin(context.Background(), u, o, q, "local_user_socket")
	if err != nil || r.State != "failed" || r.ErrorCode != "voice_media_player_not_ready" || r.CommandID != "" {
		t.Fatalf("status refusal lost: %+v, %v", r, err)
	}
	select {
	case <-owner.send:
		t.Fatal("refused intent emitted a command")
	default:
	}
}
