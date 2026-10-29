package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/voicecontrol"
	"everything-go/internal/voicetransfer"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fixedVoiceCaller struct{ caller backend.SessionControlCaller }

func TestVoiceControlDedicatedInstanceKeepsPrimaryEntryAndManifest(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	primary := &Hub{cfg: Config{DataDir: t.TempDir(), InstanceID: "primary"}}
	closePrimary, err := primary.StartVoiceControl(fixedVoiceCaller{})
	if err != nil {
		t.Fatal(err)
	}
	defer closePrimary()
	manifest, launcher, _ := voicecontrol.Paths("")
	oldManifest, _ := os.ReadFile(manifest)
	oldLauncher, _ := os.ReadFile(launcher)
	dir := filepath.Join(t.TempDir(), "dedicated voice")
	dedicated := &Hub{cfg: Config{DataDir: t.TempDir(), InstanceID: "dedicated", VoiceControlDir: dir}}
	closeDedicated, err := dedicated.StartVoiceControl(fixedVoiceCaller{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeDedicated()
	for path, old := range map[string][]byte{manifest: oldManifest, launcher: oldLauncher} {
		now, err := os.ReadFile(path)
		if err != nil || string(now) != string(old) {
			t.Fatalf("primary changed: %s", path)
		}
	}
	dm, dl, _ := voicecontrol.Paths(dir)
	script, err := os.ReadFile(dl)
	if err != nil || !strings.Contains(string(script), " voice-control-instance "+shellArgument(dm)) {
		t.Fatal(string(script), err)
	}
	if !strings.Contains(dedicated.voiceControlInstructions(), shellArgument(dl)) {
		t.Fatal("instructions route to another instance")
	}
	if _, err := dedicated.StartVoiceControl(fixedVoiceCaller{}); err == nil {
		t.Fatal("duplicate instance accepted")
	}
	foreign := &Hub{cfg: Config{DataDir: t.TempDir(), InstanceID: "foreign"}}
	if closeForeign, err := foreign.StartVoiceControl(fixedVoiceCaller{}); err == nil {
		closeForeign()
		t.Fatal("foreign instance overwrote primary manifest")
	}
}

func (f fixedVoiceCaller) ResolveVoiceControlCaller(thread, id, fp string) (backend.SessionControlCaller, error) {
	return f.caller, nil
}
func TestVoiceControlOriginalIntentIsQueryOnlyAfterAcceptance(t *testing.T) {
	h, c, caller, input := transferFixture(t)
	ctx := context.Background()
	input.Action = ""
	params, _ := json.Marshal(input)
	q := voicecontrol.Request{ID: "vtr_original_transfer", Thread: caller.Parent.ResumeID(), Method: "voice_transfer", Params: params}
	result, e := h.executeVoiceControl(ctx, fixedVoiceCaller{caller}, q)
	if e != nil {
		t.Fatal(e)
	}
	r := result.Result.(voicetransfer.Record)
	if r.State != "confirmation_required" || h.voiceClients[c] == nil {
		t.Fatal("CLI changed live voice", r)
	}
	another := caller
	another.ToolCallID = "second-native-query"
	result, e = h.executeVoiceControl(ctx, fixedVoiceCaller{another}, q)
	if e != nil || result.Result.(voicetransfer.Record).ID != r.ID {
		t.Fatal("uncertain receipt recreated intent", e)
	}
	input.ExpectedThreadID = "replacement"
	q.Params, _ = json.Marshal(input)
	if _, e = h.executeVoiceControl(ctx, fixedVoiceCaller{another}, q); e == nil {
		t.Fatal("intent parameter substitution")
	}
	q.Method = "dispatch_to_session"
	if q.Valid() {
		t.Fatal("text dispatch admitted")
	}
}
func TestVoiceControlRejectsCallerOverrideAndNoLiveOriginal(t *testing.T) {
	h, c, caller, _ := transferFixture(t)
	ctx := context.Background()
	q := voicecontrol.Request{ID: "vtr_forbidden_caller", Thread: caller.Parent.ResumeID(), Method: "list_sessions", Params: json.RawMessage(`{"caller":"other"}`)}
	if _, e := h.executeVoiceControl(ctx, fixedVoiceCaller{caller}, q); e == nil {
		t.Fatal("caller override accepted")
	}
	q.Params = json.RawMessage(`{}`)
	delete(h.voiceClients, c)
	if _, e := h.executeVoiceControl(ctx, fixedVoiceCaller{caller}, q); e == nil {
		t.Fatal("retired voice accepted")
	}
}
