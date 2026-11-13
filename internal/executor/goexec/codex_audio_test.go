package goexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/history"
	"everything-go/internal/session"
	"os"
	"path/filepath"
	"testing"
)

func TestCodexAudioCapabilityUsesRunningDaemonAndExactModel(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	s := session.NewRegistry().Create("audio", "audio", t.TempDir(), backend.Codex, "audio-model", "", "")
	c.catalog = backend.Definition{Models: []backend.Model{{ID: "audio-model", InputModalities: []string{"text", "audio"}}}}
	c.runtimeDiagnostics = map[string]any{"codex": map[string]any{"running_version": "0.162.1", "cli_version": "0.168.0"}}
	if c.SupportsAudioInput(context.Background(), s) {
		t.Fatal("CLI version must not qualify old daemon")
	}
	c.runtimeDiagnostics["codex"].(map[string]any)["running_version"] = "0.168.0"
	if !c.SupportsAudioInput(context.Background(), s) {
		t.Fatal("qualified native audio model refused")
	}
	c.catalog.Models[0].InputModalities = []string{"text", "image"}
	if c.validateAudioInput(context.Background(), s, []backend.FileAttachment{{MediaType: "audio/wav"}}) == nil {
		t.Fatal("text model accepted audio")
	}
}

func TestCodexAudioStagesVerifiedBytesAsNativeAudioWithoutTextOrTranscription(t *testing.T) {
	root := t.TempDir()
	c := NewCodex(&capSink{}, "codex")
	c.dataDir = root
	s := session.NewRegistry().Create("audio", "audio", root, backend.Codex, "audio-model", "", "")
	path := filepath.Join(root, "original.wav")
	data := []byte("RIFF-original-recording")
	if e := os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(data)
	f := backend.FileAttachment{Name: "private.wav", MediaType: "audio/wav", SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), RemotePath: path, VerifiedAudioPath: path}
	st := c.state(s.ID)
	input, e := c.codexInput(s, "r_audio", "r_audio", "Please summarize", nil, []backend.FileAttachment{f}, st)
	if e != nil {
		t.Fatal(e)
	}
	if len(input) != 2 || input[0]["text"] != "Please summarize" || input[1]["type"] != "localAudio" {
		t.Fatalf("not native audio: %#v", input)
	}
	staged := input[1]["path"].(string)
	got, e := os.ReadFile(staged)
	if e != nil || string(got) != string(data) || staged == path {
		t.Fatal("original recording not preserved in owned stage")
	}
	c.cleanupTempImages(st, "r_audio")
	if _, e = os.Stat(staged); !os.IsNotExist(e) {
		t.Fatal("stage cleanup failed")
	}
	if _, e = os.Stat(path); e != nil {
		t.Fatal("cleanup removed original")
	}
	f.SHA256 = string(make([]byte, 64))
	if _, e = c.codexInput(s, "r2", "r2", "", nil, []backend.FileAttachment{f}, st); e == nil {
		t.Fatal("tampered digest accepted")
	}
}

func TestCodexAudioTrustMarkerCannotBeDeserializedAndAudioOnlyHistorySurvives(t *testing.T) {
	var f backend.FileAttachment
	if e := json.Unmarshal([]byte(`{"VerifiedAudioPath":"/tmp/forged","RemotePath":"/tmp/forged","MediaType":"audio/wav"}`), &f); e != nil {
		t.Fatal(e)
	}
	if f.VerifiedAudioPath != "" || validateCodexInlineFiles([]backend.FileAttachment{f}) == nil {
		t.Fatal("client forged verified audio")
	}
	if !codexHistoryHasAudio([]byte(`[{"type":"input_audio","audio_url":"data:audio/wav;base64,AAA="}]`)) {
		t.Fatal("audio-only turn lost")
	}
	if codexHistoryHasAudio([]byte(`[{"type":"input_text","text":"input_audio"}]`)) {
		t.Fatal("text became audio")
	}
}

func TestCodexAnimationConfigPreservesPolicyAndOnlyEnablesNewRuntime(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	p := map[string]any{"config": map[string]any{"web_search": "disabled"}}
	c.runtimeDiagnostics = map[string]any{"codex": map[string]any{"running_version": "0.162.1"}}
	c.applyAnimationThreadConfig(p)
	if _, ok := p["config"].(map[string]any)["features.image_generation_animation"]; ok {
		t.Fatal("old daemon received new flag")
	}
	c.runtimeDiagnostics["codex"].(map[string]any)["running_version"] = "0.168.0"
	c.applyAnimationThreadConfig(p)
	if p["config"].(map[string]any)["features.image_generation_animation"] != true || p["config"].(map[string]any)["web_search"] != "disabled" {
		t.Fatal("thread setting overwritten")
	}
	if codexPMConfig(nil)["agents"].(map[string]any)["enabled"] != false {
		t.Fatal("PM subagent policy weakened")
	}
}

func TestCodexAudioOnlyNativeHistoryKeepsUserTurn(t *testing.T) {
	line := []byte(`{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_audio","audio_url":"data:audio/wav;base64,AAA="}]}}`)
	rows, _, err := parseCodexHistoryLines([]history.TailLine{{LineNo: 1, Data: line}}, "audio-thread", false)
	if err != nil || len(rows) != 1 || rows[0]["role"] != "user" || rows[0]["content"] != "" {
		t.Fatalf("pure recording lost or rewritten: %#v %v", rows, err)
	}
}
