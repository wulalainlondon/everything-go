package goexec

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/session"
)

func TestColdRecoveryWritesBoundedRedactedCheckpointAndCommitsManifest(t *testing.T) {
	root := t.TempDir()
	dataDir := t.TempDir()
	resumeID := "019fb03e-0666-7ce3-8f99-8f19c79540e2"
	rollout := filepath.Join(root, "rollout-2026-08-17T00-00-00-"+resumeID+".jsonl")
	rows := []string{
		`{"timestamp":"2026-08-17T00:00:00Z","type":"session_meta","payload":{"id":"019fb03e-0666-7ce3-8f99-8f19c79540e2","cwd":"/work"}}`,
		`{"timestamp":"2026-08-17T00:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"fix with api_key=super-secret-value"}]}}`,
		`{"timestamp":"2026-08-17T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"work remains"}]}}`,
	}
	if err := os.WriteFile(rollout, []byte(strings.Join(rows, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	c.dataDir = dataDir
	c.checkpointMaxBytes = 16 * 1024
	snap := session.Snapshot{ID: "jl_x_old", Cwd: "/work", Backend: "codex", ResumeID: resumeID}

	recovery, err := c.prepareColdRecovery(snap, rollout, fileSize(rollout), "cold_resume_hard_limit")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(recovery.Handoff, "super-secret-value") || !strings.Contains(recovery.Handoff, "[REDACTED]") {
		t.Fatalf("checkpoint secret redaction failed: %s", recovery.Handoff)
	}
	if err := c.commitColdRecovery(recovery, "01a01044-6c39-7d02-a5c0-164b2289ecdc"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(recovery.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest generationManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Pending != nil || manifest.ActiveGeneration != 2 || manifest.ActiveResumeID != "01a01044-6c39-7d02-a5c0-164b2289ecdc" {
		t.Fatalf("manifest did not commit atomically: %+v", manifest)
	}
}

func TestEnsureThreadSingleflightSendsOneResumeRPC(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	reg := session.NewRegistry()
	s := reg.Create("logical", "session", t.TempDir(), "codex", "", "", "thread-old")
	st := c.state(s.ID)
	writer := &rpcCaptureWriter{writes: make(chan []byte, 4)}
	c.rpc.setWriter(writer)

	methods := make(chan string, 4)
	go func() {
		request := <-writer.writes
		var frame struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
		}
		_ = json.Unmarshal(request, &frame)
		methods <- frame.Method
		c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"thread":{"id":"thread-old"}}}`, frame.ID)))
	}()

	errs := make(chan error, 2)
	go func() { errs <- c.ensureThread(s, st) }()
	go func() { errs <- c.ensureThread(s, st) }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if method := <-methods; method != "thread/resume" {
		t.Fatalf("first RPC method=%q", method)
	}
	select {
	case request := <-writer.writes:
		t.Fatalf("singleflight emitted a second RPC: %s", request)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestEnsureThreadLargeRolloutSkipsDirectResume(t *testing.T) {
	root := t.TempDir()
	resumeID := "019fb03e-0666-7ce3-8f99-8f19c79540e2"
	rollout := filepath.Join(root, "rollout-2026-08-17T00-00-00-"+resumeID+".jsonl")
	rows := []string{
		`{"timestamp":"2026-08-17T00:00:00Z","type":"session_meta","payload":{"id":"019fb03e-0666-7ce3-8f99-8f19c79540e2","cwd":"/work"}}`,
		`{"timestamp":"2026-08-17T00:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"continue"}]}}`,
		`{"timestamp":"2026-08-17T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"checkpoint state"}]}}`,
	}
	if err := os.WriteFile(rollout, []byte(strings.Join(rows, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	c.dataDir = t.TempDir()
	c.rolloverEnabled = true
	c.coldResumeMaxBytes = 1
	reg := session.NewRegistry()
	s := reg.Create("jl_x_old", "long", t.TempDir(), "codex", "", "", resumeID)
	st := c.state(s.ID)
	writer := &rpcCaptureWriter{writes: make(chan []byte, 2)}
	c.rpc.setWriter(writer)
	method := make(chan string, 1)
	go func() {
		request := <-writer.writes
		var frame struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
		}
		_ = json.Unmarshal(request, &frame)
		method <- frame.Method
		c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"thread":{"id":"thread-new"}}}`, frame.ID)))
	}()

	if err := c.ensureThread(s, st); err != nil {
		t.Fatal(err)
	}
	if got := <-method; got != "thread/start" {
		t.Fatalf("large rollout attempted %q, want thread/start without resume", got)
	}
	if s.ResumeID() != "thread-new" || strings.Join(s.Snapshot().HistoricalResumeIDs, ",") != resumeID {
		t.Fatalf("generation mapping not advanced: %+v", s.Snapshot())
	}
	if st.pendingHandoff == "" {
		t.Fatal("recovery handoff was not staged for the first user turn")
	}
}

func TestStripCodexBridgeHandoffKeepsOnlyCurrentUserRequest(t *testing.T) {
	text := `<bridge_session_handoff schema="1">{"recent_messages":[]}</bridge_session_handoff>

<current_user_request>
continue fixing
</current_user_request>`
	if got := stripCodexBridgeHandoff(text); got != "continue fixing" {
		t.Fatalf("stripCodexBridgeHandoff=%q", got)
	}
}
