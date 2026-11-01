package agentactivity

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestManualHostedQualification is an opt-in, one-shot schema qualification.
// It is NOT a provider reader, a callback, or a production polling path. Never
// discover files or start a model to make this test pass.
func TestManualHostedQualification(t *testing.T) {
	path, thread := os.Getenv("BRIDGE_AGENT_QUALIFICATION_FILE"), os.Getenv("BRIDGE_AGENT_QUALIFICATION_THREAD")
	if path == "" && thread == "" {
		t.Skip("explicit original-source selection required")
	}
	if !shortID(thread) || strings.ContainsAny(thread, "/\\") || filepath.Base(path) != "rollout-2026-10-29T16-56-57-"+thread+".jsonl" {
		t.Fatal("qualification source must match the explicitly selected original rollout")
	}
	before, e := os.Lstat(path)
	if e != nil || !before.Mode().IsRegular() {
		t.Fatal("regular original rollout required")
	}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal("original rollout unavailable")
	}
	defer f.Close()
	opened, e := f.Stat()
	if e != nil || !os.SameFile(before, opened) {
		t.Fatal("source changed during open")
	}
	if before.Size() > 256<<20 {
		t.Fatal("one-shot qualification source exceeds bound")
	}
	h := sha256.New()
	// The original session_meta includes tool schemas/instructions and is larger
	// than an activity record. This one-shot inspector has a separate 1 MiB
	// header bound; it does not relax the product's existing readers or guards.
	const maxHeader = 1 << 20
	reader := bufio.NewReaderSize(io.TeeReader(io.NewSectionReader(f, 0, before.Size()), h), maxHeader+1)
	header, e := reader.ReadSlice('\n')
	if e != nil || len(header) > maxHeader || !uniqueJSON(header) {
		t.Fatal("original header unavailable")
	}
	var meta struct {
		Type    string `json:"type"`
		Payload struct {
			ID string `json:"id"`
		} `json:"payload"`
	}
	if json.Unmarshal(header, &meta) != nil || meta.Type != "session_meta" || meta.Payload.ID != thread {
		t.Fatal("original thread header mismatch")
	}
	var p HostedQualifier
	if e := p.ReadRecords(reader); e != nil {
		t.Fatal(e)
	}
	after, e := os.Stat(path)
	if e != nil || !os.SameFile(opened, after) || after.Size() < before.Size() {
		t.Fatal("original source replaced or truncated")
	}
	state := "CAPTURED_PREFIX_SCHEMA_OBSERVATION_ONLY"
	if after.Size() == before.Size() && !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("same-size source changed")
	}
	q := p.Snapshot()
	if q.SpawnReceipts == 0 || q.AgentInventories == 0 || q.AgentMessages == 0 {
		t.Fatal("selected source does not establish even orchestration metadata")
	}
	if q.FullActivityReady() {
		t.Fatal("parent-only metadata must not qualify child execution")
	}
	result := map[string]any{"state": state, "capturedBytes": before.Size(), "grewDuringRead": after.Size() > before.Size(), "sourcePrefixSHA256": hex.EncodeToString(h.Sum(nil)), "observed": q, "M0": "HOLD_CHILD_EXECUTION_ROUND_AND_ORIGINAL_OWNER_READER_UNAVAILABLE", "contentRetained": false, "productOrDeviceAcceptance": false}
	b, e := json.Marshal(result)
	if e != nil || bytes.Contains(b, []byte("encrypted_content")) {
		t.Fatal("invalid safe qualification summary")
	}
	t.Log(string(b))
}
