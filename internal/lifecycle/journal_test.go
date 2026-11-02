package lifecycle

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLifecycleSurvivesNewReaderBoundsAndPreservesUnknownCause(t *testing.T) {
	dir := t.TempDir()
	j := New(dir)
	code := 7
	if err := j.Add(Event{Kind: "process_exited", Owner: "owned_child", PID: 42, ExitCode: &code, Reason: "initiator_unconfirmed"}); err != nil {
		t.Fatal(err)
	}
	j = New(dir)
	if err := j.Add(Event{Kind: "transport_disconnected", Owner: "provider_connection", Reason: "not_process_exit_proof"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "native-lifecycle-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []Event
	if json.Unmarshal(b, &rows) != nil || len(rows) != 2 || *rows[0].ExitCode != 7 || rows[1].ExitCode != nil {
		t.Fatal(string(b))
	}
	for i := 0; i < 520; i++ {
		if err = j.Add(Event{Kind: "transport_connected", Owner: "provider_connection", Reason: "connected"}); err != nil {
			t.Fatal(err)
		}
	}
	b, _ = os.ReadFile(j.path)
	if json.Unmarshal(b, &rows) != nil || len(rows) != 512 {
		t.Fatal("unbounded journal")
	}
}

func TestOversizeLifecycleIdentityIsRejectedWithoutChangingEvidence(t *testing.T) {
	j := New(t.TempDir())
	if err := j.Add(Event{Kind: "transport_connected"}); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(j.path)
	if j.Add(Event{Request: strings.Repeat("x", 1801)}) == nil {
		t.Fatal("oversize identity accepted")
	}
	after, _ := os.ReadFile(j.path)
	if string(before) != string(after) {
		t.Fatal("oversize event changed evidence")
	}
}

func TestMalformedLifecycleEvidenceIsNotOverwritten(t *testing.T) {
	j := New(t.TempDir())
	old := []byte("original invalid evidence")
	if err := os.WriteFile(j.path, old, 0600); err != nil {
		t.Fatal(err)
	}
	if j.Add(Event{}) == nil {
		t.Fatal("overwrote unknown evidence")
	}
	b, _ := os.ReadFile(j.path)
	if string(b) != string(old) {
		t.Fatal("original changed")
	}
}
