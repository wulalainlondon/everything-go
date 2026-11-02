package lifecycle

import (
	"encoding/json"
	"os"
	"os/exec"
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
	if err := j.Add(Event{Kind: "transport_connected", Owner: "provider_connection", Reason: "connected"}); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(j.path)
	if j.Add(Event{Kind: "native_turn_accepted", Owner: "bridge_submitted_turn", Reason: "exact_response", Request: strings.Repeat("x", 1801)}) == nil {
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

func TestInvalidPriorJournalIsPrivateBoundedAndUnchanged(t *testing.T) {
	for _, raw := range []string{"null", `{}`, `[{}]`, `[{"kind":{"nested":"wrong"}}]`, strings.Repeat("x", maxBytes+1)} {
		j := New(t.TempDir())
		if err := os.WriteFile(j.path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if j.Add(Event{Kind: "transport_connected", Owner: "provider_connection", Reason: "connected"}) == nil {
			t.Fatal("invalid input accepted")
		}
		after, _ := os.ReadFile(j.path)
		if string(after) != raw {
			t.Fatal("original evidence replaced")
		}
	}
	j := New(t.TempDir())
	if err := os.WriteFile(j.path, []byte("[]"), 0644); err != nil {
		t.Fatal(err)
	}
	if j.Add(Event{Kind: "transport_connected", Owner: "provider_connection", Reason: "connected"}) == nil {
		t.Fatal("nonprivate original accepted")
	}
}

func TestServiceBootDisambiguatesReusedNumericTransportEpoch(t *testing.T) {
	if dir := os.Getenv("BRIDGE_LIFECYCLE_TEST_DIR"); dir != "" {
		if err := New(dir).Add(Event{Kind: "transport_connected", Owner: "provider_connection", Epoch: 1, Reason: "own_child_instance"}); err != nil {
			t.Fatal(err)
		}
		return
	}
	dir := t.TempDir()
	j := New(dir)
	if err := j.Add(Event{Kind: "transport_connected", Owner: "provider_connection", Epoch: 1, Reason: "own_parent_instance"}); err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestServiceBootDisambiguatesReusedNumericTransportEpoch$")
	child.Env = append(os.Environ(), "BRIDGE_LIFECYCLE_TEST_DIR="+dir)
	if b, err := child.CombinedOutput(); err != nil {
		t.Fatal(err, string(b))
	}
	b, _ := os.ReadFile(j.path)
	var rows []Event
	if json.Unmarshal(b, &rows) != nil || len(rows) != 2 || rows[0].Boot == "" || rows[0].Boot == rows[1].Boot || rows[0].Service.PID == rows[1].Service.PID || rows[0].Epoch != rows[1].Epoch {
		t.Fatal("instance births merged", string(b))
	}
}

func TestChangedOriginalCannotBeReplacedByPendingPublication(t *testing.T) {
	j := New(t.TempDir())
	event := Event{Kind: "transport_connected", Owner: "provider_connection", Reason: "connected"}
	if err := j.Add(event); err != nil {
		t.Fatal(err)
	}
	j.beforePublish = func() {
		if err := os.Rename(j.path, j.path+".original"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(j.path, []byte("[]"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if j.Add(event) == nil {
		t.Fatal("changed original published over")
	}
	after, _ := os.ReadFile(j.path)
	if string(after) != "[]" {
		t.Fatal("changed original overwritten")
	}
}
