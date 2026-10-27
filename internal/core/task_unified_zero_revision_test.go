package core

import (
	"encoding/json"
	unified "everything-go/contracts/task-api/unified-v6"
	"everything-go/internal/taskapi"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// This is the same explicit original-field comparison performed by the mobile
// readTask client, rather than a schema-only parse that permits absent fields.
func compareOriginalUnifiedLocator(input, resolved map[string]any) error {
	for key, value := range input {
		if resolved[key] != value {
			return fmt.Errorf("original locator mismatch %s: supplied=%v resolved=%v", key, value, resolved[key])
		}
	}
	return nil
}
func TestTaskUnifiedExplicitZeroRequestRealHTTPResolveGetAndWrongBinding(t *testing.T) {
	h, call, a, _ := unifiedFixtureSessions(t)
	rid := seedTitleIntake(t, h, a, workTitleCase{name: "zero_original", goal: "isolated original intake; do not run"})
	manager := h.unifiedIdentity(a.ID)
	original := map[string]any{"kind": "request", "authority_instance_id": h.cfg.InstanceID, "session_id": a.ID, "request_id": rid, "thread_id": a.ResumeID(), "config_revision": float64(0)}
	before, _, e := h.messageQueue.Get(a.ID, rid)
	if e != nil {
		t.Fatal(e)
	}
	outDir := os.Getenv("TASK_UNIFIED_ZERO_REVISION_FIXTURE_DIR")
	entries := []map[string]any{}
	defer func() {
		if outDir != "" {
			m := map[string]any{"schema_hash": unified.Hash(), "source_commit": os.Getenv("TASK_UNIFIED_SOURCE_COMMIT"), "source_tree_hash": os.Getenv("TASK_UNIFIED_SOURCE_TREE_HASH"), "raw_test_refs": []string{os.Getenv("TASK_UNIFIED_RAW_LOG"), os.Getenv("TASK_UNIFIED_RAW_RECEIPT")}, "scope": "Isolated original ordinary intake seeded in original SQLite; actual paired Hub HTTP reads only, no provider/native model/device/live data. Explicit zero original locator client comparison.", "manager": manager, "initial_locator": original, "entries": entries}
			writeZeroRevisionFixture(t, outDir, "manifest.json", m)
		}
	}()
	invoke := func(name, op string, locator map[string]any, expectedOK bool) map[string]any {
		input := map[string]any{"manager": manager}
		if locator != nil {
			input["locator"] = locator
		}
		req := map[string]any{"type": "task_api_unified_request", "api_version": unified.Version, "correlation_id": "zero-" + name, "operation": op, "input": input}
		rawReq, _ := json.Marshal(req)
		w := call("POST", "task-api", string(rawReq), "phone")
		if w.Code != 200 {
			t.Fatalf("%s HTTP%d", name, w.Code)
		}
		if err := unified.ValidateResponse(w.Body.Bytes()); err != nil {
			t.Fatal(err)
		}
		var response map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if outDir != "" {
			requestFile := name + ".request.json"
			responseFile := name + ".response.json"
			writeZeroRevisionRaw(t, outDir, requestFile, rawReq)
			writeZeroRevisionRaw(t, outDir, responseFile, w.Body.Bytes())
			entries = append(entries, map[string]any{"name": name, "kind": op, "request_path": requestFile, "request_sha256": taskapi.RawDigest(rawReq), "request_bytes": len(rawReq), "response_path": responseFile, "response_sha256": taskapi.RawDigest(w.Body.Bytes()), "response_bytes": w.Body.Len(), "http_status": w.Code, "expected_ok": expectedOK})
		}
		if response["ok"] != expectedOK {
			t.Fatalf("%s expected ok=%v actual %v", name, expectedOK, response)
		}
		return response
	}
	invoke("capabilities", "capabilities", nil, true)
	resolved := unifiedResult(t, invoke("resolve-zero", "resolve", original, true))["locator"].(map[string]any)
	if err := compareOriginalUnifiedLocator(original, resolved); err != nil {
		t.Fatal(err)
	}
	got := unifiedResult(t, invoke("get-zero", "get", resolved, true))["locator"].(map[string]any)
	if taskapi.Digest(got) != taskapi.Digest(resolved) {
		t.Fatal("resolve/get original tuple differs", resolved, got)
	}
	for _, change := range []string{"revision", "thread"} {
		wrong := map[string]any{}
		for k, v := range original {
			wrong[k] = v
		}
		if change == "revision" {
			wrong["config_revision"] = float64(1)
		} else {
			wrong["thread_id"] = "different-original-thread"
		}
		for _, op := range []string{"resolve", "get"} {
			response := invoke(op+"-wrong-"+change, op, wrong, false)
			err := response["error"].(map[string]any)
			if err["code"] != "scope_changed" || err["acceptance"] != "known_none" {
				t.Fatal("wrong original tuple did not fail closed", response)
			}
		}
	}
	after, _, e := h.messageQueue.Get(a.ID, rid)
	if e != nil || before.PayloadHash != after.PayloadHash || before.Content != after.Content || string(before.Payload) != string(after.Payload) {
		t.Fatal("read mutated original intake", e)
	}
}
func TestUnifiedLocatorRevisionPresenceRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		present   bool
		revision  float64
	}{{"unknown_absent", `{"kind":"request","authority_instance_id":"a"}`, false, 0}, {"known_zero", `{"kind":"request","authority_instance_id":"a","config_revision":0}`, true, 0}, {"known_nonzero", `{"kind":"request","authority_instance_id":"a","config_revision":7}`, true, 7}} {
		t.Run(tc.name, func(t *testing.T) {
			var l unifiedLocator
			if e := json.Unmarshal([]byte(tc.raw), &l); e != nil {
				t.Fatal(e)
			}
			raw, e := json.Marshal(l)
			if e != nil {
				t.Fatal(e)
			}
			var round map[string]any
			json.Unmarshal(raw, &round)
			v, present := round["config_revision"]
			if present != tc.present || present && v != tc.revision {
				t.Fatalf("lost revision presence %s", raw)
			}
		})
	}
}
func writeZeroRevisionFixture(t *testing.T, dir, name string, value any) {
	t.Helper()
	raw, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	writeZeroRevisionRaw(t, dir, name, append(raw, '\n'))
}
func writeZeroRevisionRaw(t *testing.T, dir, name string, raw []byte) {
	t.Helper()
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.Write(raw); e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
}
