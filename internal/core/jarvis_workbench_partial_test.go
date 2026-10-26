package core

import (
	"encoding/json"
	"everything-go/internal/messagequeue"
	"testing"
)

func TestJarvisReadWorkIsolatesRejectedMissingAndUnownedRefs(t *testing.T) {
	h, call := jarvisFixture(t)
	in := assignmentFixture(t, h)
	if w := call("POST", "inbox/assign", in, "phone"); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	completeJarvisAssignmentFixture(t, h, in, "seal_partial_original")
	_, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: in.Target.SessionID, RequestID: "r_other_principal", Content: "private body", Payload: []byte("{}"), PayloadHash: jarvisTextHash("{}")})
	if err != nil {
		t.Fatal(err)
	}
	refs := []jarvisWorkRef{
		{SessionID: in.Target.SessionID, RequestID: "r_known_local_video_rejected"},
		{SessionID: in.Target.SessionID, RequestID: "r_other_principal"},
		{SessionID: "s_unknown", RequestID: "r_unknown"},
		{SessionID: in.Target.SessionID, RequestID: in.IntentID, AssignmentID: in.IntentID},
	}
	for _, reverse := range []bool{false, true} {
		if reverse {
			for i, j := 0, len(refs)-1; i < j; i, j = i+1, j-1 {
				refs[i], refs[j] = refs[j], refs[i]
			}
		}
		w := call("POST", "work-status/read", map[string]any{"authority_instance_id": h.cfg.InstanceID, "refs": refs}, "phone")
		var out struct {
			Version int `json:"schema_version"`
			Items   []struct {
				Request string            `json:"request_id"`
				State   string            `json:"state"`
				Result  map[string]string `json:"result_ref"`
			}
			Errors []jarvisWorkError
		}
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || w.Code != 200 || out.Version != 1 || len(out.Items) != 1 || len(out.Errors) != 3 {
			t.Fatal(w.Code, w.Body.String(), err)
		}
		if out.Items[0].Request != in.IntentID || out.Items[0].State != "completed" || out.Items[0].Result["seal_id"] != "seal_partial_original" {
			t.Fatal(out.Items)
		}
		codes := map[string]int{}
		for _, e := range out.Errors {
			codes[e.Code] = e.Status
			if e.Retryable {
				t.Fatal(e)
			}
		}
		if codes["work_unavailable"] != 404 || codes["principal_mismatch"] != 403 || codes["scope_unavailable"] != 403 {
			t.Fatal(codes)
		}
	}
	// Missing rows are explicit failures, never successful empty evidence.
	w := call("POST", "work-status/read", map[string]any{"authority_instance_id": h.cfg.InstanceID, "refs": []jarvisWorkRef{{SessionID: in.Target.SessionID, RequestID: "r_absent"}}}, "phone")
	var out map[string]json.RawMessage
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	if w.Code != 200 || string(out["items"]) != "[]" || len(out["errors"]) <= 2 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call("POST", "work-status/read", map[string]any{"authority_instance_id": "wrong", "refs": refs}, "phone"); w.Code != 400 {
		t.Fatal("authority failure weakened", w.Code)
	}
	if w := call("POST", "work-status/read", map[string]any{"authority_instance_id": h.cfg.InstanceID, "refs": refs}, "foreign"); w.Code != 403 {
		t.Fatal("authentication failure weakened", w.Code)
	}
}

func TestJarvisReadWorkDoesNotLabelStorageFailureAsMissing(t *testing.T) {
	h, call := jarvisFixture(t)
	in := assignmentFixture(t, h)
	if err := h.automation.Close(); err != nil {
		t.Fatal(err)
	}
	refs := []jarvisWorkRef{{SessionID: in.Target.SessionID, RequestID: "r_unknown"}, {SessionID: in.Target.SessionID, AssignmentID: "r_unknown"}}
	w := call("POST", "work-status/read", map[string]any{"authority_instance_id": h.cfg.InstanceID, "refs": refs}, "phone")
	var out struct {
		Items  []json.RawMessage
		Errors []jarvisWorkError
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || w.Code != 200 || len(out.Items) != 0 || len(out.Errors) != 2 {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	for _, e := range out.Errors {
		if e.Code != "read_unavailable" || e.Status != 503 || !e.Retryable {
			t.Fatal("storage failure claimed absent", e)
		}
	}
}
