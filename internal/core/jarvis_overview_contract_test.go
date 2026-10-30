package core

// This fixture invokes the authenticated receiver over a real local HTTP socket.
// Only synthetic temporary sources are exported, never a token or live data.
import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"everything-go/internal/attachmentjournal"
	"everything-go/internal/messagequeue"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOverviewHTTPContractFixture(t *testing.T) {
	report := os.Getenv("OVERVIEW_HTTP_CONTRACT_REPORT")
	h, _, scope := overviewFixture(t)
	server := httptest.NewServer(http.HandlerFunc(h.ServeJarvisAPI))
	defer server.Close()
	entries := []map[string]any{}
	call := func(path string, input map[string]any, status int) map[string]any {
		t.Helper()
		for k, v := range scope {
			input[k] = v
		}
		raw, _ := json.Marshal(input)
		req, e := http.NewRequest("POST", server.URL+"/api/jarvis/v1/overview/"+path, bytes.NewReader(raw))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Authorization", "Bearer jarvis-token")
		req.Header.Set("X-Bridge-Device-ID", "phone")
		req.Header.Set("Content-Type", "application/json")
		response, e := server.Client().Do(req)
		if e != nil {
			t.Fatal(e)
		}
		body, e := io.ReadAll(io.LimitReader(response.Body, 262145))
		response.Body.Close()
		if e != nil {
			t.Fatal(e)
		}
		if response.StatusCode != status || len(body) > 262144 {
			t.Fatalf("%s: %d %s", path, response.StatusCode, body)
		}
		out := map[string]any{}
		if e = json.Unmarshal(body, &out); e != nil {
			t.Fatal(e)
		}
		assertOverviewKnownArrays(t, out)
		if out["authority_instance_id"] != h.cfg.InstanceID || out["schema_version"] != float64(1) {
			t.Fatal("missing real authority envelope", path, out)
		}
		entries = append(entries, map[string]any{"path": "/api/jarvis/v1/overview/" + path, "request": input, "result": out, "status": response.StatusCode})
		return out
	}
	principal := jarvisPrincipal("phone", jarvisTokenGeneration("jarvis-token"))
	item, e := h.messageQueue.MutateWork(t.Context(), principal, "s_overview_manager", messagequeue.WorkMutation{IntentID: "r_http_original_note", WorkItem: messagequeue.WorkItem{ID: "original-note", Kind: "discussion_note", Title: "原始決策", Body: "完整隔離來源", RecordingState: "recorded", Decision: &messagequeue.WorkDecision{State: "pending"}}})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = h.messageQueue.MutateWork(t.Context(), principal, "s_overview_manager", messagequeue.WorkMutation{IntentID: "r_http_second_original", WorkItem: messagequeue.WorkItem{ID: "second-original-note", Kind: "discussion_note", Title: "第二原始資料", RecordingState: "recorded"}}); e != nil {
		t.Fatal(e)
	}
	snapshot := call("snapshot", map[string]any{"section": "all"}, 200)
	project := snapshot["projects"].(map[string]any)["items"].([]any)[0].(map[string]any)
	firstPage := call("project/get", map[string]any{"project_id": project["project_id"], "limit": 1}, 200)
	if firstPage["cursor"] == "" {
		t.Fatal("missing actual detail continuation")
	}
	call("project/get", map[string]any{"project_id": project["project_id"], "limit": 1, "cursor": firstPage["cursor"]}, 200)
	body := []byte(strings.Repeat("原計畫內容 😀\n", 7000))
	path := filepath.Join(h.cfg.RootDir, "original-plan.txt")
	if e = os.WriteFile(path, body, 0600); e != nil {
		t.Fatal(e)
	}
	plan := messagequeue.WorkPlan{Path: path, SHA256: jarvisTextHash(string(body)), Revision: 9, Checklist: []string{"一", "二"}}
	seedOverviewPlan(t, h, "s_overview_manager", "http-plan-source", plan)
	emptyDraft := plan
	emptyDraft.Checklist = nil
	seedOverviewPlan(t, h, "s_overview_manager", "http-empty-draft", emptyDraft)
	seedOverviewPlan(t, h, "s_overview_manager", "http-incomplete-draft", messagequeue.WorkPlan{Path: "unavailable-draft-plan"})
	call("project/annotation", map[string]any{"project_id": project["project_id"], "expected_revision": 0, "intent_id": "r_http_plan_annotation", "plan_ref": plan, "phase": "隔離驗收"}, 200)
	assembled := []byte{}
	offset := 0
	for {
		out := call("project/plan", map[string]any{"project_id": project["project_id"], "sha256": plan.SHA256, "revision": plan.Revision, "offset": offset, "limit": 65536}, 200)
		chunk, e := base64.StdEncoding.DecodeString(out["bytes_base64"].(string))
		if e != nil {
			t.Fatal(e)
		}
		assembled = append(assembled, chunk...)
		if out["complete"] == true {
			break
		}
		offset = int(out["next_offset"].(float64))
	}
	if !bytes.Equal(assembled, body) {
		t.Fatal("plan contract bytes incomplete")
	}
	call("project/get", map[string]any{"project_id": project["project_id"]}, 200)
	call("decision/record", map[string]any{"source_manager": "s_overview_manager", "item_id": item.ID, "expected_revision": item.Revision, "intent_id": "r_http_human_decision", "state": "approved", "instruction": "明示隔離決策"}, 200)
	call("operation/get", map[string]any{"intent_id": "r_http_human_decision"}, 200)
	img := image.NewNRGBA(image.Rect(0, 0, 40, 20))
	var imageBytes bytes.Buffer
	if e = png.Encode(&imageBytes, img); e != nil {
		t.Fatal(e)
	}
	imagePath := filepath.Join(h.cfg.RootDir, "original.png")
	if e = os.WriteFile(imagePath, imageBytes.Bytes(), 0600); e != nil {
		t.Fatal(e)
	}
	s, _ := h.registry.Get("s_overview_manager")
	queuePayload, _ := json.Marshal(queuedPayload{ExpectedTarget: &dispatchTargetExpectation{ThreadID: s.ResumeID(), Revision: s.SettingsSnapshot().ConfigRevision}})
	if _, _, e = h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: "r_http_original_picture", Payload: queuePayload}); e != nil {
		t.Fatal(e)
	}
	attachment, _ := h.attachments.AddCanonical(attachmentjournal.Record{SessionID: s.ID, RequestID: "r_http_original_picture", Kind: "media", Path: imagePath, MIMEType: "image/png", DisplayName: "原圖"})
	call("assets/list", map[string]any{"session_id": s.ID, "limit": 1}, 200)
	call("collections/mutate", map[string]any{"collection_id": "actual-http-album", "action": "create", "name": "實際圖冊", "expected_revision": 0, "intent_id": "r_http_album_create"}, 200)
	receipt := call("collections/mutate", map[string]any{"collection_id": "actual-http-album", "action": "pin", "attachment_id": attachment.AttachmentID, "sha256": jarvisTextHash(imageBytes.String()), "expected_revision": 1, "intent_id": "r_http_pin_original"}, 200)
	pin := receipt["collection"].(map[string]any)["pins"].([]any)[0].(map[string]any)
	call("collections/get", map[string]any{"collection_id": "actual-http-album"}, 200)
	for _, variant := range []string{"original", "thumbnail"} {
		call("assets/read", map[string]any{"collection_id": "actual-http-album", "pin_id": pin["pin_id"], "sha256": pin["source_sha256"], "variant": variant, "offset": 0, "limit": 65536}, 200)
	}
	call("operation/get", map[string]any{"intent_id": "r_http_pin_original"}, 200)
	call("snapshot", map[string]any{"section": "all"}, 200)
	call("collections/mutate", map[string]any{"collection_id": "actual-http-album", "action": "rename", "name": "拒絕的舊版", "expected_revision": 0, "intent_id": "r_http_rejected_rename"}, 409)
	call("operation/get", map[string]any{"intent_id": "r_http_rejected_rename"}, 200)
	if report != "" {
		payload, _ := json.MarshalIndent(map[string]any{"schema_version": 1, "scope": "isolated real HTTP receiver; temporary synthetic original stores; no live/device data or credential", "source": scope, "entries": entries}, "", "  ")
		f, e := os.OpenFile(report, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = f.Write(append(payload, '\n')); e != nil {
			t.Fatal(e)
		}
		if e = f.Close(); e != nil {
			t.Fatal(e)
		}
	}
}

func assertOverviewKnownArrays(t *testing.T, value any) {
	t.Helper()
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			switch key {
			case "completed_milestones", "checklist", "pins", "executions", "work_items", "source_refs", "sources", "links", "items":
				if item == nil {
					t.Errorf("known empty array %s encoded null", key)
				}
			}
			assertOverviewKnownArrays(t, item)
		}
	case []any:
		for _, item := range v {
			assertOverviewKnownArrays(t, item)
		}
	}
}
