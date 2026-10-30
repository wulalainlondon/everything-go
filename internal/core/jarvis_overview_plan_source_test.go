package core

import (
	"bytes"
	"encoding/json"
	"everything-go/internal/attachmentjournal"
	"everything-go/internal/messagequeue"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func seedOverviewPlan(t *testing.T, h *Hub, source, id string, plan messagequeue.WorkPlan) {
	t.Helper()
	principal := jarvisPrincipal("phone", jarvisTokenGeneration("jarvis-token"))
	if _, e := h.messageQueue.MutateWork(t.Context(), principal, source, messagequeue.WorkMutation{IntentID: "r_seed_plan_" + id, WorkItem: messagequeue.WorkItem{ID: id, Kind: "planning", Title: "原計畫來源", RecordingState: "recorded", Plan: &plan}}); e != nil {
		t.Fatal(e)
	}
}

func TestOverviewOriginalPlanBindingNeverFallsBackToProjectLead(t *testing.T) {
	h, call, scope := overviewFixture(t)
	b := h.registry.Create("z_original_plan_source", "B original", h.cfg.RootDir, "codex", "model", "read-only", "original-B-thread")
	snapshot := call("snapshot", map[string]any{"section": "projects"})
	p := snapshot["projects"].(map[string]any)["items"].([]any)[0].(map[string]any)
	raw := []byte(`{"schema_version":1,"revision":3,"milestones":[{"id":"one"}]}`)
	path := filepath.Join(h.cfg.RootDir, "B-original-plan.json")
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	plan := messagequeue.WorkPlan{Path: path, SHA256: jarvisTextHash(string(raw)), Revision: 3, Checklist: []string{"one"}}
	request := func(op string, input map[string]any, status int) overviewHTTPResult {
		t.Helper()
		for k, v := range scope {
			input[k] = v
		}
		body, _ := json.Marshal(input)
		r := overviewHTTP(h, op, body, "phone")
		if r.status != status {
			t.Fatal(op, r)
		}
		return r
	}
	request("project/annotation", map[string]any{"project_id": p["project_id"], "plan_ref": plan, "intent_id": "r_missing_original_plan", "expected_revision": 0}, 409)
	seedOverviewPlan(t, h, b.ID, "B-plan", plan)
	call("project/annotation", map[string]any{"project_id": p["project_id"], "plan_ref": plan, "intent_id": "r_unique_original_plan", "expected_revision": 0})
	assertB := func() {
		t.Helper()
		out := call("project/plan", map[string]any{"project_id": p["project_id"], "sha256": plan.SHA256, "revision": plan.Revision})
		nav := out["source_navigation"].(map[string]any)
		if nav["session_id"] != b.ID || nav["thread_id"] != b.ResumeID() || out["source_item_id"] != "B-plan" {
			t.Fatal("wrong original plan", out)
		}
	}
	assertB()
	seedOverviewPlan(t, h, "s_overview_manager", "A-identical", plan)
	assertB() // Identical bytes do not retarget the persisted B origin.
	call("project/annotation", map[string]any{"project_id": p["project_id"], "plan_ref": nil, "intent_id": "r_clear_original_plan", "expected_revision": 1})
	request("project/annotation", map[string]any{"project_id": p["project_id"], "plan_ref": plan, "intent_id": "r_ambiguous_original_plan", "expected_revision": 2}, 409)
	selected := map[string]any{"source_manager": b.ID, "source_item_id": "B-plan", "navigation": map[string]any{"authority_instance_id": "forged", "session_id": "s_overview_manager"}}
	call("project/annotation", map[string]any{"project_id": p["project_id"], "plan_ref": plan, "plan_source": selected, "intent_id": "r_explicit_original_plan", "expected_revision": 2})
	assertB() // Caller navigation is ignored; only original B navigation is stored.
	request("project/plan", map[string]any{"project_id": p["project_id"], "sha256": plan.SHA256, "revision": plan.Revision, "plan_source": map[string]any{"source_manager": "s_overview_manager", "source_item_id": "A-identical"}}, 409)
	principal := jarvisPrincipal("phone", jarvisTokenGeneration("jarvis-token"))
	originalItem, e := h.messageQueue.WorkItem(t.Context(), principal, b.ID, "B-plan")
	if e != nil {
		t.Fatal(e)
	}
	changedPlan := plan
	changedPlan.Revision++
	changedItem := originalItem
	changedItem.Plan = &changedPlan
	updated, e := h.messageQueue.MutateWork(t.Context(), principal, b.ID, messagequeue.WorkMutation{IntentID: "r_change_original_B_plan", ExpectedRevision: originalItem.Revision, WorkItem: changedItem})
	if e != nil {
		t.Fatal(e)
	}
	request("project/plan", map[string]any{"project_id": p["project_id"], "sha256": plan.SHA256, "revision": plan.Revision}, 409)
	if detail := call("project/get", map[string]any{"project_id": p["project_id"]})["project"].(map[string]any); detail["progress"].(map[string]any)["state"] != "unverified" {
		t.Fatal("changed source still numerically verified", detail)
	}
	originalItem.Revision = updated.Revision
	if _, e = h.messageQueue.MutateWork(t.Context(), principal, b.ID, messagequeue.WorkMutation{IntentID: "r_restore_original_B_plan", ExpectedRevision: updated.Revision, WorkItem: originalItem}); e != nil {
		t.Fatal(e)
	}
	assertB()
	b.SetResumeID("replacement-B-thread")
	response := request("project/plan", map[string]any{"project_id": p["project_id"], "sha256": plan.SHA256, "revision": plan.Revision}, 409)
	if strings.Contains(response.body, "bytes_base64") {
		t.Fatal("retired B fell back to A", response)
	}
}

func TestOverviewHighEntropyThumbnailFitsBoundWithoutChangingOriginal(t *testing.T) {
	h, _, _ := overviewFixture(t)
	img := image.NewNRGBA(image.Rect(0, 0, 512, 512))
	var n uint32 = 1
	for y := 0; y < 512; y++ {
		for x := 0; x < 512; x++ {
			n = n*1664525 + 1013904223
			r := uint8(n >> 24)
			n = n*1664525 + 1013904223
			g := uint8(n >> 24)
			n = n*1664525 + 1013904223
			b := uint8(n >> 24)
			img.SetNRGBA(x, y, color.NRGBA{R: r, G: g, B: b, A: 255})
		}
	}
	var raw bytes.Buffer
	if e := png.Encode(&raw, img); e != nil {
		t.Fatal(e)
	}
	if raw.Len() <= 512<<10 {
		t.Fatal("fixture not high entropy")
	}
	path := filepath.Join(h.cfg.RootDir, "original-noise.png")
	if e := os.WriteFile(path, raw.Bytes(), 0600); e != nil {
		t.Fatal(e)
	}
	asset, e := h.managedOverviewAsset(attachmentjournal.Record{Path: path})
	if e != nil || len(asset.Thumbnail) > 512<<10 || !bytes.Equal(asset.Bytes, raw.Bytes()) || asset.SHA256 != jarvisTextHash(raw.String()) {
		t.Fatal("thumbnail failure or original changed", len(asset.Thumbnail), e)
	}
	config, _, e := image.DecodeConfig(bytes.NewReader(asset.Thumbnail))
	if e != nil || config.Width >= 512 || config.Height >= 512 {
		t.Fatal("adaptive raster did not shrink", config, e)
	}
}

func TestOverviewOriginalEmptyAndIncompleteDraftPlansRemainReadable(t *testing.T) {
	h, call, _ := overviewFixture(t)
	body := []byte("原始草稿，尚無里程碑清單")
	path := filepath.Join(h.cfg.RootDir, "zero-milestone-draft.txt")
	if e := os.WriteFile(path, body, 0600); e != nil {
		t.Fatal(e)
	}
	plan := messagequeue.WorkPlan{Path: path, SHA256: jarvisTextHash(string(body)), Revision: 1}
	seedOverviewPlan(t, h, "s_overview_manager", "empty-original-plan", plan)
	seedOverviewPlan(t, h, "s_overview_manager", "incomplete-original-plan", messagequeue.WorkPlan{Path: "incomplete-draft"})
	out := call("snapshot", map[string]any{"section": "projects"})
	p := out["projects"].(map[string]any)["items"].([]any)[0].(map[string]any)
	for _, value := range p["work_items"].([]any) {
		item := value.(map[string]any)
		if item["source_item_id"] == "empty-original-plan" {
			ref := item["plan_ref"].(map[string]any)
			if len(ref["checklist"].([]any)) != 0 {
				t.Fatal(ref)
			}
		} else if item["source_item_id"] == "incomplete-original-plan" {
			if item["plan_ref"] != nil || item["detail_truncated"] != true {
				t.Fatal("unusable draft descriptor leaked as readable plan", item)
			}
		}
	}
	plan.Checklist = []string{} // Same exact zero original milestones, not invented IDs.
	call("project/annotation", map[string]any{"project_id": p["project_id"], "plan_ref": plan, "intent_id": "r_associate_empty_original", "expected_revision": 0})
	detail := call("project/get", map[string]any{"project_id": p["project_id"]})["project"].(map[string]any)
	progress := detail["progress"].(map[string]any)
	if progress["state"] != "unverified" || progress["percent"] != nil || progress["total"] != nil {
		t.Fatal("empty plan invented progress", progress)
	}
	read := call("project/plan", map[string]any{"project_id": p["project_id"], "sha256": plan.SHA256, "revision": 1})
	if read["complete"] != true || read["source_item_id"] != "empty-original-plan" {
		t.Fatal("original zero-milestone draft unavailable", read)
	}
}
