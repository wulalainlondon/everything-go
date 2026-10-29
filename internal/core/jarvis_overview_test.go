package core

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"everything-go/internal/attachmentjournal"
	"everything-go/internal/messagequeue"
	"everything-go/internal/sessiondispatch"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func overviewFixture(t *testing.T) (*Hub, func(string, map[string]any) map[string]any, map[string]any) {
	t.Helper()
	h, call := jarvisFixture(t)
	s := h.registry.Create("s_overview_manager", "manager", h.cfg.RootDir, "codex", "gpt-6.1-sol", "read-only", "thread-manager")
	scope := map[string]any{"authority_instance_id": h.cfg.InstanceID, "manager": h.overviewNavigation(s)}
	return h, func(path string, body map[string]any) map[string]any {
		t.Helper()
		for k, v := range scope {
			body[k] = v
		}
		w := call("POST", "overview/"+path, body, "phone")
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
		var out map[string]any
		if e := json.Unmarshal(w.Body.Bytes(), &out); e != nil {
			t.Fatal(e)
		}
		return out
	}, scope
}
func TestOverviewProjectsAreActualAuthorizedSourcesAndNoInventedProgress(t *testing.T) {
	h, call, _ := overviewFixture(t)
	dir := filepath.Join(h.cfg.RootDir, "project-two")
	if e := os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	h.registry.Create("s_other_project", "project two", dir, "codex", "model", "read-only", "thread-two")
	h.registry.Create("pm_private", "secret project", filepath.Join(h.cfg.RootDir, "private"), "codex", "model", "read-only", "thread-private")
	h.registry.Create("s_dg_hidden", "secret child", filepath.Join(h.cfg.RootDir, "delegated"), "codex", "model", "read-only", "thread-child")
	out := call("snapshot", map[string]any{"section": "projects", "limit": 20})
	projects := out["projects"].(map[string]any)["items"].([]any)
	if len(projects) != 2 {
		t.Fatal(projects)
	}
	raw, _ := json.Marshal(out)
	if bytes.Contains(raw, []byte("secret")) || bytes.Contains(raw, []byte(`"percent"`)) {
		t.Fatal(string(raw))
	}
	p := projects[0].(map[string]any)
	detail := call("project/get", map[string]any{"project_id": p["project_id"]})
	if detail["project"].(map[string]any)["project_id"] != p["project_id"] {
		t.Fatal(detail)
	}
	if out["scope_revision"] == "" || out["freeze_id"] == "" {
		t.Fatal(out)
	}
}
func TestOverviewPlanHashMilestonesAndDecisionDoNotExecute(t *testing.T) {
	h, call, scope := overviewFixture(t)
	out := call("snapshot", map[string]any{"section": "projects"})
	p := out["projects"].(map[string]any)["items"].([]any)[0].(map[string]any)
	raw := []byte(`{"schema_version":1,"revision":7,"milestones":[{"id":"one"},{"id":"two"}]}`)
	path := filepath.Join(h.cfg.RootDir, "plan.json")
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	plan := messagequeue.WorkPlan{Path: path, SHA256: jarvisTextHash(string(raw)), Revision: 7, Checklist: []string{"one", "two"}}
	body := map[string]any{"project_id": p["project_id"], "intent_id": "r_overview_associate", "expected_revision": 0, "title": "真計畫", "phase": "規劃中", "next_step": "等待驗收", "plan_ref": plan}
	receipt := call("project/annotation", body)
	if receipt["phase"] != "applied" || receipt["operation"] != "overview/project/annotation" || receipt["input_hash"] == "" {
		t.Fatal(receipt)
	}
	recovered := call("operation/get", map[string]any{"intent_id": "r_overview_associate"})
	if recovered["input_hash"] != receipt["input_hash"] {
		t.Fatal(recovered)
	}
	detail := call("project/get", map[string]any{"project_id": p["project_id"]})["project"].(map[string]any)
	progress := detail["progress"].(map[string]any)
	if progress["total"] != float64(2) || progress["completed"] != float64(0) || progress["percent"] != float64(0) {
		t.Fatal(detail)
	}
	// Caller cannot invent a completed milestone without sealed original QA and return evidence.
	req := map[string]any{}
	for k, v := range body {
		req[k] = v
	}
	req["intent_id"] = "r_overview_forged"
	req["expected_revision"] = 1
	req["completed_milestones"] = []overviewMilestone{{ID: "one", Evidence: "pretend", SourceManager: "s_overview_manager"}}
	for k, v := range scope {
		req[k] = v
	}
	b, _ := json.Marshal(req)
	r := overviewHTTP(h, "project/annotation", b, "phone")
	if r.status != 409 {
		t.Fatal(r)
	}
	principal := jarvisPrincipal("phone", jarvisTokenGeneration("jarvis-token"))
	item, e := h.messageQueue.MutateWork(t.Context(), principal, "s_overview_manager", messagequeue.WorkMutation{IntentID: "r_overview_note_original", WorkItem: messagequeue.WorkItem{ID: "decision-note", Kind: "discussion_note", Title: "需決定", RecordingState: "recorded"}})
	if e != nil {
		t.Fatal(e)
	}
	d := call("decision/record", map[string]any{"source_manager": "s_overview_manager", "item_id": item.ID, "expected_revision": item.Revision, "intent_id": "r_overview_decision", "state": "approved", "instruction": "人明示繼續"})
	if d["phase"] != "applied" {
		t.Fatal(d)
	}
	recovery := call("operation/get", map[string]any{"intent_id": "r_overview_decision"})
	if recovery["input_hash"] != d["input_hash"] {
		t.Fatal(recovery)
	}
	queue, e := h.messageQueue.Snapshot("s_overview_manager")
	if e != nil || len(queue.Items) != 0 {
		t.Fatal("metadata executed", queue, e)
	}
	call("project/annotation", map[string]any{"project_id": p["project_id"], "intent_id": "r_overview_human_milestone", "expected_revision": 1, "completed_milestones": []overviewMilestone{{ID: "one", Evidence: item.ID, SourceManager: "s_overview_manager"}}})
	proven := call("project/get", map[string]any{"project_id": p["project_id"]})["project"].(map[string]any)["progress"].(map[string]any)
	if proven["percent"] != float64(50) || proven["completed"] != float64(1) || proven["total"] != float64(2) {
		t.Fatal("original decision proof missing", proven)
	}
	if e = os.WriteFile(path, []byte("changed"), 0600); e != nil {
		t.Fatal(e)
	}
	detail = call("project/get", map[string]any{"project_id": p["project_id"]})["project"].(map[string]any)
	if detail["progress"].(map[string]any)["state"] != "unverified" {
		t.Fatal(detail)
	}
}
func TestOverviewManagedPinHashChunksExactSourceAndRevocation(t *testing.T) {
	h, call, scope := overviewFixture(t)
	img := image.NewNRGBA(image.Rect(0, 0, 600, 300))
	for y := 0; y < 300; y++ {
		for x := 0; x < 600; x++ {
			img.Set(x, y, color.NRGBA{R: uint8(x), G: uint8(y), B: 47, A: 255})
		}
	}
	var b bytes.Buffer
	if e := png.Encode(&b, img); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(h.cfg.RootDir, "image.png")
	if e := os.WriteFile(path, b.Bytes(), 0600); e != nil {
		t.Fatal(e)
	}
	queuePayload, _ := json.Marshal(queuedPayload{ExpectedTarget: &dispatchTargetExpectation{ThreadID: "thread-manager", Revision: func() uint64 {
		s, _ := h.registry.Get("s_overview_manager")
		return s.SettingsSnapshot().ConfigRevision
	}()}})
	if _, _, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: "s_overview_manager", RequestID: "r_picture_original", Payload: queuePayload}); e != nil {
		t.Fatal(e)
	}
	wrongPayload, _ := json.Marshal(queuedPayload{ExpectedTarget: &dispatchTargetExpectation{ThreadID: "thread-manager", Revision: 1}})
	if _, _, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: "s_overview_manager", RequestID: "r_wrong_revision", Payload: wrongPayload}); e != nil {
		t.Fatal(e)
	}
	h.attachments.AddCanonical(attachmentjournal.Record{SessionID: "s_overview_manager", RequestID: "r_wrong_revision", Kind: "media", Path: path, MIMEType: "image/png", DisplayName: "不可讀的錯版"})
	a, _ := h.attachments.AddCanonical(attachmentjournal.Record{SessionID: "s_overview_manager", RequestID: "r_picture_original", Kind: "media", Path: path, MIMEType: "image/png", DisplayName: "原圖"})
	listed := call("assets/list", map[string]any{"session_id": "s_overview_manager"})["items"].([]any)
	if len(listed) != 1 || listed[0].(map[string]any)["source_sha256"] != jarvisTextHash(b.String()) {
		t.Fatal(listed)
	}
	created := call("collections/mutate", map[string]any{"collection_id": "album-one", "action": "create", "name": "我的圖冊", "intent_id": "r_overview_album_create", "expected_revision": 0})
	if created["revision"] != float64(1) {
		t.Fatal(created)
	}
	body := map[string]any{"collection_id": "album-one", "action": "pin", "attachment_id": a.AttachmentID, "sha256": jarvisTextHash(b.String()), "intent_id": "r_overview_pin_original", "expected_revision": 1}
	pinReceipt := call("collections/mutate", body)
	c := pinReceipt["collection"].(map[string]any)
	pins := c["pins"].([]any)
	p := pins[0].(map[string]any)
	if len(pins) != 1 || c["pin_count"] != float64(1) {
		t.Fatal(c)
	}
	result := []byte{}
	for offset := 0; ; {
		part := call("assets/read", map[string]any{"collection_id": "album-one", "pin_id": p["pin_id"], "sha256": p["source_sha256"], "variant": "original", "offset": offset, "limit": 127})
		raw, e := base64.StdEncoding.DecodeString(part["bytes_base64"].(string))
		if e != nil {
			t.Fatal(e)
		}
		result = append(result, raw...)
		if part["complete"] == true {
			break
		}
		offset = int(part["next_offset"].(float64))
	}
	if !bytes.Equal(result, b.Bytes()) {
		t.Fatal("bytes changed")
	}
	thumb := call("assets/read", map[string]any{"collection_id": "album-one", "pin_id": p["pin_id"], "sha256": p["source_sha256"], "variant": "thumbnail"})
	if thumb["variant_sha256"] == thumb["source_sha256"] {
		t.Fatal("variant hash conflated")
	}
	call("collections/mutate", body) // exact replay returns original durable receipt, no duplicate
	detail := call("collections/get", map[string]any{"collection_id": "album-one"})
	if len(detail["pins"].([]any)) != 1 {
		t.Fatal(detail)
	}
	s, _ := h.registry.Get("s_overview_manager")
	s.SetResumeID("thread-replaced")
	for k, v := range scope {
		body[k] = v
	}
	raw, _ := json.Marshal(body)
	r := overviewHTTP(h, "collections/mutate", raw, "phone")
	if r.status != 403 {
		t.Fatal("old manager fence", r)
	}
	// Even a newly valid manager tuple cannot resurrect the old source pixels.
	scope["manager"] = h.overviewNavigation(s)
	req := map[string]any{"collection_id": "album-one", "pin_id": p["pin_id"], "sha256": p["source_sha256"], "variant": "original"}
	for k, v := range scope {
		req[k] = v
	}
	raw, _ = json.Marshal(req)
	if r = overviewHTTP(h, "assets/read", raw, "phone"); r.status != 403 || strings.Contains(r.body, "bytes_base64") {
		t.Fatal("source epoch leaked", r)
	}
}

func TestOverviewFrozenPaginationScopeAndCredentialFences(t *testing.T) {
	h, call, scope := overviewFixture(t)
	for i := 0; i < 24; i++ {
		dir := filepath.Join(h.cfg.RootDir, fmt.Sprintf("p-%02d", i))
		if e := os.Mkdir(dir, 0700); e != nil {
			t.Fatal(e)
		}
		h.registry.Create(fmt.Sprintf("s_p%02d", i), "source", dir, "codex", "model", "read-only", fmt.Sprintf("thread-%02d", i))
	}
	out := call("snapshot", map[string]any{"section": "projects", "limit": 4})
	freeze := out["freeze_id"]
	seen := map[string]bool{}
	for {
		for _, raw := range out["projects"].(map[string]any)["items"].([]any) {
			id := raw.(map[string]any)["project_id"].(string)
			if seen[id] {
				t.Fatal("duplicate page row")
			}
			seen[id] = true
		}
		cursor := out["cursor"].(string)
		if cursor == "" {
			break
		}
		out = call("snapshot", map[string]any{"section": "projects", "limit": 4, "cursor": cursor})
		if out["freeze_id"] != freeze {
			t.Fatal("freeze changed")
		}
	}
	if len(seen) != 25 {
		t.Fatal(len(seen))
	}
	page := call("snapshot", map[string]any{"section": "projects", "limit": 2})
	req := map[string]any{"section": "projects", "limit": 2, "cursor": page["cursor"]}
	for k, v := range scope {
		req[k] = v
	}
	bad := []byte(page["cursor"].(string))
	bad[len(bad)/2] ^= 1
	req["cursor"] = string(bad)
	raw, _ := json.Marshal(req)
	if r := overviewHTTP(h, "snapshot", raw, "phone"); r.status != 409 {
		t.Fatal(r)
	}
	req["cursor"] = page["cursor"]
	h.registry.Create("s_new_source", "new", h.cfg.RootDir, "codex", "model", "read-only", "new-thread")
	raw, _ = json.Marshal(req)
	if r := overviewHTTP(h, "snapshot", raw, "phone"); r.status != 409 {
		t.Fatal("source set did not retire cursor", r)
	}
	if r := overviewHTTP(h, "snapshot", raw, "foreign"); r.status != 403 {
		t.Fatal("foreign device", r)
	}
}
func TestOverviewFrozenExecutionGrantRevocationRedactsPrivateRows(t *testing.T) {
	h, call, a, b := unifiedFixtureSessions(t)
	loc := seedUnifiedController(t, h, a, b, "scd_overview_private")
	_, _, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: b.ID, RequestID: "r_" + loc.NativeID, Payload: []byte(`{}`)})
	if e != nil {
		t.Fatal(e)
	}
	scope := map[string]any{"authority_instance_id": h.cfg.InstanceID, "manager": h.overviewNavigation(a), "section": "projects", "limit": 1}
	first := call("POST", "overview/snapshot", scope, "phone")
	if first.Code != 200 {
		t.Fatal(first.Code, first.Body.String())
	}
	var page map[string]any
	_ = json.Unmarshal(first.Body.Bytes(), &page)
	if page["cursor"] == "" {
		t.Fatal(page)
	}
	grant, e := h.dispatches.Grant(t.Context(), a.ID)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = h.dispatches.SetGrant(t.Context(), a.ID, sessiondispatch.Grant{Enabled: false, Local: true}, grant.Revision); e != nil {
		t.Fatal(e)
	}
	scope["cursor"] = page["cursor"]
	after := call("POST", "overview/snapshot", scope, "phone")
	if after.Code != 200 || strings.Contains(after.Body.String(), "original scd_overview_private") {
		t.Fatal("revoked relation private title leaked", after.Code, after.Body.String())
	}
	var value map[string]any
	_ = json.Unmarshal(after.Body.Bytes(), &value)
	if value["coverage"].(map[string]any)["partial"] != true {
		t.Fatal(value)
	}
}
func TestOverviewUnassociatedCwdAttentionAndAnnotationPatch(t *testing.T) {
	h, call, _ := overviewFixture(t)
	workspace := h.cfg.RootDir
	h.cfg.RootDir = ""
	s := h.registry.Create("s_no_project", "no project", "", "codex", "model", "read-only", "thread-no-project")
	principal := jarvisPrincipal("phone", jarvisTokenGeneration("jarvis-token"))
	if _, e := h.messageQueue.MutateWork(t.Context(), principal, s.ID, messagequeue.WorkMutation{IntentID: "r_overview_pending_note", WorkItem: messagequeue.WorkItem{ID: "pending", Kind: "discussion_note", Title: "人的待決定", RecordingState: "recorded", Decision: &messagequeue.WorkDecision{State: "pending"}}}); e != nil {
		t.Fatal(e)
	}
	out := call("snapshot", map[string]any{"section": "all"})
	if len(out["projects"].(map[string]any)["items"].([]any)) != 1 || len(out["attention"].(map[string]any)["items"].([]any)) != 1 {
		t.Fatal(out)
	}
	p := out["projects"].(map[string]any)["items"].([]any)[0].(map[string]any)
	raw := []byte(`{"schema_version":1,"revision":1,"milestones":[{"id":"a"}]}`)
	path := filepath.Join(workspace, "phase-plan.json")
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	plan := messagequeue.WorkPlan{Path: path, SHA256: jarvisTextHash(string(raw)), Revision: 1, Checklist: []string{"a"}}
	call("project/annotation", map[string]any{"project_id": p["project_id"], "intent_id": "r_overview_plan_original", "expected_revision": 0, "plan_ref": plan, "phase": "原階段"})
	call("project/annotation", map[string]any{"project_id": p["project_id"], "intent_id": "r_overview_only_phase", "expected_revision": 1, "phase": "下一階段"})
	detail := call("project/get", map[string]any{"project_id": p["project_id"]})["project"].(map[string]any)
	if detail["annotation"].(map[string]any)["plan_ref"] == nil || detail["progress"].(map[string]any)["total"] != float64(1) {
		t.Fatal("phase erased plan", detail)
	}
}
func TestOverviewCanonicalInputHashAndUntrustedAssetSourceRejected(t *testing.T) {
	h, call, scope := overviewFixture(t)
	body := map[string]any{"intent_id": "r_overview_unicode", "collection_id": "c", "action": "create", "name": "人的<&原文\u2028", "expected_revision": 0}
	receipt := call("collections/mutate", body)
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	canonicalRaw, _ := json.Marshal(body)
	var canonicalBody map[string]any
	_ = json.Unmarshal(canonicalRaw, &canonicalBody)
	if e := enc.Encode(canonicalBody); e != nil {
		t.Fatal(e)
	}
	want := jarvisTextHash("overview/collections/mutate\n" + strings.TrimSuffix(b.String(), "\n"))
	if receipt["input_hash"] != want {
		t.Fatal(receipt["input_hash"], want)
	}
	a, _ := h.attachments.AddCanonical(attachmentjournal.Record{SessionID: "s_overview_manager", RequestID: "r_no_canonical_queue", Kind: "media", Path: filepath.Join(h.cfg.RootDir, "not-a-real-file"), MIMEType: "image/png"})
	req := map[string]any{"intent_id": "r_overview_fake_source", "collection_id": "c", "action": "pin", "attachment_id": a.AttachmentID, "sha256": strings.Repeat("a", 64), "expected_revision": 1}
	for k, v := range scope {
		req[k] = v
	}
	raw, _ := json.Marshal(req)
	if r := overviewHTTP(h, "collections/mutate", raw, "phone"); r.status == 200 {
		t.Fatal("unproved source pinned")
	}
}

func TestOverviewAssetPagesUseSignedFrozenMembershipBeyondLegacyFifty(t *testing.T) {
	h, call, scope := overviewFixture(t)
	var b bytes.Buffer
	if e := png.Encode(&b, image.NewNRGBA(image.Rect(0, 0, 2, 2))); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(h.cfg.RootDir, "tiny.png")
	if e := os.WriteFile(path, b.Bytes(), 0600); e != nil {
		t.Fatal(e)
	}
	s, _ := h.registry.Get("s_overview_manager")
	seed := func(index int) {
		t.Helper()
		id := fmt.Sprintf("r_asset_original_%03d", index)
		payload, _ := json.Marshal(queuedPayload{ExpectedTarget: &dispatchTargetExpectation{ThreadID: s.ResumeID(), Revision: s.SettingsSnapshot().ConfigRevision}})
		if _, _, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: id, Payload: payload}); e != nil {
			t.Fatal(e)
		}
		h.attachments.AddCanonical(attachmentjournal.Record{SessionID: s.ID, RequestID: id, Kind: "media", Path: path, MIMEType: "image/png"})
	}
	for i := 0; i < 55; i++ {
		seed(i)
	}
	out := call("assets/list", map[string]any{"session_id": s.ID, "limit": 20})
	cursor := out["cursor"].(string)
	if cursor == "" {
		t.Fatal(out)
	}
	seed(99)
	count := len(out["items"].([]any))
	for cursor != "" {
		out = call("assets/list", map[string]any{"session_id": s.ID, "limit": 20, "cursor": cursor})
		for _, item := range out["items"].([]any) {
			if item.(map[string]any)["request_id"] == "r_asset_original_099" {
				t.Fatal("late member crossed highwater")
			}
		}
		count += len(out["items"].([]any))
		cursor = out["cursor"].(string)
	}
	if count != 55 {
		t.Fatal(count)
	}
	bad := map[string]any{"session_id": s.ID, "cursor": "tampered"}
	for k, v := range scope {
		bad[k] = v
	}
	raw, _ := json.Marshal(bad)
	if result := overviewHTTP(h, "assets/list", raw, "phone"); result.status != 409 {
		t.Fatal(result)
	}
}
func TestOverviewDefinitiveRejectionsHaveDurableOriginalHash(t *testing.T) {
	h, call, scope := overviewFixture(t)
	call("collections/mutate", map[string]any{"intent_id": "r_reject_create", "collection_id": "c", "action": "create", "name": "c", "expected_revision": 0})
	body := map[string]any{"intent_id": "r_reject_cas_original", "collection_id": "c", "action": "rename", "name": "next", "expected_revision": 0}
	for k, v := range scope {
		body[k] = v
	}
	raw, _ := json.Marshal(body)
	response := overviewHTTP(h, "collections/mutate", raw, "phone")
	if response.status != 409 {
		t.Fatal(response)
	}
	receipt := call("operation/get", map[string]any{"intent_id": "r_reject_cas_original"})
	if receipt["phase"] != "rejected" || receipt["known_no_effect"] != true || receipt["operation"] != "overview/collections/mutate" || receipt["input_hash"] == "" {
		t.Fatal(receipt)
	}
	detail := call("collections/get", map[string]any{"collection_id": "c"})
	if detail["collection"].(map[string]any)["revision"] != float64(1) {
		t.Fatal("refused effect happened", detail)
	}
	body["name"] = "changed"
	raw, _ = json.Marshal(body)
	if r := overviewHTTP(h, "collections/mutate", raw, "phone"); r.status != 409 {
		t.Fatal("intent hash changed", r)
	}
	same := call("operation/get", map[string]any{"intent_id": "r_reject_cas_original"})
	if same["input_hash"] != receipt["input_hash"] {
		t.Fatal("original rejection overwritten")
	}
}

func TestOverviewStorageFailureCannotBeRecordedAsKnownNoEffect(t *testing.T) {
	h, _, scope := overviewFixture(t)
	if e := h.messageQueue.Close(); e != nil {
		t.Fatal(e)
	}
	body := map[string]any{"intent_id": "r_storage_unknown_original", "collection_id": "c", "action": "create", "name": "c", "expected_revision": 0}
	for k, v := range scope {
		body[k] = v
	}
	raw, _ := json.Marshal(body)
	r := overviewHTTP(h, "collections/mutate", raw, "phone")
	if r.status != 503 || strings.Contains(r.body, "known_no_effect") || strings.Contains(r.body, "\"phase\":\"rejected\"") {
		t.Fatal(r)
	}
}
