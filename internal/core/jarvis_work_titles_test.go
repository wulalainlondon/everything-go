package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf16"
	"unicode/utf8"
)

type workTitleCase struct {
	name, goal, want string
	images           []intakeImage
	files            []intakeFile
}

func workTitleCases() []workTitleCase {
	return []workTitleCase{
		{name: "pure_image", want: "附件工作", images: []intakeImage{{Data: "b3JpZ2luYWw=", MediaType: "image/png"}}},
		{name: "pure_video", want: "附件工作", files: []intakeFile{{AttachmentID: "u_original_video", Name: "original.mp4", MediaType: "video/mp4"}}},
		{name: "mixed_media", want: "附件工作", images: []intakeImage{{Data: "b3JpZ2luYWw=", MediaType: "image/png"}}, files: []intakeFile{{AttachmentID: "u_original_video", MediaType: "video/mp4"}}},
		{name: "empty_structure", want: "未命名工作"},
		{name: "whitespace_media", goal: " \n\t", want: "附件工作", files: []intakeFile{{AttachmentID: "u_original_video", MediaType: "video/mp4"}}},
		{name: "astral_goal", goal: strings.Repeat("😀", 180), want: strings.Repeat("😀", 100)},
		{name: "joined_graphemes", goal: strings.Repeat("👨‍👩‍👧‍👦", 100), want: strings.Repeat("👨‍👩‍👧‍👦", 18)},
	}
}
func seedTitleIntake(t *testing.T, h *Hub, s *session.Session, c workTitleCase) string {
	t.Helper()
	rid := "r_title_" + c.name
	thread := s.ResumeID()
	in := intakeInput{Action: "submit", SessionID: s.ID, RequestID: rid, Goal: c.goal, Images: c.images, Files: c.files, Target: intakeTarget{Instance: h.cfg.InstanceID, Session: s.ID, Thread: &thread, Revision: s.SettingsSnapshot().ConfigRevision}}
	input, _ := json.Marshal(in)
	request := taskapi.Request{Operation: "intake", IdempotencyKey: rid, Input: input}
	caller := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:phone", NamespaceGeneration: jarvisTokenGeneration("jarvis-token"), Transport: "authenticated_ws", BindingKind: "paired_human"}
	metadata, _ := json.Marshal(apiMetadata{Request: request, Caller: caller, Intake: &intakeMetadata{Goal: c.goal, Source: map[string]any{"session_id": s.ID, "request_id": rid}}})
	command := taskapi.AuthorizedCommand{Caller: caller, Request: request, Locator: taskapi.Locator{Authority: h.cfg.InstanceID, Path: "ordinary"}, Namespace: taskapi.Namespace{Authority: h.cfg.InstanceID, StableScopeID: caller.StableScopeID, Generation: caller.NamespaceGeneration, Path: "ordinary", Operation: "intake"}, IntentHash: jarvisTextHash(rid)}
	record := taskapi.IntentRecord{ReceiptID: "tr_" + rid, TaskID: apiID(h.cfg.InstanceID, "ordinary", s.ID+"/"+rid), NativeID: s.ID + "/" + rid, SessionID: s.ID, RequestID: rid, Metadata: metadata}
	if _, created, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: rid, Content: c.goal, Payload: input, API: &messagequeue.APIAdmission{Command: command, Record: record}}); err != nil || !created {
		t.Fatal(created, err)
	}
	return rid
}
func TestUnifiedMediaOnlyDerivedTitleWirePreservesOriginalIntake(t *testing.T) {
	for _, c := range workTitleCases() {
		t.Run(c.name, func(t *testing.T) {
			h, call := jarvisFixture(t)
			s := h.registry.Create("s_title_original", "Jarvis", h.cfg.RootDir, "codex", "m", "read-only", "native")
			rid := seedTitleIntake(t, h, s, c)
			before, err := h.messageQueue.TaskJournal().OriginalCommandForRequest(t.Context(), s.ID, rid)
			if err != nil {
				t.Fatal(err)
			}
			queued, _, err := h.messageQueue.Get(s.ID, rid)
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"worklist/snapshot", "worklist/get"} {
				input := unifiedBase(h, s)
				if path == "worklist/get" {
					input["item_id"] = "intake_" + rid
				}
				w := call("POST", path, input, "phone")
				if w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
				rows := decodeUnified(t, w.Body.Bytes())["items"].([]any)
				if len(rows) != 1 {
					t.Fatal(len(rows))
				}
				item := rows[0].(map[string]any)["item"].(map[string]any)
				title := item["title"].(string)
				if title != c.want || !utf8.ValidString(title) || len(utf16.Encode([]rune(title))) > 200 {
					t.Fatal(title, c.want)
				}
				if _, ok := item["links"].([]any); !ok {
					t.Fatal("links null")
				}
				if _, ok := item["source_refs"].([]any); !ok {
					t.Fatal("source_refs null")
				}
				if path == "worklist/get" && (item["body"] != c.goal || item["body_hash"] != jarvisTextHash(c.goal)) {
					t.Fatal("original body/digest changed", item)
				}
			}
			after, err := h.messageQueue.TaskJournal().OriginalCommandForRequest(t.Context(), s.ID, rid)
			if err != nil || string(before.Metadata) != string(after.Metadata) {
				t.Fatal("source metadata rewritten", err)
			}
			retained, _, err := h.messageQueue.Get(s.ID, rid)
			if err != nil || retained.Content != queued.Content || string(retained.Payload) != string(queued.Payload) || retained.State != messagequeue.Queued {
				t.Fatal("original queue payload/effect changed", err)
			}
		})
	}
}
func TestUnifiedAdjacentDerivedSourceTitleBoundaries(t *testing.T) {
	for _, value := range []string{"", " \n", strings.Repeat("中", 1000), strings.Repeat("😀", 180), strings.Repeat("👨‍👩‍👧‍👦", 100), "a" + strings.Repeat("\u0301", 400)} {
		title := jarvisDerivedWorkTitle(value, "未命名工作")
		if strings.TrimSpace(title) == "" || len(utf16.Encode([]rune(title))) > 200 || !utf8.ValidString(title) {
			t.Fatalf("bad derived title %q", title)
		}
	}
}
func seedLegacyTitleMetadata(t *testing.T, h *Hub, s *session.Session) messagequeue.WorkItem {
	t.Helper()
	dir := t.TempDir()
	store, err := messagequeue.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	h.messageQueue.Close()
	h.messageQueue = store
	in := messagequeue.WorkMutation{IntentID: "r_title_legacy_original", WorkItem: messagequeue.WorkItem{ID: "legacy-title", Kind: "discussion_note", Title: "original", Body: "原正文完全保留", RecordingState: "recorded"}}
	item, err := store.MutateWork(context.Background(), jarvisTestPrincipal("phone", "jarvis-token"), s.ID, in)
	if err != nil {
		t.Fatal(err)
	}
	item.Title = strings.Repeat("😀", 180)
	raw, _ := json.Marshal(item)
	db, err := sql.Open("sqlite", filepath.Join(dir, "message_queue.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`UPDATE jarvis_work_items SET payload=? WHERE item_id=?`, raw, item.ID); err != nil {
		t.Fatal(err)
	}
	return item
}
func TestUnifiedLegacyMetadataTitleIsMarkedReadOnlyNotOverwritten(t *testing.T) {
	h, call := jarvisFixture(t)
	s := h.registry.Create("s_legacy_title", "Jarvis", h.cfg.RootDir, "codex", "m", "read-only", "native")
	original := seedLegacyTitleMetadata(t, h, s)
	for _, path := range []string{"worklist/snapshot", "worklist/get"} {
		in := unifiedBase(h, s)
		if path == "worklist/get" {
			in["item_id"] = original.ID
		}
		w := call("POST", path, in, "phone")
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		item := decodeUnified(t, w.Body.Bytes())["items"].([]any)[0].(map[string]any)["item"].(map[string]any)
		if item["title_truncated"] != true || item["detail_truncated"] != true || item["title_hash"] != jarvisTextHash(original.Title) || item["body"] != original.Body || item["body_hash"] != jarvisTextHash(original.Body) || item["revision"] != float64(original.Revision) {
			t.Fatal(item)
		}
		if n := len(utf16.Encode([]rune(item["title"].(string)))); n > 200 {
			t.Fatal(n)
		}
	}
	after, err := h.messageQueue.WorkItem(t.Context(), jarvisTestPrincipal("phone", "jarvis-token"), s.ID, original.ID)
	if err != nil || after.Title != original.Title || after.Revision != original.Revision {
		t.Fatal("original legacy title rewritten", after, err)
	}
}

func TestUnifiedPreUpgradeFrozenEmptyTitleRemainsReadableWithoutRewrite(t *testing.T) {
	h, call := jarvisFixture(t)
	s := h.registry.Create("s_frozen_title", "Jarvis", h.cfg.RootDir, "codex", "m", "read-only", "native")
	seedTitleIntake(t, h, s, workTitleCases()[3])
	rid := seedTitleIntake(t, h, s, workTitleCases()[0])
	input := unifiedBase(h, s)
	input["limit"] = 1
	first := call("POST", "worklist/snapshot", input, "phone")
	if first.Code != 200 {
		t.Fatal(first.Code, first.Body.String())
	}
	v := decodeUnified(t, first.Body.Bytes())
	id := v["freeze_id"].(string)
	var legacyRaw []byte
	if err := h.messageQueue.TaskJournal().Transaction(t.Context(), func(tx *sql.Tx) error {
		if err := tx.QueryRow(`SELECT rows FROM task_api_freezes WHERE id=?`, id).Scan(&legacyRaw); err != nil {
			return err
		}
		var rows []map[string]any
		if err := json.Unmarshal(legacyRaw, &rows); err != nil {
			return err
		}
		found := false
		for _, row := range rows {
			value := row["Value"].(map[string]any)
			item, ok := value["item"].(map[string]any)
			if ok && item["item_id"] == "intake_"+rid {
				item["title"] = ""
				found = true
			}
		}
		if !found {
			t.Fatal("fixture target missing")
		}
		legacyRaw, _ = json.Marshal(rows)
		_, err := tx.Exec(`UPDATE task_api_freezes SET rows=? WHERE id=?`, legacyRaw, id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	next := unifiedBase(h, s)
	next["limit"] = 1
	next["cursor"] = v["cursor"]
	w := call("POST", "worklist/snapshot", next, "phone")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	item := decodeUnified(t, w.Body.Bytes())["items"].([]any)[0].(map[string]any)["item"].(map[string]any)
	if item["title"] != "未命名工作" || item["title_truncated"] != true || item["detail_truncated"] != true {
		t.Fatal(item)
	}
	var after []byte
	if err := h.messageQueue.TaskJournal().Transaction(t.Context(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT rows FROM task_api_freezes WHERE id=?`, id).Scan(&after)
	}); err != nil || string(after) != string(legacyRaw) {
		t.Fatal("old frozen bytes rewritten", err)
	}
}
