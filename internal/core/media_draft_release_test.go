package core

import (
	"errors"
	"os"
	"testing"
	"time"

	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func draftReleaseFixture(t *testing.T) (*Hub, *session.Session, *Client, clientproto.Command, string) {
	t.Helper()
	h, s, c, _ := mediaFixture(t)
	data := mediaPNG(t)
	seed, item, seedBody := mediaPrepareItem(t, h, s, c, "r_release_seed", data)
	object := mediaUploadItem(t, h, s, c, seed, item, seedBody, data)
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel-seed", SessionID: s.ID, MediaBatchID: seed.MediaBatchID, UploadRequestID: item.UploadRequestID})
	if got := waitForType(t, c, "media_batch_receipt"); got["state"] != "cancelled_before_admission" {
		t.Fatal(got)
	}
	// A real PREPARE reuses one complete object twice, with distinct original
	// draft intents. Releasing one alias cannot erase the other's reference.
	item.AttachmentID = object
	item.UploadRequestID = "vu_parent_target"
	sibling := item
	sibling.ItemID = "sibling"
	sibling.UploadRequestID = "vu_sibling"
	cfg := s.SettingsSnapshot().ConfigRevision
	cmd := clientproto.Command{Kind: "media_batch_prepare", RequestID: "prepare-parent", SessionID: s.ID, MediaBatchID: "r_release_parent", MediaBatchRevision: 1, ExpectedConfigRevision: &cfg, MediaItems: []protocol.MediaBatchItem{item, sibling}}
	h.handleMediaBatch(c, cmd)
	prep := waitForType(t, c, "media_batch_receipt")
	if prep["state"] != "prepared" {
		t.Fatal(prep)
	}
	body := prep["body_hash"].(string)
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel-original", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID, UploadRequestID: item.UploadRequestID})
	got := waitForType(t, c, "media_batch_receipt")
	if got["state"] != "cancelled_before_admission" {
		t.Fatal(got)
	}
	release := clientproto.Command{Kind: "media_draft_release", RequestID: "release-original", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: body, UploadRequestID: sibling.UploadRequestID, ItemID: sibling.ItemID, ItemRevision: 1, SHA256: sibling.SHA256, UploadID: object}
	return h, s, c, release, object
}
func draftRelease(t *testing.T, h *Hub, c *Client, cmd clientproto.Command) map[string]any {
	t.Helper()
	h.handleMediaDraftRelease(c, cmd)
	return waitForType(t, c, "media_draft_release_receipt")
}
func TestMediaDraftReleaseExplicitGraceAndOriginalReceipt(t *testing.T) {
	h, s, c, cmd, object := draftReleaseFixture(t)
	m, e := h.unifiedUpload(s.ID, object)
	if e != nil {
		t.Fatal(e)
	}
	got := draftRelease(t, h, c, cmd)
	if got["phase"] != "committed" || got["physical_delete"] != false {
		t.Fatal(got)
	}
	if _, e = os.Stat(m.Path); e != nil {
		t.Fatal("release deleted physical object", e)
	}
	// A newly released old draft gets its OWN grace, never the earlier cancel time.
	done, e := h.media.begin()
	if e != nil {
		t.Fatal(e)
	}
	b := h.media.data.Batches[mediaKey(c.deviceID, jarvisTokenGeneration("mua-token"), cmd.MediaBatchID)]
	b.Updated = time.Now().Add(-48 * time.Hour).UnixMilli()
	_ = h.media.save()
	done()
	if e = h.collectMediaGarbage(time.Now().Add(23 * time.Hour)); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(m.Path); e != nil {
		t.Fatal("new release grace lost", e)
	}
	// Receipt is original even after payload/source changes, never relabel effect.
	changed := cmd
	changed.SHA256 = "wrong"
	got = draftRelease(t, h, c, changed)
	if got["phase"] != "committed" || got["error_code"] != "idempotency_conflict" || got["sha256"] != cmd.SHA256 {
		t.Fatal(got)
	}
	s.SetName("updated")
	got = draftRelease(t, h, c, clientproto.Command{Kind: "media_draft_release_status", RequestID: "query-original", OperationID: cmd.RequestID, SessionID: s.ID})
	if got["phase"] != "committed" || got["operation_id"] != cmd.RequestID || got["request_id"] != "query-original" {
		t.Fatal(got)
	}
	if e = h.collectMediaGarbage(time.Now().Add(25 * time.Hour)); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(m.Path); !os.IsNotExist(e) {
		t.Fatal("explicitly abandoned aliases not collected", e)
	}
}
func TestMediaDraftReleaseDurableUnknownAndNoEffectRecovery(t *testing.T) {
	for _, point := range []string{"draft_release_before_save", "draft_release_after_save"} {
		t.Run(point, func(t *testing.T) {
			h, s, c, cmd, _ := draftReleaseFixture(t)
			h.media.fault = func(p string) error {
				if p == point {
					return errors.New("isolated crash")
				}
				return nil
			}
			if got := draftRelease(t, h, c, cmd); got["phase"] != "unknown" {
				t.Fatal(got)
			}
			h.media.fault = nil
			query := clientproto.Command{Kind: "media_draft_release_status", RequestID: "query", OperationID: cmd.RequestID, SessionID: s.ID}
			got := draftRelease(t, h, c, query)
			if point == "draft_release_before_save" {
				if got["phase"] != "unknown" || got["error_code"] != "not_found" {
					t.Fatal(got)
				}
				done, e := h.media.begin()
				if e != nil {
					t.Fatal(e)
				}
				b := h.media.data.Batches[mediaKey(c.deviceID, jarvisTokenGeneration("mua-token"), cmd.MediaBatchID)]
				if mediaDraftAbandonedAt(h.media, b, cmd.UploadRequestID) != 0 {
					t.Fatal("uncommitted release retired pin")
				}
				done()
			} else if got["phase"] != "committed" {
				t.Fatal(got)
			}
		})
	}
}
func TestMediaDraftReleaseRejectsExactMismatchAndKeepsRejection(t *testing.T) {
	for _, field := range []string{"batch", "revision", "body", "upload", "item", "itemrevision", "sha", "object", "handoff"} {
		t.Run(field, func(t *testing.T) {
			h, _, c, cmd, _ := draftReleaseFixture(t)
			original := cmd
			switch field {
			case "batch":
				cmd.MediaBatchID = "other"
			case "revision":
				cmd.MediaBatchRevision++
			case "body":
				cmd.MediaBodyHash = "other"
			case "upload":
				cmd.UploadRequestID = "other"
			case "item":
				cmd.ItemID = "other"
			case "itemrevision":
				cmd.ItemRevision++
			case "sha":
				cmd.SHA256 = "other"
			case "object":
				cmd.UploadID = "other"
			case "handoff":
				cmd.HandoffBatchID = cmd.MediaBatchID
			}
			got := draftRelease(t, h, c, cmd)
			if got["phase"] != "rejected_known_none" {
				t.Fatal(got)
			}
			// Original before-effect rejection is durable: correcting payload needs a new HUMAN action, never a silent replay.
			got = draftRelease(t, h, c, original)
			if got["phase"] != "rejected_known_none" || got["error_code"] != "idempotency_conflict" {
				t.Fatal(got)
			}
		})
	}
}
func TestMediaDraftReleaseHandoffAndOtherPinsAlwaysWin(t *testing.T) {
	for _, state := range []string{"prepared", "admitted", "admitting_unknown", "reader"} {
		t.Run(state, func(t *testing.T) {
			h, s, c, cmd, object := draftReleaseFixture(t)
			done, e := h.media.begin()
			if e != nil {
				t.Fatal(e)
			}
			old := h.media.data.Batches[mediaKey(c.deviceID, jarvisTokenGeneration("mua-token"), cmd.MediaBatchID)]
			generation := old.Generation
			item := old.Items[1]
			done()
			item.UploadRequestID = "vu_new"
			item.AttachmentID = object
			cfg := s.SettingsSnapshot().ConfigRevision
			next := clientproto.Command{Kind: "media_batch_prepare", SessionID: s.ID, RequestID: "prepare-handoff", MediaBatchID: "r_handoff", MediaBatchRevision: 1, ExpectedConfigRevision: &cfg, MediaItems: []protocol.MediaBatchItem{item}}
			h.handleMediaBatch(c, next)
			if got := waitForType(t, c, "media_batch_receipt"); got["state"] != "prepared" {
				t.Fatal(got)
			}
			cmd.HandoffBatchID = next.MediaBatchID
			if got := draftRelease(t, h, c, cmd); got["phase"] != "committed" {
				t.Fatal(got)
			}
			done, e = h.media.begin()
			if e != nil {
				t.Fatal(e)
			}
			if state == "reader" {
				delete(h.media.data.Batches, mediaKey(c.deviceID, generation, next.MediaBatchID))
				h.media.data.Readers["isolated-reader"] = mediaReadLease{ObjectID: object, Purpose: "history"}
			} else {
				h.media.data.Batches[mediaKey(c.deviceID, generation, next.MediaBatchID)].State = state
			}
			_ = h.media.save()
			done()
			if e = h.collectMediaGarbage(time.Now().Add(25 * time.Hour)); e != nil {
				t.Fatal(e)
			}
			manifest, e := h.unifiedUpload(s.ID, object)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = os.Stat(manifest.Path); e != nil {
				t.Fatal("newbusiness/history pin erased", e)
			}
		})
	}
}

func TestMediaDraftReleaseSourceABAAndActorFences(t *testing.T) {
	for _, mode := range []string{"source", "aba", "foreign", "generation", "not_cancelled"} {
		t.Run(mode, func(t *testing.T) {
			h, s, c, cmd, object := draftReleaseFixture(t)
			switch mode {
			case "source":
				s.SetModel("changed")
			case "aba":
				s.SetModel("changed")
				s.SetModel("fixture-model")
			case "foreign":
				other := sharedReadClient(t, h, "foreign-phone")
				other.ctx = c.ctx
				c = other
			case "generation":
				h.pairing.OpenEnrollment(time.Minute)
				if e := h.pairing.Claim("different-token", "phone-a"); e != nil {
					t.Fatal(e)
				}
				c.readIdentity.Store(&pairedReadIdentity{token: "different-token", deviceID: "phone-a"})
			case "not_cancelled":
				done, e := h.media.begin()
				if e != nil {
					t.Fatal(e)
				}
				h.media.data.Batches[mediaKey(c.deviceID, jarvisTokenGeneration("mua-token"), cmd.MediaBatchID)].State = "admitting_unknown"
				_ = h.media.save()
				done()
			}
			got := draftRelease(t, h, c, cmd)
			if got["phase"] != "rejected_known_none" {
				t.Fatal(got)
			}
			done, e := h.media.begin()
			if e != nil {
				t.Fatal(e)
			}
			if !mediaGCReferences(h.media, object, "", time.Now().Add(25*time.Hour)) {
				t.Fatal("rejected release lost original pin")
			}
			done()
		})
	}
}
func TestMediaDraftReleaseCommittedOriginalSurvivesSourceChange(t *testing.T) {
	h, s, c, cmd, _ := draftReleaseFixture(t)
	if got := draftRelease(t, h, c, cmd); got["phase"] != "committed" {
		t.Fatal(got)
	}
	s.SetModel("changed")
	if got := draftRelease(t, h, c, cmd); got["phase"] != "committed" {
		t.Fatal(got)
	}
	if got := draftRelease(t, h, c, clientproto.Command{Kind: "media_draft_release_status", SessionID: s.ID, RequestID: "query", OperationID: cmd.RequestID}); got["phase"] != "committed" {
		t.Fatal(got)
	}
}
func TestMediaDraftReleaseRouterAndCapabilities(t *testing.T) {
	h, s, c, cmd, _ := draftReleaseFixture(t)
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_capabilities", RequestID: "cap", SessionID: s.ID})
	got := waitForType(t, c, "media_batch_receipt")
	if got["draft_release_available"] != true {
		t.Fatal(got)
	}
	route(h, c, `{"type":"media_draft_release","request_id":"wire-release","session_id":"`+s.ID+`","media_batch_id":"`+cmd.MediaBatchID+`","media_batch_revision":1,"media_body_hash":"`+cmd.MediaBodyHash+`","upload_request_id":"`+cmd.UploadRequestID+`","item_id":"`+cmd.ItemID+`","item_revision":1,"sha256":"`+cmd.SHA256+`","upload_id":"`+cmd.UploadID+`"}`)
	got = waitForType(t, c, "media_draft_release_receipt")
	if got["phase"] != "committed" {
		t.Fatal(got)
	}
	route(h, c, `{"type":"media_draft_release_status","request_id":"wire-query","operation_id":"wire-release","session_id":"`+s.ID+`"}`)
	got = waitForType(t, c, "media_draft_release_receipt")
	if got["phase"] != "committed" || got["operation_id"] != "wire-release" || got["request_id"] != "wire-query" {
		t.Fatal(got)
	}
}

func TestMediaDraftReleaseUnfinishedSiblingAndOwnerStatus(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	data := mediaPNG(t)
	seed, item, _ := mediaPrepareItem(t, h, s, c, "r_unused_template", data)
	_ = seed
	sibling := item
	sibling.ItemID = "partial"
	sibling.UploadRequestID = "vu_partial_release"
	cfg := s.SettingsSnapshot().ConfigRevision
	cmd := clientproto.Command{Kind: "media_batch_prepare", RequestID: "prepare-partial", SessionID: s.ID, MediaBatchID: "r_partial_release", MediaBatchRevision: 1, ExpectedConfigRevision: &cfg, MediaItems: []protocol.MediaBatchItem{item, sibling}}
	// Distinct upload identity even though bytes are deliberately equal.
	cmd.MediaItems[0].UploadRequestID = "vu_cancel_target"
	h.handleMediaBatch(c, cmd)
	prep := waitForType(t, c, "media_batch_receipt")
	if prep["state"] != "prepared" {
		t.Fatal(prep)
	}
	body := prep["body_hash"].(string)
	c.uploads.initCommand(clientproto.Command{SessionID: s.ID, UploadRequestID: sibling.UploadRequestID, UploadKind: sibling.UploadKind, Name: sibling.Name, MediaType: sibling.MediaType, SizeBytes: sibling.SizeBytes, MediaBatchID: cmd.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: body, ItemID: sibling.ItemID, ItemRevision: 1, SHA256: sibling.SHA256})
	ready := waitForType(t, c, "attachment_upload_ready")
	id := ready["upload_id"].(string)
	c.uploads.writeFrame(makeV2AttachmentFrame(id, 0, data[:4]))
	waitForType(t, c, "attachment_upload_ack")
	path := c.uploads.active[id].partPath
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel-partial-parent", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID, UploadRequestID: cmd.MediaItems[0].UploadRequestID})
	waitForType(t, c, "media_batch_receipt")
	release := clientproto.Command{Kind: "media_draft_release", RequestID: "release-partial", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: body, UploadRequestID: sibling.UploadRequestID, ItemID: sibling.ItemID, ItemRevision: 1, SHA256: sibling.SHA256}
	got := draftRelease(t, h, c, release)
	if got["phase"] != "committed" || got["upload_id"] != id {
		t.Fatal(got)
	}
	other := sharedReadClient(t, h, "another-phone")
	other.ctx = c.ctx
	got = draftRelease(t, h, other, clientproto.Command{Kind: "media_draft_release_status", SessionID: s.ID, RequestID: "foreign-query", OperationID: release.RequestID})
	if got["phase"] != "unknown" || got["error_code"] != "not_found" || got["sha256"] != nil {
		t.Fatal("cross actor receipt leaked", got)
	}
	if e := h.collectMediaGarbage(time.Now().Add(25 * time.Hour)); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("explicit abandoned partial not reclaimed", e)
	}
}

func TestMediaDraftReleaseKeepDraftCancelThenNewSendAndRemove(t *testing.T) {
	h, s, c, _ := mediaFixture(t)
	data := mediaPNG(t)
	cmd, item, body := mediaPrepareItem(t, h, s, c, "r_keep_draft", data)
	object := mediaUploadItem(t, h, s, c, cmd, item, body, data)
	cancel := clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel-keep", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID, MediaBatchRevision: 1, UploadRequestID: item.UploadRequestID, KeepDraftItems: true}
	h.handleMediaBatch(c, cancel)
	got := waitForType(t, c, "media_batch_receipt")
	if got["state"] != "cancelled_before_admission" || got["cancel_keeps_draft"] != true {
		t.Fatal(got)
	}
	// A duplicate/defaultfalse cancel cannot silently change the original policy.
	cancel.KeepDraftItems = false
	h.handleMediaBatch(c, cancel)
	got = waitForType(t, c, "media_batch_receipt")
	if got["cancel_keeps_draft"] != true || got["cancel_request_id"] != "cancel-keep" {
		t.Fatal(got)
	}
	if e := h.collectMediaGarbage(time.Now().Add(25 * time.Hour)); e != nil {
		t.Fatal(e)
	}
	m, e := h.unifiedUpload(s.ID, object)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(m.Path); e != nil {
		t.Fatal("keepall cancel lost anchor pin", e)
	}
	// A real new business acquires its durable same-object pin before old handoff.
	nextItem := item
	nextItem.AttachmentID = object
	nextItem.UploadRequestID = "vu_kept_handoff"
	cfg := s.SettingsSnapshot().ConfigRevision
	next := clientproto.Command{Kind: "media_batch_prepare", RequestID: "prepare-kept-new", SessionID: s.ID, MediaBatchID: "r_kept_new", MediaBatchRevision: 1, ExpectedConfigRevision: &cfg, Content: "human changed draft", MediaItems: []protocol.MediaBatchItem{nextItem}}
	h.handleMediaBatch(c, next)
	got = waitForType(t, c, "media_batch_receipt")
	if got["state"] != "prepared" {
		t.Fatal(got)
	}
	nextBody := got["body_hash"].(string)
	release := clientproto.Command{Kind: "media_draft_release", RequestID: "release-kept-old", SessionID: s.ID, MediaBatchID: cmd.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: body, UploadRequestID: item.UploadRequestID, ItemID: item.ItemID, ItemRevision: 1, SHA256: item.SHA256, UploadID: object, HandoffBatchID: next.MediaBatchID}
	if got = draftRelease(t, h, c, release); got["phase"] != "committed" {
		t.Fatal(got)
	}
	if e = h.collectMediaGarbage(time.Now().Add(25 * time.Hour)); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(m.Path); e != nil {
		t.Fatal("handoff lost new draft", e)
	}
	// Explicit local removal of that new unsent draft now retires only its pin.
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel-kept-new", SessionID: s.ID, MediaBatchID: next.MediaBatchID, MediaBatchRevision: 1, UploadRequestID: nextItem.UploadRequestID, KeepDraftItems: true})
	waitForType(t, c, "media_batch_receipt")
	release.RequestID = "release-kept-new"
	release.MediaBatchID = next.MediaBatchID
	release.MediaBodyHash = nextBody
	release.UploadRequestID = nextItem.UploadRequestID
	release.HandoffBatchID = ""
	if got = draftRelease(t, h, c, release); got["phase"] != "committed" {
		t.Fatal(got)
	}
	if e = h.collectMediaGarbage(time.Now().Add(25 * time.Hour)); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(m.Path); !os.IsNotExist(e) {
		t.Fatal("explicit finalremove was never collected", e)
	}
}
