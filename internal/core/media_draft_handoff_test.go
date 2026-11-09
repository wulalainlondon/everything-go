package core

import (
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func mediaHandoffPrepared(t *testing.T, unconfigured ...bool) (*Hub, *session.Session, *Client, clientproto.Command, clientproto.Command, protocol.MediaBatchItem, string, chan mediaIntakeInvocation) {
	t.Helper()
	h, s, c, starts := mediaFixture(t)
	if len(unconfigured) > 0 && unconfigured[0] {
		s.SetModel("")
	}
	data := mediaPNG(t)
	old, item, body := mediaPrepareItem(t, h, s, c, "r_handoff_old", data)
	object := mediaUploadItem(t, h, s, c, old, item, body, data)
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_cancel", RequestID: "cancel-handoff-old", SessionID: s.ID, MediaBatchID: old.MediaBatchID, UploadRequestID: item.UploadRequestID, KeepDraftItems: true})
	if got := waitForType(t, c, "media_batch_receipt"); got["state"] != "cancelled_before_admission" || got["cancel_keeps_draft"] != true {
		t.Fatal(got)
	}
	release := clientproto.Command{Kind: "media_draft_release", RequestID: "release-handoff-old", SessionID: s.ID, MediaBatchID: old.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: body, UploadRequestID: item.UploadRequestID, ItemID: item.ItemID, ItemRevision: 1, SHA256: item.SHA256, UploadID: object, HandoffBatchID: "r_handoff_new"}
	item.AttachmentID = object
	item.UploadRequestID = "vu_handoff_new"
	cfg := s.SettingsSnapshot().ConfigRevision
	next := clientproto.Command{Kind: "media_batch_prepare", RequestID: "prepare-handoff-new", SessionID: s.ID, MediaBatchID: release.HandoffBatchID, MediaBatchRevision: 1, ExpectedConfigRevision: &cfg, MediaItems: []protocol.MediaBatchItem{item}}
	h.handleMediaBatch(c, next)
	if got := waitForType(t, c, "media_batch_receipt"); got["state"] != "prepared" {
		t.Fatal(got)
	}
	return h, s, c, release, next, item, object, starts
}
func mediaAdvanceHandoff(t *testing.T, h *Hub, s *session.Session, c *Client, next clientproto.Command, item protocol.MediaBatchItem, object, state string, starts chan mediaIntakeInvocation) {
	t.Helper()
	if state == "prepared" {
		return
	}
	if state == "admitting_unknown" {
		actor, e := h.mediaActor(c)
		if e != nil {
			t.Fatal(e)
		}
		done, e := h.media.begin()
		if e != nil {
			t.Fatal(e)
		}
		_, e = h.mediaBeginAdmission(actor, s.ID, next.MediaBatchID, next.MediaBatchID, "", []backend.ImageAttachment{{AttachmentID: object, MediaType: item.MediaType, SHA256: item.SHA256, SizeBytes: item.SizeBytes}}, nil)
		done()
		if e != nil {
			t.Fatal(e)
		}
		return
	}
	raw, _ := json.Marshal(map[string]any{"type": "message", "session_id": s.ID, "request_id": next.MediaBatchID, "media_batch_id": next.MediaBatchID, "content": "", "images": []any{map[string]any{"attachment_id": object, "media_type": item.MediaType, "sha256": item.SHA256, "size_bytes": item.SizeBytes}}})
	route(h, c, string(raw))
	waitForType(t, c, "message_ack")
	select {
	case <-starts:
	case <-time.After(3 * time.Second):
		t.Fatal("original fake native did not start")
	}
	if state == "admitted_effort_first_thread" {
		high := "high"
		_, _, proof := s.ApplyProviderMediaRuntimeMetadata(nil, &high, 0)
		if proof == nil {
			t.Fatal("missing private host metadata proof")
		}
		h.Emit(backend.MediaDefaultEffortObserved{Transition: proof})
	}
	if state == "admitted_first_thread" || state == "admitted_effort_first_thread" {
		s.SetResumeID("observed-handoff-thread")
		h.Emit(backend.NewSessionUUID(s.ID, s.ResumeID()))
	}
}
func TestMediaDraftHandoffLateAdmissionKeepsExactOriginalPins(t *testing.T) {
	for _, state := range []string{"prepared", "admitting_unknown", "admitted", "admitted_first_thread", "admitted_effort_first_thread"} {
		t.Run(state, func(t *testing.T) {
			h, s, c, release, next, item, object, starts := mediaHandoffPrepared(t)
			mediaAdvanceHandoff(t, h, s, c, next, item, object, state, starts)
			if got := draftRelease(t, h, c, release); got["phase"] != "committed" {
				t.Fatal("late source-compatible handoff was permanently refused", got)
			}
			if e := h.collectMediaGarbage(time.Now().Add(25 * time.Hour)); e != nil {
				t.Fatal(e)
			}
			m, e := h.unifiedUpload(s.ID, object)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = os.Stat(m.Path); e != nil {
				t.Fatal("new admitted/unknown protected reference erased", e)
			}
			done, e := h.media.begin()
			if e != nil {
				t.Fatal(e)
			}
			old := h.media.data.Batches[mediaKey(c.deviceID, jarvisTokenGeneration("mua-token"), release.MediaBatchID)]
			if old.State != "cancelled_before_admission" || old.CancelID != "cancel-handoff-old" || old.ReleasedItems[release.UploadRequestID] != release.RequestID {
				t.Fatal("old business/receipt changed", old)
			}
			done()
		})
	}
}
func TestMediaDraftHandoffLateAdmissionDoesNotAdoptArbitrarySource(t *testing.T) {
	for _, change := range []string{"model", "aba", "thread", "unwitnessed_first_thread", "retired_role", "next_frozen_source", "direct_remove"} {
		t.Run(change, func(t *testing.T) {
			h, s, c, release, next, item, object, starts := mediaHandoffPrepared(t)
			state := "admitted_effort_first_thread"
			if change == "unwitnessed_first_thread" {
				state = "admitting_unknown"
			}
			mediaAdvanceHandoff(t, h, s, c, next, item, object, state, starts)
			switch change {
			case "model":
				s.SetModel("different")
			case "aba":
				s.SetModel("different")
				s.SetModel("fixture-model")
			case "thread":
				s.SetResumeID("replacement-thread")
			case "unwitnessed_first_thread":
				s.SetResumeID("unwitnessed-thread")
			case "direct_remove":
				release.HandoffBatchID = ""
			case "retired_role":
				done, e := h.media.begin()
				if e != nil {
					t.Fatal(e)
				}
				b := h.media.data.Batches[mediaKey(c.deviceID, jarvisTokenGeneration("mua-token"), next.MediaBatchID)]
				b.RoleEpoch++
				_ = h.media.save()
				done()
			case "next_frozen_source":
				done, e := h.media.begin()
				if e != nil {
					t.Fatal(e)
				}
				b := h.media.data.Batches[mediaKey(c.deviceID, jarvisTokenGeneration("mua-token"), next.MediaBatchID)]
				b.SourceRevision++
				_ = h.media.save()
				done()
			}
			got := draftRelease(t, h, c, release)
			if got["phase"] != "rejected_known_none" {
				t.Fatal("arbitrary source adopted", got)
			}
			done, e := h.media.begin()
			if e != nil {
				t.Fatal(e)
			}
			old := h.media.data.Batches[mediaKey(c.deviceID, jarvisTokenGeneration("mua-token"), release.MediaBatchID)]
			if mediaDraftAbandonedAt(h.media, old, release.UploadRequestID) != 0 {
				t.Fatal("refused handoff retired original pin")
			}
			done()
		})
	}
}

func TestMediaDraftHandoffConcurrentAdmissionReleaseSerializes(t *testing.T) {
	for round := 0; round < 8; round++ {
		h, s, c, release, next, item, object, _ := mediaHandoffPrepared(t)
		actor, e := h.mediaActor(c)
		if e != nil {
			t.Fatal(e)
		}
		start := make(chan struct{})
		admitted := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; h.handleMediaDraftRelease(c, release) }()
		go func() {
			defer wg.Done()
			<-start
			done, e := h.media.begin()
			if e != nil {
				admitted <- e
				return
			}
			_, e = h.mediaBeginAdmission(actor, s.ID, next.MediaBatchID, next.MediaBatchID, "", []backend.ImageAttachment{{AttachmentID: object, MediaType: item.MediaType, SHA256: item.SHA256, SizeBytes: item.SizeBytes}}, nil)
			done()
			admitted <- e
		}()
		close(start)
		wg.Wait()
		if e := <-admitted; e != nil {
			t.Fatal(e)
		}
		if got := waitForType(t, c, "media_draft_release_receipt"); got["phase"] != "committed" {
			t.Fatal("serial order changed release result", round, got)
		}
		done, e := h.media.begin()
		if e != nil {
			t.Fatal(e)
		}
		newBatch := h.media.data.Batches[mediaKey(c.deviceID, actor.NamespaceGeneration, next.MediaBatchID)]
		if newBatch.State != "admitting_unknown" || !mediaGCReferences(h.media, object, "", time.Now().Add(25*time.Hour)) {
			t.Fatal("uncertain original successor lost durable pin")
		}
		done()
	}
}
