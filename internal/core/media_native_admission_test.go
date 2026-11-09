package core

import (
	"encoding/json"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
)

func TestMediaNativeConversationWithoutModelKeepsOriginalConfiguration(t *testing.T) {
	h, _, c, starts := mediaFixture(t)
	s, _ := h.registry.UpsertExternal("jl_x_media_native", "Native", h.cfg.RootDir, backend.Codex, "original-native-thread", 100)
	// mediaFixture releases its fake executor during cleanup. Closing the
	// imported session must not wait for that later cleanup callback.
	t.Cleanup(s.Close)
	before := s.SettingsSnapshot()
	if before.Model != "" || before.ConfigRevision != 0 {
		t.Fatal("fixture must use the real native import contract", before)
	}
	// Formal task intake still requires a complete configuration. Ordinary
	// attachments continue the existing native conversation without inventing one.
	if h.intakeWritePolicy(s) == nil {
		t.Fatal("formal intake unexpectedly accepts unknown configuration")
	}
	h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_capabilities", SessionID: s.ID, RequestID: "native-caps"})
	if caps := waitForType(t, c, "media_batch_receipt"); caps["available"] != true {
		t.Fatal("ordinary media was rejected for the native import's empty model", caps)
	}
	data := mediaPNG(t)
	cmd, item, body := mediaPrepareItem(t, h, s, c, "r_native_original_media", data)
	id := mediaUploadItem(t, h, s, c, cmd, item, body, data)
	raw, _ := json.Marshal(map[string]any{"type": "message", "request_id": cmd.MediaBatchID, "session_id": s.ID, "media_batch_id": cmd.MediaBatchID, "content": "", "images": []any{map[string]any{"attachment_id": id, "media_type": item.MediaType, "sha256": item.SHA256, "size_bytes": item.SizeBytes}}})
	route(h, c, string(raw))
	waitForType(t, c, "message_ack")
	select {
	case got := <-starts:
		if got.content != "" || len(got.images) != 1 || got.images[0].VerifiedPath == "" {
			t.Fatal("original native attachment did not reach the adapter", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("original native attachment never started")
	}
	after := s.SettingsSnapshot()
	if after.Model != before.Model || after.ConfigRevision != before.ConfigRevision || after.ResumeID != before.ResumeID || after.Sandbox != before.Sandbox {
		t.Fatal("media admission rewrote original native configuration", before, after)
	}
}

func TestMediaNativeUnknownModelHandoffKeepsExactPins(t *testing.T) {
	for _, state := range []string{"prepared", "admitting_unknown", "admitted"} {
		t.Run(state, func(t *testing.T) {
			h, s, c, release, next, item, object, starts := mediaHandoffPrepared(t, true)
			before := s.SettingsSnapshot()
			mediaAdvanceHandoff(t, h, s, c, next, item, object, state, starts)
			if got := draftRelease(t, h, c, release); got["phase"] != "committed" {
				t.Fatal("original unknown-model attachment handoff was refused", got)
			}
			if after := s.SettingsSnapshot(); after.Model != "" || after.ConfigRevision != before.ConfigRevision {
				t.Fatal("handoff changed configuration", after)
			}
		})
	}
}

func TestMediaNativeUnknownModelPreservesSourceAndOwnerGates(t *testing.T) {
	for _, gate := range []string{"desktop-owner", "outside-root", "wrong-thread", "wrong-config"} {
		t.Run(gate, func(t *testing.T) {
			h, s, _, _ := mediaFixture(t)
			s.SetModel("")
			s.SetResumeID("original-thread")
			revision := s.SettingsSnapshot().ConfigRevision
			thread := s.ResumeID()
			switch gate {
			case "desktop-owner":
				h.controls.MarkDesktopOwner(s.ID, s.ResumeID())
			case "outside-root":
				h.cfg.RootDir = t.TempDir()
			case "wrong-thread":
				thread = "foreign-thread"
			case "wrong-config":
				revision++
			}
			if h.mediaTarget(s.ID, thread, revision) == nil {
				t.Fatal("ordinary attachment bypassed original gate", gate)
			}
		})
	}
}
