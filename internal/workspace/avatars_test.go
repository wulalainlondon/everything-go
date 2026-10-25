package workspace

import (
	"bytes"
	"encoding/base64"
	"image"
	"image/color"
	"image/png"
	"path/filepath"
	"strings"
	"testing"
)

func avatarCommand(f *fixture, kind, id, preset string, revision uint64) Command {
	return Command{Action: "set_avatar", RoomID: f.room.ID, ExpectedRevision: ptr(revision), Data: raw(Avatar{SubjectKind: kind, SubjectID: id, PresetID: preset})}
}
func TestAvatarIdentityCASReceiptAndPersistence(t *testing.T) {
	f := setup(t)
	command := f.command(avatarCommand(f, "member", f.owner.ID, "a-human-2", 0))
	first, err := f.s.Apply(f.ctx, f.owner, command)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.s.Apply(f.ctx, f.owner, command)
	if err != nil || second.Entity.Hash != first.Entity.Hash {
		t.Fatal("receipt replay changed result", err)
	}
	otherDevice := f.s.Owner("another-original-device", strings.Repeat("b", 64))
	snapshot := f.call(otherDevice, Command{Action: "snapshot", RoomID: f.room.ID}).Snapshot
	if len(snapshot.Data["avatar"].Items) != 1 || snapshot.Data["avatar"].Items[0].ID != first.Entity.ID {
		t.Fatal("same principal device did not see original profile")
	}
	f.reject(otherDevice, avatarCommand(f, "member", f.owner.ID, "a-human-3", 0), "revision_conflict")
	updated := f.call(otherDevice, avatarCommand(f, "member", f.owner.ID, "initial", 1)).Entity
	if updated.ID != first.Entity.ID || updated.Revision != 2 {
		t.Fatal("identity or revision changed")
	}
	dir := filepath.Dir(f.s.root)
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, "fixture-authority")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	result, err := reopened.Apply(f.ctx, otherDevice, Command{Authority: "fixture-authority", Action: "snapshot", RoomID: f.room.ID})
	if err != nil || result.Snapshot.Data["avatar"].Items[0].Hash != updated.Hash {
		t.Fatal("profile not durable", err)
	}
}
func TestAvatarSelfPermissionAndRoomIsolation(t *testing.T) {
	f := setup(t)
	viewer, _ := f.invite("viewer")
	other, _ := f.invite("viewer")
	self := f.call(viewer, avatarCommand(f, "member", viewer.ID, "a-human-1", 0)).Entity
	f.reject(viewer, avatarCommand(f, "member", other.ID, "a-human-1", 0), "avatar_self_required")
	f.reject(f.owner, avatarCommand(f, "member", viewer.ID, "a-human-2", 1), "avatar_self_required")
	agent := f.agent("same display name", "synthetic-avatar-session", true)
	f.reject(viewer, avatarCommand(f, "agent", agent.ID, "a-agent-1", 0), "room_permission_denied")
	before := agent.Hash
	f.call(f.owner, avatarCommand(f, "agent", agent.ID, "a-agent-1", 0))
	after := f.call(f.owner, Command{Action: "get", RoomID: f.room.ID, EntityID: agent.ID}).Entity
	if before != after.Hash || after.Revision != agent.Revision {
		t.Fatal("avatar changed native role")
	}
	private := f.call(f.owner, Command{Action: "create_room", WorkspaceID: f.workspace.ID, Data: raw(map[string]any{"name": "private"})}).Room
	f.reject(viewer, Command{Action: "get", RoomID: private.ID, EntityID: self.ID}, "room_permission_denied")
	f.reject(f.owner, Command{Action: "set_avatar", RoomID: private.ID, ExpectedRevision: ptr(1), EntityID: self.ID, Data: raw(Avatar{SubjectKind: "member", SubjectID: f.owner.ID, PresetID: "initial"})}, "avatar_subject_mismatch")
	for _, preset := range []string{"a-agent-1", "a-human-9", "https://example.invalid/avatar", "../secret"} {
		f.reject(viewer, avatarCommand(f, "member", viewer.ID, preset, 1), "avatar_preset_invalid")
	}
	// Revoked members cannot read avatars or replay their original receipts.
	f.call(f.owner, Command{Action: "update_member", RoomID: f.room.ID, ExpectedRevision: ptr(1), Data: raw(map[string]any{"principal_id": viewer.ID, "role": "viewer", "active": false})})
	f.reject(viewer, Command{Action: "snapshot", RoomID: f.room.ID}, "room_permission_denied")
	f.reject(viewer, avatarCommand(f, "member", viewer.ID, "initial", 1), "room_permission_denied")
}
func TestAvatarImageValidation(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 32, 32))
	img.Set(3, 3, color.RGBA{R: 255, A: 255})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	good := "data:image/png;base64," + base64.StdEncoding.EncodeToString(buf.Bytes())
	f := setup(t)
	c := avatarCommand(f, "member", f.owner.ID, "", 0)
	c.Data = raw(Avatar{SubjectKind: "member", SubjectID: f.owner.ID, ImageData: good})
	f.call(f.owner, c)
	for _, imageData := range []string{"data:image/svg+xml;base64,PHN2Zz4=", "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(buf.Bytes()), "https://example.invalid/x", good[:len(good)-7], "data:image/png;base64," + strings.Repeat("A", MaxAvatarBytes*2)} {
		c.Data = raw(Avatar{SubjectKind: "member", SubjectID: f.owner.ID, ImageData: imageData})
		c.ExpectedRevision = ptr(1)
		_, err := f.s.Apply(f.ctx, f.owner, f.command(c))
		if err == nil {
			t.Fatal("accepted invalid image")
		}
	}
	var oversized bytes.Buffer
	_ = png.Encode(&oversized, image.NewRGBA(image.Rect(0, 0, 513, 513)))
	if err := validateAvatar(Avatar{SubjectKind: "member", ImageData: "data:image/png;base64," + base64.StdEncoding.EncodeToString(oversized.Bytes())}); err == nil {
		t.Fatal("oversized dimensions accepted")
	}
}

func TestAvatarAdminAndArchivedBoundaries(t *testing.T) {
	f := setup(t)
	admin, _ := f.invite("admin")
	agent := f.agent("Synthetic", "qa-admin-avatar", true)
	f.call(admin, avatarCommand(f, "agent", agent.ID, "a-agent-2", 0))
	f.reject(admin, avatarCommand(f, "member", f.owner.ID, "a-human-3", 0), "avatar_self_required")
	current := f.call(f.owner, Command{Action: "snapshot", RoomID: f.room.ID}).Snapshot.Room
	r := f.call(f.owner, Command{Action: "archive_room", RoomID: f.room.ID, ExpectedRevision: ptr(current.Revision), Data: raw(map[string]any{"archived": true})}).Room
	if r == nil || !r.Archived {
		t.Fatal("fixture archive failed")
	}
	f.reject(admin, avatarCommand(f, "member", admin.ID, "a-human-4", 0), "room_archived")
	f.reject(f.owner, avatarCommand(f, "agent", agent.ID, "a-agent-3", 1), "room_archived")
}
