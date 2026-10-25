package workspace

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"strings"
)

// Avatar is a presentation-only, room-scoped identity. It never updates an
// agent binding/role revision or changes the authenticated principal or ACL.
// The existing entity/receipt path makes old clients wire-compatible and keeps
// CAS, authorization, immutable revisions and reconnect events in one place.
type Avatar struct {
	SubjectKind string `json:"subject_kind"`
	SubjectID   string `json:"subject_id"`
	PresetID    string `json:"preset_id"`
	ImageData   string `json:"image_data"`
}

const MaxAvatarBytes = 48 * 1024

func validateAvatar(v Avatar) error {
	if v.PresetID != "" {
		if v.ImageData != "" {
			return errors.New("avatar_invalid")
		}
		if v.PresetID == "initial" {
			return nil
		}
		prefix := "a-human-"
		if v.SubjectKind == "agent" {
			prefix = "a-agent-"
		}
		if len(v.PresetID) != len(prefix)+1 || !strings.HasPrefix(v.PresetID, prefix) || v.PresetID[len(prefix)] < '1' || v.PresetID[len(prefix)] > '8' {
			return errors.New("avatar_preset_invalid")
		}
		return nil
	}
	if len(v.ImageData) > MaxAvatarBytes*4/3+32 {
		return errors.New("avatar_image_limit")
	}
	format := "jpeg"
	prefix := "data:image/jpeg;base64,"
	if strings.HasPrefix(v.ImageData, "data:image/png;base64,") {
		format = "png"
		prefix = "data:image/png;base64,"
	}
	if !strings.HasPrefix(v.ImageData, prefix) {
		return errors.New("avatar_image_invalid")
	}
	b, err := base64.StdEncoding.Strict().DecodeString(strings.TrimPrefix(v.ImageData, prefix))
	if err != nil || len(b) == 0 || len(b) > MaxAvatarBytes {
		return errors.New("avatar_image_limit")
	}
	config, actual, err := image.DecodeConfig(bytes.NewReader(b))
	if err != nil || actual != format || config.Width < 1 || config.Height != config.Width || config.Width > 512 {
		return errors.New("avatar_image_invalid")
	}
	if _, _, err = image.Decode(bytes.NewReader(b)); err != nil {
		return errors.New("avatar_image_invalid")
	}
	return nil
}

func (s *Store) setAvatar(ctx context.Context, tx *sql.Tx, a Actor, r Room, c Command) (Result, error) {
	_, role, err := s.access(ctx, tx, a, r.ID, "read")
	if err != nil {
		return Result{}, err
	}
	if r.Archived || (r.ExpiresAt > 0 && s.now().UnixMilli() >= r.ExpiresAt) {
		return Result{}, errors.New("room_archived")
	}
	var v Avatar
	if err = decode(c.Data, &v); err != nil {
		return Result{}, err
	}
	switch v.SubjectKind {
	case "member":
		// Even admins cannot impersonate another member's presentation.
		if v.SubjectID != a.ID {
			return Result{}, errors.New("avatar_self_required")
		}
	case "agent":
		if !permits(role, "manage") {
			return Result{}, errors.New("room_permission_denied")
		}
		agent, err := getEntity(ctx, tx, r.ID, v.SubjectID)
		if err != nil || agent.Kind != "agent" {
			return Result{}, errors.New("avatar_subject_unavailable")
		}
	default:
		return Result{}, errors.New("avatar_subject_invalid")
	}
	if err = validateAvatar(v); err != nil {
		return Result{}, err
	}
	id := "cw_avatar_" + digest([]byte(r.ID + "\x00" + v.SubjectKind + "\x00" + v.SubjectID))[:40]
	if c.EntityID != "" && c.EntityID != id {
		return Result{}, errors.New("avatar_subject_mismatch")
	}
	e, err := scanEntity(tx.QueryRowContext(ctx, "SELECT "+entityColumns+" FROM entities WHERE room_id=? AND id=?", r.ID, id))
	if errors.Is(err, sql.ErrNoRows) {
		// Creation also requires an explicit base revision, so two devices
		// cannot silently overwrite each other's first selection.
		if c.ExpectedRevision == nil || *c.ExpectedRevision != 0 {
			return Result{}, errors.New("revision_conflict")
		}
		e = Entity{ID: id, Kind: "avatar"}
	} else if err != nil {
		return Result{}, err
	} else if err = cas(c, e); err != nil {
		return Result{}, err
	}
	e, err = s.put(ctx, tx, a, r, e, v)
	return withEntity(Result{}, e, err)
}
