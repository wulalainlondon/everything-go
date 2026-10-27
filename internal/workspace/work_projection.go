package workspace

import (
	"context"
	"database/sql"
	"errors"
)

// WorkProjectionState is host-only metadata for the existing WorkItem service.
// It is not a wire action or a way to retrieve task/source text as a guest.
func (s *Store) WorkProjectionState(ctx context.Context, roomID, taskID string) (Room, Task, error) {
	r, err := roomByID(ctx, s.db, roomID)
	if err != nil {
		return r, Task{}, err
	}
	e, err := getEntity(ctx, s.db, roomID, taskID)
	if err != nil || e.Kind != "task" {
		return r, Task{}, errors.New("task_unavailable")
	}
	t, err := bodyOf[Task](e)
	return r, t, err
}

// Notification joins need to distinguish a proven absent/revoked read grant
// from storage failure; ordinary presentation helpers intentionally redact both.
// This uses the original owner validator, membership and task tables only.
func (s *Store) PairedOwnerTaskReadState(ctx context.Context, a Actor, roomID, taskID string) (Task, bool, error) {
	if !a.Owner {
		return Task{}, false, nil
	}
	if e := s.actorOK(ctx, s.db, a); e != nil {
		return Task{}, false, nil
	}
	var id, role string
	e := s.db.QueryRowContext(ctx, `SELECT id FROM rooms WHERE id=?`, roomID).Scan(&id)
	if errors.Is(e, sql.ErrNoRows) {
		return Task{}, false, nil
	}
	if e != nil {
		return Task{}, false, e
	}
	e = s.db.QueryRowContext(ctx, `SELECT role FROM members WHERE room_id=? AND principal_id=? AND active=1`, roomID, a.ID).Scan(&role)
	if errors.Is(e, sql.ErrNoRows) {
		return Task{}, false, nil
	}
	if e != nil {
		return Task{}, false, e
	}
	if !permits(role, "read") {
		return Task{}, false, nil
	}
	entity, e := scanEntity(s.db.QueryRowContext(ctx, `SELECT `+entityColumns+` FROM entities WHERE room_id=? AND id=?`, roomID, taskID))
	if errors.Is(e, sql.ErrNoRows) {
		return Task{}, false, nil
	}
	if e != nil {
		return Task{}, false, e
	}
	if entity.Kind != "task" {
		return Task{}, false, nil
	}
	task, e := bodyOf[Task](entity)
	return task, e == nil, e
}
