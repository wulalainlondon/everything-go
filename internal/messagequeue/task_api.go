package messagequeue

import (
	"context"
	"database/sql"
	"errors"
	"everything-go/internal/taskapi"
)

func (s *Store) TaskJournal() *taskapi.Journal { return taskapi.AttachedJournal(s.db, "ordinary") }

func (s *Store) CancelOrigin(session, request string) (State, error) {
	return s.CancelOriginContext(context.Background(), session, request)
}
func (s *Store) CancelOriginContext(ctx context.Context, session, request string) (State, error) {
	var state string
	err := taskapi.ReadSQL(ctx, s.db).QueryRowContext(ctx, "SELECT from_state FROM task_cancel_origins WHERE session_id=? AND request_id=?", session, request).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return State(state), err
}
