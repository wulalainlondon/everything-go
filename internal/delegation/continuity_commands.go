package delegation

import (
	"context"
	"database/sql"
	"errors"
)

// The compatibility route uses the same immutable delegation owner and rounds.
// This separate journal binds caller-supplied idempotency keys across native turns.
const continuityCommandDDL = `CREATE TABLE IF NOT EXISTS delegation_continuity_commands (
 parent TEXT NOT NULL, command_key TEXT NOT NULL, intent_hash TEXT NOT NULL,
 action TEXT NOT NULL, origin_request TEXT NOT NULL, origin_call TEXT NOT NULL,
 state TEXT NOT NULL, result BLOB, error_code TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(parent,command_key));`

type ContinuityCommand struct {
	Parent, Key, Hash, Action, Request, Call, State, Error string
	Result                                                 []byte
}

func (s *Store) ContinuityCommand(ctx context.Context, parent, key string) (ContinuityCommand, bool, error) {
	var c ContinuityCommand
	err := s.db.QueryRowContext(ctx, `SELECT parent,command_key,intent_hash,action,origin_request,origin_call,state,result,error_code FROM delegation_continuity_commands WHERE parent=? AND command_key=?`, parent, key).Scan(&c.Parent, &c.Key, &c.Hash, &c.Action, &c.Request, &c.Call, &c.State, &c.Result, &c.Error)
	if errors.Is(err, sql.ErrNoRows) {
		return c, false, nil
	}
	return c, err == nil, err
}

func (s *Store) ReserveContinuityCommand(ctx context.Context, c ContinuityCommand) (bool, error) {
	r, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO delegation_continuity_commands(parent,command_key,intent_hash,action,origin_request,origin_call,state) VALUES(?,?,?,?,?,?,'reserved')`, c.Parent, c.Key, c.Hash, c.Action, c.Request, c.Call)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

func (s *Store) FinishContinuityCommand(ctx context.Context, c ContinuityCommand, state string, result []byte, code string) error {
	if state != "completed" && state != "refused" {
		return errors.New("invalid_continuity_command_state")
	}
	r, err := s.db.ExecContext(ctx, `UPDATE delegation_continuity_commands SET state=?,result=?,error_code=? WHERE parent=? AND command_key=? AND intent_hash=? AND state='reserved'`, state, result, code, c.Parent, c.Key, c.Hash)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("continuity_command_changed")
	}
	return nil
}
