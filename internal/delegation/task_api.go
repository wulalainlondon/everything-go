package delegation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
	"time"
)

func (s *Store) TaskJournal() *taskapi.Journal { return taskapi.AttachedJournal(s.db, "delegation") }

func (s *Store) CreateAPI(ctx context.Context, c taskapi.AuthorizedCommand, intent taskapi.IntentRecord, r Record, backend string) (taskapi.IntentRecord, bool, error) {
	var result taskapi.IntentRecord
	created := false
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		var err error
		result, created, err = s.TaskJournal().ClaimTx(tx, c, intent)
		if err != nil || !created {
			return err
		}
		var count int
		if err = tx.QueryRow("SELECT COUNT(*) FROM session_delegations WHERE parent_session_id=? AND (state!='terminal' OR delivery_state IN ('pending','queued'))", r.ParentSessionID).Scan(&count); err != nil {
			return err
		}
		if count >= 3 {
			return taskapi.Failure("busy", "known_none", "wait")
		}
		now := time.Now().UnixMilli()
		_, err = tx.Exec(`INSERT INTO session_delegations(id,parent_session_id,origin_request_id,tool_call_id,intent_hash,child_session_id,child_request_id,child_name,cwd,instruction,model,effort,sandbox,parent_request_id,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'provisioning',?,?)`, r.ID, r.ParentSessionID, r.OriginRequestID, r.ToolCallID, r.IntentHash, r.ChildSessionID, r.ChildRequestID, r.ChildName, r.Cwd, r.Instruction, r.Model, r.Effort, r.Sandbox, r.ParentRequestID, now, now)
		if err != nil {
			return err
		}
		data, _ := json.Marshal(map[string]any{"backend": backend, "metadata": json.RawMessage(intent.Metadata)})
		if _, err = tx.Exec("INSERT INTO task_api_child_profiles VALUES(?,?)", r.ChildSessionID, data); err != nil {
			return err
		}
		return taskapi.ChangeTx(tx, r.ChildSessionID, r.ChildRequestID, "admission")
	})
	return result, created, err
}
func (s *Store) APIBackend(ctx context.Context, child string) (string, bool, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT profile FROM task_api_child_profiles WHERE session_id=?", child).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	var data struct{ Backend string }
	err = json.Unmarshal(raw, &data)
	return data.Backend, true, err
}

func (s *Store) APIChildMetadata(ctx context.Context, child string) ([]byte, bool, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT profile FROM task_api_child_profiles WHERE session_id=?", child).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var profile struct{ Metadata json.RawMessage }
	err = json.Unmarshal(raw, &profile)
	return profile.Metadata, true, err
}
