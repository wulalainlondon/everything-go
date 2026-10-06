package messagequeue

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
)

func (s *Store) RecordNativeAcceptance(session, request, thread, turn string) error {
	if session == "" || request == "" || thread == "" || turn == "" {
		return errors.New("invalid native acceptance")
	}
	_, err := s.db.Exec(`INSERT INTO task_native_acceptance(session_id,request_id,thread_id,turn_id) VALUES(?,?,?,?) ON CONFLICT(session_id,request_id) DO NOTHING`, session, request, thread, turn)
	return err
}
func (s *Store) NativeAcceptance(session, request, thread string) string {
	var turn string
	_ = s.db.QueryRow(`SELECT turn_id FROM task_native_acceptance WHERE session_id=? AND request_id=? AND thread_id=?`, session, request, thread).Scan(&turn)
	return turn
}

// OriginEntries queries typed admission payloads only, never runtime directories.
func (s *Store) OriginEntries(instance, parent string) ([]Entry, error) {
	rows, err := s.db.Query("SELECT q."+strings.ReplaceAll(columns, ",", ",q.")+` FROM queue_commands q JOIN task_admissions a ON q.session_id=a.session_id AND q.request_id=a.request_id WHERE a.origin_instance=? AND a.origin_session=? ORDER BY q.seq DESC LIMIT 100`, instance, parent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Entry{}
	for rows.Next() {
		e, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Admission survives terminal payload/attachment clearing in the same enqueue
// transaction. No attachment bytes or credentials enter this typed relation.
func saveTaskAdmission(tx *sql.Tx, e Entry) error {
	var raw struct {
		OwnerDevice string          `json:"owner_device"`
		Origin      json.RawMessage `json:"task_origin"`
		Target      json.RawMessage `json:"expected_target"`
		Content     string          `json:"content"`
	}
	if json.Unmarshal(e.Payload, &raw) != nil || raw.OwnerDevice == "" && len(raw.Origin) == 0 {
		return nil
	}
	var origin struct {
		InstanceID string `json:"instance_id"`
		SessionID  string `json:"session_id"`
	}
	if len(raw.Origin) > 0 {
		if err := json.Unmarshal(raw.Origin, &origin); err != nil {
			return err
		}
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO task_admissions(session_id,request_id,origin_instance,origin_session,metadata) VALUES(?,?,?,?,?)`, e.SessionID, e.RequestID, origin.InstanceID, origin.SessionID, data)
	return err
}
func (s *Store) TaskAdmission(session, request string) ([]byte, bool, error) {
	var data []byte
	err := s.db.QueryRow(`SELECT metadata FROM task_admissions WHERE session_id=? AND request_id=?`, session, request).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	return data, err == nil, err
}
