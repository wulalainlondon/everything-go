package messagequeue

import (
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/protocol"
)

type QuestionBatch struct {
	Request         protocol.AsyncBatchRequest `json:"request"`
	Hash            string                     `json:"hash"`
	Phase           string                     `json:"phase"`
	Error           string                     `json:"error,omitempty"`
	OwnerGeneration uint64                     `json:"owner_generation"`
	Owner           string                     `json:"owner"`
	ContentHash     string                     `json:"content_hash,omitempty"`
}
type QuestionBatchAdmission struct {
	CanCommit func() error

	Hash, ContentHash string
	Covered           []protocol.AsyncCoveredQuestion
}

func installQuestionBatches(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS question_batches(session_id TEXT NOT NULL,request_id TEXT NOT NULL,hash TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(session_id,request_id));
 CREATE TABLE IF NOT EXISTS question_batch_covers(session_id TEXT NOT NULL,question_id TEXT NOT NULL,revision TEXT NOT NULL,batch_id TEXT NOT NULL,PRIMARY KEY(session_id,question_id,revision));
 CREATE INDEX IF NOT EXISTS question_batch_native ON question_batches(session_id,json_extract(payload,'$.phase'),json_extract(payload,'$.request.expected_native_thread_id'),json_extract(payload,'$.content_hash'));
 CREATE TABLE IF NOT EXISTS question_projection_clock(id INTEGER PRIMARY KEY CHECK(id=1),revision INTEGER NOT NULL); INSERT OR IGNORE INTO question_projection_clock VALUES(1,0);`)
	return err
}
func (s *Store) NextQuestionRevision() (uint64, error) {
	var v uint64
	err := s.db.QueryRow(`UPDATE question_projection_clock SET revision=revision+1 WHERE id=1 AND revision<9007199254740991 RETURNING revision`).Scan(&v)
	return v, err
}
func (s *Store) QuestionBatch(sid, id string) (QuestionBatch, bool, error) {
	var v QuestionBatch
	var raw []byte
	err := s.db.QueryRow(`SELECT payload FROM question_batches WHERE session_id=? AND request_id=?`, sid, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return v, false, nil
	}
	if err != nil {
		return v, false, err
	}
	err = json.Unmarshal(raw, &v)
	return v, err == nil, err
}

// Claim is an immutable original intent, including before-effect refusals. A
// crash before queue admission leaves unknown; this original is never replayed.
func (s *Store) ClaimQuestionBatch(v QuestionBatch) (QuestionBatch, bool, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return v, false, err
	}
	result, err := s.db.Exec(`INSERT OR IGNORE INTO question_batches VALUES(?,?,?,?)`, v.Request.SessionID, v.Request.RequestID, v.Hash, raw)
	if err != nil {
		return v, false, err
	}
	n, _ := result.RowsAffected()
	old, _, err := s.QuestionBatch(v.Request.SessionID, v.Request.RequestID)
	if err == nil && old.Hash != v.Hash {
		err = ErrConflict
	}
	return old, n == 1, err
}
func (s *Store) RejectQuestionBatch(sid, id, hash, code string) error {
	v, found, err := s.QuestionBatch(sid, id)
	if err != nil || !found {
		return err
	}
	if v.Hash != hash || v.Phase != "reserved" {
		return ErrConflict
	}
	v.Phase = "rejected"
	v.Error = code
	raw, _ := json.Marshal(v)
	result, err := s.db.Exec(`UPDATE question_batches SET payload=? WHERE session_id=? AND request_id=? AND hash=? AND json_extract(payload,'$.phase')='reserved' AND NOT EXISTS(SELECT 1 FROM queue_commands WHERE session_id=? AND request_id=?)`, raw, sid, id, hash, sid, id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return ErrConflict
	}
	return nil
}
func admitQuestionBatchTx(tx *sql.Tx, e Entry) error {
	if e.QuestionBatch != nil && e.QuestionBatch.CanCommit != nil {
		if err := e.QuestionBatch.CanCommit(); err != nil {
			return err
		}
	}
	if e.QuestionBatch == nil {
		var n int
		err := tx.QueryRow(`SELECT COUNT(*) FROM question_batch_covers WHERE session_id=? AND question_id=?`, e.SessionID, e.RequestID).Scan(&n)
		if err != nil {
			return err
		}
		if n > 0 {
			return ErrConflict
		}
		return nil
	}
	var raw []byte
	if err := tx.QueryRow(`SELECT payload FROM question_batches WHERE session_id=? AND request_id=? AND hash=?`, e.SessionID, e.RequestID, e.QuestionBatch.Hash).Scan(&raw); err != nil {
		return err
	}
	var v QuestionBatch
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	if v.Phase != "reserved" || len(v.Request.Selected) == 0 || len(v.Request.Selected) != len(e.QuestionBatch.Covered) {
		return ErrConflict
	}
	for i, q := range e.QuestionBatch.Covered {
		if q.RequestID != v.Request.Selected[i].RequestID || q.QuestionRevision != v.Request.Selected[i].QuestionRevision {
			return ErrConflict
		}
		var n int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM queue_commands WHERE session_id=? AND request_id=?`, e.SessionID, q.RequestID).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return ErrConflict
		}
		if _, err := tx.Exec(`INSERT INTO question_batch_covers VALUES(?,?,?,?)`, e.SessionID, q.RequestID, q.QuestionRevision, e.RequestID); err != nil {
			return ErrConflict
		}
	}
	v.Phase = "admitted"
	v.ContentHash = e.QuestionBatch.ContentHash
	raw, _ = json.Marshal(v)
	_, err := tx.Exec(`UPDATE question_batches SET payload=? WHERE session_id=? AND request_id=?`, raw, e.SessionID, e.RequestID)
	return err
}
func (s *Store) ConsumeQuestionBatch(sid, thread, contentHash string) error {
	_, err := s.db.Exec(`UPDATE question_batches SET payload=json_set(payload,'$.phase','consumed') WHERE session_id=? AND json_extract(payload,'$.phase')='admitted' AND json_extract(payload,'$.request.expected_native_thread_id')=? AND json_extract(payload,'$.content_hash')=?`, sid, thread, contentHash)
	return err
}
func (s *Store) QuestionCoverage(sid, id, revision string) (QuestionBatch, bool, error) {
	var batch string
	err := s.db.QueryRow(`SELECT batch_id FROM question_batch_covers WHERE session_id=? AND question_id=? AND revision=?`, sid, id, revision).Scan(&batch)
	if errors.Is(err, sql.ErrNoRows) {
		return QuestionBatch{}, false, nil
	}
	if err != nil {
		return QuestionBatch{}, false, err
	}
	return s.QuestionBatch(sid, batch)
}
