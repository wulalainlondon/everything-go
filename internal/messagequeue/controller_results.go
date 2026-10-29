package messagequeue

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
)

// ControllerResultAdmission is server-owned metadata for the original MQ
// transaction. It is never decoded from socket input or a provider argument.
// The receipt remains after the normal terminal writer erases queue payload.
type ControllerResultAdmission struct {
	Batch       string `json:"batch_id"`
	Scope       string `json:"scope_hash"`
	Session     string `json:"session_id"`
	Request     string `json:"request_id"`
	IntentHash  string `json:"intent_hash"`
	PayloadHash string `json:"payload_hash"`
	Payload     []byte `json:"payload"`
}

func controllerResultAdmitTx(tx *sql.Tx, e Entry, hash string) error {
	if e.ControllerResult == nil {
		return nil
	}
	p := *e.ControllerResult
	if p.Batch == "" || len(p.Scope) != 64 || p.Session != e.SessionID || p.Request != e.RequestID {
		return ErrConflict
	}
	p.IntentHash = hash
	p.Payload = append([]byte{}, e.Payload...)
	sum := sha256.Sum256(p.Payload)
	p.PayloadHash = hex.EncodeToString(sum[:])
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO controller_result_native_inputs VALUES(?,?,?)`, e.SessionID, e.RequestID, b)
	return err
}
func (s *Store) ControllerResultInput(ctx context.Context, session, request string) (ControllerResultAdmission, bool, error) {
	var p ControllerResultAdmission
	var b []byte
	err := taskapi.ReadSQL(ctx, s.db).QueryRowContext(ctx, `SELECT payload FROM controller_result_native_inputs WHERE session_id=? AND request_id=?`, session, request).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return p, false, nil
	}
	if err != nil {
		return p, false, err
	}
	if err = json.Unmarshal(b, &p); err != nil {
		return p, false, err
	}
	sum := sha256.Sum256(p.Payload)
	if p.Session != session || p.Request != request || p.PayloadHash != hex.EncodeToString(sum[:]) {
		return p, false, ErrConflict
	}
	return p, true, nil
}
