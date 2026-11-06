package messagequeue

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// This journal is in the SAME writer as the eventual effective payload and
// operation receipt. A recovery decision installs an irreversible operation
// fence; an absent media or queue row by itself never proves known-none.
type preparedHumanSeal struct {
	Binding     HumanBinding   `json:"binding"`
	Operation   HumanOperation `json:"operation"`
	Proof       string         `json:"proof"`
	PayloadHash string         `json:"payload_hash"`
}

func humanPrepared(seal *HumanSeal, payload []byte) preparedHumanSeal {
	return preparedHumanSeal{seal.Binding, seal.Operation, seal.Proof, humanHash(json.RawMessage(payload))}
}
func preparedHumanTx(tx *sql.Tx, b HumanBinding, id string) (preparedHumanSeal, bool, error) {
	var raw []byte
	var value preparedHumanSeal
	err := tx.QueryRow(`SELECT body FROM input_prepared_operations WHERE device=? AND authority=? AND session=? AND id=?`, b.Device, b.Authority, b.Session, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return value, false, nil
	}
	if err != nil {
		return value, false, err
	}
	if err = json.Unmarshal(raw, &value); err != nil {
		return value, false, err
	}
	if value.Binding != b {
		return value, false, ErrConflict
	}
	return value, true, nil
}
func (s *Store) PrepareHumanSeal(seal *HumanSeal, payload []byte) error {
	if seal == nil {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	value := humanPrepared(seal, payload)
	if receipt, found, err := humanReceiptTx(tx, seal.Binding, seal.Operation.ID, humanHash(seal.Operation)); err != nil {
		return err
	} else if found {
		if receipt.Status != "committed" {
			return ErrRejected
		}
		return nil
	}
	old, found, err := preparedHumanTx(tx, seal.Binding, seal.Operation.ID)
	if err != nil {
		return err
	}
	if found {
		if humanHash(old) != humanHash(value) {
			return ErrConflict
		}
	} else {
		// Validate current original source/base/lease inside this writer as well as
		// again at commit. No state is promoted to ready during this preparation.
		if _, err = beginHumanSealTx(tx, Entry{SessionID: seal.Binding.Session, RequestID: seal.Operation.Request, HumanSeal: seal}); err != nil {
			return err
		}
		raw, _ := json.Marshal(value)
		if _, err = tx.Exec(`INSERT INTO input_prepared_operations VALUES(?,?,?,?,?)`, seal.Binding.Device, seal.Binding.Authority, seal.Binding.Session, seal.Operation.ID, raw); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func verifyPreparedHumanTx(tx *sql.Tx, e Entry) error {
	if e.HumanSeal == nil || !e.HumanSeal.Prepared {
		return nil
	}
	value, found, err := preparedHumanTx(tx, e.HumanSeal.Binding, e.HumanSeal.Operation.ID)
	if err != nil {
		return err
	}
	if !found || humanHash(value) != humanHash(humanPrepared(e.HumanSeal, e.Payload)) {
		return ErrConflict
	}
	return nil
}
func abortPreparedHumanTx(tx *sql.Tx, b HumanBinding, op HumanOperation, code string) (HumanReceipt, error) {
	if receipt, found, err := humanReceiptTx(tx, b, op.ID, humanHash(op)); err != nil || found {
		return receipt, err
	}
	value, found, err := preparedHumanTx(tx, b, op.ID)
	if err != nil {
		return HumanReceipt{}, err
	}
	if !found || humanHash(value.Operation) != humanHash(op) {
		return HumanReceipt{}, ErrConflict
	}
	if code == "" {
		return HumanReceipt{}, fmt.Errorf("abort reason required")
	}
	return humanRecordTx(tx, b, op, nil, code)
}
func (s *Store) AbortPreparedHumanSeal(b HumanBinding, op HumanOperation) (HumanReceipt, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return HumanReceipt{}, err
	}
	defer tx.Rollback()
	receipt, err := abortPreparedHumanTx(tx, b, op, "admission_interrupted")
	if err != nil {
		return receipt, err
	}
	return receipt, tx.Commit()
}

// Only the authenticated original synchronous command owner may call this on
// a definite refusal. The receipt is itself a fence in the admission writer.
func (s *Store) RejectHumanOperation(b HumanBinding, op HumanOperation, code string) (HumanReceipt, error) {
	if !validHumanBinding(b) || op.ID == "" || op.Request == "" || code == "" {
		return HumanReceipt{}, ErrConflict
	}
	tx, err := s.db.Begin()
	if err != nil {
		return HumanReceipt{}, err
	}
	defer tx.Rollback()
	if receipt, found, err := humanReceiptTx(tx, b, op.ID, humanHash(op)); err != nil || found {
		return receipt, err
	}
	if prepared, found, err := preparedHumanTx(tx, b, op.ID); err != nil {
		return HumanReceipt{}, err
	} else if found && humanHash(prepared.Operation) != humanHash(op) {
		return HumanReceipt{}, ErrConflict
	}
	receipt, err := humanRecordTx(tx, b, op, nil, code)
	if err != nil {
		return receipt, err
	}
	return receipt, tx.Commit()
}
func (s *Store) RecoverPreparedHumanSeals() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT body FROM input_prepared_operations`)
	if err != nil {
		return err
	}
	var values []preparedHumanSeal
	for rows.Next() {
		var raw []byte
		var value preparedHumanSeal
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &value)
		}
		if err != nil {
			rows.Close()
			return err
		}
		values = append(values, value)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, value := range values {
		if _, err = abortPreparedHumanTx(tx, value.Binding, value.Operation, "admission_recovered_without_commit"); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func commitHumanAdmission(tx *sql.Tx, e Entry) error {
	commit := func() error {
		if e.HumanSeal != nil && e.HumanSeal.Operation.Kind == "commit_edit" && e.HumanSeal.Clock != nil {
			boot, now := e.HumanSeal.Clock()
			if boot != e.HumanSeal.Boot || e.HumanSeal.leaseExpires == 0 || now.UnixMilli() >= e.HumanSeal.leaseExpires {
				return fmt.Errorf("edit_expired")
			}
		}
		return tx.Commit()
	}
	if e.HumanSeal != nil && e.HumanSeal.Commit != nil {
		return e.HumanSeal.Commit(commit)
	}
	return commit()
}
