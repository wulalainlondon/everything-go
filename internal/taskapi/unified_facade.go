package taskapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

const facadeDDL = `CREATE TABLE IF NOT EXISTS unified_facade_pointers(scope TEXT NOT NULL,generation INTEGER NOT NULL,operation TEXT NOT NULL,intent TEXT NOT NULL,input_hash TEXT NOT NULL,original_request BLOB NOT NULL,original_locator BLOB NOT NULL,receipt BLOB,PRIMARY KEY(scope,generation,operation,intent));`

type FacadePointer struct {
	Hash            string
	OriginalRequest json.RawMessage
	Locator         Locator
	Receipt         *MutationReceipt
}

func (j *Journal) UnifiedFacadeRead(ctx context.Context, m QueueMutation) (FacadePointer, bool, error) {
	var p FacadePointer
	var l, b []byte
	e := j.db.QueryRowContext(ctx, "SELECT input_hash,original_request,original_locator,receipt FROM unified_facade_pointers WHERE scope=? AND generation=? AND operation=? AND intent=?", m.Scope, m.Generation, m.Operation, m.Intent).Scan(&p.Hash, &p.OriginalRequest, &l, &b)
	if errors.Is(e, sql.ErrNoRows) {
		return p, false, nil
	}
	if e != nil {
		return p, false, e
	}
	if m.Hash != "" && p.Hash != m.Hash {
		return p, true, Failure("idempotency_conflict", "known_receipt", "lookup_original")
	}
	if e = json.Unmarshal(l, &p.Locator); e != nil {
		return p, true, e
	}
	if len(b) > 0 {
		var r MutationReceipt
		if e = json.Unmarshal(b, &r); e != nil {
			return p, true, e
		}
		p.Receipt = &r
	}
	return p, true, nil
}

// This is solely an original-journal pointer. It owns no execution state,
// scheduling, cancellation or delivery. Pending pointers are NEVER re-executed.
func (j *Journal) UnifiedFacadeClaim(ctx context.Context, m QueueMutation, original []byte, l Locator) (bool, error) {
	fresh := false
	e := j.Transaction(ctx, func(tx *sql.Tx) error {
		if m.Revalidate == nil {
			return Failure("caller_unbound", "known_none", "refresh_identity")
		}
		if e := m.Revalidate(ctx); e != nil {
			return e
		}
		b, _ := json.Marshal(l)
		r, e := tx.ExecContext(ctx, "INSERT OR IGNORE INTO unified_facade_pointers(scope,generation,operation,intent,input_hash,original_request,original_locator) VALUES(?,?,?,?,?,?,?)", m.Scope, m.Generation, m.Operation, m.Intent, m.Hash, original, b)
		if e != nil {
			return e
		}
		n, e := r.RowsAffected()
		fresh = n == 1
		if e != nil {
			return e
		}
		return m.Revalidate(ctx)
	})
	return fresh, e
}
func (j *Journal) UnifiedFacadeFinish(ctx context.Context, m QueueMutation, response any, l Locator) (MutationReceipt, error) {
	data, e := json.Marshal(map[string]any{"adapter_response": response, "original_receipt_locator": l})
	if e != nil {
		return MutationReceipt{}, e
	}
	state := "applied"
	if original, ok := response.(Response); ok && !original.OK {
		state = "rejected"
	}
	r := MutationReceipt{m.Intent, m.Hash, state, data}
	raw, _ := json.Marshal(r)
	e = j.Transaction(ctx, func(tx *sql.Tx) error {
		if e := m.Revalidate(ctx); e != nil {
			return e
		}
		_, e := tx.ExecContext(ctx, "UPDATE unified_facade_pointers SET receipt=COALESCE(receipt,?) WHERE scope=? AND generation=? AND operation=? AND intent=? AND input_hash=?", raw, m.Scope, m.Generation, m.Operation, m.Intent, m.Hash)
		return e
	})
	return r, e
}
