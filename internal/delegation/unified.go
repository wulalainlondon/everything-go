package delegation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
)

func unifiedQueueRows(ctx context.Context, tx *sql.Tx, eligible map[string]bool) ([]taskapi.QueueRow, error) {
	rows, e := tx.QueryContext(ctx, `SELECT rowid,id,parent_session_id,child_session_id,child_request_id,state,intent_hash FROM session_delegations WHERE state IN ('queued','provisioning','running','collecting') ORDER BY rowid`)
	if e != nil {
		return nil, e
	}
	out := []taskapi.QueueRow{}
	for rows.Next() {
		var r taskapi.QueueRow
		if e = rows.Scan(&r.Sequence, &r.ID, &r.Parent, &r.Session, &r.Request, &r.State, &r.ContentHash); e != nil {
			rows.Close()
			return nil, e
		}
		r.Eligible = eligible[r.ID] && r.State == "queued"
		r.ContentRevision = 1
		if !r.Eligible {
			r.Barrier = "original_owner_capacity_or_native_boundary"
		}
		out = append(out, r)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	for i := range out {
		var v uint64
		var h string
		e = tx.QueryRowContext(ctx, "SELECT version,hash FROM unified_input_versions WHERE queue_key='waiting' AND item_id=? ORDER BY version DESC LIMIT 1", out[i].ID).Scan(&v, &h)
		if e == nil {
			out[i].ContentRevision = v
			out[i].ContentHash = h
		} else if !errors.Is(e, sql.ErrNoRows) {
			return nil, e
		}
	}
	e = taskapi.LoadRanks(ctx, tx, "waiting", out)
	return out, e
}
func (s *Store) UnifiedQueue(ctx context.Context, eligible map[string]bool) (taskapi.QueueView, error) {
	var v taskapi.QueueView
	e := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		r, e := unifiedQueueRows(ctx, tx, eligible)
		if e != nil {
			return e
		}
		v = taskapi.QueueFingerprint(r)
		v.Revision = taskapi.ViewRevision(v)
		return nil
	})
	return v, e
}
func (s *Store) UnifiedOperation(ctx context.Context, m taskapi.QueueMutation) (taskapi.MutationReceipt, bool, error) {
	var r taskapi.MutationReceipt
	var found bool
	e := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error { var e error; r, found, e = taskapi.OriginalOperation(ctx, tx, m); return e })
	return r, found, e
}
func (s *Store) UnifiedMutate(ctx context.Context, eligible map[string]bool, m taskapi.QueueMutation) (taskapi.MutationReceipt, error) {
	var receipt taskapi.MutationReceipt
	e := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		if m.Revalidate == nil {
			return taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
		}
		if e := m.Revalidate(ctx); e != nil {
			return e
		}
		r, found, e := taskapi.OriginalOperation(ctx, tx, m)
		if e != nil {
			return e
		}
		if found {
			receipt = r
			return nil
		}
		rows, e := unifiedQueueRows(ctx, tx, eligible)
		if e != nil {
			return e
		}
		v, e := taskapi.CheckQueueMutation(ctx, tx, "waiting", rows, m)
		if e != nil {
			return e
		}
		var data any
		if m.Operation == "queue_move" {
			ids, e := taskapi.MoveRanks(ctx, tx, "waiting", v, m)
			if e != nil {
				return e
			}
			data = map[string]any{"order": taskapi.PublicQueueOrder(ids, m), "previous_revision": v.Revision}
		} else if m.Operation == "queue_edit" {
			var selected *taskapi.QueueRow
			for i := range rows {
				if rows[i].ID == m.Item {
					selected = &rows[i]
				}
			}
			if selected == nil || selected.State != "queued" {
				return taskapi.Failure("native_boundary_crossed", "known_none", "use_continuation")
			}
			if selected.ContentRevision != m.ExpectedContentRevision || selected.ContentHash != m.ExpectedHash {
				return taskapi.Failure("stale_revision", "known_none", "refresh_snapshot")
			}
			var original, hash string
			if e = tx.QueryRowContext(ctx, "SELECT instruction,intent_hash FROM session_delegations WHERE id=?", m.Item).Scan(&original, &hash); e != nil {
				return e
			}
			if selected.ContentRevision == 1 {
				if _, e = tx.ExecContext(ctx, "INSERT INTO unified_input_versions VALUES('waiting',?,?,?,?,?,?)", m.Item, 1, []byte(original), original, hash, hash); e != nil {
					return e
				}
			}
			version := selected.ContentRevision + 1
			effectivePayload := []byte(m.Content)
			if len(m.Payload) > 0 {
				effectivePayload = m.Payload
			}
			h := taskapi.RawDigest(effectivePayload)
			if _, e = tx.ExecContext(ctx, "INSERT INTO unified_input_versions VALUES('waiting',?,?,?,?,?,?)", m.Item, version, effectivePayload, m.Content, h, hash); e != nil {
				return e
			}
			if len(m.Binding) > 0 {
				if _, e = tx.ExecContext(ctx, "INSERT INTO unified_version_bindings VALUES('waiting',?,?,?)", m.Item, version, m.Binding); e != nil {
					return e
				}
			}
			data = map[string]any{"content_revision": version, "content_hash": h, "original_hash": hash}
		} else {
			return taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
		if e = m.Revalidate(ctx); e != nil {
			return e
		}
		for _, row := range v.Items {
			if err := taskapi.ChangeTx(tx, row.Session, row.Request, "unified_input_or_order"); err != nil {
				return err
			}
		}
		receipt, e = taskapi.SaveOperation(ctx, tx, m, data)
		return e
	})
	return receipt, e
}
func (s *Store) EffectiveInstruction(ctx context.Context, id, original string) (string, error) {
	var text string
	e := s.db.QueryRowContext(ctx, "SELECT content FROM unified_input_versions WHERE queue_key='waiting' AND item_id=? ORDER BY version DESC LIMIT 1", id).Scan(&text)
	if errors.Is(e, sql.ErrNoRows) {
		return original, nil
	}
	return text, e
}

func (s *Store) CreateUnifiedReply(ctx context.Context, m taskapi.QueueMutation, r Record, body []byte, payload []byte) (taskapi.MutationReceipt, error) {
	var receipt taskapi.MutationReceipt
	e := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		if m.Revalidate == nil {
			return taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
		}
		if e := m.Revalidate(ctx); e != nil {
			return e
		}
		prior, found, e := taskapi.OriginalOperation(ctx, tx, m)
		if e != nil {
			return e
		}
		if found {
			receipt = prior
			return nil
		}
		if _, e = insertBounded(ctx, tx, r); e != nil {
			return e
		}
		if len(m.Binding) > 0 {
			if _, e = tx.ExecContext(ctx, "INSERT INTO unified_version_bindings VALUES('waiting',?,?,?)", r.ID, 1, m.Binding); e != nil {
				return e
			}
		}
		if len(payload) > 0 {
			if _, e = tx.ExecContext(ctx, "INSERT INTO unified_reply_payloads VALUES(?,?)", r.ID, payload); e != nil {
				return e
			}
		}
		if e = m.Revalidate(ctx); e != nil {
			return e
		}
		receipt, e = taskapi.SaveOperation(ctx, tx, m, map[string]any{"metadata_recorded": true, "admission": "accepted", "native_start": "unknown", "logical_task_id": r.LogicalID, "round_id": r.ID, "request_id": r.ChildRequestID, "reply": json.RawMessage(body)})
		return e
	})
	return receipt, e
}

func (s *Store) UnifiedEffectiveBinding(ctx context.Context, key, id string) ([]byte, error) {
	var b []byte
	e := s.db.QueryRowContext(ctx, "SELECT binding FROM unified_version_bindings WHERE queue_key=? AND item_id=? ORDER BY version DESC LIMIT 1", key, id).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	return b, e
}

func (s *Store) UnifiedReplyPayload(ctx context.Context, id string) ([]byte, error) {
	var b []byte
	e := s.db.QueryRowContext(ctx, "SELECT payload FROM unified_reply_payloads WHERE item_id=?", id).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	return b, e
}

func (s *Store) UnifiedEffectivePayload(ctx context.Context, key, id string) ([]byte, error) {
	var b []byte
	e := s.db.QueryRowContext(ctx, "SELECT payload FROM unified_input_versions WHERE queue_key=? AND item_id=? ORDER BY version DESC LIMIT 1", key, id).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	if !json.Valid(b) {
		return nil, nil
	}
	return b, nil
}

func (s *Store) UnifiedVersion(ctx context.Context, key, id string) (uint64, string, string, error) {
	var version uint64
	var hash, original string
	e := s.db.QueryRowContext(ctx, "SELECT version,hash,original_hash FROM unified_input_versions WHERE queue_key=? AND item_id=? ORDER BY version DESC LIMIT 1", key, id).Scan(&version, &hash, &original)
	if errors.Is(e, sql.ErrNoRows) {
		return 1, "", "", nil
	}
	return version, hash, original, e
}
