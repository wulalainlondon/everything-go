package sessiondispatch

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
	"time"
)

func unifiedRows(ctx context.Context, tx *sql.Tx, target string, eligible map[string]bool) ([]taskapi.QueueRow, error) {
	rows, e := tx.QueryContext(ctx, `SELECT rowid,payload FROM session_dispatches WHERE json_extract(payload,'$.target_session_id')=? AND state NOT IN ('completed','failed','cancelled','rejected') ORDER BY rowid`, target)
	if e != nil {
		return nil, e
	}
	out := []taskapi.QueueRow{}
	for rows.Next() {
		var seq int64
		var raw []byte
		var d Record
		if e = rows.Scan(&seq, &raw); e != nil {
			rows.Close()
			return nil, e
		}
		if e = json.Unmarshal(raw, &d); e != nil {
			rows.Close()
			return nil, e
		}
		state := d.State
		if state == "prepared" || state == "pending" {
			state = "queued"
		}
		r := taskapi.QueueRow{ID: d.ID, Sequence: seq, State: state, Parent: d.ParentID, Session: d.SessionID, Request: d.RequestID, ContentHash: d.IntentHash, ContentRevision: 1, Eligible: eligible[d.ID] && state == "queued"}
		if !r.Eligible {
			r.Barrier = "original_authority_or_admission_boundary"
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
		e = tx.QueryRowContext(ctx, "SELECT version,hash FROM unified_input_versions WHERE queue_key=? AND item_id=? ORDER BY version DESC LIMIT 1", target, out[i].ID).Scan(&v, &h)
		if e == nil {
			out[i].ContentRevision = v
			out[i].ContentHash = h
		} else if !errors.Is(e, sql.ErrNoRows) {
			return nil, e
		}
	}
	e = taskapi.LoadRanks(ctx, tx, target, out)
	return out, e
}
func (s *Store) UnifiedQueue(ctx context.Context, target string, eligible map[string]bool) (taskapi.QueueView, error) {
	var v taskapi.QueueView
	e := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		r, e := unifiedRows(ctx, tx, target, eligible)
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
func (s *Store) UnifiedMutate(ctx context.Context, target string, eligible map[string]bool, m taskapi.QueueMutation) (taskapi.MutationReceipt, error) {
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
		if m.RoleSession != "" {
			var revision uint64
			if e = tx.QueryRowContext(ctx, "SELECT revision FROM controller_grants WHERE parent_id=?", m.RoleSession).Scan(&revision); e != nil {
				return e
			}
			if revision != m.RoleRevision {
				return taskapi.Failure("scope_changed", "known_none", "refresh_snapshot")
			}
		}
		for parent, expected := range m.GrantRevisions {
			var revision uint64
			if e := tx.QueryRowContext(ctx, "SELECT revision FROM controller_grants WHERE parent_id=?", parent).Scan(&revision); e != nil {
				return e
			}
			if revision != expected {
				return taskapi.Failure("scope_changed", "known_none", "refresh_snapshot")
			}
		}
		rows, e := unifiedRows(ctx, tx, target, eligible)
		if e != nil {
			return e
		}
		v, e := taskapi.CheckQueueMutation(ctx, tx, target, rows, m)
		if e != nil {
			return e
		}
		var data any
		if m.Operation == "queue_move" {
			ids, e := taskapi.MoveRanks(ctx, tx, target, v, m)
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
			var raw []byte
			var d Record
			if e = tx.QueryRowContext(ctx, "SELECT payload FROM session_dispatches WHERE id=?", m.Item).Scan(&raw); e != nil {
				return e
			}
			if e = json.Unmarshal(raw, &d); e != nil {
				return e
			}
			if d.State != "prepared" && d.State != "pending" {
				return taskapi.Failure("native_boundary_crossed", "known_none", "use_continuation")
			}
			if selected.ContentRevision == 1 {
				if _, e = tx.ExecContext(ctx, "INSERT INTO unified_input_versions VALUES(?,?,?,?,?,?,?)", target, m.Item, 1, []byte(d.Content), d.Content, d.IntentHash, d.IntentHash); e != nil {
					return e
				}
			}
			version := selected.ContentRevision + 1
			effectivePayload := []byte(m.Content)
			if len(m.Payload) > 0 {
				effectivePayload = m.Payload
			}
			h := taskapi.RawDigest(effectivePayload)
			if _, e = tx.ExecContext(ctx, "INSERT INTO unified_input_versions VALUES(?,?,?,?,?,?,?)", target, m.Item, version, effectivePayload, m.Content, h, d.IntentHash); e != nil {
				return e
			}
			if len(m.Binding) > 0 {
				if _, e = tx.ExecContext(ctx, "INSERT INTO unified_version_bindings VALUES(?,?,?,?)", target, m.Item, version, m.Binding); e != nil {
					return e
				}
			}
			data = map[string]any{"content_revision": version, "content_hash": h, "original_hash": d.IntentHash}
		} else {
			return taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
		for parent, expected := range m.GrantRevisions {
			var revision uint64
			if e := tx.QueryRowContext(ctx, "SELECT revision FROM controller_grants WHERE parent_id=?", parent).Scan(&revision); e != nil {
				return e
			}
			if revision != expected {
				return taskapi.Failure("scope_changed", "known_none", "refresh_snapshot")
			}
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

// ReserveAdmission atomically freezes the effective queued revision BEFORE the
// original enqueue boundary. Accepted-without-ACK is never made waiting again.
func (s *Store) ReserveAdmission(ctx context.Context, id string) (Record, bool, error) {
	var d Record
	reserved := false
	e := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		var raw []byte
		if e := tx.QueryRowContext(ctx, "SELECT payload FROM session_dispatches WHERE id=?", id).Scan(&raw); e != nil {
			return e
		}
		if e := json.Unmarshal(raw, &d); e != nil {
			return e
		}
		if d.State != "prepared" && d.State != "pending" {
			return nil
		}
		var older bool
		e := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM session_dispatches q WHERE q.id!=? AND q.state IN ('prepared','pending') AND json_extract(q.payload,'$.target_session_id')=? AND COALESCE((SELECT rank FROM unified_queue_ranks WHERE queue_key=? AND item_id=q.id),q.rowid)<COALESCE((SELECT rank FROM unified_queue_ranks WHERE queue_key=? AND item_id=?),(SELECT rowid FROM session_dispatches WHERE id=?)))`, id, d.SessionID, d.SessionID, d.SessionID, id, id).Scan(&older)
		if e != nil || older {
			return e
		}
		var text string
		e = tx.QueryRowContext(ctx, "SELECT content FROM unified_input_versions WHERE queue_key=? AND item_id=? ORDER BY version DESC LIMIT 1", d.SessionID, d.ID).Scan(&text)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if e == nil {
			d.Content = text
		}
		d.State = "accepted"
		d.UpdatedAt = time.Now().UnixMilli()
		saved := d
		// Keep accepted original content immutable; effective text exists in version row.
		var original Record
		if e = json.Unmarshal(raw, &original); e != nil {
			return e
		}
		saved.Content = original.Content
		b, _ := json.Marshal(saved)
		if _, e = tx.ExecContext(ctx, "UPDATE session_dispatches SET state='accepted',payload=? WHERE id=?", b, id); e != nil {
			return e
		}
		reserved = true
		return nil
	})
	return d, reserved, e
}

func (s *Store) UnifiedEffectiveBinding(ctx context.Context, key, id string) ([]byte, error) {
	var b []byte
	e := s.db.QueryRowContext(ctx, "SELECT binding FROM unified_version_bindings WHERE queue_key=? AND item_id=? ORDER BY version DESC LIMIT 1", key, id).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	return b, e
}

// Unique exact request membership. Duplicate native bindings are ambiguous,
// never a first/latest selection or a namespace-prefix guess.
func (s *Store) UnifiedByExecution(ctx context.Context, instance, target, request string) (Record, bool, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT payload FROM session_dispatches WHERE json_extract(payload,'$.instance_id')=? AND json_extract(payload,'$.target_session_id')=? AND json_extract(payload,'$.request_id')=? LIMIT 2`, instance, target, request)
	if e != nil {
		return Record{}, false, e
	}
	defer rows.Close()
	var out Record
	n := 0
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return out, false, e
		}
		if e = json.Unmarshal(b, &out); e != nil {
			return out, false, e
		}
		n++
	}
	if e = rows.Err(); e != nil {
		return out, false, e
	}
	if n > 1 {
		return out, false, taskapi.Failure("ambiguous_owner", "known_none", "lookup_original")
	}
	return out, n == 1, nil
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

func (s *Store) EffectiveContent(ctx context.Context, key, id, original string) (string, error) {
	var text string
	e := s.db.QueryRowContext(ctx, "SELECT content FROM unified_input_versions WHERE queue_key=? AND item_id=? ORDER BY version DESC LIMIT 1", key, id).Scan(&text)
	if errors.Is(e, sql.ErrNoRows) {
		return original, nil
	}
	return text, e
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
