package messagequeue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
)

func unifiedRows(ctx context.Context, tx *sql.Tx, session string, eligible map[string]bool) ([]taskapi.QueueRow, error) {
	rows, e := tx.QueryContext(ctx, `SELECT seq,request_id,state,payload_hash FROM queue_commands WHERE session_id=? AND state IN ('queued','running','steering','uncertain') ORDER BY seq`, session)
	if e != nil {
		return nil, e
	}
	out := []taskapi.QueueRow{}
	for rows.Next() {
		r := taskapi.QueueRow{Session: session}
		if e = rows.Scan(&r.Sequence, &r.ID, &r.State, &r.ContentHash); e != nil {
			rows.Close()
			return nil, e
		}
		r.Request = r.ID
		r.ContentRevision = 1
		r.Eligible = eligible[r.ID] && r.State == "queued"
		if !r.Eligible {
			r.Barrier = "original_input_or_native_boundary"
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
		e = tx.QueryRowContext(ctx, `SELECT version,hash FROM unified_input_versions WHERE queue_key=? AND item_id=? ORDER BY version DESC LIMIT 1`, session, out[i].ID).Scan(&v, &h)
		if e == nil {
			out[i].ContentRevision = v
			out[i].ContentHash = h
		} else if !errors.Is(e, sql.ErrNoRows) {
			return nil, e
		}
	}
	if e = taskapi.LoadRanks(ctx, tx, session, out); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *Store) UnifiedQueue(ctx context.Context, session string, eligible map[string]bool) (taskapi.QueueView, error) {
	var v taskapi.QueueView
	e := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		r, e := unifiedRows(ctx, tx, session, eligible)
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
func effectiveInput(ctx context.Context, tx *sql.Tx, e Entry) (Entry, error) {
	var payload []byte
	var content string
	err := tx.QueryRowContext(ctx, `SELECT payload,content,hash,version FROM unified_input_versions WHERE queue_key=? AND item_id=? ORDER BY version DESC LIMIT 1`, e.SessionID, e.RequestID).Scan(&payload, &content, &e.EffectiveHash, &e.ContentRevision)
	if errors.Is(err, sql.ErrNoRows) {
		e.ContentRevision = 1
		e.EffectiveHash = e.PayloadHash
		return e, nil
	}
	if err != nil {
		return e, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT binding FROM unified_version_bindings WHERE queue_key=? AND item_id=? AND version=?", e.SessionID, e.RequestID, e.ContentRevision).Scan(&e.EffectiveBinding); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return e, err
	}
	e.Payload = payload
	e.Content = content
	return e, nil
}
func (s *Store) EffectiveInput(ctx context.Context, session, request string) (Entry, bool, error) {
	var e Entry
	var found bool
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		var err error
		e, found, err = lookup(tx, session, request)
		if err != nil || !found {
			return err
		}
		e, err = effectiveInput(ctx, tx, e)
		return err
	})
	return e, found, err
}
func (s *Store) UnifiedMutate(ctx context.Context, session string, eligible map[string]bool, m taskapi.QueueMutation) (taskapi.MutationReceipt, error) {
	var receipt taskapi.MutationReceipt
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
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
		rows, e := unifiedRows(ctx, tx, session, eligible)
		if e != nil {
			return e
		}
		v, e := taskapi.CheckQueueMutation(ctx, tx, session, rows, m)
		if e != nil {
			return e
		}
		var data any
		switch m.Operation {
		case "queue_move":
			ids, e := taskapi.MoveRanks(ctx, tx, session, v, m)
			if e != nil {
				return e
			}
			data = map[string]any{"order": taskapi.PublicQueueOrder(ids, m), "previous_revision": v.Revision}
		case "queue_edit":
			var native bool
			if e := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM task_native_acceptance WHERE session_id=? AND request_id=?)", session, m.Item).Scan(&native); e != nil {
				return e
			}
			if native {
				return taskapi.Failure("native_boundary_crossed", "known_none", "use_continuation")
			}
			entry, found, e := lookup(tx, session, m.Item)
			if e != nil {
				return e
			}
			if !found || entry.State != Queued {
				return taskapi.Failure("native_boundary_crossed", "known_none", "use_continuation")
			}
			effective, e := effectiveInput(ctx, tx, entry)
			if e != nil {
				return e
			}
			if effective.ContentRevision != m.ExpectedContentRevision || effective.EffectiveHash != m.ExpectedHash {
				return taskapi.Failure("stale_revision", "known_none", "refresh_snapshot")
			}
			// The server-supplied new payload must only change text. All attachment,
			// source, configuration, callback, credential and safety bindings survive.
			var old, next map[string]json.RawMessage
			if json.Unmarshal(effective.Payload, &old) != nil || json.Unmarshal(m.Payload, &next) != nil {
				return taskapi.Failure("invalid_argument", "known_none", "correct_input")
			}
			delete(old, "content")
			delete(next, "content")
			if m.AllowAttachments {
				delete(old, "images")
				delete(next, "images")
				delete(old, "files")
				delete(next, "files")
			}
			if taskapi.Digest(old) != taskapi.Digest(next) {
				return taskapi.Failure("permission", "known_none", "preserve_attachments")
			}
			if effective.ContentRevision == 1 {
				if _, e = tx.ExecContext(ctx, "INSERT INTO unified_input_versions VALUES(?,?,?,?,?,?,?)", session, m.Item, 1, entry.Payload, entry.Content, entry.PayloadHash, entry.PayloadHash); e != nil {
					return e
				}
			}
			hash := taskapi.RawDigest(m.Payload)
			version := effective.ContentRevision + 1
			if _, e = tx.ExecContext(ctx, "INSERT INTO unified_input_versions VALUES(?,?,?,?,?,?,?)", session, m.Item, version, m.Payload, m.Content, hash, entry.PayloadHash); e != nil {
				return e
			}
			if len(m.Binding) > 0 {
				if _, e = tx.ExecContext(ctx, "INSERT INTO unified_version_bindings VALUES(?,?,?,?)", session, m.Item, version, m.Binding); e != nil {
					return e
				}
			}
			if e = bump(tx, session); e != nil {
				return e
			}
			data = map[string]any{"content_revision": version, "content_hash": hash, "original_hash": entry.PayloadHash, "request_id": m.Item}
		default:
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
	return receipt, err
}

// InheritedInput carries an already committed effective revision from the
// original admission owner into its original session queue transaction.
type InheritedInput struct {
	Revision           uint64
	Hash, OriginalHash string
	Content            string
	Binding            []byte
}
