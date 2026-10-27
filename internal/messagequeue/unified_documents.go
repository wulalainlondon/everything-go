package messagequeue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
)

type UnifiedDocument struct {
	ID           string          `json:"document_id"`
	OwnerSession string          `json:"owner_session_id"`
	OwnerRequest string          `json:"owner_request_id"`
	SealID       string          `json:"seal_id"`
	Hash         string          `json:"full_hash"`
	PathIdentity string          `json:"collector_path_identity"`
	Manifest     json.RawMessage `json:"manifest"`
	Body         []byte          `json:"-"`
}
type UnifiedDocumentGrant struct {
	ID              string
	Document        string
	OwnerScope      string
	OwnerGeneration uint64
	Consumer        json.RawMessage
	Purpose         string
	Expires         int64
	Revoked         bool
}

func (s *Store) UnifiedDocument(ctx context.Context, id string) (UnifiedDocument, error) {
	var d UnifiedDocument
	e := s.db.QueryRowContext(ctx, "SELECT id,owner_session,owner_request,seal_id,hash,path_identity,manifest,body FROM unified_documents WHERE id=?", id).Scan(&d.ID, &d.OwnerSession, &d.OwnerRequest, &d.SealID, &d.Hash, &d.PathIdentity, &d.Manifest, &d.Body)
	return d, e
}
func (s *Store) UnifiedGrant(ctx context.Context, id string) (UnifiedDocumentGrant, error) {
	var g UnifiedDocumentGrant
	e := s.db.QueryRowContext(ctx, "SELECT id,document,owner_scope,owner_generation,consumer,purpose,expires,revoked FROM unified_document_grants WHERE id=?", id).Scan(&g.ID, &g.Document, &g.OwnerScope, &g.OwnerGeneration, &g.Consumer, &g.Purpose, &g.Expires, &g.Revoked)
	return g, e
}
func (s *Store) UnifiedRecord(ctx context.Context, m taskapi.QueueMutation, body any) (taskapi.MutationReceipt, error) {
	return s.UnifiedDocumentMutation(ctx, m, nil, nil, "", body)
}
func (s *Store) UnifiedDocumentMutation(ctx context.Context, m taskapi.QueueMutation, document *UnifiedDocument, grant *UnifiedDocumentGrant, revoke string, data any) (taskapi.MutationReceipt, error) {
	var r taskapi.MutationReceipt
	e := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		if m.Revalidate == nil {
			return taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
		}
		if e := m.Revalidate(ctx); e != nil {
			return e
		}
		old, ok, e := taskapi.OriginalOperation(ctx, tx, m)
		if e != nil {
			return e
		}
		if ok {
			r = old
			return nil
		}
		if document != nil {
			d := document
			if taskapi.RawDigest(d.Body) != d.Hash {
				return taskapi.Failure("invalid_argument", "known_none", "correct_input")
			}
			_, e = tx.ExecContext(ctx, "INSERT INTO unified_documents VALUES(?,?,?,?,?,?,?,?)", d.ID, d.OwnerSession, d.OwnerRequest, d.SealID, d.Hash, d.PathIdentity, d.Manifest, d.Body)
			if e != nil {
				return e
			}
		}
		if grant != nil {
			g := grant
			_, e = tx.ExecContext(ctx, "INSERT INTO unified_document_grants VALUES(?,?,?,?,?,?,?,0)", g.ID, g.Document, g.OwnerScope, g.OwnerGeneration, g.Consumer, g.Purpose, g.Expires)
			if e != nil {
				return e
			}
		}
		if revoke != "" {
			res, e := tx.ExecContext(ctx, "UPDATE unified_document_grants SET revoked=1 WHERE id=? AND owner_scope=? AND owner_generation=?", revoke, m.Scope, m.Generation)
			if e != nil {
				return e
			}
			n, _ := res.RowsAffected()
			if n != 1 {
				return taskapi.Failure("not_found", "known_none", "lookup_original")
			}
		}
		if e = m.Revalidate(ctx); e != nil {
			return e
		}
		r, e = taskapi.SaveOperation(ctx, tx, m, data)
		return e
	})
	return r, e
}
func (s *Store) UnifiedReply(ctx context.Context, m taskapi.QueueMutation, body []byte) (taskapi.MutationReceipt, error) {
	var r taskapi.MutationReceipt
	e := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		if m.Revalidate == nil {
			return taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
		}
		if e := m.Revalidate(ctx); e != nil {
			return e
		}
		old, ok, e := taskapi.OriginalOperation(ctx, tx, m)
		if e != nil {
			return e
		}
		if ok {
			r = old
			return nil
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO unified_replies VALUES(?,?,?,?,?)", m.Scope, m.Generation, m.Intent, m.Hash, body); e != nil {
			return e
		}
		if e = m.Revalidate(ctx); e != nil {
			return e
		}
		r, e = taskapi.SaveOperation(ctx, tx, m, map[string]any{"metadata_recorded": true, "native_start": "not_requested", "reply": json.RawMessage(body)})
		return e
	})
	return r, e
}
func (s *Store) UnifiedHasReply(ctx context.Context, scope string, generation uint64, intent string) (bool, error) {
	var found int
	e := s.db.QueryRowContext(ctx, "SELECT 1 FROM unified_replies WHERE scope=? AND generation=? AND intent=?", scope, generation, intent).Scan(&found)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	return e == nil, e
}

// UnifiedReplySource returns only the exact persisted original reply relation.
func (s *Store) UnifiedReplySource(ctx context.Context, session, request string) (string, uint64, []byte, bool, error) {
	var scope string
	var generation uint64
	var body []byte
	e := s.db.QueryRowContext(ctx, "SELECT scope,generation,body FROM unified_reply_sources WHERE session_id=? AND request_id=?", session, request).Scan(&scope, &generation, &body)
	if errors.Is(e, sql.ErrNoRows) {
		return "", 0, nil, false, nil
	}
	return scope, generation, body, e == nil, e
}
