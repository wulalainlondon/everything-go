package messagequeue

import (
	"context"
	"database/sql"
	"everything-go/internal/taskpipeline"
)

// Same durable connection and transaction domain as native admissions/seals.
// This is a control ledger, never another execution queue.
func (s *Store) PipelineStore() *taskpipeline.Store {
	return taskpipeline.NewWithContext(s.db, s.nativeAcceptanceContext, contextBarrierTx)
}

// ConsumedInput is an immutable original-provider ingress receipt. It is never
// backfilled from a newer effective version or an old model's summary.
func (s *Store) ConsumedInput(ctx context.Context, session, request, thread string) (uint64, string, bool, error) {
	var rev uint64
	var hash, acceptedThread, token string
	e := s.db.QueryRowContext(ctx, `SELECT revision,input_hash,thread_id,token FROM task_pipeline_native_inputs WHERE session_id=? AND request_id=?`, session, request).Scan(&rev, &hash, &acceptedThread, &token)
	if e == sql.ErrNoRows {
		return 0, "", false, nil
	}
	if e != nil {
		return 0, "", false, e
	}
	proof, found, e := s.ExactNativeAcceptance(session, request)
	if e != nil {
		return 0, "", false, e
	}
	if found {
		return rev, hash, acceptedThread == thread && proof.ThreadID == thread && proof.TurnID == token, nil
	}
	start, started, e := s.NativeStart(session, request)
	if e != nil {
		return 0, "", false, e
	}
	return rev, hash, started && start.Provider == "claude" && acceptedThread == thread && start.ThreadID == thread && start.Token == token, nil
}

// PipelineArtifactHash reads only an immutable collected manifest identity owned
// by the exact source request/seal. A reported path or a model-supplied hash is
// never evidence that this snapshot exists.
func (s *Store) PipelineArtifactHash(ctx context.Context, session, request, seal, hash string) (bool, error) {
	if session == "" || request == "" || seal == "" || len(hash) != 64 {
		return false, nil
	}
	var count int
	e := s.db.QueryRowContext(ctx, `SELECT count(*) FROM unified_documents WHERE owner_session=? AND owner_request=? AND seal_id=? AND hash=?`, session, request, seal, hash).Scan(&count)
	return count > 0, e
}
