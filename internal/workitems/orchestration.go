package workitems

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type StartRunInput struct {
	ID              string
	WorkItemID      string
	SessionID       string
	RequestID       string
	Kind            string
	ExpectedVersion uint64
	Actor           Actor
}

type RunUpdate struct {
	Run       Run
	Item      WorkItem
	Changed   bool
	Attention string
}

func (s *Store) StartRun(ctx context.Context, in StartRunInput) (Run, WorkItem, error) {
	if in.ID == "" {
		in.ID = newID("wr")
	}
	if in.RequestID == "" || in.SessionID == "" {
		return Run{}, WorkItem{}, errors.New("run request and session are required")
	}
	if in.Kind == "" {
		in.Kind = "implementation"
	}
	if in.Kind != "implementation" && in.Kind != "research" && in.Kind != "verification" {
		return Run{}, WorkItem{}, fmt.Errorf("invalid run kind %q", in.Kind)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Run{}, WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return Run{}, WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return Run{}, WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	if item.Lifecycle != LifecycleReady && item.Lifecycle != LifecycleActive {
		return Run{}, WorkItem{}, fmt.Errorf("%w: start run from %s", ErrInvalidTransition, item.Lifecycle)
	}
	var linkID string
	err = tx.QueryRowContext(ctx, `SELECT id FROM work_item_session_links
		WHERE work_item_id=? AND session_id=? AND unlinked_at IS NULL LIMIT 1`, in.WorkItemID, in.SessionID).Scan(&linkID)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, WorkItem{}, errors.New("run session is not linked to this work item")
	}
	if err != nil {
		return Run{}, WorkItem{}, err
	}
	now := s.now().UnixMilli()
	run := Run{ID: in.ID, WorkItemID: in.WorkItemID, SessionLinkID: linkID,
		RequestID: in.RequestID, Kind: in.Kind, Status: "queued", StartedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_runs
		(id,work_item_id,session_link_id,request_id,kind,status,started_at)
		VALUES(?,?,?,?,?,?,?)`, run.ID, run.WorkItemID, run.SessionLinkID, run.RequestID,
		run.Kind, run.Status, run.StartedAt); err != nil {
		return Run{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = now
	if item.Lifecycle == LifecycleReady {
		item.Lifecycle = LifecycleActive
	}
	updated, err := s.writeItemMutation(ctx, tx, item, "run_started", in.Actor, ChangePayload{Run: &run})
	return run, updated, err
}

// OwnsRequest reports whether the exact session request was explicitly started
// as a WorkItem run. Terminal runs remain owned: completion notification
// routing happens independently from lifecycle projection, and may run before
// or after AdvanceRun records finished_at (including after a Bridge restart).
func (s *Store) OwnsRequest(ctx context.Context, sessionID, requestID string) (bool, error) {
	if sessionID == "" || requestID == "" {
		return false, nil
	}
	var exists int
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM work_item_runs r
		JOIN work_item_session_links l ON l.id=r.session_link_id
		WHERE l.session_id=? AND r.request_id=?
	)`, sessionID, requestID).Scan(&exists)
	return exists == 1, err
}

// AdvanceRun projects only explicitly-started WorkItem runs. Ordinary messages
// in a linked session never move the human-owned WorkItem lifecycle.
func (s *Store) AdvanceRun(ctx context.Context, sessionID, requestID, status, reason string) (RunUpdate, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RunUpdate{}, err
	}
	defer tx.Rollback()
	var run Run
	err = tx.QueryRowContext(ctx, `SELECT r.id,r.work_item_id,r.session_link_id,r.request_id,r.kind,
		r.status,r.started_at,r.finished_at,r.terminal_reason
		FROM work_item_runs r JOIN work_item_session_links l ON l.id=r.session_link_id
		WHERE l.session_id=? AND r.finished_at IS NULL AND (?='' OR r.request_id=?)
		ORDER BY r.started_at DESC LIMIT 1`, sessionID, requestID, requestID).Scan(
		&run.ID, &run.WorkItemID, &run.SessionLinkID, &run.RequestID, &run.Kind,
		&run.Status, &run.StartedAt, &run.FinishedAt, &run.TerminalReason)
	if errors.Is(err, sql.ErrNoRows) {
		return RunUpdate{}, nil
	}
	if err != nil {
		return RunUpdate{}, err
	}
	if run.Status == status {
		return RunUpdate{Run: run}, nil
	}
	now := s.now().UnixMilli()
	run.Status = status
	run.TerminalReason = reason
	terminal := status == "succeeded" || status == "failed" || status == "interrupted"
	if terminal {
		run.FinishedAt = &now
	}
	if _, err := tx.ExecContext(ctx, `UPDATE work_item_runs SET status=?,finished_at=?,terminal_reason=? WHERE id=?`,
		run.Status, run.FinishedAt, run.TerminalReason, run.ID); err != nil {
		return RunUpdate{}, err
	}
	item, err := getItemTx(ctx, tx, run.WorkItemID)
	if err != nil {
		return RunUpdate{}, err
	}
	item.Version++
	item.UpdatedAt = now
	attention := ""
	if status == "succeeded" && item.Lifecycle == LifecycleActive {
		item.Lifecycle = LifecycleReview
		attention = "review_ready"
	} else if status == "failed" {
		item.BlockedReasonCode = "execution_failed"
		item.BlockedNote = reason
		attention = "execution_failed"
	} else if status == "waiting_user" {
		attention = "needs_input"
	}
	updated, err := s.writeItemMutation(ctx, tx, item, "run_"+status, Actor{Type: ActorSystem}, ChangePayload{Run: &run})
	if err != nil {
		return RunUpdate{}, err
	}
	return RunUpdate{Run: run, Item: updated, Changed: true, Attention: attention}, nil
}
