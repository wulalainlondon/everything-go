package workitems

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const schemaVersion = 1

const schema = `
CREATE TABLE IF NOT EXISTS work_schema (
  version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS work_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS work_projects (
  id TEXT PRIMARY KEY,
  authority_instance_id TEXT NOT NULL,
  name TEXT NOT NULL,
  workspace_path TEXT NOT NULL DEFAULT '',
  version INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  archived_at INTEGER
);

CREATE TABLE IF NOT EXISTS work_items (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES work_projects(id),
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  lifecycle TEXT NOT NULL CHECK (lifecycle IN ('inbox','ready','active','review','done','cancelled')),
  priority TEXT NOT NULL CHECK (priority IN ('none','low','medium','high','urgent')),
  sort_key INTEGER NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  activity_revision INTEGER NOT NULL DEFAULT 0,
  blocked_reason_code TEXT NOT NULL DEFAULT '',
  blocked_note TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  accepted_at INTEGER,
  cancelled_at INTEGER,
  archived_at INTEGER
);

CREATE INDEX IF NOT EXISTS work_items_project_column
  ON work_items(project_id, lifecycle, sort_key, id);
CREATE INDEX IF NOT EXISTS work_items_search_projection
  ON work_items(project_id, title COLLATE NOCASE, updated_at DESC);

CREATE TABLE IF NOT EXISTS work_item_session_links (
  id TEXT PRIMARY KEY,
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  session_id TEXT NOT NULL,
  thread_id_snapshot TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL CHECK (role IN ('primary','support','research','verification')),
  linked_at INTEGER NOT NULL,
  unlinked_at INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS work_link_one_active_item_per_session
  ON work_item_session_links(session_id) WHERE unlinked_at IS NULL;
CREATE INDEX IF NOT EXISTS work_links_item ON work_item_session_links(work_item_id, linked_at, id);

CREATE TABLE IF NOT EXISTS work_item_runs (
  id TEXT PRIMARY KEY,
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  session_link_id TEXT REFERENCES work_item_session_links(id),
  request_id TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL CHECK (kind IN ('implementation','research','verification')),
  status TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  finished_at INTEGER,
  terminal_reason TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS work_item_comments (
  id TEXT PRIMARY KEY,
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  author_type TEXT NOT NULL,
  author_device_id TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  edited_at INTEGER,
  deleted_at INTEGER
);

CREATE TABLE IF NOT EXISTS work_item_dependencies (
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  depends_on_id TEXT NOT NULL REFERENCES work_items(id),
  created_at INTEGER NOT NULL,
  PRIMARY KEY(work_item_id, depends_on_id),
  CHECK(work_item_id <> depends_on_id)
);

CREATE TABLE IF NOT EXISTS work_item_attachments (
  id TEXT PRIMARY KEY,
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  comment_id TEXT REFERENCES work_item_comments(id),
  attachment_id TEXT NOT NULL,
  display_name TEXT NOT NULL DEFAULT '',
  sort_key INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS work_item_activity (
  revision INTEGER PRIMARY KEY,
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  kind TEXT NOT NULL,
  actor TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS work_changes (
  revision INTEGER PRIMARY KEY AUTOINCREMENT,
  entity TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS work_device_cursors (
  device_id TEXT NOT NULL,
  authority_instance_id TEXT NOT NULL,
  delivered_revision INTEGER NOT NULL DEFAULT 0,
  acked_revision INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(device_id, authority_instance_id)
);

CREATE TABLE IF NOT EXISTS work_item_reads (
  device_id TEXT NOT NULL,
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  read_activity_revision INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(device_id, work_item_id)
);

CREATE TABLE IF NOT EXISTS work_mutation_dedup (
  device_id TEXT NOT NULL,
  mutation_id TEXT NOT NULL,
  response TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(device_id, mutation_id)
);
`

type Store struct {
	db         *sql.DB
	instanceID string
	now        func() time.Time
}

func Open(dataDir, instanceID string) (*Store, error) {
	if strings.TrimSpace(instanceID) == "" {
		return nil, errors.New("authority instance id is required")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, "everything_go_work_items.db")
	dsn := "file:" + path +
		"?_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	store := &Store{db: db, instanceID: instanceID, now: time.Now}
	if err := store.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return err
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM work_schema").Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		if _, err := tx.ExecContext(ctx, "INSERT INTO work_schema(version) VALUES (?)", schemaVersion); err != nil {
			return err
		}
	} else {
		var version int
		if err := tx.QueryRowContext(ctx, "SELECT version FROM work_schema LIMIT 1").Scan(&version); err != nil {
			return err
		}
		if version != schemaVersion {
			return fmt.Errorf("unsupported work item schema version %d", version)
		}
	}
	return tx.Commit()
}

type CreateProjectInput struct {
	ID            string
	Name          string
	WorkspacePath string
}

func (s *Store) CreateProject(ctx context.Context, in CreateProjectInput) (Project, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return Project{}, errors.New("project name is required")
	}
	id := in.ID
	if id == "" {
		id = newID("wp")
	}
	now := s.now().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Project{}, err
	}
	defer tx.Rollback()
	project := Project{ID: id, AuthorityInstanceID: s.instanceID, Name: name,
		WorkspacePath: strings.TrimSpace(in.WorkspacePath), Version: 1, CreatedAt: now, UpdatedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_projects
		(id,authority_instance_id,name,workspace_path,version,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?)`, project.ID, project.AuthorityInstanceID, project.Name,
		project.WorkspacePath, project.Version, now, now); err != nil {
		return Project{}, err
	}
	if _, err := recordChange(ctx, tx, "project", id, "created", project, now); err != nil {
		return Project{}, err
	}
	if err := tx.Commit(); err != nil {
		return Project{}, err
	}
	return project, nil
}

type CreateItemInput struct {
	ID          string
	ProjectID   string
	Title       string
	Description string
	Priority    Priority
	SortKey     int64
	Actor       Actor
}

func (s *Store) CreateItem(ctx context.Context, in CreateItemInput) (WorkItem, error) {
	title, err := normalizeTitle(in.Title)
	if err != nil {
		return WorkItem{}, err
	}
	if in.Priority == "" {
		in.Priority = PriorityNone
	}
	if !validPriority(in.Priority) {
		return WorkItem{}, fmt.Errorf("invalid work item priority %q", in.Priority)
	}
	if in.ID == "" {
		in.ID = newID("wi")
	}
	if in.SortKey == 0 {
		in.SortKey = 1_000_000
	}
	now := s.now().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, err
	}
	defer tx.Rollback()
	if err := s.requireProject(ctx, tx, in.ProjectID); err != nil {
		return WorkItem{}, err
	}
	item := WorkItem{ID: in.ID, ProjectID: in.ProjectID, Title: title,
		Description: in.Description, Lifecycle: LifecycleInbox, Priority: in.Priority,
		SortKey: in.SortKey, Version: 1, CreatedAt: now, UpdatedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_items
		(id,project_id,title,description,lifecycle,priority,sort_key,version,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?)`, item.ID, item.ProjectID, item.Title, item.Description,
		item.Lifecycle, item.Priority, item.SortKey, item.Version, now, now); err != nil {
		return WorkItem{}, err
	}
	revision, err := recordChange(ctx, tx, "work_item", item.ID, "created", ChangePayload{Item: &item}, now)
	if err != nil {
		return WorkItem{}, err
	}
	item.ActivityRevision = revision
	if _, err := tx.ExecContext(ctx, "UPDATE work_items SET activity_revision=? WHERE id=?", revision, item.ID); err != nil {
		return WorkItem{}, err
	}
	if err := insertActivity(ctx, tx, revision, item.ID, "created", in.Actor, item, now); err != nil {
		return WorkItem{}, err
	}
	if err := updateChangePayload(ctx, tx, revision, ChangePayload{Item: &item}); err != nil {
		return WorkItem{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkItem{}, err
	}
	return item, nil
}

type UpdateItemInput struct {
	ID                string
	ExpectedVersion   uint64
	Title             *string
	Description       *string
	Priority          *Priority
	BlockedReasonCode *string
	BlockedNote       *string
	Actor             Actor
}

func (s *Store) UpdateItem(ctx context.Context, in UpdateItemInput) (WorkItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, err
	}
	defer tx.Rollback()
	current, err := getItemTx(ctx, tx, in.ID)
	if err != nil {
		return WorkItem{}, err
	}
	if current.Version != in.ExpectedVersion {
		return WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: current}
	}
	if in.Title != nil {
		current.Title, err = normalizeTitle(*in.Title)
		if err != nil {
			return WorkItem{}, err
		}
	}
	if in.Description != nil {
		current.Description = *in.Description
	}
	if in.Priority != nil {
		if !validPriority(*in.Priority) {
			return WorkItem{}, fmt.Errorf("invalid work item priority %q", *in.Priority)
		}
		current.Priority = *in.Priority
	}
	if in.BlockedReasonCode != nil {
		current.BlockedReasonCode = strings.TrimSpace(*in.BlockedReasonCode)
	}
	if in.BlockedNote != nil {
		current.BlockedNote = strings.TrimSpace(*in.BlockedNote)
	}
	current.Version++
	current.UpdatedAt = s.now().UnixMilli()
	return s.writeItemMutation(ctx, tx, current, "updated", in.Actor, ChangePayload{})
}

type MoveItemInput struct {
	ID              string
	ExpectedVersion uint64
	Lifecycle       Lifecycle
	SortKey         int64
	Actor           Actor
}

func (s *Store) MoveItem(ctx context.Context, in MoveItemInput) (WorkItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, err
	}
	defer tx.Rollback()
	current, err := getItemTx(ctx, tx, in.ID)
	if err != nil {
		return WorkItem{}, err
	}
	if current.Version != in.ExpectedVersion {
		return WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: current}
	}
	if err := validateTransition(current.Lifecycle, in.Lifecycle, in.Actor); err != nil {
		return WorkItem{}, err
	}
	now := s.now().UnixMilli()
	current.Lifecycle = in.Lifecycle
	if in.SortKey != 0 {
		current.SortKey = in.SortKey
	}
	current.Version++
	current.UpdatedAt = now
	if in.Lifecycle == LifecycleDone {
		current.AcceptedAt = &now
	} else {
		current.AcceptedAt = nil
	}
	if in.Lifecycle == LifecycleCancelled {
		current.CancelledAt = &now
	} else {
		current.CancelledAt = nil
	}
	return s.writeItemMutation(ctx, tx, current, "moved", in.Actor, ChangePayload{})
}

func (s *Store) writeItemMutation(ctx context.Context, tx *sql.Tx, item WorkItem, kind string, actor Actor, payload ChangePayload) (WorkItem, error) {
	result, err := tx.ExecContext(ctx, `UPDATE work_items SET
		title=?,description=?,lifecycle=?,priority=?,sort_key=?,version=?,
		blocked_reason_code=?,blocked_note=?,updated_at=?,accepted_at=?,cancelled_at=?,archived_at=?
		WHERE id=? AND version=?`, item.Title, item.Description, item.Lifecycle, item.Priority,
		item.SortKey, item.Version, item.BlockedReasonCode, item.BlockedNote, item.UpdatedAt,
		item.AcceptedAt, item.CancelledAt, item.ArchivedAt, item.ID, item.Version-1)
	if err != nil {
		return WorkItem{}, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		latest, getErr := getItemTx(ctx, tx, item.ID)
		if getErr != nil {
			return WorkItem{}, getErr
		}
		return WorkItem{}, &ConflictError{Expected: item.Version - 1, Current: latest}
	}
	payload.Item = &item
	revision, err := recordChange(ctx, tx, "work_item", item.ID, kind, payload, item.UpdatedAt)
	if err != nil {
		return WorkItem{}, err
	}
	item.ActivityRevision = revision
	if _, err := tx.ExecContext(ctx, "UPDATE work_items SET activity_revision=? WHERE id=?", revision, item.ID); err != nil {
		return WorkItem{}, err
	}
	if err := insertActivity(ctx, tx, revision, item.ID, kind, actor, item, item.UpdatedAt); err != nil {
		return WorkItem{}, err
	}
	payload.Item = &item
	if err := updateChangePayload(ctx, tx, revision, payload); err != nil {
		return WorkItem{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkItem{}, err
	}
	return item, nil
}

type LinkSessionInput struct {
	ID               string
	WorkItemID       string
	SessionID        string
	ThreadIDSnapshot string
	Role             string
	ExpectedVersion  uint64
	Actor            Actor
}

func (s *Store) LinkSession(ctx context.Context, in LinkSessionInput) (SessionLink, WorkItem, error) {
	if strings.TrimSpace(in.SessionID) == "" {
		return SessionLink{}, WorkItem{}, errors.New("session id is required")
	}
	if in.Role == "" {
		in.Role = "primary"
	}
	if in.Role != "primary" && in.Role != "support" && in.Role != "research" && in.Role != "verification" {
		return SessionLink{}, WorkItem{}, fmt.Errorf("invalid session role %q", in.Role)
	}
	if in.ID == "" {
		in.ID = newID("wl")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return SessionLink{}, WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	now := s.now().UnixMilli()
	link := SessionLink{ID: in.ID, WorkItemID: in.WorkItemID, SessionID: in.SessionID,
		ThreadIDSnapshot: in.ThreadIDSnapshot, Role: in.Role, LinkedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_session_links
		(id,work_item_id,session_id,thread_id_snapshot,role,linked_at) VALUES(?,?,?,?,?,?)`,
		link.ID, link.WorkItemID, link.SessionID, link.ThreadIDSnapshot, link.Role, link.LinkedAt); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return SessionLink{}, WorkItem{}, ErrSessionLinked
		}
		return SessionLink{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = now
	updated, err := s.writeItemMutation(ctx, tx, item, "session_linked", in.Actor, ChangePayload{Link: &link})
	if err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	return link, updated, nil
}

type UnlinkSessionInput struct {
	LinkID          string
	ExpectedVersion uint64
	Actor           Actor
}

func (s *Store) UnlinkSession(ctx context.Context, in UnlinkSessionInput) (SessionLink, WorkItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	defer tx.Rollback()
	var link SessionLink
	err = tx.QueryRowContext(ctx, `SELECT id,work_item_id,session_id,thread_id_snapshot,role,linked_at,unlinked_at
		FROM work_item_session_links WHERE id=?`, in.LinkID).Scan(&link.ID, &link.WorkItemID,
		&link.SessionID, &link.ThreadIDSnapshot, &link.Role, &link.LinkedAt, &link.UnlinkedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return SessionLink{}, WorkItem{}, ErrNotFound
	}
	if err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	item, err := getItemTx(ctx, tx, link.WorkItemID)
	if err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return SessionLink{}, WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	if link.UnlinkedAt != nil {
		return link, item, nil
	}
	now := s.now().UnixMilli()
	link.UnlinkedAt = &now
	if _, err := tx.ExecContext(ctx, `UPDATE work_item_session_links SET unlinked_at=?
		WHERE id=? AND unlinked_at IS NULL`, now, link.ID); err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = now
	updated, err := s.writeItemMutation(ctx, tx, item, "session_unlinked", in.Actor, ChangePayload{Link: &link})
	return link, updated, err
}

type AddDependencyInput struct {
	WorkItemID      string
	DependsOnID     string
	ExpectedVersion uint64
	Actor           Actor
}

func (s *Store) AddDependency(ctx context.Context, in AddDependencyInput) (WorkItem, error) {
	if in.WorkItemID == in.DependsOnID {
		return WorkItem{}, ErrDependencyCycle
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	dep, err := getItemTx(ctx, tx, in.DependsOnID)
	if err != nil {
		return WorkItem{}, err
	}
	if item.ProjectID != dep.ProjectID {
		return WorkItem{}, ErrCrossProject
	}
	var cycle int
	err = tx.QueryRowContext(ctx, `WITH RECURSIVE chain(id) AS (
		SELECT depends_on_id FROM work_item_dependencies WHERE work_item_id=?
		UNION
		SELECT d.depends_on_id FROM work_item_dependencies d JOIN chain c ON d.work_item_id=c.id
	) SELECT EXISTS(SELECT 1 FROM chain WHERE id=?)`, in.DependsOnID, in.WorkItemID).Scan(&cycle)
	if err != nil {
		return WorkItem{}, err
	}
	if cycle != 0 {
		return WorkItem{}, ErrDependencyCycle
	}
	now := s.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_dependencies(work_item_id,depends_on_id,created_at)
		VALUES(?,?,?)`, in.WorkItemID, in.DependsOnID, now); err != nil {
		return WorkItem{}, err
	}
	dependency := Dependency{WorkItemID: in.WorkItemID, DependsOn: in.DependsOnID, CreatedAt: now}
	item.Version++
	item.UpdatedAt = now
	return s.writeItemMutation(ctx, tx, item, "dependency_added", in.Actor, ChangePayload{Dependency: &dependency})
}

func (s *Store) RemoveDependency(ctx context.Context, in AddDependencyInput) (WorkItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM work_item_dependencies
		WHERE work_item_id=? AND depends_on_id=?`, in.WorkItemID, in.DependsOnID)
	if err != nil {
		return WorkItem{}, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return WorkItem{}, ErrNotFound
	}
	now := s.now().UnixMilli()
	dep := Dependency{WorkItemID: in.WorkItemID, DependsOn: in.DependsOnID, CreatedAt: now}
	item.Version++
	item.UpdatedAt = now
	return s.writeItemMutation(ctx, tx, item, "dependency_removed", in.Actor, ChangePayload{Dependency: &dep})
}

type ArchiveItemInput struct {
	ID              string
	ExpectedVersion uint64
	Restore         bool
	Actor           Actor
}

func (s *Store) ArchiveItem(ctx context.Context, in ArchiveItemInput) (WorkItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.ID)
	if err != nil {
		return WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	now := s.now().UnixMilli()
	kind := "archived"
	if in.Restore {
		item.ArchivedAt = nil
		kind = "restored"
	} else {
		item.ArchivedAt = &now
	}
	item.Version++
	item.UpdatedAt = now
	return s.writeItemMutation(ctx, tx, item, kind, in.Actor, ChangePayload{})
}

type AddCommentInput struct {
	ID              string
	WorkItemID      string
	ExpectedVersion uint64
	Body            string
	Actor           Actor
}

func (s *Store) AddComment(ctx context.Context, in AddCommentInput) (Comment, WorkItem, error) {
	body := strings.TrimSpace(in.Body)
	if body == "" {
		return Comment{}, WorkItem{}, errors.New("comment body is required")
	}
	if in.ID == "" {
		in.ID = newID("wc")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Comment{}, WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return Comment{}, WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return Comment{}, WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	now := s.now().UnixMilli()
	author := in.Actor.Type
	if author == "" {
		author = ActorUser
	}
	comment := Comment{ID: in.ID, WorkItemID: in.WorkItemID, AuthorType: author,
		AuthorDeviceID: in.Actor.DeviceID, Body: body, CreatedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_comments
		(id,work_item_id,author_type,author_device_id,body,created_at) VALUES(?,?,?,?,?,?)`,
		comment.ID, comment.WorkItemID, comment.AuthorType, comment.AuthorDeviceID, comment.Body, now); err != nil {
		return Comment{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = now
	updated, err := s.writeItemMutation(ctx, tx, item, "comment_added", in.Actor, ChangePayload{Comment: &comment})
	return comment, updated, err
}

type EditCommentInput struct {
	CommentID       string
	ExpectedVersion uint64
	Body            string
	Actor           Actor
}

func (s *Store) EditComment(ctx context.Context, in EditCommentInput) (Comment, WorkItem, error) {
	body := strings.TrimSpace(in.Body)
	if body == "" {
		return Comment{}, WorkItem{}, errors.New("comment body is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Comment{}, WorkItem{}, err
	}
	defer tx.Rollback()
	var comment Comment
	err = tx.QueryRowContext(ctx, `SELECT id,work_item_id,author_type,author_device_id,body,
		created_at,edited_at,deleted_at FROM work_item_comments WHERE id=?`, in.CommentID).Scan(
		&comment.ID, &comment.WorkItemID, &comment.AuthorType, &comment.AuthorDeviceID, &comment.Body,
		&comment.CreatedAt, &comment.EditedAt, &comment.DeletedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Comment{}, WorkItem{}, ErrNotFound
	}
	if err != nil {
		return Comment{}, WorkItem{}, err
	}
	item, err := getItemTx(ctx, tx, comment.WorkItemID)
	if err != nil {
		return Comment{}, WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return Comment{}, WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	now := s.now().UnixMilli()
	comment.Body = body
	comment.EditedAt = &now
	if _, err := tx.ExecContext(ctx, `UPDATE work_item_comments SET body=?,edited_at=? WHERE id=?`,
		body, now, comment.ID); err != nil {
		return Comment{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = now
	updated, err := s.writeItemMutation(ctx, tx, item, "comment_edited", in.Actor, ChangePayload{Comment: &comment})
	return comment, updated, err
}

func (s *Store) GetItem(ctx context.Context, id string) (WorkItem, error) {
	return getItemQuery(ctx, s.db, id)
}

func (s *Store) Snapshot(ctx context.Context) (Snapshot, error) {
	var snap Snapshot
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(revision),0) FROM work_changes").Scan(&snap.Revision); err != nil {
		return snap, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,authority_instance_id,name,workspace_path,version,
		created_at,updated_at,archived_at FROM work_projects ORDER BY created_at,id`)
	if err != nil {
		return snap, err
	}
	for rows.Next() {
		var p Project
		if err := rows.Scan(&p.ID, &p.AuthorityInstanceID, &p.Name, &p.WorkspacePath, &p.Version,
			&p.CreatedAt, &p.UpdatedAt, &p.ArchivedAt); err != nil {
			rows.Close()
			return snap, err
		}
		snap.Projects = append(snap.Projects, p)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	rows, err = s.db.QueryContext(ctx, itemSelect+" ORDER BY project_id,lifecycle,sort_key,id")
	if err != nil {
		return snap, err
	}
	for rows.Next() {
		item, err := scanItem(rows)
		if err != nil {
			rows.Close()
			return snap, err
		}
		snap.Items = append(snap.Items, item)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT id,work_item_id,session_id,thread_id_snapshot,role,linked_at,unlinked_at
		FROM work_item_session_links WHERE unlinked_at IS NULL ORDER BY linked_at,id`)
	if err != nil {
		return snap, err
	}
	for rows.Next() {
		var link SessionLink
		if err := rows.Scan(&link.ID, &link.WorkItemID, &link.SessionID, &link.ThreadIDSnapshot,
			&link.Role, &link.LinkedAt, &link.UnlinkedAt); err != nil {
			rows.Close()
			return snap, err
		}
		snap.SessionLinks = append(snap.SessionLinks, link)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT work_item_id,depends_on_id,created_at
		FROM work_item_dependencies ORDER BY work_item_id,depends_on_id`)
	if err != nil {
		return snap, err
	}
	defer rows.Close()
	for rows.Next() {
		var dep Dependency
		if err := rows.Scan(&dep.WorkItemID, &dep.DependsOn, &dep.CreatedAt); err != nil {
			return snap, err
		}
		snap.Dependencies = append(snap.Dependencies, dep)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT id,work_item_id,author_type,author_device_id,body,
		created_at,edited_at,deleted_at FROM work_item_comments WHERE deleted_at IS NULL ORDER BY created_at,id`)
	if err != nil {
		return snap, err
	}
	defer rows.Close()
	for rows.Next() {
		var comment Comment
		if err := rows.Scan(&comment.ID, &comment.WorkItemID, &comment.AuthorType, &comment.AuthorDeviceID,
			&comment.Body, &comment.CreatedAt, &comment.EditedAt, &comment.DeletedAt); err != nil {
			return snap, err
		}
		snap.Comments = append(snap.Comments, comment)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT id,work_item_id,session_link_id,request_id,kind,status,
		started_at,finished_at,terminal_reason FROM work_item_runs ORDER BY started_at,id`)
	if err != nil {
		return snap, err
	}
	defer rows.Close()
	for rows.Next() {
		var run Run
		if err := rows.Scan(&run.ID, &run.WorkItemID, &run.SessionLinkID, &run.RequestID,
			&run.Kind, &run.Status, &run.StartedAt, &run.FinishedAt, &run.TerminalReason); err != nil {
			return snap, err
		}
		snap.Runs = append(snap.Runs, run)
	}
	return snap, rows.Err()
}

func (s *Store) requireProject(ctx context.Context, tx *sql.Tx, id string) error {
	var authority string
	if err := tx.QueryRowContext(ctx, "SELECT authority_instance_id FROM work_projects WHERE id=?", id).Scan(&authority); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if authority != s.instanceID {
		return errors.New("project belongs to another authority")
	}
	return nil
}

const itemSelect = `SELECT id,project_id,title,description,lifecycle,priority,sort_key,version,
	activity_revision,blocked_reason_code,blocked_note,created_at,updated_at,accepted_at,cancelled_at,archived_at
	FROM work_items`

type rowScanner interface{ Scan(...any) error }

func scanItem(row rowScanner) (WorkItem, error) {
	var item WorkItem
	err := row.Scan(&item.ID, &item.ProjectID, &item.Title, &item.Description, &item.Lifecycle,
		&item.Priority, &item.SortKey, &item.Version, &item.ActivityRevision,
		&item.BlockedReasonCode, &item.BlockedNote, &item.CreatedAt, &item.UpdatedAt,
		&item.AcceptedAt, &item.CancelledAt, &item.ArchivedAt)
	return item, err
}

func getItemTx(ctx context.Context, tx *sql.Tx, id string) (WorkItem, error) {
	return getItemQuery(ctx, tx, id)
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getItemQuery(ctx context.Context, q queryRower, id string) (WorkItem, error) {
	item, err := scanItem(q.QueryRowContext(ctx, itemSelect+" WHERE id=?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return WorkItem{}, ErrNotFound
	}
	return item, err
}

func recordChange(ctx context.Context, tx *sql.Tx, entity, entityID, kind string, payload any, now int64) (uint64, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO work_changes(entity,entity_id,kind,payload,created_at)
		VALUES(?,?,?,?,?)`, entity, entityID, kind, string(body), now)
	if err != nil {
		return 0, err
	}
	revision, err := result.LastInsertId()
	return uint64(revision), err
}

func updateChangePayload(ctx context.Context, tx *sql.Tx, revision uint64, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE work_changes SET payload=? WHERE revision=?", string(body), revision)
	return err
}

func insertActivity(ctx context.Context, tx *sql.Tx, revision uint64, itemID, kind string, actor Actor, payload any, now int64) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	actorType := actor.Type
	if actorType == "" {
		actorType = ActorSystem
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO work_item_activity
		(revision,work_item_id,kind,actor,payload,created_at) VALUES(?,?,?,?,?,?)`,
		revision, itemID, kind, actorType, string(body), now)
	return err
}

func newID(prefix string) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(b)
}
