// Package workitems owns Bridge's human work model. A WorkItem describes an
// outcome; sessions and Codex threads are execution details linked to it.
package workitems

import (
	"errors"
	"fmt"
	"strings"
)

type Lifecycle string

const (
	LifecycleInbox     Lifecycle = "inbox"
	LifecycleReady     Lifecycle = "ready"
	LifecycleActive    Lifecycle = "active"
	LifecycleReview    Lifecycle = "review"
	LifecycleDone      Lifecycle = "done"
	LifecycleCancelled Lifecycle = "cancelled"
)

type ActorType string

const (
	ActorUser    ActorType = "user"
	ActorAgent   ActorType = "agent"
	ActorDesktop ActorType = "desktop"
	ActorMobile  ActorType = "mobile"
	ActorSystem  ActorType = "system"
)

type Priority string

const (
	PriorityNone   Priority = "none"
	PriorityLow    Priority = "low"
	PriorityMedium Priority = "medium"
	PriorityHigh   Priority = "high"
	PriorityUrgent Priority = "urgent"
)

var (
	ErrNotFound          = errors.New("work item not found")
	ErrConflict          = errors.New("work item version conflict")
	ErrInvalidTransition = errors.New("invalid work item lifecycle transition")
	ErrHumanRequired     = errors.New("human acceptance is required")
	ErrDependencyCycle   = errors.New("work item dependency cycle")
	ErrCrossProject      = errors.New("work item relation crosses projects")
	ErrSessionLinked     = errors.New("session is already linked to an active work item")
)

type Actor struct {
	Type     ActorType `json:"type"`
	DeviceID string    `json:"device_id,omitempty"`
}

type Project struct {
	ID                  string `json:"id"`
	AuthorityInstanceID string `json:"authority_instance_id"`
	Name                string `json:"name"`
	WorkspacePath       string `json:"workspace_path,omitempty"`
	Version             uint64 `json:"version"`
	CreatedAt           int64  `json:"created_at"`
	UpdatedAt           int64  `json:"updated_at"`
	ArchivedAt          *int64 `json:"archived_at,omitempty"`
}

type WorkItem struct {
	ID                string    `json:"id"`
	ProjectID         string    `json:"project_id"`
	Title             string    `json:"title"`
	Description       string    `json:"description,omitempty"`
	Lifecycle         Lifecycle `json:"lifecycle"`
	Priority          Priority  `json:"priority"`
	SortKey           int64     `json:"sort_key"`
	Version           uint64    `json:"version"`
	ActivityRevision  uint64    `json:"activity_revision"`
	BlockedReasonCode string    `json:"blocked_reason_code,omitempty"`
	BlockedNote       string    `json:"blocked_note,omitempty"`
	CreatedAt         int64     `json:"created_at"`
	UpdatedAt         int64     `json:"updated_at"`
	AcceptedAt        *int64    `json:"accepted_at,omitempty"`
	CancelledAt       *int64    `json:"cancelled_at,omitempty"`
	ArchivedAt        *int64    `json:"archived_at,omitempty"`
}

type SessionLink struct {
	ID               string `json:"id"`
	WorkItemID       string `json:"work_item_id"`
	SessionID        string `json:"session_id"`
	ThreadIDSnapshot string `json:"thread_id_snapshot,omitempty"`
	Role             string `json:"role"`
	LinkedAt         int64  `json:"linked_at"`
	UnlinkedAt       *int64 `json:"unlinked_at,omitempty"`
}

type Dependency struct {
	WorkItemID string `json:"work_item_id"`
	DependsOn  string `json:"depends_on_id"`
	CreatedAt  int64  `json:"created_at"`
}

type Activity struct {
	Revision   uint64    `json:"revision"`
	WorkItemID string    `json:"work_item_id"`
	Kind       string    `json:"kind"`
	Actor      ActorType `json:"actor"`
	Payload    string    `json:"payload"`
	CreatedAt  int64     `json:"created_at"`
}

type Change struct {
	Revision  uint64 `json:"revision"`
	Entity    string `json:"entity"`
	EntityID  string `json:"entity_id"`
	Kind      string `json:"kind"`
	Payload   string `json:"payload"`
	CreatedAt int64  `json:"created_at"`
}

type Snapshot struct {
	Revision     uint64        `json:"revision"`
	Projects     []Project     `json:"projects"`
	Items        []WorkItem    `json:"items"`
	SessionLinks []SessionLink `json:"session_links"`
	Dependencies []Dependency  `json:"dependencies"`
}

type ConflictError struct {
	Expected uint64
	Current  WorkItem
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("%v: expected %d, current %d", ErrConflict, e.Expected, e.Current.Version)
}

func (e *ConflictError) Unwrap() error { return ErrConflict }

func validLifecycle(v Lifecycle) bool {
	switch v {
	case LifecycleInbox, LifecycleReady, LifecycleActive, LifecycleReview, LifecycleDone, LifecycleCancelled:
		return true
	default:
		return false
	}
}

func validPriority(v Priority) bool {
	switch v {
	case PriorityNone, PriorityLow, PriorityMedium, PriorityHigh, PriorityUrgent:
		return true
	default:
		return false
	}
}

func validateTransition(from, to Lifecycle, actor Actor) error {
	if !validLifecycle(to) {
		return fmt.Errorf("%w: unknown target %q", ErrInvalidTransition, to)
	}
	if from == to {
		return nil
	}
	allowed := map[Lifecycle]map[Lifecycle]bool{
		LifecycleInbox:     {LifecycleReady: true, LifecycleCancelled: true},
		LifecycleReady:     {LifecycleInbox: true, LifecycleActive: true, LifecycleCancelled: true},
		LifecycleActive:    {LifecycleReady: true, LifecycleReview: true, LifecycleCancelled: true},
		LifecycleReview:    {LifecycleActive: true, LifecycleDone: true, LifecycleCancelled: true},
		LifecycleDone:      {LifecycleActive: true},
		LifecycleCancelled: {LifecycleInbox: true},
	}
	if !allowed[from][to] {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	if to == LifecycleDone && actor.Type != ActorUser && actor.Type != ActorDesktop && actor.Type != ActorMobile {
		return ErrHumanRequired
	}
	return nil
}

func normalizeTitle(title string) (string, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return "", errors.New("work item title is required")
	}
	if len([]rune(title)) > 240 {
		return "", errors.New("work item title exceeds 240 characters")
	}
	return title, nil
}
