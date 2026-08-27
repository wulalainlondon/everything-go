package core

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"

	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/workitems"
)

func (h *Hub) sendWorkSnapshot(c *Client) {
	if h.work == nil {
		return
	}
	snapshot, err := h.work.SnapshotForDevice(context.Background(), c.deviceID)
	if err != nil {
		c.enqueueEvent(protocol.WorkError{Type: "work_error", Code: "snapshot_failed", Message: err.Error()})
		return
	}
	c.enqueueEvent(protocol.NewWorkSnapshot(snapshot))
	_ = h.work.AckSync(context.Background(), c.deviceID, snapshot.Revision, 0)
}

func (h *Hub) sendWorkSync(c *Client, since uint64) {
	if h.work == nil {
		c.enqueueEvent(protocol.WorkError{Type: "work_error", Code: "unsupported", Message: "native work items are unavailable"})
		return
	}
	changes, latest, compacted, err := h.work.ChangesSince(context.Background(), since, 256)
	if err != nil {
		c.enqueueEvent(protocol.WorkError{Type: "work_error", Code: "sync_failed", Message: err.Error()})
		return
	}
	if compacted || since == 0 {
		h.sendWorkSnapshot(c)
		return
	}
	event := protocol.NewWorkDeltaBatch(since, latest, changes)
	c.enqueueEvent(event)
	_ = h.work.AckSync(context.Background(), c.deviceID, event.ToRevision, 0)
}

func (h *Hub) handleWorkSyncAck(c *Client, cmd clientproto.Command) {
	if h.work == nil {
		return
	}
	delivered := cmd.DeliveredRevision
	acked := cmd.AckedRevision
	if delivered == 0 {
		delivered = cmd.Revision
	}
	if acked == 0 {
		acked = cmd.Revision
	}
	if err := h.work.AckSync(context.Background(), c.deviceID, delivered, acked); err != nil {
		c.enqueueEvent(protocol.WorkError{Type: "work_error", Code: "ack_failed", Message: err.Error()})
	}
}

func (h *Hub) handleWorkCommand(c *Client, cmd clientproto.Command) {
	if h.work == nil {
		c.enqueueEvent(protocol.WorkError{Type: "work_error", MutationID: cmd.MutationID,
			Code: "unsupported", Message: "native work items are unavailable"})
		return
	}
	ctx := context.Background()
	actor := workitems.Actor{Type: workitems.ActorUser, DeviceID: c.deviceID}
	if cmd.Kind != "work_item_read" {
		if strings.TrimSpace(c.deviceID) == "" || strings.TrimSpace(cmd.MutationID) == "" {
			h.sendWorkError(c, cmd.MutationID, errors.New("device id and mutation id are required"))
			return
		}
		h.workMutationMu.Lock()
		defer h.workMutationMu.Unlock()
		cached, ok, err := h.work.MutationResponse(ctx, c.deviceID, cmd.MutationID)
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		if ok {
			c.enqueue(cached)
			return
		}
	}
	switch cmd.Kind {
	case "work_project_create":
		project, err := h.work.CreateProject(ctx, workitems.CreateProjectInput{
			ID: cmd.ProjectID, Name: cmd.Name, WorkspacePath: cmd.WorkspacePath,
		})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		snapshot, _ := h.work.SnapshotForDevice(ctx, c.deviceID)
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: project.Version, Revision: snapshot.Revision, Project: &project})

	case "work_item_create":
		item, err := h.work.CreateItem(ctx, workitems.CreateItemInput{
			ID: cmd.WorkItemID, ProjectID: cmd.ProjectID, Title: cmd.Title,
			Description: cmd.Description, Priority: workitems.Priority(cmd.Priority),
			SortKey: cmd.SortKey, Actor: actor,
		})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item})

	case "work_item_update":
		if len(cmd.Fields) == 0 {
			h.sendWorkError(c, cmd.MutationID, errors.New("work item update requires fields"))
			return
		}
		input := workitems.UpdateItemInput{ID: cmd.WorkItemID, ExpectedVersion: cmd.ExpectedVersion, Actor: actor}
		for _, field := range cmd.Fields {
			switch field {
			case "title":
				input.Title = &cmd.Title
			case "description":
				input.Description = &cmd.Description
			case "priority":
				priority := workitems.Priority(cmd.Priority)
				input.Priority = &priority
			case "blocked_reason_code":
				input.BlockedReasonCode = &cmd.BlockedReasonCode
			case "blocked_note":
				input.BlockedNote = &cmd.BlockedNote
			default:
				h.sendWorkError(c, cmd.MutationID, errors.New("unsupported work item field: "+field))
				return
			}
		}
		item, err := h.work.UpdateItem(ctx, input)
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item})

	case "work_item_move":
		item, err := h.work.MoveItem(ctx, workitems.MoveItemInput{ID: cmd.WorkItemID,
			ExpectedVersion: cmd.ExpectedVersion, Lifecycle: workitems.Lifecycle(cmd.Lifecycle),
			SortKey: cmd.SortKey, Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item})

	case "work_item_archive", "work_item_restore":
		item, err := h.work.ArchiveItem(ctx, workitems.ArchiveItemInput{ID: cmd.WorkItemID,
			ExpectedVersion: cmd.ExpectedVersion, Restore: cmd.Kind == "work_item_restore", Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item})

	case "work_item_link_session":
		if _, ok := h.registry.Get(cmd.SessionID); !ok {
			h.sendWorkError(c, cmd.MutationID, errors.New("cannot link an unknown Bridge session"))
			return
		}
		link, item, err := h.work.LinkSession(ctx, workitems.LinkSessionInput{
			WorkItemID: cmd.WorkItemID, SessionID: cmd.SessionID, ThreadIDSnapshot: cmd.ThreadID,
			Role: cmd.Role, ExpectedVersion: cmd.ExpectedVersion, Actor: actor,
		})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item, Link: &link})

	case "work_item_unlink_session":
		link, item, err := h.work.UnlinkSession(ctx, workitems.UnlinkSessionInput{LinkID: cmd.WorkLinkID,
			ExpectedVersion: cmd.ExpectedVersion, Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item, Link: &link})

	case "work_item_dependency_add":
		item, err := h.work.AddDependency(ctx, workitems.AddDependencyInput{WorkItemID: cmd.WorkItemID,
			DependsOnID: cmd.DependsOnID, ExpectedVersion: cmd.ExpectedVersion, Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item})

	case "work_item_dependency_remove":
		item, err := h.work.RemoveDependency(ctx, workitems.AddDependencyInput{WorkItemID: cmd.WorkItemID,
			DependsOnID: cmd.DependsOnID, ExpectedVersion: cmd.ExpectedVersion, Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item})

	case "work_item_comment_add":
		comment, item, err := h.work.AddComment(ctx, workitems.AddCommentInput{ID: cmd.CommentID,
			WorkItemID: cmd.WorkItemID, ExpectedVersion: cmd.ExpectedVersion, Body: cmd.Body, Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision,
			Item: &item, Comment: &comment})

	case "work_item_comment_edit":
		comment, item, err := h.work.EditComment(ctx, workitems.EditCommentInput{CommentID: cmd.CommentID,
			ExpectedVersion: cmd.ExpectedVersion, Body: cmd.Body, Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision,
			Item: &item, Comment: &comment})

	case "work_item_read":
		view, err := h.work.MarkRead(ctx, c.deviceID, cmd.WorkItemID, cmd.Revision)
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		c.enqueueEvent(protocol.WorkReadAck{Type: "work_read_ack", Item: view})
	}
}

func (h *Hub) completeWorkMutation(c *Client, mutationID string, event protocol.WorkMutationAck) {
	raw, err := json.Marshal(event)
	if err != nil {
		h.sendWorkError(c, mutationID, err)
		return
	}
	if err := h.work.RememberMutation(context.Background(), c.deviceID, mutationID, raw); err != nil {
		h.sendWorkError(c, mutationID, err)
		return
	}
	c.enqueue(raw)
	h.broadcastWorkRevision(event.Revision)
}

func (h *Hub) sendWorkError(c *Client, mutationID string, err error) {
	var conflict *workitems.ConflictError
	if errors.As(err, &conflict) {
		c.enqueueEvent(protocol.WorkConflict{Type: "work_conflict", MutationID: mutationID,
			Reason: "version_conflict", Current: conflict.Current})
		return
	}
	code := "invalid_request"
	switch {
	case errors.Is(err, workitems.ErrNotFound):
		code = "not_found"
	case errors.Is(err, workitems.ErrHumanRequired):
		code = "human_acceptance_required"
	case errors.Is(err, workitems.ErrDependencyCycle):
		code = "dependency_cycle"
	case errors.Is(err, workitems.ErrCrossProject):
		code = "cross_project_relation"
	case errors.Is(err, workitems.ErrSessionLinked):
		code = "session_already_linked"
	case errors.Is(err, workitems.ErrInvalidTransition):
		code = "invalid_transition"
	}
	c.enqueueEvent(protocol.WorkError{Type: "work_error", MutationID: mutationID, Code: code, Message: err.Error()})
}

func (h *Hub) broadcastWorkRevision(revision uint64) {
	if h.work == nil || revision == 0 {
		return
	}
	changes, latest, compacted, err := h.work.ChangesSince(context.Background(), revision-1, 16)
	if err != nil || compacted || len(changes) == 0 {
		if err != nil {
			log.Printf("[work] broadcast delta revision=%d: %v", revision, err)
		}
		return
	}
	event := protocol.NewWorkDeltaBatch(revision-1, latest, changes)
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for client := range h.clients {
		if !client.enrollmentOnly && strings.TrimSpace(client.deviceID) != "" {
			clients = append(clients, client)
		}
	}
	h.mu.RUnlock()
	for _, client := range clients {
		client.enqueueEvent(event)
		_ = h.work.AckSync(context.Background(), client.deviceID, event.ToRevision, 0)
	}
}
