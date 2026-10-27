package core

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/eventinbox"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type jarvisNoticePolicy struct {
	jarvisWorkScope
	IntentID          string `json:"intent_id"`
	ExpectedRevision  uint64 `json:"expected_revision"`
	Enabled           bool   `json:"enabled"`
	PushSchemaVersion int    `json:"push_schema_version"`
}

func (h *Hub) serveNoticePolicy(w http.ResponseWriter, r *http.Request, device string) {
	var in jarvisNoticePolicy
	if !jarvisDecode(w, r, &in) {
		return
	}
	manager, scope, e := h.workManager(r, device, in.jarvisWorkScope)
	if e != nil {
		http.Error(w, e.Error(), 403)
		return
	}
	if !jarvisID.MatchString(in.IntentID) || in.PushSchemaVersion != 1 {
		http.Error(w, "original policy intent and native schema1 required", 400)
		return
	}
	raw, _ := json.Marshal(in)
	v, e := h.messageQueue.SetNoticePolicy(r.Context(), messagequeue.NoticeSubscriber{Scope: scope, Manager: manager.ID, Input: raw, Enabled: in.Enabled}, in.IntentID, in.ExpectedRevision)
	if e != nil {
		http.Error(w, e.Error(), 409)
		return
	}
	if !h.workScopeCurrent(r, device, in.jarvisWorkScope, manager) {
		http.Error(w, "scope changed; lookup original", 403)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"schema_version": 1, "authority_instance_id": h.cfg.InstanceID, "subscription": v, "provider_sent": false})
}

// A failed original-store read is unknown, never proof that a fact is unrelated.
func (h *Hub) noticeSubscriberState(sub messagequeue.NoticeSubscriber) (*session.Session, string, uint64, bool, error) {
	current, _, err := h.messageQueue.CurrentNoticePolicy(context.Background(), sub.Scope, sub.Manager)
	if err != nil {
		return nil, "", 0, false, err
	}
	if !current.Enabled || string(current.Input) != string(sub.Input) {
		return nil, "", 0, false, nil
	}
	parts := strings.Split(sub.Scope, "\x00")
	if len(parts) != 2 {
		return nil, "", 0, false, nil
	}
	generation, e := strconv.ParseUint(parts[1], 10, 64)
	if e != nil || !h.jarvisCredentialCurrent(parts[0], generation) {
		return nil, "", 0, false, nil
	}
	var in jarvisNoticePolicy
	if json.Unmarshal(sub.Input, &in) != nil || !in.Enabled || in.PushSchemaVersion != 1 || in.Authority != h.cfg.InstanceID || in.Manager.Authority != h.cfg.InstanceID {
		return nil, "", 0, false, nil
	}
	manager, ok := h.registry.Get(sub.Manager)
	if !ok || !h.eventSessionAllowed(manager) || manager.ResumeID() != in.Manager.ThreadID || manager.SettingsSnapshot().ConfigRevision != in.Manager.Revision {
		return nil, "", 0, false, nil
	}
	return manager, parts[0], generation, true, nil
}
func (h *Hub) noticeSubscriberValid(sub messagequeue.NoticeSubscriber) (*session.Session, string, uint64, bool) {
	s, d, g, ok, _ := h.noticeSubscriberState(sub)
	return s, d, g, ok
}
func (h *Hub) startRelationRelevant(ctx context.Context, sub messagequeue.NoticeSubscriber, p messagequeue.NativeStart) bool {
	ok, _ := h.startRelationDecision(ctx, sub, p)
	return ok
}
func (h *Hub) startIntakeDecision(ctx context.Context, device string, generation uint64, target *session.Session, p messagequeue.NativeStart) (bool, error) {
	if !h.eventSessionAllowed(target) {
		return false, nil
	}
	record, e := h.messageQueue.TaskJournal().OriginalCommandForRequest(ctx, p.SessionID, p.RequestID)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if record.Expired || record.ScopeID != "device:"+device || record.Generation != generation {
		return false, nil
	}
	var meta apiMetadata
	var input intakeInput
	if json.Unmarshal(record.Metadata, &meta) != nil {
		return false, errors.New("original_intake_metadata_unreadable")
	}
	if meta.Intake == nil {
		return false, nil
	}
	if json.Unmarshal(meta.Request.Input, &input) != nil {
		return false, errors.New("original_intake_input_unreadable")
	}
	if input.Target.Thread != nil && *input.Target.Thread != "" && *input.Target.Thread != p.ThreadID {
		return false, nil
	}
	return input.Target.Revision == target.SettingsSnapshot().ConfigRevision, nil
}
func (h *Hub) startAssignmentDecision(ctx context.Context, device string, generation uint64, target *session.Session, p messagequeue.NativeStart, l messagequeue.WorkLink) (bool, error) {
	if h.automation == nil || h.events == nil {
		return false, errors.New("original_assignment_source_unavailable")
	}
	a, e := h.automation.EventAssignment(ctx, p.RequestID)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	g, found, e := h.automation.JarvisAssignmentGeneration(ctx, a.ID)
	if e != nil {
		return false, e
	}
	if !found || g != generation || a.OwnerDevice != device || a.SessionID != p.SessionID || a.ThreadID != p.ThreadID || a.ConfigRevision != target.SettingsSnapshot().ConfigRevision || l.Authority != h.cfg.InstanceID || l.ThreadID != a.ThreadID || l.ConfigRevision != a.ConfigRevision || !h.eventSessionAllowed(target) {
		return false, nil
	}
	for _, ref := range assignmentEventRefs(a) {
		event, e := h.events.Get(ctx, ref.ID)
		if errors.Is(e, eventinbox.ErrNotFound) {
			return false, nil
		}
		if e != nil {
			return false, e
		}
		if event.Sequence != ref.Revision || event.AuthorityInstanceID != h.cfg.InstanceID || event.ExpiresAt > 0 && event.ExpiresAt <= time.Now().UnixMilli() || strings.HasPrefix(event.Source, lineSourcePrefix) {
			return false, nil
		}
	}
	return true, nil
}
func (h *Hub) startWorkspaceDecision(ctx context.Context, device string, generation uint64, p messagequeue.NativeStart, l messagequeue.WorkLink) (bool, error) {
	if h.workspace == nil {
		return false, errors.New("original_workspace_source_unavailable")
	}
	record, e := h.messageQueue.TaskJournal().OriginalCommandForRequest(ctx, p.SessionID, p.RequestID)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if record.Expired {
		return false, nil
	}
	var meta apiMetadata
	if json.Unmarshal(record.Metadata, &meta) != nil {
		return false, errors.New("original_workspace_metadata_unreadable")
	}
	if meta.WorkspaceRoomID == "" || record.NativeID != l.NativeID || l.Authority != h.cfg.InstanceID {
		return false, nil
	}
	token := ""
	for _, binding := range h.pairing.DeviceBindings() {
		if binding.DeviceID == device && jarvisTokenGeneration(binding.Token) == generation {
			token = binding.Token
			break
		}
	}
	if token == "" {
		return false, nil
	}
	digest := sha256.Sum256([]byte(token))
	actor := h.workspace.Owner(device, hex.EncodeToString(digest[:]))
	task, allowed, e := h.workspace.PairedOwnerTaskReadState(ctx, actor, meta.WorkspaceRoomID, meta.WorkspaceTaskID)
	if e != nil {
		return false, e
	}
	return allowed && task.SessionID == p.SessionID && task.RequestID == p.RequestID, nil
}
func (h *Hub) startRelationDecision(ctx context.Context, sub messagequeue.NoticeSubscriber, p messagequeue.NativeStart) (bool, error) {
	manager, device, generation, ok, e := h.noticeSubscriberState(sub)
	if e != nil || !ok {
		return false, e
	}
	target, ok := h.registry.Get(p.SessionID)
	if !ok || target.Snapshot().Hidden {
		return false, nil
	}
	if p.SessionID == manager.ID {
		return h.startIntakeDecision(ctx, device, generation, target, p)
	}
	links, e := h.messageQueue.WorkExecutionLinks(ctx, sub.Scope, sub.Manager, p.SessionID, p.RequestID)
	if e != nil {
		return false, e
	}
	var unknown error
	intakeLinked := false
	for _, l := range links {
		var allowed bool
		var err error
		switch l.Kind {
		case "assignment":
			allowed, err = h.startAssignmentDecision(ctx, device, generation, target, p, l)
		case "workspace":
			allowed, err = h.startWorkspaceDecision(ctx, device, generation, p, l)
		case "intake":
			intakeLinked = true
		}
		if allowed {
			return true, nil
		}
		if err != nil {
			unknown = err
		}
	}
	if h.delegations != nil {
		d, found, err := h.delegations.ByChildRequest(ctx, p.SessionID, p.RequestID)
		if err != nil {
			unknown = err
		} else if found && d.ParentSessionID == manager.ID {
			thread, revision, bound, err := h.delegations.SourceBinding(ctx, d.ID)
			if err != nil {
				unknown = err
			} else if bound && thread == manager.ResumeID() && revision == manager.SettingsSnapshot().ConfigRevision {
				thread, revision, _, err = h.delegations.ExecutionBinding(ctx, d.ID)
				if err != nil {
					unknown = err
				} else if thread == p.ThreadID && revision == target.SettingsSnapshot().ConfigRevision {
					return true, nil
				}
			}
		}
	}
	if h.dispatches != nil {
		d, found, err := h.dispatches.ByTargetRequest(ctx, manager.ID, p.SessionID, p.RequestID)
		if err != nil {
			unknown = err
		} else if found && d.InstanceID == h.cfg.InstanceID && d.ParentThreadID == manager.ResumeID() && d.ParentConfigKnown && d.ParentConfigRevision == manager.SettingsSnapshot().ConfigRevision && d.ThreadID == p.ThreadID && d.ConfigRevision == target.SettingsSnapshot().ConfigRevision {
			g, err := h.dispatches.Grant(ctx, manager.ID)
			if err != nil {
				unknown = err
			} else if g.Allows(h.cfg.InstanceID, h.cfg.InstanceID, p.SessionID) && h.eventSessionAllowed(target) {
				return true, nil
			}
		}
	}
	if intakeLinked {
		allowed, err := h.startIntakeDecision(ctx, device, generation, target, p)
		if allowed || err != nil {
			return allowed, err
		}
	}
	return false, unknown
}
func (h *Hub) startDeliveryDecision(ctx context.Context, sub messagequeue.NoticeSubscriber, p messagequeue.NativeStart) (bool, error) {
	related, e := h.startRelationDecision(ctx, sub, p)
	if e != nil || !related {
		return false, e
	}
	if p.ObservedAt+180000 <= time.Now().UnixMilli() {
		return false, nil
	}
	target, ok := h.registry.Get(p.SessionID)
	if !ok || target.ResumeID() != p.ThreadID {
		return false, nil
	}
	conflict, e := h.messageQueue.NativeConflict(p.SessionID, p.RequestID)
	if e != nil {
		return false, e
	}
	if conflict {
		return false, nil
	}
	entry, found, e := h.messageQueue.Get(p.SessionID, p.RequestID)
	if e != nil {
		return false, e
	}
	if !found {
		return false, errors.New("native_fact_original_queue_unavailable")
	}
	if entry.State != messagequeue.Running {
		return false, nil
	}
	_, _, _, current, e := h.noticeSubscriberState(sub)
	if e != nil {
		return false, e
	}
	return current && target.ResumeID() == p.ThreadID && !target.Snapshot().Hidden && p.ObservedAt+180000 > time.Now().UnixMilli(), nil
}
func (h *Hub) startRelationAllowed(ctx context.Context, sub messagequeue.NoticeSubscriber, p messagequeue.NativeStart) bool {
	ok, _ := h.startDeliveryDecision(ctx, sub, p)
	return ok
}

// The existing coordinator tick advances durable bounded source sequences. A
// crash before/after a join can repeat the read, never the outbox's effect.
func (h *Hub) reconcileJarvisStartNotices(ctx context.Context) {
	if h.messageQueue == nil || h.fcm == nil {
		return
	}
	subs, e := h.messageQueue.StartJoinSubscribers(ctx, 20)
	if e != nil {
		return
	}
	for _, sub := range subs {
		facts, e := h.messageQueue.StartJoinFacts(ctx, sub, 100)
		if e != nil {
			continue
		}
		for _, p := range facts {
			outcome := ""
			related, err := h.startRelationDecision(ctx, sub, p.Start)
			if err != nil {
				break
			} // Unknown source: do not consume its durable sequence.
			if related {
				outcome = "prepared"
				if p.Start.ObservedAt+180000 <= time.Now().UnixMilli() {
					outcome = "expired"
				} else {
					allowed, err := h.startDeliveryDecision(ctx, sub, p.Start)
					if err != nil {
						break
					}
					if !allowed {
						outcome = "terminal_or_scope_changed_before_delivery"
					}
				}
			}
			if e = h.messageQueue.CommitStartJoin(ctx, sub, p, outcome); e != nil {
				break
			}
		}
	}
	prepared, e := h.messageQueue.PreparedStartPushes(ctx)
	if e != nil {
		return
	}
	for _, notice := range prepared {
		sub, _, err := h.messageQueue.CurrentNoticePolicy(ctx, notice.Scope, notice.Manager)
		var p messagequeue.NativeStart
		if err != nil || json.Unmarshal(notice.Payload, &p) != nil {
			continue
		}
		allowed, deliveryErr := h.startDeliveryDecision(ctx, sub, p)
		if deliveryErr != nil {
			continue
		} // Prepared remains retryable; issued never replays.
		if !allowed {
			state := "revoked_or_terminal"
			if notice.ExpiresAt <= time.Now().UnixMilli() {
				state = "expired"
			}
			_ = h.messageQueue.RetirePreparedStartPush(ctx, notice, state)
			continue
		}
		claimed, err := h.messageQueue.ClaimStartPush(ctx, notice)
		if err != nil || !claimed {
			continue
		}
		go h.sendJarvisStartNotice(sub, p, notice)
	}
}

func (h *Hub) sendJarvisStartNotice(sub messagequeue.NoticeSubscriber, p messagequeue.NativeStart, notice messagequeue.StartPush) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, device, _, ok := h.noticeSubscriberValid(sub)
	if !ok {
		raw, _ := json.Marshal(map[string]string{"state": "revoked"})
		_ = h.messageQueue.FinishStartPush(ctx, notice, "revoked", raw)
		return
	}
	operation := fmt.Sprintf("%d_%s", p.ObservedAt/1000, jarvisHash([]string{sub.Scope, sub.Manager, p.EventID})[:40])
	receipt := h.fcm.SendJarvisStart(ctx, operation, h.cfg.InstanceID, sub.Manager, p.SessionID, p.RequestID, p.EventID, device, notice.ExpiresAt, func() bool { return h.startRelationAllowed(ctx, sub, p) })
	raw, _ := json.Marshal(receipt)
	_ = h.messageQueue.FinishStartPush(context.Background(), notice, receipt.State, raw)
}
