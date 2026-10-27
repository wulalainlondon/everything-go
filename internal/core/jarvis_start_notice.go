package core

import (
	"context"
	"encoding/json"
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
func (h *Hub) noticeSubscriberValid(sub messagequeue.NoticeSubscriber) (*session.Session, string, uint64, bool) {
	current, _, err := h.messageQueue.CurrentNoticePolicy(context.Background(), sub.Scope, sub.Manager)
	if err != nil || !current.Enabled || string(current.Input) != string(sub.Input) {
		return nil, "", 0, false
	}
	parts := strings.Split(sub.Scope, "\x00")
	if len(parts) != 2 {
		return nil, "", 0, false
	}
	generation, e := strconv.ParseUint(parts[1], 10, 64)
	if e != nil || !h.jarvisCredentialCurrent(parts[0], generation) {
		return nil, "", 0, false
	}
	var in jarvisNoticePolicy
	if json.Unmarshal(sub.Input, &in) != nil || !in.Enabled || in.PushSchemaVersion != 1 || in.Authority != h.cfg.InstanceID || in.Manager.Authority != h.cfg.InstanceID {
		return nil, "", 0, false
	}
	s, ok := h.registry.Get(sub.Manager)
	if !ok || !h.eventSessionAllowed(s) || s.ResumeID() != in.Manager.ThreadID || s.SettingsSnapshot().ConfigRevision != in.Manager.Revision {
		return nil, "", 0, false
	}
	return s, parts[0], generation, true
}
func (h *Hub) startRelationRelevant(ctx context.Context, sub messagequeue.NoticeSubscriber, p messagequeue.NativeStart) bool {
	manager, device, generation, ok := h.noticeSubscriberValid(sub)
	if !ok {
		return false
	}
	target, ok := h.registry.Get(p.SessionID)
	if !ok || target.Snapshot().Hidden {
		return false
	}
	if h.dispatches != nil {
		d, found, e := h.dispatches.ByTargetRequest(ctx, manager.ID, p.SessionID, p.RequestID)
		if e == nil && found && d.InstanceID == h.cfg.InstanceID && d.ParentThreadID == manager.ResumeID() && d.ParentConfigKnown && d.ParentConfigRevision == manager.SettingsSnapshot().ConfigRevision && d.ThreadID == p.ThreadID && d.ConfigRevision == target.SettingsSnapshot().ConfigRevision {
			g, e := h.dispatches.Grant(ctx, manager.ID)
			return e == nil && g.Allows(h.cfg.InstanceID, h.cfg.InstanceID, p.SessionID) && h.eventSessionAllowed(target)
		}
	}
	if h.delegations != nil {
		d, found, e := h.delegations.ByChildRequest(ctx, p.SessionID, p.RequestID)

		if e == nil && found && d.ParentSessionID == manager.ID {
			sourceThread, sourceRevision, bound, e := h.delegations.SourceBinding(ctx, d.ID)
			if e != nil || !bound || sourceThread != manager.ResumeID() || sourceRevision != manager.SettingsSnapshot().ConfigRevision {
				return false
			}
			thread, revision, _, e := h.delegations.ExecutionBinding(ctx, d.ID)
			return e == nil && thread == p.ThreadID && revision == target.SettingsSnapshot().ConfigRevision
		}

	}
	// Manager ordinary intakes are first-class source records. Other intakes
	// additionally require an explicit original metadata link, queried by exact
	// JSON identity rather than a latest-N scan.
	linked := p.SessionID == manager.ID
	if !linked {
		var e error
		linked, e = h.messageQueue.HasWorkExecutionLink(ctx, sub.Scope, sub.Manager, p.SessionID, p.RequestID)
		if e != nil {
			return false
		}
	}
	if !linked || !h.eventSessionAllowed(target) {
		return false
	}
	record, e := h.messageQueue.TaskJournal().OriginalCommandForRequest(ctx, p.SessionID, p.RequestID)
	if e != nil || record.ScopeID != "device:"+device || record.Generation != generation {
		return false
	}
	var meta apiMetadata
	var input intakeInput
	if json.Unmarshal(record.Metadata, &meta) != nil || meta.Intake == nil || json.Unmarshal(meta.Request.Input, &input) != nil {
		return false
	}
	if input.Target.Thread != nil && *input.Target.Thread != "" && *input.Target.Thread != p.ThreadID {
		return false
	}
	return input.Target.Revision == target.SettingsSnapshot().ConfigRevision
}
func (h *Hub) startRelationAllowed(ctx context.Context, sub messagequeue.NoticeSubscriber, p messagequeue.NativeStart) bool {
	if !h.startRelationRelevant(ctx, sub, p) || p.ObservedAt+180000 <= time.Now().UnixMilli() {
		return false
	}
	target, ok := h.registry.Get(p.SessionID)
	if !ok || target.ResumeID() != p.ThreadID {
		return false
	}
	if conflict, e := h.messageQueue.NativeConflict(p.SessionID, p.RequestID); e != nil || conflict {
		return false
	}
	entry, found, e := h.messageQueue.Get(p.SessionID, p.RequestID)
	if e != nil || !found || entry.State != messagequeue.Running {
		return false
	}
	// Relation/queue reads can wait on the original stores. Recheck the
	// credential, selected source and target locally after those waits.
	_, _, _, current := h.noticeSubscriberValid(sub)
	return current && target.ResumeID() == p.ThreadID && !target.Snapshot().Hidden && p.ObservedAt+180000 > time.Now().UnixMilli()
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
			if h.startRelationRelevant(ctx, sub, p.Start) {
				outcome = "prepared"
				if p.Start.ObservedAt+180000 <= time.Now().UnixMilli() {
					outcome = "expired"
				} else if !h.startRelationAllowed(ctx, sub, p.Start) {
					outcome = "terminal_or_scope_changed_before_delivery"
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
		if !h.startRelationAllowed(ctx, sub, p) {
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
