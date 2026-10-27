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
func (h *Hub) startRelationAllowed(ctx context.Context, sub messagequeue.NoticeSubscriber, p messagequeue.NativeStart) bool {
	manager, device, generation, ok := h.noticeSubscriberValid(sub)
	if !ok {
		return false
	}
	target, ok := h.registry.Get(p.SessionID)
	if !ok || target.Snapshot().Hidden || target.ResumeID() != p.ThreadID {
		return false
	}
	if conflict, e := h.messageQueue.NativeConflict(p.SessionID, p.RequestID); e != nil || conflict {
		return false
	}
	entry, found, e := h.messageQueue.Get(p.SessionID, p.RequestID)
	if e != nil || !found || entry.State != messagequeue.Running {
		return false
	}
	if p.ObservedAt+180000 <= time.Now().UnixMilli() {
		return false
	}
	if h.dispatches != nil {
		if list, e := h.dispatches.List(ctx, manager.ID); e == nil {
			for _, d := range list {
				if d.SessionID == p.SessionID && d.RequestID == p.RequestID && d.InstanceID == h.cfg.InstanceID && d.ParentThreadID == manager.ResumeID() && d.ParentConfigKnown && d.ParentConfigRevision == manager.SettingsSnapshot().ConfigRevision && d.ThreadID == p.ThreadID {
					g, e := h.dispatches.Grant(ctx, manager.ID)
					return e == nil && g.Allows(h.cfg.InstanceID, h.cfg.InstanceID, p.SessionID) && h.eventSessionAllowed(target)
				}
			}
		}
	}
	if h.delegations != nil {
		if list, e := h.delegations.ListByParent(ctx, manager.ID); e == nil {
			for _, d := range list {
				if d.ChildSessionID == p.SessionID && d.ChildRequestID == p.RequestID && d.SourceThread == manager.ResumeID() {
					return true
				}
			}
		}
	}
	// Ordinary intakes only qualify when an explicit metadata link joins the
	// original same credential command. Merely knowing a session ID is not enough.
	after := ""
	for pages := 0; pages < 17; pages++ {
		items, next, e := h.messageQueue.WorkItems(ctx, sub.Scope, sub.Manager, after, 30)
		if e != nil {
			return false
		}
		for _, item := range items {
			for _, l := range item.Links {
				if l.Kind != "intake" || l.SessionID != p.SessionID || l.RequestID != p.RequestID || l.ThreadID != p.ThreadID {
					continue
				}
				record, e := h.messageQueue.TaskJournal().OriginalCommandForRequest(ctx, p.SessionID, p.RequestID)
				return e == nil && record.ScopeID == "device:"+device && record.Generation == generation && h.eventSessionAllowed(target)
			}
		}
		if next == "" {
			break
		}
		after = next
	}
	return false
}

// Called by the existing controller/coordinator tick: it joins durable facts to
// durable subscriber outboxes. It does not schedule or resume model work.
func (h *Hub) reconcileJarvisStartNotices(ctx context.Context) {
	if h.messageQueue == nil || h.fcm == nil {
		return
	}
	subs, e := h.messageQueue.StartSubscribers(ctx)
	if e != nil {
		return
	}
	starts, e := h.messageQueue.RecentNativeStarts(ctx, time.Now().Add(-3*time.Minute).UnixMilli())
	if e != nil {
		return
	}
	for _, sub := range subs {
		for _, p := range starts {
			if !h.startRelationAllowed(ctx, sub, p) {
				continue
			}
			raw, _ := json.Marshal(p)
			notice := messagequeue.StartPush{Scope: sub.Scope, Manager: sub.Manager, EventID: p.EventID, Payload: raw, ExpiresAt: p.ObservedAt + 180000}
			if e = h.messageQueue.PrepareStartPush(ctx, notice); e != nil {
				continue
			}
			claimed, e := h.messageQueue.ClaimStartPush(ctx, notice)
			if e != nil || !claimed {
				continue
			}
			go h.sendJarvisStartNotice(sub, p, notice)
		}
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
