package goexec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"time"

	"everything-go/internal/recovery"
	"everything-go/internal/session"
)

const automaticContinuation = "上一輪因已確認的暫時連線或容量錯誤終止。請先核對原任務、已完成項目、工具結果與仍在執行的操作，只接續尚未完成部分，依原驗收條件驗證並回報；若需要使用者回答或權限，請停止並說明。"

// Read the latest exact canonical turn and every item page. We deliberately do
// not treat opaque exec/MCP/dynamic tool results as proof of no external effect.
func (c *Codex) automaticCanonical(s *session.Session, st *codexState, turn string) (recovery.CanonicalEvidence, error) {
	p := recovery.CanonicalEvidence{Thread: s.ResumeID(), Turn: turn}
	if c.hasPendingInteraction(s.ID) {
		p.WaitingUser = true
		return p, recovery.ErrGuard
	}
	epoch := c.rpc.transportEpoch.Load()
	admission, e := c.readStartAdmission(s, st, st.reqID)
	if e != nil {
		return p, e
	}
	if admission.Epoch != epoch {
		return p, recovery.ErrGuard
	}
	p.Idle = true
	raw, e := c.rpc.requestAtEpoch("thread/turns/list", map[string]any{"threadId": p.Thread, "limit": 1, "sortDirection": "desc", "itemsView": "full"}, 15*time.Second, &epoch)
	if e != nil || len(raw) > 8<<20 {
		return p, recovery.ErrGuard
	}
	var page struct {
		Data []json.RawMessage `json:"data"`
		Next *string           `json:"nextCursor"`
	}
	if _, e = automaticCanonicalJSON(raw); e != nil {
		return p, e
	}
	if json.Unmarshal(raw, &page) != nil || len(page.Data) != 1 {
		return p, recovery.ErrGuard
	}
	var parent struct {
		ID        string            `json:"id"`
		Status    string            `json:"status"`
		ItemsView string            `json:"itemsView"`
		Items     []json.RawMessage `json:"items"`
	}
	if json.Unmarshal(page.Data[0], &parent) != nil || parent.ID != turn || parent.Status != "failed" || parent.ItemsView != "full" || parent.Items == nil {
		return p, recovery.ErrGuard
	}
	p.Status = "failed"
	// The cursor on a one-turn page refers to older turns. All items of THIS
	// turn are separately paged by the exact current public thread/items/list.
	cursor := ""
	seen := map[string]any{}
	var itemEvidence []json.RawMessage
	progress := []json.RawMessage{}
	for pages := 0; pages < 64; pages++ {
		params := map[string]any{"threadId": p.Thread, "turnId": turn, "limit": 100, "sortDirection": "asc"}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, e = c.rpc.requestAtEpoch("thread/items/list", params, 15*time.Second, &epoch)
		if e != nil || len(raw) > 8<<20 {
			return p, recovery.ErrGuard
		}
		var items struct {
			Data []struct {
				Item json.RawMessage `json:"item"`
				Turn string          `json:"turnId"`
			} `json:"data"`
			Next *string `json:"nextCursor"`
		}
		if _, e = automaticCanonicalJSON(raw); e != nil {
			return p, e
		}
		if json.Unmarshal(raw, &items) != nil || items.Data == nil {
			return p, recovery.ErrGuard
		}
		for _, entry := range items.Data {
			if entry.Turn != turn {
				return p, recovery.ErrGuard
			}
			var item struct {
				Type  string `json:"type"`
				Phase string `json:"phase"`
				ID    string `json:"id"`
			}
			if json.Unmarshal(entry.Item, &item) != nil || item.ID == "" {
				return p, recovery.ErrGuard
			}
			if _, exists := seen[item.ID]; exists {
				return p, recovery.ErrGuard
			}
			value, er := automaticCanonicalItem(entry.Item)
			if er != nil {
				return p, er
			}
			seen[item.ID] = value
			switch item.Type {
			case "userMessage", "agentMessage", "reasoning":
				// Conversation content alone cannot prove externally delivered artifacts.
				if item.Type == "agentMessage" && item.Phase == "final_answer" {
					progress = append(progress, entry.Item)
				}
			default:
				return p, errors.New("canonical tool/background/provider coverage not independently verifiable")
			}
			itemEvidence = append(itemEvidence, entry.Item)
		}
		if items.Next == nil || *items.Next == "" {
			p.Complete = true
			break
		}
		if *items.Next == cursor {
			return p, recovery.ErrGuard
		}
		cursor = *items.Next
	}
	if !p.Complete || c.rpc.transportEpoch.Load() != epoch {
		return p, recovery.ErrGuard
	}
	if _, e = c.readStartAdmission(s, st, st.reqID); e != nil {
		return p, e
	}
	if len(parent.Items) != len(itemEvidence) {
		return p, recovery.ErrGuard
	}
	canonicalIDs := map[string]bool{}
	for _, raw := range parent.Items {
		var it struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(raw, &it) != nil || it.ID == "" || canonicalIDs[it.ID] {
			return p, recovery.ErrGuard
		}
		full, er := automaticCanonicalItem(raw)
		paged, exists := seen[it.ID]
		if er != nil || !exists || !reflect.DeepEqual(full, paged) {
			return p, recovery.ErrGuard
		}
		canonicalIDs[it.ID] = true
	}
	// A provider can append another terminal turn without a local notification
	// while pages are read. Idle alone does not preserve the parent identity.
	latest, er := c.rpc.requestAtEpoch("thread/turns/list", map[string]any{"threadId": p.Thread, "limit": 1, "sortDirection": "desc", "itemsView": "full"}, 15*time.Second, &epoch)
	if er != nil || len(latest) > 8<<20 {
		return p, recovery.ErrGuard
	}
	var check struct {
		Data []json.RawMessage `json:"data"`
	}
	if json.Unmarshal(latest, &check) != nil || len(check.Data) != 1 {
		return p, recovery.ErrGuard
	}
	beforeValue, er := automaticCanonicalJSON(page.Data[0])
	afterValue, afterErr := automaticCanonicalJSON(check.Data[0])
	if er != nil || afterErr != nil || !reflect.DeepEqual(beforeValue, afterValue) {
		return p, recovery.ErrGuard
	}
	if _, er = c.readStartAdmission(s, st, st.reqID); er != nil || c.rpc.transportEpoch.Load() != epoch {
		return p, recovery.ErrGuard
	}
	b, _ := json.Marshal(itemEvidence)
	p.EvidenceHash = recovery.Hash(b)
	if c.hasPendingInteraction(s.ID) {
		p.WaitingUser = true
		return p, recovery.ErrGuard
	}
	p.ToolCoverage = true
	p.NoUnresolvedEffects = true
	// A failed turn's commentary/reasoning is NOT objective progress. Only
	// canonical independently verifiable deliverables could improve this hash;
	// this conservative first adapter counts none, bounding repeated no-progress.
	_ = progress
	p.ProgressHash = ""
	return p, nil
}

func (c *Codex) automaticTryAgain(s *session.Session, st *codexState, detail *codexFailureDetail) (bool, error) {
	st.mu.Lock()
	a := st.automatic
	stopping := st.stopping
	st.mu.Unlock()
	if a == nil || stopping {
		return false, nil
	}
	o := recovery.AutomaticOutcome{Failure: detail.Failure, Acceptance: detail.Acceptance, IngressRejected: detail.IngressRejected, Terminal: detail.Terminal, NativeWillRetry: detail.NativeWillRetry, Thread: s.ResumeID(), Turn: detail.TurnID}
	// The first structured failure is known before potentially slow canonical
	// pagination. Its entire reconciliation wait belongs to the same budget.
	if e := a.ObserveFailure(o); e != nil {
		return false, e
	}
	if o.Acceptance == recovery.Accepted && o.Terminal {
		proof, e := c.automaticCanonical(s, st, o.Turn)
		if e == nil {
			o.Canonical = proof
		}
	}
	retry, e := a.Failed(o)
	if e != nil || !retry {
		return false, e
	}
	if e = a.Wait(context.Background()); e != nil {
		return false, e
	}
	if e = a.Guard(); e != nil {
		return false, e
	}
	r, e := a.Resume()
	if e != nil {
		return false, e
	}
	if r.Kind == "continue" {
		if _, e = c.automaticCanonical(s, st, r.ParentTurn); e != nil {
			return false, e
		}
	}
	return true, nil
}
func (c *Codex) resetAutomaticTurn(s *session.Session, st *codexState) ([]map[string]any, chan struct{}, error) {
	st.mu.Lock()
	auto := st.automatic
	req := st.reqID
	stopping := st.stopping
	st.mu.Unlock()
	if auto == nil || stopping {
		return nil, nil, recovery.ErrGuard
	}
	if e := auto.Guard(); e != nil {
		return nil, nil, e
	}
	r, e := auto.Resume()
	if e != nil {
		return nil, nil, e
	}
	admission, e := c.readStartAdmission(s, st, req)
	if e != nil {
		return nil, nil, e
	}
	input := st.automaticInput
	if r.Kind == "continue" {
		if _, e = c.automaticCanonical(s, st, r.ParentTurn); e != nil {
			return nil, nil, e
		}
		input = []map[string]any{{"type": "text", "text": automaticContinuation}}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.stopping {
		return nil, nil, recovery.ErrGuard
	}
	st.startAdmission = admission
	st.turnErr = ""
	st.turnErrorCode = ""
	st.turnFailure = nil
	st.turnActive = true
	st.turnDone = make(chan struct{})
	st.currentTurnID = ""
	st.turnStartPending = true
	st.pendingTerminals = nil
	st.pendingNotifications = nil
	st.pendingNotificationBytes = 0
	st.confirmedRequestID = ""
	st.confirmedTurnID = ""
	st.unconfirmedActivity = false
	st.accumulatedText = ""
	st.lastEventAt = time.Now()
	st.stallWarned = false
	return input, st.turnDone, nil
}

// account/read deliberately has no stable account ID. Only the provider's
// actual account/rateLimits/read accountId binds retries; token bytes/email are
// neither a subject nor a model-level cooldown key. Unknown means no recovery.
func (c *Codex) automaticProviderSubject() (string, error) {
	epoch := c.rpc.transportEpoch.Load()
	raw, e := c.rpc.requestAtEpoch("account/read", map[string]any{"refreshToken": false}, 5*time.Second, &epoch)
	if e != nil || len(raw) > 64<<10 {
		return "", recovery.ErrGuard
	}
	var account struct {
		Account *struct {
			Type string `json:"type"`
		} `json:"account"`
	}
	if json.Unmarshal(raw, &account) != nil || account.Account == nil || account.Account.Type != "chatgpt" {
		return "", recovery.ErrGuard
	}
	raw, e = c.rpc.requestAtEpoch("account/rateLimits/read", map[string]any{}, 5*time.Second, &epoch)
	if e != nil || len(raw) > 256<<10 {
		return "", recovery.ErrGuard
	}
	var usage struct {
		AccountID *string         `json:"accountId"`
		Allowed   *bool           `json:"ordinaryUsageAllowed"`
		Upsell    json.RawMessage `json:"rateLimitUpsell"`
		Limits    *struct {
			Credits *struct {
				HasCredits *bool `json:"hasCredits"`
				Unlimited  *bool `json:"unlimited"`
			} `json:"credits"`
			SpendReached *bool   `json:"spendControlReached"`
			ReachedType  *string `json:"rateLimitReachedType"`
		} `json:"rateLimits"`
	}
	if json.Unmarshal(raw, &usage) != nil {
		return "", recovery.ErrGuard
	}
	if usage.AccountID == nil || *usage.AccountID == "" || c.rpc.transportEpoch.Load() != epoch {
		return "", recovery.ErrGuard
	}
	// Only this full, identity-bound read can establish eligibility. Sparse
	// notifications, percentages and display balances never authorize recovery.
	if usage.Allowed == nil || usage.Limits == nil {
		return "", recovery.ErrGuard
	}
	if credits := usage.Limits.Credits; credits != nil && (credits.HasCredits == nil || credits.Unlimited == nil) {
		return "", recovery.ErrGuard
	}
	// Included quota exhaustion can be covered by existing paid/unlimited
	// credits. Backend-owned hard restrictions still take precedence, matching
	// Codex's full-read recovery policy; no billing setting is changed here.
	usableCredits := usage.Limits.Credits != nil && (*usage.Limits.Credits.HasCredits || *usage.Limits.Credits.Unlimited)
	if (len(usage.Upsell) > 0 && string(usage.Upsell) != "null") ||
		(usage.Limits.SpendReached != nil && *usage.Limits.SpendReached) ||
		(!*usage.Allowed && !usableCredits) {
		return "", errAutomaticUsageBlocked
	}
	if reached := usage.Limits.ReachedType; reached != nil {
		// The protocol distinguishes included-plan exhaustion from workspace
		// credit/spend restrictions. Only that exact plan-limit classification
		// can be covered by affirmative existing credits; unknown types stay
		// blocked. This exception requires explicit full-read hard-limit proof.
		if *reached != "rate_limit_reached" || !usableCredits {
			return "", errAutomaticUsageBlocked
		}
		if usage.Limits.SpendReached == nil || len(usage.Upsell) == 0 {
			return "", recovery.ErrGuard
		}
	}
	return recovery.Hash([]byte("chatgpt:" + *usage.AccountID)), nil
}

var errAutomaticUsageBlocked = errors.New("provider ordinary usage is explicitly blocked")

func (c *Codex) automaticLocalSubject() (string, error) {
	if c.appSandbox != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		tokens, e := c.appSandbox.tokens(ctx)
		if e != nil || tokens.AccountID == "" {
			return "", recovery.ErrGuard
		}
		return recovery.Hash([]byte("chatgpt:" + tokens.AccountID)), nil
	}
	f, e := os.Open(c.authPath)
	if e != nil {
		return "", recovery.ErrGuard
	}
	defer f.Close()
	raw, e := io.ReadAll(io.LimitReader(f, 1<<20))
	if e != nil || len(raw) >= 1<<20 {
		return "", recovery.ErrGuard
	}
	var auth struct {
		Tokens struct {
			AccountID string `json:"account_id"`
		} `json:"tokens"`
	}
	if json.Unmarshal(raw, &auth) != nil || auth.Tokens.AccountID == "" {
		return "", recovery.ErrGuard
	}
	return recovery.Hash([]byte("chatgpt:" + auth.Tokens.AccountID)), nil
}

// Compare every field of each canonical item, not just its ID. Both source
// projections must independently be whitelisted and structurally identical.
func automaticCanonicalItem(raw json.RawMessage) (any, error) {
	var item struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}
	if json.Unmarshal(raw, &item) != nil || item.ID == "" {
		return nil, recovery.ErrGuard
	}
	switch item.Type {
	case "userMessage", "agentMessage", "reasoning":
	default:
		return nil, recovery.ErrGuard
	}
	return automaticCanonicalJSON(raw)
}

// Strict, bounded semantic JSON equality: key ordering is irrelevant, but a
// duplicate key, changed type/content/number, or omitted field is uncertainty.
func automaticCanonicalJSON(raw json.RawMessage) (any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var read func(int) (any, error)
	read = func(depth int) (any, error) {
		if depth > 64 {
			return nil, recovery.ErrGuard
		}
		token, e := d.Token()
		if e != nil {
			return nil, e
		}
		switch token {
		case json.Delim('{'):
			value := map[string]any{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return nil, e
				}
				key, ok := k.(string)
				if !ok {
					return nil, recovery.ErrGuard
				}
				if _, ok = value[key]; ok {
					return nil, recovery.ErrGuard
				}
				v, e := read(depth + 1)
				if e != nil {
					return nil, e
				}
				value[key] = v
			}
			end, e := d.Token()
			if e != nil || end != json.Delim('}') {
				return nil, recovery.ErrGuard
			}
			return value, nil
		case json.Delim('['):
			value := []any{}
			for d.More() {
				v, e := read(depth + 1)
				if e != nil {
					return nil, e
				}
				value = append(value, v)
			}
			end, e := d.Token()
			if e != nil || end != json.Delim(']') {
				return nil, recovery.ErrGuard
			}
			return value, nil
		}
		if _, ok := token.(json.Delim); ok {
			return nil, recovery.ErrGuard
		}
		return token, nil
	}
	value, e := read(0)
	if e != nil {
		return nil, e
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, recovery.ErrGuard
	}
	return value, nil
}
