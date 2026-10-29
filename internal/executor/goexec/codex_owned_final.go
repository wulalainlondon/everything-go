package goexec

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/recovery"
	"everything-go/internal/session"
)

var errOwnedFinalUnavailable = errors.New("exact_owned_provider_final_unavailable")

type ownedFinalCacheEntry struct {
	Final *backend.ExactProviderFinal
	Err   error
}

// ExactProviderFinalForRequest is an opt-in connect-only read path. It never
// changes sessionsRoot, discovers global histories, resumes a thread or starts a
// model. Canonical queue acceptance must additionally be checked by the caller.
func (c *Codex) ExactProviderFinalForRequest(s *session.Session, request, thread, turn string) (*backend.ExactProviderFinal, error) {
	pin := os.Getenv("EVERYTHING_GO_CODEX_EXACT_FINAL_SUBJECT_SHA256")
	if !c.connectOnly || pin == "" {
		return nil, nil
	}
	decoded, e := hex.DecodeString(pin)
	if e != nil || len(decoded) != 32 || strings.ToLower(pin) != pin || s == nil || request == "" || thread == "" || turn == "" || strings.HasPrefix(request, "codex_external_") {
		return nil, errOwnedFinalUnavailable
	}
	snap := s.Snapshot()
	if snap.ResumeID != thread || snap.Backend != backend.Codex || snap.Hidden || s.State() == session.Closed || c.ValidateNativeAcceptance(s, thread, request, turn) != nil {
		return nil, errOwnedFinalUnavailable
	}
	local, e := c.automaticLocalSubject()
	if e != nil || local != pin {
		return nil, errOwnedFinalUnavailable
	}
	// Do not create a provider connection on a history read. Normal owned app
	// bootstrap establishes it, and epoch-bound requests cannot move to a successor.
	epoch := c.rpc.transportEpoch.Load()
	if epoch == 0 {
		return nil, errOwnedFinalUnavailable
	}
	c.ownedFinalMu.Lock()
	defer c.ownedFinalMu.Unlock()
	current := s.Snapshot()
	local, e = c.automaticLocalSubject()
	if e != nil || local != pin || c.rpc.transportEpoch.Load() != epoch || current.ResumeID != thread || current.SourceRevision != snap.SourceRevision || current.ConfigRevision != snap.ConfigRevision || c.ValidateNativeAcceptance(s, thread, request, turn) != nil {
		return nil, errOwnedFinalUnavailable
	}
	key := pin + ":" + thread + ":" + request + ":" + turn + ":" + strconv.FormatUint(epoch, 10) + ":" + strconv.FormatUint(snap.ConfigRevision, 10) + ":" + strconv.FormatUint(snap.SourceRevision, 10)
	if cached, ok := c.ownedFinals[key]; ok {
		return cloneOwnedFinal(cached.Final), cached.Err
	}
	if c.ownedFinals == nil {
		c.ownedFinals = map[string]ownedFinalCacheEntry{}
	}
	if len(c.ownedFinals) >= 128 {
		return nil, errOwnedFinalUnavailable
	}
	// Store a failure before I/O. An uncertain read is not polled repeatedly by
	// the coordinator. A new writer requires a fresh, independently fenced read.
	c.ownedFinals[key] = ownedFinalCacheEntry{Err: errOwnedFinalUnavailable}
	calls, verified := 0, false
	defer func() {
		log.Printf("[codex] exact owned final session=%s request=%s epoch=%d rpc_count=%d verified=%v", s.ID, request, epoch, calls, verified)
	}()
	guard := func() bool {
		now := s.Snapshot()
		subject, err := c.automaticLocalSubject()
		return err == nil && subject == pin && c.rpc.transportEpoch.Load() == epoch && now.ResumeID == thread && now.ConfigRevision == snap.ConfigRevision && now.SourceRevision == snap.SourceRevision && now.Backend == snap.Backend && c.ValidateNativeAcceptance(s, thread, request, turn) == nil
	}
	if !guard() {
		return nil, errOwnedFinalUnavailable
	}
	rpc := func(method string, params map[string]any, max int) (json.RawMessage, error) {
		if !guard() {
			return nil, errOwnedFinalUnavailable
		}
		calls++
		raw, err := c.rpc.requestAtEpoch(method, params, 10*time.Second, &epoch)
		if err != nil || len(raw) > max || !guard() {
			return nil, errOwnedFinalUnavailable
		}
		if _, err = automaticCanonicalJSON(raw); err != nil {
			return nil, errOwnedFinalUnavailable
		}
		return raw, nil
	}
	// Subject identity only: lack of future quota must not prevent reading a
	// result already paid for. Never use this read as new turn eligibility.
	raw, e := rpc("account/read", map[string]any{"refreshToken": false}, 64<<10)
	var acct struct {
		Account *struct {
			Type string `json:"type"`
		} `json:"account"`
	}
	if e != nil || json.Unmarshal(raw, &acct) != nil || acct.Account == nil || acct.Account.Type != "chatgpt" {
		return nil, errOwnedFinalUnavailable
	}
	raw, e = rpc("account/rateLimits/read", map[string]any{}, 256<<10)
	var usage struct {
		AccountID *string `json:"accountId"`
	}
	if e != nil || json.Unmarshal(raw, &usage) != nil || usage.AccountID == nil || *usage.AccountID == "" || recovery.Hash([]byte("chatgpt:"+*usage.AccountID)) != pin {
		return nil, errOwnedFinalUnavailable
	}
	subjectBefore := recovery.Hash(raw)
	raw, e = rpc("thread/turns/list", map[string]any{"threadId": thread, "limit": 1, "sortDirection": "desc", "itemsView": "full"}, 8<<20)
	var turns struct {
		Data []struct {
			ID        string  `json:"id"`
			Status    string  `json:"status"`
			ItemsView *string `json:"itemsView"`
		} `json:"data"`
	}
	if e != nil || json.Unmarshal(raw, &turns) != nil || len(turns.Data) != 1 || turns.Data[0].ID != turn || turns.Data[0].Status != "completed" || (turns.Data[0].ItemsView != nil && *turns.Data[0].ItemsView != "full") {
		return nil, errOwnedFinalUnavailable
	}
	proof := recovery.Hash(raw)
	cursor := ""
	seenCursor := map[string]bool{}
	seenItems := map[string]bool{}
	pages := []string{}
	finalID, text := "", ""
	for page := 0; page < 4; page++ {
		params := map[string]any{"threadId": thread, "turnId": turn, "limit": 100, "sortDirection": "asc"}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, e = rpc("thread/items/list", params, 8<<20)
		var items struct {
			Data []struct {
				TurnID string          `json:"turnId"`
				Item   json.RawMessage `json:"item"`
			} `json:"data"`
			Next *string `json:"nextCursor"`
		}
		if e != nil || json.Unmarshal(raw, &items) != nil || items.Data == nil || len(items.Data) > 100 {
			return nil, errOwnedFinalUnavailable
		}
		var pageFields map[string]json.RawMessage
		if json.Unmarshal(raw, &pageFields) != nil {
			return nil, errOwnedFinalUnavailable
		}
		if _, present := pageFields["nextCursor"]; !present {
			return nil, errOwnedFinalUnavailable
		}
		pages = append(pages, recovery.Hash(raw))
		for _, entry := range items.Data {
			var item struct {
				ID    string `json:"id"`
				Type  string `json:"type"`
				Phase string `json:"phase"`
				Text  string `json:"text"`
			}
			if entry.TurnID != turn || json.Unmarshal(entry.Item, &item) != nil || item.ID == "" || len(item.ID) > 512 || item.Type == "" || seenItems[item.ID] {
				return nil, errOwnedFinalUnavailable
			}
			seenItems[item.ID] = true
			if item.Type == "agentMessage" && item.Phase == "final_answer" {
				if finalID != "" || item.Text == "" || len(item.Text) > 262144 {
					return nil, errOwnedFinalUnavailable
				}
				finalID, text = item.ID, item.Text
			}
		}
		if items.Next == nil || *items.Next == "" {
			if finalID == "" || !guard() {
				return nil, errOwnedFinalUnavailable
			}
			raw, e = rpc("account/rateLimits/read", map[string]any{}, 256<<10)
			var after struct {
				AccountID *string `json:"accountId"`
			}
			if e != nil || json.Unmarshal(raw, &after) != nil || after.AccountID == nil || recovery.Hash([]byte("chatgpt:"+*after.AccountID)) != pin {
				return nil, errOwnedFinalUnavailable
			}
			out := &backend.ExactProviderFinal{SessionID: s.ID, RequestID: request, ThreadID: thread, TurnID: turn, ConfigRevision: snap.ConfigRevision, SourceRevision: snap.SourceRevision, SourceMessageID: "codex:" + thread + ":item:" + finalID, Text: text, SubjectSHA256: pin, SubjectBeforeSHA256: subjectBefore, SubjectAfterSHA256: recovery.Hash(raw), TransportEpoch: epoch, TurnResponseSHA256: proof, ItemPageSHA256: pages}
			c.ownedFinals[key] = ownedFinalCacheEntry{Final: cloneOwnedFinal(out)}
			verified = true
			return out, nil
		}
		if seenCursor[*items.Next] {
			return nil, errOwnedFinalUnavailable
		}
		seenCursor[*items.Next] = true
		cursor = *items.Next
	}
	return nil, errOwnedFinalUnavailable
}

func cloneOwnedFinal(f *backend.ExactProviderFinal) *backend.ExactProviderFinal {
	if f == nil {
		return nil
	}
	v := *f
	v.ItemPageSHA256 = append([]string(nil), f.ItemPageSHA256...)
	return &v
}
