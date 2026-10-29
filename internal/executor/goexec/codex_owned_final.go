package goexec

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/recovery"
	"everything-go/internal/session"
)

var errOwnedFinalUnavailable = errors.New("exact_owned_provider_final_unavailable")

type ownedFinalCacheEntry struct {
	Final                 *backend.ExactProviderFinal
	Err                   error
	Subject               string
	Epoch, Config, Source uint64
}

// ExactProviderFinalForRequest reads an accepted execution on its existing connection. It never
// changes sessionsRoot, discovers global histories, resumes a thread or starts a
// model. Canonical queue acceptance must additionally be checked by the caller.
func (c *Codex) ExactProviderFinalForRequest(s *session.Session, request, thread, turn string) (*backend.ExactProviderFinal, error) {
	pin := os.Getenv("EVERYTHING_GO_CODEX_EXACT_FINAL_SUBJECT_SHA256")
	production := !c.connectOnly && pin == "" && c.appSandbox == nil
	if !production && (!c.connectOnly || pin == "") {
		return nil, nil
	}
	if production {
		var err error
		pin, err = c.automaticLocalSubject()
		// Unsupported local identities retain the original file-history path. Do not
		// connect, refresh credentials or ask the provider merely to qualify this read.
		if err != nil || pin == "" {
			return nil, nil
		}
	}
	decoded, e := hex.DecodeString(pin)
	if e != nil || len(decoded) != 32 || strings.ToLower(pin) != pin || s == nil || request == "" || thread == "" || turn == "" || strings.HasPrefix(request, "codex_external_") {
		return nil, errOwnedFinalUnavailable
	}
	if production {
		c.historyRequestMu.Lock()
		accepted, err := c.loadTurnRequests(thread)
		c.historyRequestMu.Unlock()
		if err != nil || accepted.ExactFinalReadRequests[turn] != request {
			return nil, nil
		}
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
	if e != nil || local != pin || c.rpc.transportEpoch.Load() != epoch || current.ResumeID != thread || current.SourceRevision != snap.SourceRevision || current.ConfigRevision != snap.ConfigRevision || current.Hidden || current.Backend != snap.Backend || s.State() == session.Closed || c.ValidateNativeAcceptance(s, thread, request, turn) != nil {
		return nil, errOwnedFinalUnavailable
	}
	// This opt-in reader has one provider epoch per host lifetime. A retired
	// transport cannot spend the same read budget again under a successor key.
	if !production && c.ownedFinalEpoch == 0 {
		c.ownedFinalEpoch = epoch
	}
	if !production && c.ownedFinalEpoch != epoch {
		return nil, errOwnedFinalUnavailable
	}
	key := pin + ":" + s.ID + ":" + thread + ":" + request + ":" + turn + ":" + strconv.FormatUint(epoch, 10) + ":" + strconv.FormatUint(snap.ConfigRevision, 10) + ":" + strconv.FormatUint(snap.SourceRevision, 10)
	if production {
		identity, _ := json.Marshal([]string{s.ID, request, thread, turn})
		key = recovery.Hash(identity)
	}
	if cached, ok := c.ownedFinals[key]; ok {
		if production && (cached.Subject != pin || cached.Epoch != epoch || cached.Config != snap.ConfigRevision || cached.Source != snap.SourceRevision || !c.ownedFinalMarkerMatches(key, pin, epoch, snap.ConfigRevision, snap.SourceRevision)) {
			return nil, errOwnedFinalUnavailable
		}
		fresh := s.Snapshot()
		if s.State() == session.Closed || fresh.Hidden || fresh.Backend != backend.Codex || fresh.ResumeID != thread || fresh.SourceRevision != snap.SourceRevision || fresh.ConfigRevision != snap.ConfigRevision || c.rpc.transportEpoch.Load() != epoch || c.ValidateNativeAcceptance(s, thread, request, turn) != nil {
			return nil, errOwnedFinalUnavailable
		}
		return cloneOwnedFinal(cached.Final), cached.Err
	}
	if c.ownedFinals == nil {
		c.ownedFinals = map[string]ownedFinalCacheEntry{}
	}
	if !production && len(c.ownedFinals) >= 128 {
		return nil, errOwnedFinalUnavailable
	}
	if production {
		if err := c.reserveOwnedFinal(key, pin, epoch, snap.ConfigRevision, snap.SourceRevision); err != nil {
			return nil, errOwnedFinalUnavailable
		}
		if len(c.ownedFinalOrder) >= 128 {
			delete(c.ownedFinals, c.ownedFinalOrder[0])
			c.ownedFinalOrder = c.ownedFinalOrder[1:]
		}
		c.ownedFinalOrder = append(c.ownedFinalOrder, key)
	}
	// Store a failure before I/O. An uncertain read is not polled repeatedly by
	// the coordinator. A new writer requires a fresh, independently fenced read.
	c.ownedFinals[key] = ownedFinalCacheEntry{Err: errOwnedFinalUnavailable, Subject: pin, Epoch: epoch, Config: snap.ConfigRevision, Source: snap.SourceRevision}
	calls, verified := 0, false
	defer func() {
		log.Printf("[codex] exact owned final session=%s request=%s epoch=%d rpc_count=%d verified=%v", s.ID, request, epoch, calls, verified)
	}()
	guard := func() bool {
		now := s.Snapshot()
		subject, err := c.automaticLocalSubject()
		return err == nil && subject == pin && c.rpc.transportEpoch.Load() == epoch && s.State() != session.Closed && !now.Hidden && now.ResumeID == thread && now.ConfigRevision == snap.ConfigRevision && now.SourceRevision == snap.SourceRevision && now.Backend == snap.Backend && c.ValidateNativeAcceptance(s, thread, request, turn) == nil
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
			c.ownedFinals[key] = ownedFinalCacheEntry{Final: cloneOwnedFinal(out), Subject: pin, Epoch: epoch, Config: snap.ConfigRevision, Source: snap.SourceRevision}
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

// reserveOwnedFinal durably spends this execution's read before any RPC. The
// identity omits account/epoch/revisions, so changing them cannot grant a replay.
// Only bounded metadata is stored; canonical text remains in the original queue.
// Outcome RAM may be evicted, but these once markers are never evicted or reset.
func (c *Codex) reserveOwnedFinal(key, subject string, epoch, config, source uint64) error {
	dir, err := c.ownedFinalJournalDir(true)
	if err != nil {
		return err
	}
	payload, err := ownedFinalMarkerPayload(key, subject, epoch, config, source)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, key+".json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	// Even an incomplete write retains its exclusive claim and fails closed.
	written, writeErr := f.Write(payload)
	if writeErr == nil && written != len(payload) {
		writeErr = io.ErrShortWrite
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	d, openErr := os.Open(dir)
	if openErr != nil {
		return openErr
	}
	dirErr := d.Sync()
	d.Close()
	root, rootErr := os.Open(c.dataDir)
	if rootErr != nil {
		return rootErr
	}
	rootErr = root.Sync()
	root.Close()
	for _, err := range []error{writeErr, syncErr, closeErr, dirErr, rootErr} {
		if err != nil {
			return err
		}
	}
	return nil
}

func ownedFinalMarkerPayload(key, subject string, epoch, config, source uint64) ([]byte, error) {
	return json.Marshal(struct {
		Version         int    `json:"version"`
		ExecutionSHA256 string `json:"execution_sha256"`
		SubjectSHA256   string `json:"subject_sha256"`
		Epoch           uint64 `json:"epoch"`
		Config          uint64 `json:"config_revision"`
		Source          uint64 `json:"source_revision"`
	}{1, key, subject, epoch, config, source})
}
func (c *Codex) ownedFinalMarkerMatches(key, subject string, epoch, config, source uint64) bool {
	dir, err := c.ownedFinalJournalDir(false)
	if err != nil {
		return false
	}
	expected, err := ownedFinalMarkerPayload(key, subject, epoch, config, source)
	if err != nil {
		return false
	}
	f, err := os.OpenFile(filepath.Join(dir, key+".json"), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return false
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Getuid() {
		return false
	}
	actual, err := io.ReadAll(io.LimitReader(f, 4097))
	return err == nil && len(actual) <= 4096 && bytes.Equal(actual, expected)
}

func (c *Codex) ownedFinalJournalDir(create bool) (string, error) {
	if !filepath.IsAbs(c.dataDir) {
		return "", errOwnedFinalUnavailable
	}
	root, err := os.Lstat(c.dataDir)
	if err != nil || !root.IsDir() || root.Mode().Perm()&0022 != 0 {
		return "", errOwnedFinalUnavailable
	}
	rootStat, ok := root.Sys().(*syscall.Stat_t)
	if !ok || int(rootStat.Uid) != os.Getuid() {
		return "", errOwnedFinalUnavailable
	}
	dir := filepath.Join(c.dataDir, "codex-exact-final-reads")
	if create {
		if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
			return "", err
		}
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return "", errOwnedFinalUnavailable
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Getuid() {
		return "", errOwnedFinalUnavailable
	}
	return dir, nil
}
