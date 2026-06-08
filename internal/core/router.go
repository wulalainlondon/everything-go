package core

import (
	"context"
	"fmt"
	"log"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/runtime"
	"everything-go/internal/search"
	"everything-go/internal/session"
)

// truncate caps a preview string to n bytes (best-effort, byte-wise).
func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// route dispatches an inbound frame on its envelope `type`. Transport-cheap
// commands are answered locally from the Hub's own state; the rest are forwarded
// to the Executor. The payload beyond {type, session_id} is only inspected by
// the specific handler that needs it.
func (h *Hub) route(ctx context.Context, c *Client, cmd clientproto.Command) {
	switch cmd.Kind {
	case "hello":
		c.deviceID = cmd.DeviceID
		// Latest-device-wins: evict any older client from the same device so the
		// half-disconnect storm can't pile up zombie clients (#1).
		h.registerLatest(c)
		h.tunnelURLMu.RLock()
		tunnelURL := h.tunnelURL
		h.tunnelURLMu.RUnlock()
		c.enqueueEvent(h.client.HelloAck(clientproto.HelloInput{
			ClientID: c.clientID, DeviceID: cmd.DeviceID,
			DeviceName: cmd.DeviceName, InstanceID: h.cfg.InstanceID, Gen: h.gen,
			IsLocked:     h.pairing.IsLocked(),
			LockedToMe:   h.pairing.LockedTo(cmd.AuthToken),
			InstanceName: h.cfg.InstanceName, RootDir: h.cfg.RootDir,
			DataDir: h.cfg.DataDir, LanIP: h.cfg.LanIP,
			TunnelURL: tunnelURL,
			Backends:  h.cfg.Backends,
		}))
		// Proactively push the session list, then recover any events buffered
		// while this (or the previous) client was offline — same ordering as the
		// Python bridge so the app reconciles before replayed events arrive.
		c.enqueueEvent(h.client.SessionsList(h.sessionSummaries()))
		h.replayOffline(c)
		// Replay any file pushes this device hasn't acked yet (parity with the
		// Python bridge, which re-emits pending file_push frames on hello).
		h.sendPendingPushes(c)

	case "ping":
		c.enqueueEvent(h.client.Pong())

	case "claim_bridge":
		if cmd.AuthToken == "" {
			c.enqueueEvent(h.client.Error("", "", "auth_token required for claim_bridge"))
			return
		}
		if err := h.pairing.Claim(cmd.AuthToken, cmd.DeviceID); err != nil {
			c.enqueueEvent(h.client.Error("", "", err.Error()))
			return
		}
		c.enqueueEvent(h.client.ClaimAck())

	case "unclaim_bridge":
		if err := h.pairing.Unclaim(cmd.AuthToken); err != nil {
			c.enqueueEvent(h.client.Error("", "", err.Error()))
			return
		}
		c.enqueueEvent(h.client.UnclaimAck())

	case "request_sessions_list":
		c.enqueueEvent(h.client.SessionsList(h.sessionSummaries()))

	case "get_all_sessions":
		go h.handleGetAllSessions(c)

	case "restart_bridge":
		h.handleRestart(c)

	case "new_session":
		// Expand "~"/"~/..." at creation time, mirroring Python's
		// os.path.expanduser(msg["cwd"] or default_cwd) in session_routes.py.
		// Storing the resolved path keeps get_git_diff / get_tasks / spawn all
		// consistent — the app sends a literal "~" as the default cwd.
		cwd := runtime.ExpandPath(cmd.Cwd)
		s := h.registry.Create(cmd.SessionID, cmd.Name, cwd, cmd.Backend, cmd.Model, cmd.Sandbox, cmd.ResumeClaudeID)
		snap := s.Snapshot()
		c.enqueueEvent(h.client.SessionCreated(clientproto.SessionCreatedInput{
			ID: snap.ID, Name: snap.Name, CreatedAt: snap.CreatedAt, Cwd: snap.Cwd,
			Backend: snap.Backend, Model: snap.Model, Sandbox: snap.Sandbox,
		}))
		go h.registry.Persist()
		h.Emit(h.client.SessionsList(h.sessionSummaries()))

	case "message":
		s, ok := h.registry.Get(cmd.SessionID)
		if !ok {
			h.Emit(h.client.Error(cmd.SessionID, "no_session", "unknown session"))
			return
		}
		// Enqueue on the session's turn worker: turns for one session run one at
		// a time, in order, so two messages can't interleave a backend's stdin.
		// The turn outlives this connection, so it gets its own context.
		reqID, content := cmd.RequestID, cmd.Content
		images, files := cmd.Images, cmd.Files
		if !s.Submit(func() {
			if err := h.exec.Send(context.Background(), s, reqID, content, images, files); err != nil {
				log.Printf("[%s] send error: %v", s.ID, err)
			}
		}) {
			h.Emit(h.client.Error(cmd.SessionID, "session_closed", "session is closed"))
		}

	case "stop":
		if s, ok := h.registry.Get(cmd.SessionID); ok {
			s.MarkStopping()
			go func() {
				_ = h.exec.Stop(context.Background(), s)
				s.EndTurn() // release the queue even if the backend emits no terminal event
			}()
		}

	case "clear_session":
		if s, ok := h.registry.Get(cmd.SessionID); ok {
			go func() {
				_ = h.exec.Clear(context.Background(), s)
				s.EndTurn() // clear cancels an in-flight turn without a done/stopped
			}()
		}

	case "close_session":
		if s, ok := h.registry.Get(cmd.SessionID); ok {
			go func() { _ = h.exec.Close(context.Background(), s) }()
			h.registry.Delete(cmd.SessionID) // also stops the session's turn worker
			h.Emit(h.client.SessionClosed(cmd.SessionID))
			go h.registry.Persist()
		}

	case "request_history":
		s, ok := h.registry.Get(cmd.SessionID)
		if !ok {
			c.enqueueEvent(h.client.HistorySnapshot(cmd.SessionID, []map[string]any{}, 0, false, true, ""))
			return
		}
		go h.sendHistory(c, s, cmd)

	case "get_resumable_sessions":
		go h.sendResumable(c, 100)

	case "rename_session":
		if s, ok := h.registry.Get(cmd.SessionID); ok {
			s.SetName(cmd.Name)
			h.Emit(h.client.SessionRenamed(s.ID, cmd.Name))
			go h.registry.Persist()
		}

	case "set_session_meta":
		if s, ok := h.registry.Get(cmd.SessionID); ok {
			s.SetMeta(cmd.Pinned, cmd.Hidden)
			h.Emit(h.client.SessionMetaUpdated(cmd.SessionID, cmd.Pinned, cmd.Hidden))
			go h.registry.Persist()
		}

	case "set_effort":
		// Stored on the session; applied as --effort on the next claude spawn.
		if s, ok := h.registry.Get(cmd.SessionID); ok {
			s.SetEffort(cmd.Effort)
			go h.registry.Persist()
		}

	case "switch_session_config":
		if s, ok := h.registry.Get(cmd.SessionID); ok {
			s.ApplyConfig(cmd.Backend, cmd.Model, cmd.Sandbox)
			go h.registry.Persist()
		}

	case "fork_session":
		go h.handleFork(c, cmd)

	case "get_agent_tree":
		go h.handleAgentTree(c, cmd.SessionID)

	// --- Runtime ops: usage / shell / tasks / processes / browse ----------

	case "get_usage":
		go h.sendUsage(c, cmd.SessionID)

	case "shell_create":
		shellID, errMsg := h.shells.Create(cmd.Cwd)
		if errMsg != "" {
			c.enqueueEvent(h.client.Error("", "", errMsg))
			return
		}
		c.enqueueEvent(h.client.ShellCreated(shellID))

	case "shell_input":
		// Gate behind permission approval. Run in a goroutine so the read loop
		// keeps reading — the permission_response arrives on this same loop and
		// would deadlock if we blocked here (see permission.go).
		shellID, data, dev := cmd.ShellID, cmd.Data, c.deviceID
		go func() {
			if !h.perms.Request(dev, "shell_input", "Allow shell command?",
				"Execute command in bridge shell session", truncate(data, 300), "high", "") {
				return
			}
			h.shells.Input(shellID, data)
		}()

	case "shell_close":
		h.shells.Close(cmd.ShellID)

	case "get_tasks":
		c.enqueueEvent(h.client.TasksList(h.collectTasks()))

	case "kill_task":
		c.enqueueEvent(h.client.TaskKilled(cmd.ID, h.killTask(cmd.ID)))

	case "get_processes":
		go func() { c.enqueueEvent(h.client.ProcessesList(runtime.CollectProcesses(200))) }()

	case "kill_process":
		pid, force, dev := cmd.PID, cmd.Force, c.deviceID
		go func() {
			if !h.perms.Request(dev, "kill_process", "Allow process kill?",
				"Terminate a local OS process", fmt.Sprintf("pid=%d force=%v", pid, force), "high", "") {
				c.enqueueEvent(h.client.ProcessKilled(pid, false, "permission_denied"))
				return
			}
			ok, msg := runtime.KillProcess(pid, force)
			c.enqueueEvent(h.client.ProcessKilled(pid, ok, msg))
		}()

	case "browse_dir":
		go h.sendDirListing(c, cmd)

	case "open_file":
		go h.sendFileOpened(c, cmd)

	case "request_status":
		c.enqueueEvent(h.statusResult(cmd.SessionID))

	case "get_git_diff":
		s, ok := h.registry.Get(cmd.SessionID)
		cwd := ""
		if ok {
			// Expand "~" defensively: new sessions store a resolved cwd, but
			// sessions restored from an older persistence file may still hold a
			// literal "~" that os.Stat can't resolve (→ spurious no_cwd).
			cwd = runtime.ExpandPath(s.Cwd())
		}
		go func() {
			r := runtime.GitDiff(cwd)
			c.enqueueEvent(h.client.GitDiffResult(cmd.SessionID, r.Diff, r.Error, r.Initialized))
		}()

	case "fcm_token":
		if h.fcm != nil {
			h.fcm.SetToken(cmd.Token)
		}

	case "permission_response":
		h.perms.Resolve(cmd.RequestID, cmd.Decision, c.deviceID)

	// --- WebRTC P2P signaling --------------------------------------------
	// Bridge is the answerer: webrtc_offer → webrtc_answer (+ baked ICE),
	// webrtc_ice applies the app's trickled candidates. On DataChannel open the
	// server promotes it to a full client (see webrtc.go). webrtc_answer is
	// never sent by the app and is ignored if received.

	case "webrtc_offer":
		h.handleWebRTCOffer(ctx, c, cmd.WebRTCOffer())

	case "webrtc_ice":
		h.handleWebRTCICE(c, cmd.WebRTCICE())

	case "webrtc_answer":
		// Bridge is always the answerer; clients should not send answers back.
		log.Printf("client %s: ignoring inbound webrtc_answer (server is answerer)", c.clientID)

	// --- Phase 5: instances (stub) / inbox + feed (implemented) ----------
	// list_instances is still an empty-list stub (no multi-instance supervisor
	// in Go yet). The file-push inbox and the article feed below are fully
	// implemented; the app polls all of these on connect.

	case "list_instances":
		c.enqueueEvent(h.client.InstancesList())

	case "push_file":
		go h.handlePushFile(c, cmd.Path)

	case "file_push_ack":
		h.handleFilePushAck(cmd.FileID, c.deviceID)

	case "get_inbox":
		c.enqueueEvent(h.client.InboxListItems(h.inboxItems(c.deviceID)))

	case "feed_list_request":
		if h.feed == nil {
			c.enqueueEvent(h.client.FeedList(nil))
			return
		}
		c.enqueueEvent(h.client.FeedList(h.feed.List()))

	case "feed_push":
		if h.feed == nil {
			return
		}
		id, deduped, err := h.feed.Push(cmd.Title, cmd.HTML, cmd.Source, cmd.URL, cmd.ClientDedupKey, cmd.ContentType)
		if err != nil {
			c.enqueueEvent(h.client.Error("", "", err.Error()))
			return
		}
		c.enqueueEvent(h.client.FeedAck(id))
		if deduped {
			return // already pushed; no new broadcast / push
		}
		// Broadcast the new item to all clients, and push FCM.
		for _, m := range h.feed.List() {
			if m.FeedID == id {
				h.Emit(h.client.FeedNew(m))
				break
			}
		}
		if h.fcm != nil {
			go h.fcm.NotifyFeedNew(id, cmd.Title)
		}

	case "feed_fetch":
		if h.feed == nil {
			return
		}
		if html, ct, ok := h.feed.Fetch(cmd.FeedID); ok {
			c.enqueueEvent(h.client.FeedDetail(cmd.FeedID, html, ct))
		} else {
			c.enqueueEvent(h.client.Error("", "", "Feed item not found: "+cmd.FeedID))
		}

	case "feed_mark_read":
		if h.feed == nil {
			return
		}
		if m, ok := h.feed.MarkRead(cmd.FeedID); ok {
			h.Emit(h.client.FeedUpdated(m.FeedID, m.Read, m.Deleted))
		}

	case "feed_delete":
		if h.feed == nil {
			return
		}
		if m, ok := h.feed.Delete(cmd.FeedID); ok {
			h.Emit(h.client.FeedUpdated(m.FeedID, m.Read, m.Deleted))
		}

	// --- Interactions: AskUserQuestion ------------------------------------

	case "user_input_response":
		cancelled := cmd.Cancelled != nil && *cmd.Cancelled
		if ir, ok := h.exec.(interactionResponder); ok {
			if !ir.RespondUserInput(cmd.RequestID, cmd.Answers, cancelled) {
				log.Printf("client %s: user_input_response for unknown request %q", c.clientID, cmd.RequestID)
			}
		}

	case "pending_interactions_list":
		var items []backend.UserInputPayload
		if ir, ok := h.exec.(interactionResponder); ok {
			items = ir.PendingInteractions(cmd.SessionID)
		}
		c.enqueueEvent(h.client.PendingInteractionsList(items))

	// --- Search (FTS5) ----------------------------------------------------

	case "request_search":
		go h.sendSearch(c, cmd)

	case "request_search_health":
		go func() {
			if h.search == nil {
				return
			}
			c.enqueueEvent(h.search.Health())
		}()

	case "request_session_list":
		go func() {
			if h.search == nil {
				return
			}
			c.enqueueEvent(h.search.ListSessions(cmd.Cursor, clampLimit(cmd.Limit, 30), cmd.ProjectDir, cmd.IncludeHidden))
		}()

	case "request_search_context":
		go func() {
			if h.search == nil {
				return
			}
			c.enqueueEvent(h.search.GetContext(cmd.SessionID, cmd.MsgUUID, cmd.Around))
		}()

	default:
		// Not yet implemented in the Go core (history/search/fork/etc.).
		log.Printf("client %s: unhandled type %q", c.clientID, cmd.Kind)
	}
}

// historyRouter is the subset of the executor that can serve history. The Mux
// implements it; if the wired executor doesn't, history is simply unavailable.
type historyRouter interface {
	ProviderFor(s *session.Session) (backend.HistoryProvider, bool)
	AllProviders() []backend.HistoryProvider
}

// interactionResponder is the subset of the executor that can answer/list paused
// AskUserQuestion interactions. The Mux implements it (delegating to the Claude
// backend); if unavailable, the commands are no-ops / empty.
type interactionResponder interface {
	RespondUserInput(id string, answers map[string]any, cancelled bool) bool
	PendingInteractions(sessionID string) []backend.UserInputPayload
}

func (h *Hub) sendHistory(c *Client, s *session.Session, cmd clientproto.Command) {
	if !c.live() {
		return
	}
	hr, ok := h.exec.(historyRouter)
	if !ok {
		return
	}
	provider, ok := hr.ProviderFor(s)
	resumeID := s.ResumeID()
	if !ok || resumeID == "" {
		// No history backend or no resume id yet → empty snapshot.
		c.enqueueEvent(h.client.HistorySnapshot(s.ID, []map[string]any{}, 0, false, cmd.KnownLast == "", ""))
		return
	}
	// Coalesce + cache identical history requests so a reconnect burst triggers
	// LoadHistory (JSONL parse) at most once per key within the TTL (#4/#5).
	key := c.deviceID + "|" + s.ID + "|" + resumeID + "|" + cmd.Mode + "|" + cmd.Before + "|" + cmd.KnownLast + "|" + itoa(cmd.Limit)
	v := h.coalesce(&h.storm.histSF, h.storm.histCache, key, historyCacheTTL, func() any {
		res, err := provider.LoadHistory(resumeID, history.Opts{
			Limit: cmd.Limit, KnownLast: cmd.KnownLast, Mode: cmd.Mode, Before: cmd.Before,
		})
		if err != nil {
			return nil
		}
		return res
	})
	if v == nil {
		return
	}
	res := v.(*history.Result)
	if !c.live() {
		return // client replaced/gone while loading → drop (#3)
	}
	msgs := res.Messages
	if msgs == nil {
		msgs = []map[string]any{}
	}
	if res.Kind == "delta" {
		c.enqueueEvent(h.client.HistoryDelta(s.ID, cmd.KnownLast, msgs, res.SourceCount))
		return
	}
	c.enqueueEvent(h.client.HistorySnapshot(s.ID, msgs, res.SourceCount, res.HasMoreBefore, res.KnownIDFound, res.SnapshotReason))
}

func (h *Hub) sendResumable(c *Client, limit int) {
	if !c.live() {
		return
	}
	if _, ok := h.exec.(historyRouter); !ok {
		c.enqueueEvent(h.client.ResumableSessions([]history.ResumableSession{}))
		return
	}
	// One provider scan per (limit) within the TTL, shared across reconnect
	// bursts and browse_dir (#6). Re-check liveness after the (slow) scan (#3).
	all := h.coalescedResumable(limit)
	if !c.live() {
		return
	}
	c.enqueueEvent(h.client.ResumableSessions(all))
}

func (h *Hub) sessionSummaries() []protocol.SessionSummary {
	sessions := h.registry.List()
	out := make([]protocol.SessionSummary, 0, len(sessions))

	// Batch-fetch recent messages + real last-activity for preview, keyed by hub
	// session ID. Search DB uses "claude:{resumeID}" or "codex:rollout-{ts}-{uid}"
	// as keys; RecentMessagesByUID handles the mapping transparently.
	var previewByHubID map[string]*search.SessionPreview
	if h.search != nil {
		var uids []search.SessionUID
		for _, s := range sessions {
			snap := s.Snapshot()
			if snap.ResumeID != "" && snap.Backend != "" {
				uids = append(uids, search.SessionUID{
					HubID: snap.ID, Backend: snap.Backend, UID: snap.ResumeID,
				})
			}
		}
		if len(uids) > 0 {
			previewByHubID = h.search.RecentMessagesByUID(uids, 3)
		}
	}

	for _, s := range sessions {
		snap := s.Snapshot()
		var recent []protocol.RecentMessage
		// last_activity must reflect real activity. The session store's last_used
		// can be flattened/stale; the search index's newest message ts is the
		// source of truth, so prefer it when available.
		lastActivity := snap.LastActivity
		if pv := previewByHubID[snap.ID]; pv != nil {
			for _, m := range pv.Recent {
				recent = append(recent, protocol.RecentMessage{Role: m.Role, Text: m.Text})
			}
			if pv.LastTS > 0 {
				lastActivity = float64(pv.LastTS)
			}
		}
		out = append(out, protocol.SessionSummary{
			ID: snap.ID, Name: snap.Name, IsStreaming: snap.Streaming,
			CreatedAt: snap.CreatedAt, LastActivity: lastActivity,
			Cwd: snap.Cwd, Model: snap.Model, Backend: snap.Backend,
			Sandbox: snap.Sandbox, Pinned: snap.Pinned, Hidden: snap.Hidden,
			RecentMessages: recent,
		})
	}
	return out
}

// enqueueEvent marshals + enqueues a reply to this specific client.
func (c *Client) enqueueEvent(event any) {
	logOutbound(event)
	data, err := marshalEvent(event)
	if err != nil {
		log.Printf("enqueueEvent marshal: %v", err)
		return
	}
	c.enqueue(data)
}
