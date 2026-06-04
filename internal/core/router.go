package core

import (
	"context"
	"fmt"
	"log"

	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/runtime"
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
func (h *Hub) route(ctx context.Context, c *Client, in protocol.Inbound) {
	switch in.Type {
	case "hello":
		c.deviceID = in.DeviceID
		// Latest-device-wins: evict any older client from the same device so the
		// half-disconnect storm can't pile up zombie clients (#1).
		h.registerLatest(c)
		c.enqueueEvent(protocol.HelloAck{
			Type: "hello_ack", ClientID: c.clientID, DeviceID: in.DeviceID,
			DeviceName: in.DeviceName, InstanceID: h.cfg.InstanceID, Gen: h.gen,
			IsLocked:     h.pairing.IsLocked(),
			LockedToMe:   h.pairing.LockedTo(in.AuthToken),
			InstanceName: h.cfg.InstanceName, RootDir: h.cfg.RootDir,
			DataDir: h.cfg.DataDir, LanIP: h.cfg.LanIP,
		})
		// Proactively push the session list, then recover any events buffered
		// while this (or the previous) client was offline — same ordering as the
		// Python bridge so the app reconciles before replayed events arrive.
		c.enqueueEvent(protocol.NewSessionsList(h.sessionSummaries()))
		h.replayOffline(c)
		// Replay any file pushes this device hasn't acked yet (parity with the
		// Python bridge, which re-emits pending file_push frames on hello).
		h.sendPendingPushes(c)

	case "ping":
		c.enqueueEvent(protocol.NewPong())

	case "claim_bridge":
		if in.AuthToken == "" {
			c.enqueueEvent(protocol.NewError("", "", "auth_token required for claim_bridge"))
			return
		}
		if err := h.pairing.Claim(in.AuthToken, in.DeviceID); err != nil {
			c.enqueueEvent(protocol.NewError("", "", err.Error()))
			return
		}
		c.enqueueEvent(protocol.NewClaimAck())

	case "unclaim_bridge":
		if err := h.pairing.Unclaim(in.AuthToken); err != nil {
			c.enqueueEvent(protocol.NewError("", "", err.Error()))
			return
		}
		c.enqueueEvent(protocol.NewUnclaimAck())

	case "request_sessions_list":
		c.enqueueEvent(protocol.NewSessionsList(h.sessionSummaries()))

	case "get_all_sessions":
		go h.handleGetAllSessions(c)

	case "restart_bridge":
		h.handleRestart(c)

	case "new_session":
		// Expand "~"/"~/..." at creation time, mirroring Python's
		// os.path.expanduser(msg["cwd"] or default_cwd) in session_routes.py.
		// Storing the resolved path keeps get_git_diff / get_tasks / spawn all
		// consistent — the app sends a literal "~" as the default cwd.
		cwd := runtime.ExpandPath(in.Cwd)
		s := h.registry.Create(in.SessionID, in.Name, cwd, in.Backend, in.Model, in.Sandbox, in.ResumeClaudeID)
		snap := s.Snapshot()
		c.enqueueEvent(protocol.SessionCreated{
			Type: "session_created", SessionID: snap.ID, Name: snap.Name,
			CreatedAt: snap.CreatedAt, Cwd: snap.Cwd, Backend: snap.Backend,
			Model: snap.Model, Sandbox: snap.Sandbox,
		})
		go h.registry.Persist()

	case "message":
		s, ok := h.registry.Get(in.SessionID)
		if !ok {
			h.Emit(protocol.NewError(in.SessionID, "no_session", "unknown session"))
			return
		}
		// Enqueue on the session's turn worker: turns for one session run one at
		// a time, in order, so two messages can't interleave a backend's stdin.
		// The turn outlives this connection, so it gets its own context.
		reqID, content := in.RequestID, in.Content
		images, files := in.Images, in.Files
		if !s.Submit(func() {
			if err := h.exec.Send(context.Background(), s, reqID, content, images, files); err != nil {
				log.Printf("[%s] send error: %v", s.ID, err)
			}
		}) {
			h.Emit(protocol.NewError(in.SessionID, "session_closed", "session is closed"))
		}

	case "stop":
		if s, ok := h.registry.Get(in.SessionID); ok {
			s.MarkStopping()
			go func() {
				_ = h.exec.Stop(context.Background(), s)
				s.EndTurn() // release the queue even if the backend emits no terminal event
			}()
		}

	case "clear_session":
		if s, ok := h.registry.Get(in.SessionID); ok {
			go func() {
				_ = h.exec.Clear(context.Background(), s)
				s.EndTurn() // clear cancels an in-flight turn without a done/stopped
			}()
		}

	case "close_session":
		if s, ok := h.registry.Get(in.SessionID); ok {
			go func() { _ = h.exec.Close(context.Background(), s) }()
			h.registry.Delete(in.SessionID) // also stops the session's turn worker
			h.Emit(protocol.NewSessionClosed(in.SessionID))
			go h.registry.Persist()
		}

	case "request_history":
		s, ok := h.registry.Get(in.SessionID)
		if !ok {
			return
		}
		go h.sendHistory(c, s, in)

	case "get_resumable_sessions":
		go h.sendResumable(c, 100)

	case "rename_session":
		if s, ok := h.registry.Get(in.SessionID); ok {
			s.SetName(in.Name)
			c.enqueueEvent(protocol.NewSessionRenamed(s.ID, in.Name))
			go h.registry.Persist()
		}

	case "set_session_meta":
		c.enqueueEvent(protocol.SessionMetaUpdated{
			Type: "session_meta_updated", SessionID: in.SessionID,
			Pinned: in.Pinned, Hidden: in.Hidden,
		})

	case "set_effort":
		// Stored on the session; applied as --effort on the next claude spawn.
		if s, ok := h.registry.Get(in.SessionID); ok {
			s.SetEffort(in.Effort)
			go h.registry.Persist()
		}

	case "switch_session_config":
		if s, ok := h.registry.Get(in.SessionID); ok {
			s.ApplyConfig(in.Backend, in.Model, in.Sandbox)
			go h.registry.Persist()
		}

	case "fork_session":
		go h.handleFork(c, in)

	case "get_agent_tree":
		go h.handleAgentTree(c, in.SessionID)

	// --- Runtime ops: usage / shell / tasks / processes / browse ----------

	case "get_usage":
		go h.sendUsage(c, in.SessionID)

	case "shell_create":
		shellID, errMsg := h.shells.Create(in.Cwd)
		if errMsg != "" {
			c.enqueueEvent(protocol.NewError("", "", errMsg))
			return
		}
		c.enqueueEvent(protocol.NewShellCreated(shellID))

	case "shell_input":
		// Gate behind permission approval. Run in a goroutine so the read loop
		// keeps reading — the permission_response arrives on this same loop and
		// would deadlock if we blocked here (see permission.go).
		shellID, data, dev := in.ShellID, in.Data, c.deviceID
		go func() {
			if !h.perms.Request(dev, "shell_input", "Allow shell command?",
				"Execute command in bridge shell session", truncate(data, 300), "high", "") {
				return
			}
			h.shells.Input(shellID, data)
		}()

	case "shell_close":
		h.shells.Close(in.ShellID)

	case "get_tasks":
		c.enqueueEvent(protocol.NewTasksList(h.collectTasks()))

	case "kill_task":
		c.enqueueEvent(protocol.NewTaskKilled(in.ID, h.killTask(in.ID)))

	case "get_processes":
		go func() { c.enqueueEvent(protocol.NewProcessesList(runtime.CollectProcesses(200))) }()

	case "kill_process":
		pid, force, dev := in.PID, in.Force, c.deviceID
		go func() {
			if !h.perms.Request(dev, "kill_process", "Allow process kill?",
				"Terminate a local OS process", fmt.Sprintf("pid=%d force=%v", pid, force), "high", "") {
				c.enqueueEvent(protocol.NewProcessKilled(pid, false, "permission_denied"))
				return
			}
			ok, msg := runtime.KillProcess(pid, force)
			c.enqueueEvent(protocol.NewProcessKilled(pid, ok, msg))
		}()

	case "browse_dir":
		go h.sendDirListing(c, in)

	case "open_file":
		go h.sendFileOpened(c, in)

	case "request_status":
		c.enqueueEvent(h.statusResult(in.SessionID))

	case "get_git_diff":
		s, ok := h.registry.Get(in.SessionID)
		cwd := ""
		if ok {
			// Expand "~" defensively: new sessions store a resolved cwd, but
			// sessions restored from an older persistence file may still hold a
			// literal "~" that os.Stat can't resolve (→ spurious no_cwd).
			cwd = runtime.ExpandPath(s.Cwd())
		}
		go func() {
			r := runtime.GitDiff(cwd)
			c.enqueueEvent(protocol.NewGitDiffResult(in.SessionID, r.Diff, r.Error, r.Initialized))
		}()

	case "fcm_token":
		if h.fcm != nil {
			h.fcm.SetToken(in.Token)
		}

	case "permission_response":
		h.perms.Resolve(in.RequestID, in.Decision, c.deviceID)

	// --- WebRTC P2P signaling --------------------------------------------
	// Bridge is the answerer: webrtc_offer → webrtc_answer (+ baked ICE),
	// webrtc_ice applies the app's trickled candidates. On DataChannel open the
	// server promotes it to a full client (see webrtc.go). webrtc_answer is
	// never sent by the app and is ignored if received.

	case "webrtc_offer":
		h.handleWebRTCOffer(ctx, c, in)

	case "webrtc_ice":
		h.handleWebRTCICE(c, in)

	case "webrtc_answer":
		// Bridge is always the answerer; clients should not send answers back.
		log.Printf("client %s: ignoring inbound webrtc_answer (server is answerer)", c.clientID)

	// --- Phase 5: instances (stub) / inbox + feed (implemented) ----------
	// list_instances is still an empty-list stub (no multi-instance supervisor
	// in Go yet). The file-push inbox and the article feed below are fully
	// implemented; the app polls all of these on connect.

	case "list_instances":
		c.enqueueEvent(protocol.NewInstancesList())

	case "push_file":
		go h.handlePushFile(c, in.Path)

	case "file_push_ack":
		h.handleFilePushAck(in.FileID, c.deviceID)

	case "get_inbox":
		c.enqueueEvent(protocol.NewInboxListItems(h.inboxItems(c.deviceID)))

	case "feed_list_request":
		if h.feed == nil {
			c.enqueueEvent(protocol.NewFeedList(nil))
			return
		}
		c.enqueueEvent(protocol.NewFeedList(h.feed.List()))

	case "feed_push":
		if h.feed == nil {
			return
		}
		id, deduped, err := h.feed.Push(in.Title, in.HTML, in.Source, in.URL, in.ClientDedupKey, in.ContentType)
		if err != nil {
			c.enqueueEvent(protocol.NewError("", "", err.Error()))
			return
		}
		c.enqueueEvent(protocol.NewFeedAck(id))
		if deduped {
			return // already pushed; no new broadcast / push
		}
		// Broadcast the new item to all clients, and push FCM.
		for _, m := range h.feed.List() {
			if m.FeedID == id {
				h.Emit(protocol.NewFeedNew(m))
				break
			}
		}
		if h.fcm != nil {
			go h.fcm.NotifyFeedNew(id, in.Title)
		}

	case "feed_fetch":
		if h.feed == nil {
			return
		}
		if html, ct, ok := h.feed.Fetch(in.FeedID); ok {
			c.enqueueEvent(protocol.NewFeedDetail(in.FeedID, html, ct))
		} else {
			c.enqueueEvent(protocol.NewError("", "", "Feed item not found: "+in.FeedID))
		}

	case "feed_mark_read":
		if h.feed == nil {
			return
		}
		if m, ok := h.feed.MarkRead(in.FeedID); ok {
			h.Emit(protocol.NewFeedUpdated(m.FeedID, m.Read, m.Deleted))
		}

	case "feed_delete":
		if h.feed == nil {
			return
		}
		if m, ok := h.feed.Delete(in.FeedID); ok {
			h.Emit(protocol.NewFeedUpdated(m.FeedID, m.Read, m.Deleted))
		}

	// --- Interactions: AskUserQuestion ------------------------------------

	case "user_input_response":
		cancelled := in.Cancelled != nil && *in.Cancelled
		if ir, ok := h.exec.(interactionResponder); ok {
			if !ir.RespondUserInput(in.RequestID, in.Answers, cancelled) {
				log.Printf("client %s: user_input_response for unknown request %q", c.clientID, in.RequestID)
			}
		}

	case "pending_interactions_list":
		var items []protocol.UserInputRequestPayload
		if ir, ok := h.exec.(interactionResponder); ok {
			items = ir.PendingInteractions(in.SessionID)
		}
		c.enqueueEvent(protocol.NewPendingInteractionsList(items))

	// --- Search (FTS5) ----------------------------------------------------

	case "request_search":
		go h.sendSearch(c, in)

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
			c.enqueueEvent(h.search.ListSessions(in.Cursor, clampLimit(in.Limit, 30), in.ProjectDir, in.IncludeHidden))
		}()

	case "request_search_context":
		go func() {
			if h.search == nil {
				return
			}
			c.enqueueEvent(h.search.GetContext(in.SessionID, in.MsgUUID, in.Around))
		}()

	default:
		// Not yet implemented in the Go core (history/search/fork/etc.).
		log.Printf("client %s: unhandled type %q", c.clientID, in.Type)
	}
}

// historyRouter is the subset of the executor that can serve history. The Mux
// implements it; if the wired executor doesn't, history is simply unavailable.
type historyRouter interface {
	ProviderFor(s *session.Session) (history.Provider, bool)
	AllProviders() []history.Provider
}

// interactionResponder is the subset of the executor that can answer/list paused
// AskUserQuestion interactions. The Mux implements it (delegating to the Claude
// backend); if unavailable, the commands are no-ops / empty.
type interactionResponder interface {
	RespondUserInput(id string, answers map[string]any, cancelled bool) bool
	PendingInteractions(sessionID string) []protocol.UserInputRequestPayload
}

func (h *Hub) sendHistory(c *Client, s *session.Session, in protocol.Inbound) {
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
		c.enqueueEvent(protocol.HistorySnapshot{Type: "history_snapshot", SessionID: s.ID,
			Messages: []map[string]any{}, KnownIDFound: in.KnownLast == ""})
		return
	}
	// Coalesce + cache identical history requests so a reconnect burst triggers
	// LoadHistory (JSONL parse) at most once per key within the TTL (#4/#5).
	key := c.deviceID + "|" + s.ID + "|" + resumeID + "|" + in.Mode + "|" + in.Before + "|" + in.KnownLast + "|" + itoa(in.Limit)
	v := h.coalesce(&h.storm.histSF, h.storm.histCache, key, historyCacheTTL, func() any {
		res, err := provider.LoadHistory(resumeID, history.Opts{
			Limit: in.Limit, KnownLast: in.KnownLast, Mode: in.Mode, Before: in.Before,
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
		c.enqueueEvent(protocol.HistoryDelta{
			Type: "history_delta", SessionID: s.ID, AfterSourceMessageID: in.KnownLast,
			Messages: msgs, SourceCount: res.SourceCount,
		})
		return
	}
	c.enqueueEvent(protocol.HistorySnapshot{
		Type: "history_snapshot", SessionID: s.ID, Messages: msgs,
		SourceCount: res.SourceCount, HasMoreBefore: res.HasMoreBefore,
		KnownIDFound: res.KnownIDFound, SnapshotReason: res.SnapshotReason,
	})
}

func (h *Hub) sendResumable(c *Client, limit int) {
	if !c.live() {
		return
	}
	if _, ok := h.exec.(historyRouter); !ok {
		c.enqueueEvent(protocol.NewResumableSessions([]history.ResumableSession{}))
		return
	}
	// One provider scan per (limit) within the TTL, shared across reconnect
	// bursts and browse_dir (#6). Re-check liveness after the (slow) scan (#3).
	all := h.coalescedResumable(limit)
	if !c.live() {
		return
	}
	c.enqueueEvent(protocol.NewResumableSessions(all))
}

func (h *Hub) sessionSummaries() []protocol.SessionSummary {
	sessions := h.registry.List()
	out := make([]protocol.SessionSummary, 0, len(sessions))
	for _, s := range sessions {
		snap := s.Snapshot()
		out = append(out, protocol.SessionSummary{
			ID: snap.ID, Name: snap.Name, IsStreaming: snap.Streaming,
			CreatedAt: snap.CreatedAt, LastActivity: snap.LastActivity,
			Cwd: snap.Cwd, Model: snap.Model, Backend: snap.Backend,
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
