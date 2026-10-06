package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/history"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

func (h *Hub) apiTask(ctx context.Context, r taskapi.IntentRecord) (map[string]any, error) {
	var legacy struct{ LegacyLinkID string }
	json.Unmarshal(r.Metadata, &legacy)
	if legacy.LegacyLinkID != "" {
		return h.projectLegacyAPITask(ctx, r, legacy.LegacyLinkID)
	}
	var meta apiMetadata
	if json.Unmarshal(r.Metadata, &meta) != nil {
		return nil, taskapi.Failure("key_expired", "known_receipt", "lookup_original")
	}
	var input map[string]any
	json.Unmarshal(meta.Request.Input, &input)
	axes := map[string]string{"admission": "prepared", "execution": "not_started", "final": "unavailable", "artifact": "none", "qa": "not_requested", "delivery": "not_requested", "cancellation": "none"}
	target, exists := h.registry.Get(r.SessionID)
	identity := map[string]any{"instance_id": h.cfg.InstanceID, "session_id": r.SessionID, "request_id": r.RequestID}
	revision := uint64(0)
	var anchor any
	sealID, finalHash, finalText := "", "", ""
	if exists {
		var original apiInput
		json.Unmarshal(meta.Request.Input, &original)
		if original.Existing != nil {
			identity["native_thread_id"] = original.Existing.ResumeID
			identity["config_revision"] = original.Existing.ConfigRevision
		} else if proof, found, _ := h.messageQueue.ProviderEvidence(r.SessionID, r.RequestID); found {
			identity["native_thread_id"] = proof.ConversationID
		}
	}
	nativeThread, nativeTurn, nativeKnown := h.messageQueue.NativeExecution(r.SessionID, r.RequestID)
	if nativeKnown {
		if expected, ok := identity["native_thread_id"].(string); ok && expected != "" && expected != nativeThread {
			nativeKnown = false
		} else {
			identity["native_thread_id"] = nativeThread
			identity["native_turn_id"] = nativeTurn
		}
	}
	entry, found, err := h.messageQueue.Get(r.SessionID, r.RequestID)
	if err != nil {
		return nil, err
	}
	snapshot, err := h.messageQueue.Snapshot(r.SessionID)
	if err != nil {
		return nil, err
	}
	commands, e := h.apiJournals()[r.Path].TaskCommands(ctx, r.TaskID)
	if e != nil {
		return nil, e
	}
	for _, command := range commands {
		n, e := h.messageQueue.TaskRevision(ctx, command.SessionID, command.RequestID)
		if e != nil {
			return nil, e
		}
		if n > revision {
			revision = n
		}
	}
	_ = snapshot.Revision
	if !found {
		if state, e := h.apiJournals()[r.Path].OutboxState(ctx, r.Key); e == nil && state == "unknown" {
			axes["admission"] = "unknown"
			axes["execution"] = "unknown"
		}
	}
	if found {
		axes["admission"] = "queued"
		switch entry.State {
		case messagequeue.Running:
			axes["execution"] = "handoff"
		case messagequeue.Uncertain, messagequeue.Steering:
			axes["execution"] = "unknown"
		case messagequeue.Cancelled:
			axes["cancellation"] = "confirmed"
			if from, e := h.messageQueue.CancelOrigin(r.SessionID, r.RequestID); e != nil || from == "" || from != messagequeue.Queued {
				axes["cancellation"] = "unknown"
				axes["execution"] = "unknown"
			}
		case messagequeue.Completed:
			axes["execution"] = "succeeded"
			axes["final"] = "unanchored"
		case messagequeue.Failed:
			axes["execution"] = "failed"
		}
		if exists {
			conflict, _ := h.messageQueue.NativeConflict(r.SessionID, r.RequestID)
			if conflict {
				axes["execution"] = "unknown"
			} else if turn := h.nativeTaskTurn(target, r.RequestID); turn != "" {
				identity["native_turn_id"] = turn
				if entry.State == messagequeue.Cancelled {
					axes["execution"] = "unknown"
					axes["cancellation"] = "unknown"
				}
				if entry.State == messagequeue.Running {
					axes["execution"] = "native_consumed"
				}
			}
		}
	}
	if found && entry.State == messagequeue.Completed {
		if stored, e := h.messageQueue.TaskSeal(ctx, r.SessionID, r.RequestID, ""); e == nil {
			var saved map[string]any
			if json.Unmarshal(stored.Anchor, &saved) == nil {
				anchor = saved
				sealID = stored.ID
				finalHash = stored.Hash
				finalText = stored.Text
				axes["final"] = "sealed"
			}
		}
	}
	if exists && found && entry.State == messagequeue.Completed && target.Backend() == backend.Codex && anchor == nil {
		finals, _ := h.sessionTaskFinals(target)
		if final := finals[r.RequestID]; final != nil {
			message, _ := final["source_message_id"].(string)
			turn, _ := final["source_turn_id"].(string)
			thread, _ := final["source_thread_id"].(string)
			if thread == "" {
				thread, _ = identity["native_thread_id"].(string)
			}
			text, _ := final["content"].(string)
			if message != "" && nativeKnown && turn == nativeTurn && thread == nativeThread {
				finalText = text
				hash := sha256.Sum256([]byte(text))
				finalHash = hex.EncodeToString(hash[:])
				sealID = "seal_" + finalHash
				anchor = map[string]any{"instance_id": h.cfg.InstanceID, "session_id": r.SessionID, "request_id": r.RequestID, "source_message_id": message, "execution": map[string]string{"backend": "codex", "native_thread_id": thread, "native_turn_id": turn}}
				axes["final"] = "sealed"
			}
		}
	}
	if proof, ok, err := h.messageQueue.ProviderEvidence(r.SessionID, r.RequestID); err == nil && ok && proof.Backend == "claude" {
		identity["provider_execution"] = map[string]string{"backend": "claude", "provider_session_id": proof.ConversationID, "token_kind": proof.TokenKind, "provider_token": proof.Token}
		if found && entry.State == messagequeue.Running {
			axes["execution"] = "native_consumed"
		}
		if found && entry.State == messagequeue.Completed && proof.Status == "succeeded" && proof.MessageID != "" && anchor == nil && h.claudeAnchorID(target, proof) != "" {
			finalText = proof.Text
			hash := sha256.Sum256([]byte(proof.Text))
			finalHash = hex.EncodeToString(hash[:])
			sealID = "seal_" + finalHash
			anchor = map[string]any{"instance_id": h.cfg.InstanceID, "session_id": r.SessionID, "request_id": r.RequestID, "source_message_id": h.claudeAnchorID(target, proof), "execution": identity["provider_execution"]}
			axes["final"] = "sealed"
		}
	}
	source := map[string]any{"identity": map[string]any{"instance_id": h.cfg.InstanceID}, "binding_kind": meta.Caller.BindingKind, "provenance": "verified"}
	if meta.Caller.SourceSessionID != "" {
		source["identity"] = map[string]any{"instance_id": h.cfg.InstanceID, "session_id": meta.Caller.SourceSessionID, "request_id": meta.Caller.SourceRequestID, "native_thread_id": meta.SourceThread, "config_revision": meta.SourceRevision}
	}
	task := map[string]any{"task_id": r.TaskID, "task_ref": map[string]string{"authority_instance_id": h.cfg.InstanceID, "path": r.Path, "native_record_id": r.NativeID}, "revision": revision, "definition_revision": uint64(1), "source": source, "target": identity, "axes": axes, "artifact_refs": []any{}, "qa": map[string]any{"status": "not_requested", "evidence_refs": []any{}}, "evidence_refs": []any{}, "receipt_ids": []string{r.ReceiptID}, "child_task_ids": []string{}, "provenance": "legacy", "views": []string{"self"}, "capabilities": []string{"get", "read_result"}}
	for _, field := range []string{"goal", "scope", "acceptance", "dependencies", "workspace"} {
		if value, ok := input[field]; ok {
			task[field] = value
		}
	}
	if worker, ok := input["new_worker"].(map[string]any); ok {
		task["worker_profile"] = worker["profile"]
	}
	if worker, ok := input["new_worker"].(map[string]any); ok && worker["profile"] != nil {
		task["provenance"] = "verified"
	}
	if criteria, ok := input["acceptance"].([]any); ok {
		for _, raw := range criteria {
			if criterion, ok := raw.(map[string]any); ok && criterion["evidence_kind"] == "review" && criterion["mandatory"] == true {
				axes["qa"] = "pending"
				task["qa"] = map[string]any{"status": "pending", "evidence_refs": []any{}}
			}
		}
	}
	if r.Path == "delegation" {
		if original, found, e := h.delegations.ByID(ctx, r.NativeID); e == nil && found && len(original.Artifacts) > 0 {
			artifacts := []any{}
			for i, path := range original.Artifacts {
				if len(artifacts) == 100 {
					break
				}
				artifacts = append(artifacts, map[string]any{"artifact_id": r.NativeID + ":artifact:" + stringID(uint64(i)), "path": path, "status": "reported", "authority_instance_id": h.cfg.InstanceID})
			}
			task["artifact_refs"] = artifacts
			axes["artifact"] = "reported"
		}
	}

	if axes["execution"] == "not_started" || axes["execution"] == "handoff" {
		task["views"] = []string{"inbox"}
	}
	if axes["execution"] == "succeeded" || axes["execution"] == "failed" {
		task["views"] = []string{"results"}
	}
	if meta.Caller.SourceSessionID != "" && axes["admission"] == "prepared" {
		task["views"] = append(task["views"].([]string), "manager_arranging")
	}
	if meta.Caller.SourceSessionID != "" {
		task["views"] = append(task["views"].([]string), "source_children")
	}
	if anchor != nil {
		conflict, _ := h.messageQueue.NativeConflict(r.SessionID, r.RequestID)
		encoded, _ := json.Marshal(anchor)
		if conflict {
			axes["execution"] = "unknown"
			axes["final"] = "unknown"
		} else if _, err := h.messageQueue.SealTask(ctx, messagequeue.TaskSeal{ID: sealID, SessionID: r.SessionID, RequestID: r.RequestID, Hash: finalHash, Text: finalText, Anchor: encoded}); err != nil {
			axes["final"] = "unknown"
		} else {
			task["result_anchor"] = anchor
			task["seal_id"] = sealID
			task["final_hash"] = finalHash
		}
	}
	if conflict, e := h.messageQueue.NativeConflict(r.SessionID, r.RequestID); e != nil || conflict {
		axes["execution"] = "unknown"
		axes["final"] = "unknown"
		delete(task, "result_anchor")
		delete(task, "seal_id")
		delete(task, "final_hash")
	} else if found && entry.State == messagequeue.Completed && anchor == nil {
		if persisted, e := h.messageQueue.TaskSeal(ctx, r.SessionID, r.RequestID, ""); e == nil {
			var saved map[string]any
			if json.Unmarshal(persisted.Anchor, &saved) == nil {
				task["result_anchor"] = saved
				task["seal_id"] = persisted.ID
				task["final_hash"] = persisted.Hash
				axes["final"] = "sealed"
			}
		}
	}
	if r.Path == "delegation" || r.Path == "controller" {
		state, deliveryAnchor := h.apiDelivery(ctx, r)
		if state == "delivered" && task["seal_id"] == nil {
			state = "unknown"
		}
		axes["delivery"] = state
		if state == "delivered" {
			task["source_delivery_anchor"] = deliveryAnchor
		}
	}
	return task, nil
}
func (h *Hub) apiList(ctx context.Context, c taskapi.VerifiedContext, sessionID, pathFilter string) ([]any, error) {
	out := []any{}
	seen := map[string]bool{}
	for path, j := range h.apiJournals() {
		if pathFilter != "" && path != pathFilter {
			continue
		}
		records, err := j.List(ctx, c.StableScopeID, c.NamespaceGeneration)
		if c.BindingKind == "paired_human" {
			records, err = j.All(ctx)
		}
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			var meta apiMetadata
			json.Unmarshal(record.Metadata, &meta)
			var legacy legacySourceMetadata
			json.Unmarshal(record.Metadata, &legacy)
			if legacy.LegacyLinkID != "" {
				continue
			} // Read relations from durable adapter, independent of prunable intent metadata.
			linkedSource := ""
			if seen[record.TaskID] || sessionID != "" && record.SessionID != sessionID && meta.Caller.SourceSessionID != sessionID && linkedSource != sessionID {
				continue
			}

			if e := h.checkAPIRecord(ctx, c, record); e != nil {
				continue
			}
			task, err := h.apiTask(ctx, record)
			if err != nil {
				return nil, err
			}
			out = append(out, task)
			seen[record.TaskID] = true
		}
	}
	if pathFilter == "" || pathFilter == "ordinary" {
		links, e := h.messageQueue.LegacySourceLinks(ctx)
		if e != nil {
			return nil, e
		}
		for _, link := range links {
			if !h.legacyReadAllowed(c, link) || (sessionID != "" && link.Source.SessionID != sessionID && link.Target.SessionID != sessionID) {
				continue
			}
			record := h.legacyProjectionRecord(link)
			if seen[record.TaskID] {
				continue
			}
			task, e := h.projectLegacyAPITask(ctx, record, link.ID)
			if e != nil {
				return nil, e
			}
			out = append(out, task)
			seen[record.TaskID] = true
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].(map[string]any)["task_id"].(string) < out[j].(map[string]any)["task_id"].(string)
	})
	return out, nil
}
func (h *Hub) Read(ctx context.Context, c taskapi.AuthorizedCommand) (any, error) {
	var in apiInput
	json.Unmarshal(c.Request.Input, &in)
	switch c.Request.Operation {
	case "legacy_source":
		return h.readLegacySource(ctx, c)
	case "read_input":
		source, ok := h.registry.Get(c.Caller.SourceSessionID)
		if !ok {
			return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		scope, err := h.TaskWorkerScope(source)
		if err != nil || scope == nil {
			return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		var file struct {
			Relative string `json:"relative_path"`
		}
		json.Unmarshal(c.Request.Input, &file)
		if filepath.IsAbs(file.Relative) {
			return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		raw, found, err := h.delegations.APIChildMetadata(ctx, source.ID)
		if err != nil || !found {
			return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		var metadata apiMetadata
		if json.Unmarshal(raw, &metadata) != nil || metadata.CanonicalCwd == "" {
			return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		path := filepath.Clean(filepath.Join(metadata.CanonicalCwd, file.Relative))
		approvedRoot := ""
		relative := ""
		for _, root := range scope.Roots {
			rel, e := filepath.Rel(root, path)
			if e == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
				approvedRoot = root
				relative = rel
				break
			}
		}
		if approvedRoot == "" {
			return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		// OpenRoot confines every component/symlink beneath the approved root,
		// rather than checking a narrow root and opening the broader cwd.
		confined, err := os.OpenRoot(approvedRoot)
		if err != nil {
			return nil, err
		}
		defer confined.Close()
		actual, err := confined.Stat(".")
		if err != nil {
			return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		identity, err := taskRootInfoIdentity(actual)
		if err != nil || metadata.RootIdentities[approvedRoot] == "" || identity != metadata.RootIdentities[approvedRoot] {
			return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		fileHandle, err := confined.Open(relative)
		if err != nil {
			return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		defer fileHandle.Close()
		info, err := fileHandle.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() > 64000 {
			return nil, taskapi.Failure("invalid_argument", "known_none", "correct_input")
		}
		bytes, err := io.ReadAll(io.LimitReader(fileHandle, 64001))
		if err != nil || len(bytes) > 64000 || !utf8.Valid(bytes) {
			return nil, taskapi.Failure("invalid_argument", "known_none", "correct_input")
		}
		hash := sha256.Sum256(bytes)
		return map[string]any{"relative_path": file.Relative, "text": string(bytes), "hash": hex.EncodeToString(hash[:]), "truncated": false}, nil

	case "capabilities":
		ops := []string{"capabilities", "create_dispatch", "list", "get", "read_result", "append", "cancel", "snapshot", "events"}
		routes := map[string]string{}
		if _, ok := h.exec.(backend.ExistingTaskScopeEnforcer); ok {
			routes["existing_target"] = "ordinary"
		}
		if c.Caller.SourceSessionID != "" {
			if _, ok := h.exec.(backend.ExistingTaskScopeEnforcer); ok {
				routes["existing_target"] = "controller"
			}
			routes["new_worker"] = "delegation"
		}
		providers := []taskapi.ProviderCapability{taskapi.UnloadedCapability("codex", "unknown", "Adapter not registered."), taskapi.UnloadedCapability("claude", "unknown", "Adapter not registered.")}
		if source, ok := h.exec.(backend.TaskCapabilitySource); ok {
			providers = source.TaskAPICapabilities()
		}
		if c.Caller.BindingKind == "paired_human" {
			ops = append(ops, "legacy_source")
		}
		if !apiOwnedAdmissionEnabled {
			ops = []string{"capabilities", "list", "get", "read_result", "cancel", "snapshot", "events"}
			if c.Caller.BindingKind == "paired_human" {
				ops = append(ops, "legacy_source")
			}
			routes = map[string]string{}
		}
		return map[string]any{"api_versions": []string{contract.Version}, "schema_hash": contract.Hash(), "allowed_operations": ops, "paths": []string{"ordinary", "controller", "delegation"}, "providers": providers, "limits": map[string]int{"max_instruction_chars": 32000, "max_pending_children": 3, "event_retention_days": 30, "max_events_per_scope": 100000, "receipt_retention_days": 90}, "default_worker_profile": map[string]string{"backend": "codex", "model": "gpt-6.1-sol", "effort": "high"}, "mutation_authority_instance_id": h.cfg.InstanceID, "create_routes": routes}, nil
	case "get":
		if in.Original != nil {
			j := h.apiJournals()[c.Locator.Path]
			r, err := j.Lookup(ctx, c.Namespace, c.Locator.Key)
			if err != nil {
				return nil, err
			}
			if e := h.checkAPIRecord(ctx, c.Caller, r); e != nil {
				return nil, e
			}
			var original apiMetadata
			json.Unmarshal(r.Metadata, &original)
			command := c
			command.Request = original.Request
			command.Locator = *in.Original
			command.Caller.Transport = original.Transport
			return h.apiReceipt(ctx, command, r)
		}
		r, err := h.apiRecord(ctx, c.Caller, in.TaskID)
		if err != nil {
			return nil, err
		}
		return h.apiTask(ctx, r)
	case "list":
		scope, err := h.apiReadScope(ctx, c.Caller, in)
		if err != nil {
			return nil, err
		}
		views := []string{"inbox", "self", "results", "source_children", "manager_arranging"}
		engine := taskapi.SnapshotEngine{Codec: h.taskCursor, Source: apiSnapshotSource{h, c.Caller, in.SessionID, views, in.Path}}
		page, err := engine.Page(ctx, scope, views, in.Cursor, in.Limit)
		if err != nil {
			return nil, err
		}
		result := map[string]any{"items": page.Items, "has_more": page.HasMore, "partial": false, "history_status": "partial"}
		if page.NextPageCursor != "" {
			result["next_cursor"] = page.NextPageCursor
		}
		return result, nil

	case "snapshot":
		scope, err := h.apiReadScope(ctx, c.Caller, in)
		if err != nil {
			return nil, err
		}
		source := apiSnapshotSource{h, c.Caller, in.SessionID, in.Views, in.Path}
		engine := taskapi.SnapshotEngine{Codec: h.taskCursor, Source: source}
		page, err := engine.Page(ctx, scope, in.Views, in.Cursor, in.Limit)
		if err != nil {
			return nil, err
		}
		for _, raw := range page.Items {
			task, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			id, _ := task["task_id"].(string)
			if _, err := h.apiRecord(ctx, c.Caller, id); err != nil {
				return nil, taskapi.Failure("cursor_expired", "known_none", "refresh_snapshot")
			}
		}
		if c.Revalidate == nil || c.Revalidate(ctx) != nil {
			return nil, taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
		}
		return page, nil
	case "events":
		return h.apiEvents(ctx, c, in)

	case "read_result":
		r, err := h.apiRecord(ctx, c.Caller, in.TaskID)
		if err != nil {
			return nil, err
		}
		task, err := h.apiTask(ctx, r)
		if err != nil {
			return nil, err
		}
		if task["seal_id"] != in.SealID {
			return nil, taskapi.Failure("result_not_ready", "known_none", "wait")
		}
		stored, err := h.messageQueue.TaskSeal(ctx, r.SessionID, r.RequestID, in.SealID)
		if err != nil {
			return nil, err
		}
		text := stored.Text
		chars := []rune(text)
		if in.Offset > len(chars) {
			return nil, taskapi.Failure("invalid_argument", "known_none", "correct_input")
		}
		end := in.Offset + in.Limit
		if end > len(chars) {
			end = len(chars)
		}
		return map[string]any{"task_id": r.TaskID, "seal_id": in.SealID, "final_hash": task["final_hash"], "anchor": task["result_anchor"], "terminal_status": "succeeded", "text": string(chars[in.Offset:end]), "offset": in.Offset, "next_offset": end, "has_more": end < len(chars), "artifacts": []any{}, "qa": task["qa"], "limitations": []string{}}, nil
	}
	return nil, taskapi.Failure("unsupported", "known_none", "read_capabilities")
}
func stringID(n uint64) string { raw, _ := json.Marshal(n); return string(raw) }
func (h *Hub) apiReadScope(ctx context.Context, c taskapi.VerifiedContext, in apiInput) (taskapi.ReadScope, error) {
	policies := map[string]any{}
	if c.SourceSessionID != "" && h.dispatches != nil {
		grant, err := h.dispatches.Grant(ctx, c.SourceSessionID)
		if err == nil {
			policies["controller_grant"] = grant
		}
		if s, ok := h.registry.Get(c.SourceSessionID); ok {
			policies["source_hidden"] = s.Snapshot().Hidden
			policies["source_control"] = h.controls.MobileMayWrite(s.ID)
			policies["source_thread"] = s.ResumeID()
		}
	}
	if h.work != nil {
		if state, err := h.work.Collaboration(ctx); err == nil {
			policies["pm_policy_revision"] = state.Revision
		}
	}
	hash, err := taskapi.FilterHash(map[string]any{"session_id": in.SessionID, "path": in.Path, "policy": policies})
	return taskapi.ReadScope{Authority: h.cfg.InstanceID, StableScopeID: c.StableScopeID, NamespaceGeneration: c.NamespaceGeneration, FilterHash: hash}, err
}

type apiSnapshotSource struct {
	hub       *Hub
	caller    taskapi.VerifiedContext
	sessionID string
	views     []string
	path      string
}

// Optimistic original-store snapshot: every projected persistent mutation has
// a same-transaction change sequence. Equal before/after vectors prove the
// frozen membership and rows were read while every participating store was
// unchanged. The materialization commit rechecks its local vector in SQLite.
func (s apiSnapshotSource) Capture(ctx context.Context, scope taskapi.ReadScope) (taskapi.Freeze, error) {
	s.hub.taskMu.Lock()
	defer s.hub.taskMu.Unlock()
	for attempt := 0; attempt < 5; attempt++ {
		before, err := s.hub.apiWatermarks(ctx)
		if err != nil {
			return taskapi.Freeze{}, err
		}
		items, err := s.hub.apiList(ctx, s.caller, s.sessionID, s.path)
		if err != nil {
			return taskapi.Freeze{}, err
		}
		rows := []taskapi.SnapshotRow{}
		for _, item := range items {
			task := item.(map[string]any)
			selected := false
			target := task["target"].(map[string]any)
			source := task["source"].(map[string]any)["identity"].(map[string]any)
			for _, actual := range task["views"].([]string) {
				for _, wanted := range s.views {
					membershipSession := target["session_id"]
					if actual == "source_children" || actual == "manager_arranging" {
						membershipSession = source["session_id"]
					}
					if actual == wanted && (s.sessionID == "" || membershipSession == s.sessionID) {
						selected = true
					}
				}
			}
			if selected {
				rows = append(rows, taskapi.SnapshotRow{Key: task["task_id"].(string), Value: item})
			}
		}
		after, err := s.hub.apiWatermarks(ctx)
		if err != nil {
			return taskapi.Freeze{}, err
		}
		a, _ := json.Marshal(before)
		b, _ := json.Marshal(after)
		if string(a) != string(b) {
			continue
		}
		current, err := s.hub.apiReadScope(ctx, s.caller, apiInput{SessionID: s.sessionID, Path: s.path})
		if err != nil || current != scope {
			return taskapi.Freeze{}, taskapi.Failure("cursor_expired", "known_none", "refresh_snapshot")
		}
		f := taskapi.Freeze{ID: "sf_" + randomID(), ExpiresAt: time.Now().Add(5 * time.Minute).UnixMilli(), Watermarks: before}
		local := uint64(0)
		for _, m := range before {
			if m.Store == "ordinary" {
				local = m.Sequence
			}
		}
		err = s.hub.messageQueue.TaskJournal().FreezeChecked(ctx, scope, f, rows, local)
		if failure, ok := err.(*taskapi.APIError); ok && failure.Code == "busy" {
			continue
		}
		return f, err
	}
	return taskapi.Freeze{}, taskapi.Failure("busy", "known_none", "wait")
}
func (h *Hub) apiWatermarks(ctx context.Context) ([]taskapi.Watermark, error) {
	marks := []taskapi.Watermark{}
	for path, j := range h.apiJournals() {
		if path == "pm_v1" {
			continue
		}
		n, err := j.Watermark(ctx)
		if err != nil {
			return nil, err
		}
		marks = append(marks, taskapi.Watermark{Store: path, Sequence: n})
	}
	sort.Slice(marks, func(i, j int) bool { return marks[i].Store < marks[j].Store })
	return marks, nil
}

// Events are durable invalidations, not invented historical axes. Consumers
// fetch get/snapshot. One precise command identity maps to at most one task.
func (h *Hub) apiEvents(ctx context.Context, c taskapi.AuthorizedCommand, in apiInput) (any, error) {
	scope, err := h.apiReadScope(ctx, c.Caller, in)
	if err != nil {
		return nil, err
	}
	byStore := map[string]uint64{}
	if in.Cursor != "" {
		marks, e := h.taskCursor.EventWatermarks(in.Cursor, scope)
		if e != nil {
			return nil, e
		}
		for _, m := range marks {
			byStore[m.Store] = m.Sequence
		}
	}
	items, err := h.apiList(ctx, c.Caller, in.SessionID, in.Path)
	if err != nil {
		return nil, err
	}
	index := map[string]map[string]any{}
	for _, item := range items {
		task := item.(map[string]any)
		ref := task["task_ref"].(map[string]string)
		records, e := h.apiJournals()[ref["path"]].TaskCommands(ctx, task["task_id"].(string))
		if e != nil {
			return nil, e
		}
		for _, r := range records {
			index[r.SessionID+"/"+r.RequestID] = task
		}
	}
	links, e := h.messageQueue.LegacySourceLinks(ctx)
	if e != nil {
		return nil, e
	}
	for _, link := range links {
		if (in.Path != "" && in.Path != "ordinary") || !h.legacyHistoryReadAllowed(c.Caller, link) || (in.SessionID != "" && link.Source.SessionID != in.SessionID && link.Target.SessionID != in.SessionID) {
			continue
		}
		record := h.legacyProjectionRecord(link)
		task, e := h.projectLegacyAPITask(ctx, record, link.ID)
		if e != nil {
			return nil, e
		}
		index[link.Target.SessionID+"/"+link.RequestID] = task
	}
	paths := []string{}
	for path := range h.apiJournals() {
		if path != "pm_v1" {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	events := []any{}
	floors := []taskapi.Watermark{}
	more := false
	for _, path := range paths {
		j := h.apiJournals()[path]
		changes, floor, e := j.Changes(ctx, byStore[path], in.Limit+1)
		if e != nil {
			return nil, e
		}
		floors = append(floors, taskapi.Watermark{Store: path, Sequence: floor})
		scanned := 0
		for _, change := range changes {
			task := index[change.SessionID+"/"+change.RequestID]
			if scanned >= in.Limit || (task != nil && len(events) >= in.Limit) {
				more = true
				break
			}
			scanned++
			byStore[path] = change.Sequence
			if task == nil {
				continue
			}
			kind := "execution"
			switch change.Kind {
			case "admission", "intent_prepared", "human_source_linked":
				kind = "admission"
			case "final_sealed":
				kind = "final"
			case "human_source_revoked":
				kind = "scope_revoked"
			case "cancelled":
				kind = "cancel"
			}
			events = append(events, map[string]any{"event_id": path + ":" + stringID(change.Sequence), "authority_instance_id": h.cfg.InstanceID, "store_sequence": taskapi.Watermark{Store: path, Sequence: change.Sequence}, "task_ref": task["task_ref"], "task_revision": task["revision"], "kind": kind, "occurred_at_ms": change.At})
		}
		if _, ok := byStore[path]; !ok {
			byStore[path] = 0
		}
	}
	marks := []taskapi.Watermark{}
	for _, path := range paths {
		marks = append(marks, taskapi.Watermark{Store: path, Sequence: byStore[path]})
	}
	cursor, err := h.taskCursor.EventsFromWatermarks(scope, marks, time.Now().Add(30*24*time.Hour))
	if err != nil {
		return nil, err
	}
	return map[string]any{"events": events, "next_cursor": cursor, "retention_floor": floors, "has_more": more, "partial": false}, nil
}

func (s apiSnapshotSource) Page(ctx context.Context, scope taskapi.ReadScope, f taskapi.Freeze, after string, limit int) ([]taskapi.SnapshotRow, bool, error) {
	rows, err := s.hub.messageQueue.TaskJournal().Frozen(ctx, scope, f)
	if err != nil {
		return nil, false, err
	}
	out := []taskapi.SnapshotRow{}
	for _, r := range rows {
		if r.Key > after {
			if _, e := s.hub.apiRecord(ctx, s.caller, r.Key); e != nil {
				return nil, false, taskapi.Failure("cursor_expired", "known_none", "refresh_snapshot")
			}
			out = append(out, r)
		}
	}
	more := len(out) > limit
	if more {
		out = out[:limit]
	}
	return out, more, nil
}

func (h *Hub) claudeAnchorID(target *session.Session, proof messagequeue.ProviderEvidence) string {
	if target == nil || target.Backend() != backend.Claude {
		return ""
	}
	router, ok := h.exec.(historyRouter)
	if !ok {
		return ""
	}
	provider, ok := router.ProviderFor(target)
	if !ok {
		return ""
	}
	authority, ok := provider.(interface {
		NativeTaskFinalAnchor(string, string, string) (string, bool)
	})
	if !ok {
		return ""
	}
	id, ok := authority.NativeTaskFinalAnchor(proof.ConversationID, proof.MessageID, proof.Text)
	if !ok {
		return ""
	}
	result, err := loadLogicalSessionHistory(provider, []string{proof.ConversationID}, history.Opts{Limit: 10000, Mode: "snapshot"})
	if err != nil || result == nil {
		return ""
	}
	for _, message := range result.Messages {
		if message["source_message_id"] == id && message["history_read_result_verified"] == true && message["content"] == proof.Text {
			return id
		}
	}
	return ""
}

func (h *Hub) apiDelivery(ctx context.Context, r taskapi.IntentRecord) (string, any) {
	parentID, requestID, threadID, state := "", "", "", "pending"
	var metadata apiMetadata
	json.Unmarshal(r.Metadata, &metadata)
	if r.Path == "delegation" {
		original, found, e := h.delegations.ByID(ctx, r.NativeID)
		if e != nil || !found {
			return "unknown", nil
		}
		if r.RequestID != original.ChildRequestID {
			return "not_requested", nil
		}
		parentID = original.ParentSessionID
		requestID = original.ParentRequestID
		threadID = metadata.SourceThread
		state = original.DeliveryState
	}
	if r.Path == "controller" {
		original, found, e := h.dispatches.Get(ctx, r.NativeID)
		if e != nil || !found {
			return "unknown", nil
		}
		if r.RequestID != original.RequestID {
			return "not_requested", nil
		}
		parentID = original.ParentID
		requestID = "screturn_" + original.ID
		threadID = original.ParentThreadID
		state = original.DeliveryState
	}
	if state == "failed" {
		return "failed", nil
	}
	entry, found, e := h.messageQueue.Get(parentID, requestID)
	if e != nil {
		return "unknown", nil
	}
	if !found {
		return "pending", nil
	}
	parent, ok := h.registry.Get(parentID)
	if !ok || parent.ResumeID() != threadID {
		return "unknown", nil
	}
	if conflict, e := h.messageQueue.NativeConflict(parentID, requestID); e != nil || conflict {
		return "unknown", nil
	}
	var anchor any
	if parent.Backend() == backend.Codex {
		turn := h.nativeTaskTurn(parent, requestID)
		if turn != "" {
			if entry.State == messagequeue.Completed {
				finals, _ := h.sessionTaskFinals(parent)
				final := finals[requestID]
				if final != nil && final["source_turn_id"] == turn && final["source_message_id"] != "" {
					anchor = map[string]any{"instance_id": h.cfg.InstanceID, "session_id": parentID, "request_id": requestID, "source_message_id": final["source_message_id"], "execution": map[string]string{"backend": "codex", "native_thread_id": threadID, "native_turn_id": turn}}
				}
			}
			if anchor == nil {
				return "native_consumed", nil
			}
		}
	} else if proof, found, e := h.messageQueue.ProviderEvidence(parentID, requestID); e == nil && found && proof.ConversationID == threadID {
		if entry.State == messagequeue.Completed {
			if id := h.claudeAnchorID(parent, proof); id != "" {
				anchor = map[string]any{"instance_id": h.cfg.InstanceID, "session_id": parentID, "request_id": requestID, "source_message_id": id, "execution": map[string]string{"backend": "claude", "provider_session_id": proof.ConversationID, "token_kind": proof.TokenKind, "provider_token": proof.Token}}
			}
		}
		if anchor == nil {
			return "native_consumed", nil
		}
	}
	if anchor != nil {
		return "delivered", anchor
	}
	if entry.State == messagequeue.Queued || entry.State == messagequeue.Running {
		return "queued", nil
	}
	return "unknown", nil
}
