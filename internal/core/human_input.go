package core

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"

	"everything-go/internal/session"
	"fmt"
	"time"
)

type humanInputCommand struct {
	IntakeRequest   json.RawMessage             `json:"intake_request,omitempty"`
	Binding         messagequeue.HumanBinding   `json:"binding"`
	Operation       messagequeue.HumanOperation `json:"operation"`
	LookupOperation string                      `json:"lookup_operation,omitempty"`
	LookupHash      string                      `json:"lookup_hash,omitempty"`
	Read            bool                        `json:"read,omitempty"`
}

func (h *Hub) humanInputBinding(c *Client, s *session.Session, b messagequeue.HumanBinding, write bool) error {
	if h.messageQueue == nil || h.pairedTaskDevice(c) == "" || b.Device != h.pairedTaskDevice(c) || b.Authority != h.cfg.InstanceID || b.Session != s.ID || !h.controllerInScope(s) || s.Snapshot().Hidden || s.State() == session.Closed {
		return fmt.Errorf("source_unavailable")
	}
	actor, actorErr := h.mediaActor(c)
	if actorErr != nil || actor.NamespaceGeneration != b.Generation {
		return fmt.Errorf("source_generation_changed")
	}
	if write && (!h.messageQueue.HumanThreadCompatible(b, s.ResumeID()) || b.Config != s.SettingsSnapshot().ConfigRevision || b.Epoch != h.SessionRoleEpoch(s) || !h.controls.MobileMayWrite(s.ID) || h.contextAdmissionBlocked(s)) {
		return fmt.Errorf("source_changed")
	}
	if write && h.work != nil {
		v, e := h.work.Collaboration(context.Background())
		if e != nil {
			return e
		}
		_, task, managed := v.ProjectForSession(s.ID)
		if managed && (task == nil || task.Owner != "human") {
			return fmt.Errorf("managed_source")
		}
	}
	return nil
}
func (h *Hub) handleHumanInput(c *Client, cmd clientproto.Command) {
	var in humanInputCommand
	send := func(r any) {
		c.enqueueEvent(map[string]any{"type": "queue_input_result", "session_id": cmd.SessionID, "request_id": cmd.RequestID, "instance_id": h.cfg.InstanceID, "result": r})
	}
	verifiedOperation := false
	reject := func(code string) {
		if verifiedOperation && h.messageQueue != nil && in.Operation.ID != "" && !in.Read && in.LookupOperation == "" {
			if receipt, err := h.messageQueue.RejectHumanOperation(in.Binding, in.Operation, code); err == nil {
				send(receipt)
				return
			}
		}
		send(map[string]any{"status": "unavailable", "code": code})
	}
	decoder := json.NewDecoder(bytes.NewReader(cmd.QueueInput))
	decoder.DisallowUnknownFields()
	if len(cmd.QueueInput) > 512*1024 || decoder.Decode(&in) != nil || decoder.Decode(new(any)) != io.EOF {
		reject("invalid_input")
		return
	}
	s, ok := h.registry.Get(cmd.SessionID)
	if !ok {
		reject("source_unavailable")
		return
	}
	identity := c.readIdentity.Load()
	if in.Read && in.Binding.Device == "" {
		actor, e := h.mediaActor(c)
		if e != nil || h.pairedTaskDevice(c) == "" || s.Snapshot().Hidden || s.State() == session.Closed || !h.controllerInScope(s) {
			reject("source_unavailable")
			return
		}
		b := messagequeue.HumanBinding{Device: h.pairedTaskDevice(c), Authority: h.cfg.InstanceID, Session: s.ID, Thread: s.ResumeID(), Config: s.SettingsSnapshot().ConfigRevision, Epoch: h.SessionRoleEpoch(s), Generation: actor.NamespaceGeneration}
		compatible, err := h.messageQueue.HumanCompatibleBindings(b)
		if err != nil {
			reject("snapshot_unavailable")
			return
		}
		if identity != c.readIdentity.Load() {
			reject("source_changed")
			return
		}
		send(map[string]any{"status": "source", "binding": b, "compatible_bindings": compatible, "limit": messagequeue.HumanInputLimit})
		return
	}
	sourceSnapshot := s.SettingsSnapshot()
	write := !in.Read && in.LookupOperation == ""
	if e := h.humanInputBinding(c, s, in.Binding, write); e != nil {
		reject(e.Error())
		return
	}
	if write && (!h.messageQueue.HumanThreadCompatible(in.Binding, sourceSnapshot.ResumeID) || sourceSnapshot.ConfigRevision != in.Binding.Config) {
		reject("source_changed")
		return
	}
	verifiedOperation = write
	if !write {
		if in.LookupOperation != "" {
			r, e := h.messageQueue.ReadHumanOperation(in.Binding, in.LookupOperation, in.LookupHash)
			if e != nil {
				reject("receipt_unavailable")
				return
			}
			if identity != c.readIdentity.Load() || h.humanInputBinding(c, s, in.Binding, false) != nil {
				reject("source_changed")
				return
			}
			send(r)
		} else {
			rows, e := h.messageQueue.HumanInputs(in.Binding)
			if e != nil {
				reject("snapshot_unavailable")
				return
			}
			if identity != c.readIdentity.Load() || h.humanInputBinding(c, s, in.Binding, false) != nil {
				reject("source_changed")
				return
			}
			revision, membership, e := h.messageQueue.HumanMembership(in.Binding)
			if e != nil {
				reject("snapshot_unavailable")
				return
			}
			send(map[string]any{"status": "snapshot", "items": rows, "queue_revision": revision, "membership_hash": membership})
		}
		return
	}
	op := in.Operation
	if op.Kind == "seal" || op.Kind == "commit_edit" {
		if cmd.RequestID != op.Request || cmd.MessagePurpose != "instruction" || cmd.TaskOrigin != nil || cmd.VoiceContext || len(cmd.AsyncCovers) > 0 || cmd.AsyncBatch != nil {
			reject("ordinary_input_required")
			return
		}
		if cmd.Content != op.Content {
			reject("content_mismatch")
			return
		}
		// enabled with its original-version writer below
		inputs, e := h.messageQueue.HumanInputs(in.Binding)
		if e != nil {
			reject("snapshot_unavailable")
			return
		}
		var owned *messagequeue.HumanInput
		for i := range inputs {
			if inputs[i].ID == op.Request {
				owned = &inputs[i]
				break
			}
		}
		if owned == nil {
			reject("reservation_required")
			return
		}
		if (owned.Origin == "formal_intake" && op.Kind == "seal") != (len(in.IntakeRequest) > 0) {
			reject("original_input_origin_required")
			return
		}
		if owned.Origin == "formal_intake" && h.intakeWritePolicy(s) != nil {
			reject("intake_scope_changed")
			return
		}
		var assets []map[string]any
		assetBody := owned.Assets
		if op.Kind == "commit_edit" && len(op.Assets) > 0 {
			assetBody = op.Assets
		}
		if json.Unmarshal(assetBody, &assets) != nil || len(assets) != len(cmd.Images)+len(cmd.Files) {
			reject("media_manifest_mismatch")
			return
		}
		// The old media owner performs full file/body/source/generation validation
		// during enqueue. A typed media proof cannot be supplied as a bare UI claim.
		proof := owned.IntentHash
		if len(cmd.Images)+len(cmd.Files) > 0 {
			actor, e := h.mediaActor(c)
			if e != nil {
				reject("media_source_unavailable")
				return
			}
			release, e := h.media.begin()
			if e != nil {
				reject("media_store_unavailable")
				return
			}
			b := h.media.data.Batches[mediaKey(in.Binding.Device, actor.NamespaceGeneration, cmd.MediaBatchID)]
			if b == nil || b.Session != s.ID || b.Device != in.Binding.Device || b.Content != cmd.Content || b.BodyHash != op.ReadyProof {
				release()
				reject("media_proof_required")
				return
			}
			// Match every originally reserved asset to one complete media item.
			// Count alone would permit swapping a prepared image or file while
			// retaining the original opaque native intent hash.
			remaining := append([]protocol.MediaBatchItem(nil), b.Items...)
			for _, asset := range assets {
				found := -1
				for i, item := range remaining {
					if asset["kind"] == item.UploadKind && asset["media_type"] == item.MediaType && asset["sha256"] == item.SHA256 && asset["name"] == item.Name && asset["size"] == float64(item.SizeBytes) {
						found = i
						break
					}
				}
				if found < 0 {
					release()
					reject("media_manifest_mismatch")
					return
				}
				remaining = append(remaining[:found], remaining[found+1:]...)
			}
			if len(remaining) != 0 {
				release()
				reject("media_manifest_mismatch")
				return
			}
			proof = b.BodyHash
			release()
		}
		if proof != op.ReadyProof {
			reject("ready_proof_mismatch")
			return
		}
		boot, now := h.messageQueue.HumanClock()
		cmd.HumanSeal = &messagequeue.HumanSeal{Binding: in.Binding, Operation: op, Proof: proof, Boot: boot, Now: now, Clock: h.messageQueue.HumanClock, Commit: func(commit func() error) error {
			if identity != c.readIdentity.Load() {
				return fmt.Errorf("source_changed")
			}
			return s.CommitInputSource(sourceSnapshot, func() error { return c.readIdentity.WithCurrent(identity, commit) })
		}}
		release := s.HoldQueue()
		defer release()
		if s.ActiveQueuedID() == op.Request {
			reject("native_boundary")
			return
		}
		admitted := false
		if owned.Origin == "formal_intake" && op.Kind == "seal" {
			admitted = h.enqueueHumanIntake(c, cmd, in.IntakeRequest)
		} else {
			admitted = h.enqueueChatMessageExpected(c, cmd, &dispatchTargetExpectation{ThreadID: sourceSnapshot.ResumeID, Revision: in.Binding.Config})
		}
		if admitted {
			receipt, e := h.messageQueue.ReadHumanOperation(in.Binding, op.ID, "")
			if e != nil {
				reject("receipt_unavailable")
				return
			}
			send(receipt)
		} else {
			// This exact synchronous writer has returned. Atomically fence
			// its prepared operation, or return its existing commit receipt.
			if receipt, err := h.messageQueue.RejectHumanOperation(in.Binding, op, "admission_interrupted"); err == nil {
				send(receipt)
			}
		}
		return
	}
	// Freeze the actor's dequeue boundary before inspecting/persisting a waiting
	// mutation. A callback already removed from the mailbox is not editable.
	release := s.HoldQueue()
	defer release()
	h.messageQueueMu.Lock()
	defer h.messageQueueMu.Unlock()
	if identity != c.readIdentity.Load() || h.humanInputBinding(c, s, in.Binding, true) != nil {
		reject("source_changed")
		return
	}
	if op.Kind != "reserve" && s.ActiveQueuedID() == op.Request {
		reject("native_boundary")
		return
	}
	boot, now := h.messageQueue.HumanClock()
	var orderCommit func([]string, func() error) error
	if op.Kind == "move" {
		before := s.WaitingIDs()
		orderCommit = func(order []string, commit func() error) error {
			present := map[string]bool{}
			for _, id := range before {
				if id == "" || present[id] {
					return fmt.Errorf("anonymous_barrier")
				}
				present[id] = true
			}
			after := []string{}
			for _, id := range order {
				if present[id] {
					after = append(after, id)
				}
			}
			if len(after) != len(before) {
				return fmt.Errorf("membership_changed")
			}
			return s.ReorderWaitingCommitted(before, after, commit)
		}
	}
	r, e := h.messageQueue.HumanApplyOrder(in.Binding, op, boot, now, orderCommit)
	if e != nil {
		reject("operation_conflict")
		return
	}
	if r.Status == "committed" {
		switch op.Kind {
		case "begin_edit", "renew_edit":
			time.AfterFunc(messagequeue.HumanEditTTL, func() { boot, now := h.messageQueue.HumanClock(); _ = h.messageQueue.ExpireHumanEdits(boot, now) })
		case "reserve":
			if !h.submitQueuedMessage(s, op.Request) {
				reject("actor_recovery_required")
				return
			}
		case "cancel":
			_ = s.CancelQueued(op.Request)
		}
	}
	send(r)
	h.publishMessageQueue(s.ID)
}
func (h *Hub) syncHumanWaiting(s *session.Session) error {
	order, e := h.messageQueue.HumanOrder(s.ID)
	if e != nil {
		return e
	}
	before := s.WaitingIDs()
	set := map[string]bool{}
	for _, id := range before {
		if id == "" {
			return fmt.Errorf("anonymous_barrier")
		}
		set[id] = true
	}
	after := []string{}
	for _, id := range order {
		if set[id] {
			after = append(after, id)
		}
	}
	if len(after) != len(before) {
		return fmt.Errorf("membership_changed")
	}
	return s.ReorderWaiting(before, after)
}
