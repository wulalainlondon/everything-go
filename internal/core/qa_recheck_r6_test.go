package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/automation"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"reflect"
	"strings"
	"testing"
	"time"
)

func qaR6Formal(t *testing.T, origin string) (*Hub, *session.Session, *Client, messagequeue.HumanBinding, messagequeue.HumanOperation, clientproto.Command, func()) {
	t.Helper()
	h, s, c, _ := mediaFixture(t)
	release := s.HoldQueue()
	t.Cleanup(func() { s.Close(); release() })
	actor, e := h.mediaActor(c)
	if e != nil {
		t.Fatal(e)
	}
	b := messagequeue.HumanBinding{Device: "phone-a", Authority: h.cfg.InstanceID, Session: s.ID, Thread: s.ResumeID(), Config: s.SettingsSnapshot().ConfigRevision, Epoch: h.SessionRoleEpoch(s), Generation: actor.NamespaceGeneration}
	reserve := messagequeue.HumanOperation{ID: "reserve_cohort_formal", Kind: "reserve", Origin: origin, Request: "r_" + strings.Repeat("9", 32), Content: "original body", IntentHash: strings.Repeat("a", 64), Assets: json.RawMessage(`[]`)}
	h.handleHumanInput(c, humanCoreCommand(b, reserve))
	out := waitForType(t, c, "queue_input_result")
	if out["result"].(map[string]any)["status"] != "committed" {
		t.Fatal(out)
	}
	seal := messagequeue.HumanOperation{ID: "seal_cohort_formal", Kind: "seal", Request: reserve.Request, ExpectedRevision: 1, ContentRevision: 1, IntentHash: reserve.IntentHash, Content: reserve.Content, ReadyProof: reserve.IntentHash}
	return h, s, c, b, seal, humanTypedIntakeCommand(b, seal, nil, nil, ""), release
}
func qaR6Namespace(b messagequeue.HumanBinding) taskapi.Namespace {
	return taskapi.Namespace{Authority: b.Authority, StableScopeID: "device:" + b.Device, Generation: b.Generation, Path: "ordinary", Operation: "intake"}
}
func qaR6FirstEffect(t *testing.T, h *Hub, b messagequeue.HumanBinding, request, goal, purpose string) {
	t.Helper()
	if h.automation == nil {
		attachAutomationStore(t, h, t.TempDir())
	}
	in := jarvisCreate{IntentID: "r_create_cohort", SessionID: b.Session, Authority: b.Authority, Cwd: h.cfg.RootDir, Backend: "codex", Model: "fixture-model", Sandbox: "read-only", Name: "Original", Purpose: purpose, FirstEffectID: request, InstructionHash: jarvisTextHash(goal)}
	raw, _ := json.Marshal(in)
	op, e := h.automation.ClaimWorkbench(context.Background(), automation.WorkbenchOperation{IntentID: in.IntentID, Kind: "create", Device: jarvisPrincipal(b.Device, b.Generation), InputHash: jarvisHash(in), SessionID: b.Session, Input: raw})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = h.automation.CommitWorkbench(context.Background(), op); e != nil {
		t.Fatal(e)
	}
}
func TestQARecheckFormalRefusalsNeverDowngradeOrLeaveAPIEntry(t *testing.T) {
	for _, change := range []string{"ordinary_downgrade", "ordinary_upgrade", "goal", "target_authority", "target_session", "target_config", "target_thread", "nested_request", "unknown_field", "first_effect_request", "first_effect_goal", "first_effect_purpose"} {
		t.Run(change, func(t *testing.T) {
			origin := "formal_intake"
			if change == "ordinary_upgrade" {
				origin = ""
			}
			h, s, c, b, seal, cmd, _ := qaR6Formal(t, origin)
			if strings.HasPrefix(change, "first_effect") {
				request, goal, purpose := seal.Request, seal.Content, "ordinary_prompt"
				switch change {
				case "first_effect_request":
					request = "r_" + strings.Repeat("8", 32)
				case "first_effect_goal":
					goal = "different original instruction"
				case "first_effect_purpose":
					purpose = "legacy_message"
				}
				qaR6FirstEffect(t, h, b, request, goal, purpose)
			}
			if change == "ordinary_downgrade" {
				cmd = humanCoreCommand(b, seal)
			} else if !strings.HasPrefix(change, "first_effect") && change != "ordinary_upgrade" {
				var in humanInputCommand
				json.Unmarshal(cmd.QueueInput, &in)
				var envelope map[string]any
				json.Unmarshal(in.IntakeRequest, &envelope)
				req := envelope["request"].(map[string]any)
				input := req["input"].(map[string]any)
				target := input["target"].(map[string]any)
				switch change {
				case "goal":
					input["goal"] = "different"
				case "target_authority":
					target["instance_id"] = "foreign"
				case "target_session":
					target["session_id"] = "s_foreign"
				case "target_config":
					target["expected_config_revision"] = float64(b.Config + 1)
				case "target_thread":
					target["expected_native_thread_id"] = "foreign"
				case "nested_request":
					req["idempotency_key"] = "r_" + strings.Repeat("7", 32)
				case "unknown_field":
					input["fabricated_caller"] = "operator"
				}
				in.IntakeRequest, _ = json.Marshal(envelope)
				cmd.QueueInput, _ = json.Marshal(in)
			}
			h.handleHumanInput(c, cmd)
			receipt, e := h.messageQueue.ReadHumanOperation(b, seal.ID, "")
			if e != nil || receipt.Status != "rejected_known_none" {
				t.Fatal("not exact refused original", receipt, e)
			}
			if _, found, e := h.messageQueue.Get(s.ID, seal.Request); e != nil || found {
				t.Fatal("refusal queued/downgraded", found, e)
			}
			if _, e := h.messageQueue.TaskJournal().Lookup(context.Background(), qaR6Namespace(b), seal.Request); e == nil {
				t.Fatal("refusal left API intent record")
			}
		})
	}
}
func TestQARecheckFormalFirstEffectEditPreservesOriginalTaskAndActualSend(t *testing.T) {
	h, s, c, b, seal, cmd, release := qaR6Formal(t, "formal_intake")
	qaR6FirstEffect(t, h, b, seal.Request, seal.Content, "ordinary_prompt")
	h.handleHumanInput(c, cmd)
	first, e := h.messageQueue.ReadHumanOperation(b, seal.ID, "")
	if e != nil || first.Status != "committed" {
		t.Fatal(first, e)
	}
	record, e := h.messageQueue.TaskJournal().Lookup(context.Background(), qaR6Namespace(b), seal.Request)
	if e != nil {
		t.Fatal(e)
	}
	entry, _, _ := h.messageQueue.GetEffective(s.ID, seal.Request)
	var before queuedPayload
	json.Unmarshal(entry.Payload, &before)
	rows, _ := h.messageQueue.HumanInputs(b)
	v := rows[0]
	rev, membership, _ := h.messageQueue.HumanMembership(b)
	boot, now := h.messageQueue.HumanClock()
	begin, e := h.messageQueue.HumanApply(b, messagequeue.HumanOperation{ID: "begin_cohort_formal", Kind: "begin_edit", Request: v.ID, ExpectedRevision: v.Revision, ContentRevision: v.ContentRevision, ExpectedHash: v.EffectiveHash, QueueRevision: &rev, MembershipHash: membership}, boot, now)
	if e != nil || begin.Status != "committed" {
		t.Fatal(begin, e)
	}
	op := messagequeue.HumanOperation{ID: "commit_cohort_formal", Kind: "commit_edit", Request: v.ID, ExpectedRevision: begin.Input.Revision, ContentRevision: v.ContentRevision, ExpectedHash: v.EffectiveHash, IntentHash: v.IntentHash, LeaseID: begin.Input.Lease.ID, LeaseGeneration: begin.Input.Lease.Generation, Content: "human edited after original admission", Assets: json.RawMessage(`[]`), ReadyProof: v.IntentHash}
	editCmd := humanCoreCommand(b, op)
	truth := true
	editCmd.CallbackOnComplete = &truth
	h.handleHumanInput(c, editCmd)
	edited, e := h.messageQueue.ReadHumanOperation(b, op.ID, "")
	if e != nil || edited.Status != "committed" {
		t.Fatal(edited, e)
	}
	afterRecord, e := h.messageQueue.TaskJournal().Lookup(context.Background(), qaR6Namespace(b), seal.Request)
	if e != nil || !reflect.DeepEqual(afterRecord, record) {
		t.Fatal("original API receipt/metadata replaced", afterRecord, record, e)
	}
	entry, _, _ = h.messageQueue.GetEffective(s.ID, seal.Request)
	var after queuedPayload
	json.Unmarshal(entry.Payload, &after)
	before.Content = after.Content
	before.Images = after.Images
	before.Files = after.Files
	before.MediaBatchID = after.MediaBatchID
	before.MediaBodyHash = after.MediaBodyHash
	before.VisualUserHash = after.VisualUserHash
	before.VisualEffectiveHash = after.VisualEffectiveHash
	if !after.FormalIntake || after.OwnerDevice != b.Device || after.RoleEpoch != b.Epoch || !reflect.DeepEqual(before, after) {
		t.Fatal("formal policy/target/owner/first effect metadata changed", before, after)
	}
	calls := make(chan qaCorrectionConsumed, 2)
	h.SetExecutor(&qaCorrectionExecutor{fakeExec: &fakeExec{sink: h}, calls: calls})
	release()
	select {
	case got := <-calls:
		if got.request != op.Request || got.content != op.Content || got.guard != nil {
			t.Fatal(got)
		}
	case <-time.After(3 * time.Second):
		entry, _, _ := h.messageQueue.GetEffective(s.ID, op.Request)
		t.Fatal("formal edited native boundary unavailable", entry.State, entry.Message)
	}
}

func TestQARecheckFormalMediaOriginalAtomicFaultAndFinalBoundary(t *testing.T) {
	for _, point := range []string{"prepare_before_queue", "queue_commit_before_receipt", "identity_change", "source_ABA"} {
		t.Run(point, func(t *testing.T) {
			h, s, c, b, _, _, _ := qaR6Formal(t, "formal_intake")
			data := mediaPNG(t)
			sum := sha256.Sum256(data)
			assets, _ := json.Marshal([]map[string]any{{"kind": "image", "media_type": "image/png", "sha256": hex.EncodeToString(sum[:]), "name": "圖片.png", "size": len(data)}})
			reserve := messagequeue.HumanOperation{ID: "reserve_formal_media_atomic", Kind: "reserve", Origin: "formal_intake", Request: "r_" + strings.Repeat("6", 32), IntentHash: strings.Repeat("c", 64), Assets: assets}
			h.handleHumanInput(c, humanCoreCommand(b, reserve))
			out := waitForType(t, c, "queue_input_result")
			if out["result"].(map[string]any)["status"] != "committed" {
				t.Fatal(out)
			}
			prepare, item, body := mediaPrepareItem(t, h, s, c, reserve.Request, data)
			upload := mediaUploadItem(t, h, s, c, prepare, item, body, data)
			seal := messagequeue.HumanOperation{ID: "seal_formal_media_atomic", Kind: "seal", Request: reserve.Request, ExpectedRevision: 1, ContentRevision: 1, IntentHash: reserve.IntentHash, ReadyProof: body}
			images := []backend.ImageAttachment{{AttachmentID: upload, MediaType: item.MediaType, SHA256: item.SHA256, SizeBytes: item.SizeBytes}}
			identity := c.readIdentity.Load()
			injected := false
			h.media.fault = func(at string) error {
				if injected {
					return nil
				}
				if (point == "prepare_before_queue" || point == "queue_commit_before_receipt") && at == point {
					injected = true
					return errors.New("QA original atomic boundary")
				}
				if at == "prepare_before_queue" {
					switch point {
					case "identity_change":
						replacement := *identity
						replacement.token = "retired"
						c.readIdentity.Store(&replacement)
						injected = true
					case "source_ABA":
						s.SetResumeID("foreign")
						s.SetResumeID("")
						injected = true
					}
				}
				return nil
			}
			h.handleHumanInput(c, humanTypedIntakeCommand(b, seal, images, nil, prepare.MediaBatchID))
			h.media.fault = nil
			if !injected {
				t.Fatal("did not exercise exact original fault")
			}
			receipt, e := h.messageQueue.ReadHumanOperation(b, seal.ID, "")
			if e != nil {
				t.Fatal(e)
			}
			entry, found, e := h.messageQueue.GetEffective(s.ID, reserve.Request)
			if e != nil {
				t.Fatal(e)
			}
			record, recordErr := h.messageQueue.TaskJournal().Lookup(context.Background(), qaR6Namespace(b), reserve.Request)
			committed := point == "queue_commit_before_receipt"
			if found != committed || (recordErr == nil) != committed || (receipt.Status == "committed") != committed {
				t.Fatalf("original API/queue/seal not one atomic outcome: point=%s queue=%v apiErr=%v human=%s", point, found, recordErr, receipt.Status)
			}
			if committed {
				var payload queuedPayload
				json.Unmarshal(entry.Payload, &payload)
				if !payload.FormalIntake || record.RequestID != reserve.Request || payload.OwnerDevice != b.Device {
					t.Fatal("formal source lost", payload, record)
				}
			} else if receipt.Status != "rejected_known_none" {
				t.Fatal("failure not fenced", receipt)
			}
			if point == "identity_change" {
				c.readIdentity.Store(identity)
			}
			h.handleMediaBatch(c, clientproto.Command{Kind: "media_batch_status", RequestID: "query_original_atomic", SessionID: s.ID, MediaBatchID: prepare.MediaBatchID})
			batch := h.media.data.Batches[mediaKey(b.Device, b.Generation, prepare.MediaBatchID)]
			wanted := "prepared"
			if committed {
				wanted = "admitted"
			}
			if batch.State != wanted {
				t.Fatal("media saga not exact original resolution", point, batch.State, wanted)
			}
			if e := h.messageQueue.RecoverPreparedHumanSeals(); e != nil {
				t.Fatal(e)
			}
			after, _ := h.messageQueue.ReadHumanOperation(b, seal.ID, "")
			if after.Status != receipt.Status {
				t.Fatal("cold fence rewrote original winner", receipt, after)
			}
			if point == "prepare_before_queue" {
				retry := seal
				retry.ID = "explicit_new_human_seal"
				h.handleHumanInput(c, humanTypedIntakeCommand(b, retry, images, nil, prepare.MediaBatchID))
				applied, e := h.messageQueue.ReadHumanOperation(b, retry.ID, "")
				if e != nil || applied.Status != "committed" {
					t.Fatal("explicit retry after precise known-none could not use original formal API business", applied, e)
				}
				original, _ := h.messageQueue.ReadHumanOperation(b, seal.ID, "")
				if original.Status != "rejected_known_none" {
					t.Fatal("replayed/reforged original failure")
				}
			}
		})
	}
}

func TestQARecheckStagedEditOfReadySuccessorKeepsOriginalActorContinuation(t *testing.T) {
	for _, transition := range []string{"config_activation", "initial_thread", "both"} {
		t.Run(transition, func(t *testing.T) {
			h, s, c, _ := mediaFixture(t)
			release := s.HoldQueue()
			defer func() { s.Close(); release() }()
			if transition != "initial_thread" {
				cfg := session.ConfigurationFrom(s.SettingsSnapshot())
				cfg.Model = "next"
				if _, e := s.SetFutureConfiguration(cfg, nil); e != nil {
					t.Fatal(e)
				}
			}
			actor, _ := h.mediaActor(c)
			b := messagequeue.HumanBinding{Device: "phone-a", Authority: h.cfg.InstanceID, Session: s.ID, Config: s.SettingsSnapshot().ConfigRevision, Epoch: h.SessionRoleEpoch(s), Generation: actor.NamespaceGeneration}
			reserveSeal := func(id, previous string) {
				reserve := messagequeue.HumanOperation{ID: "reserve_" + id, Kind: "reserve", Request: id, Content: "original " + id, IntentHash: strings.Repeat("d", 64), Assets: json.RawMessage(`[]`), Predecessor: previous}
				h.handleHumanInput(c, humanCoreCommand(b, reserve))
				waitForType(t, c, "queue_input_result")
				seal := messagequeue.HumanOperation{ID: "seal_" + id, Kind: "seal", Request: id, ExpectedRevision: 1, ContentRevision: 1, IntentHash: reserve.IntentHash, Content: reserve.Content, ReadyProof: reserve.IntentHash}
				h.handleHumanInput(c, humanCoreCommand(b, seal))
				receipt, e := h.messageQueue.ReadHumanOperation(b, seal.ID, "")
				if e != nil || receipt.Status != "committed" {
					t.Fatal(receipt, e)
				}
			}
			reserveSeal("r_sib_a", "")
			reserveSeal("r_sib_b", "r_sib_a")
			rows, _ := h.messageQueue.HumanInputs(b)
			var base messagequeue.HumanInput
			for _, v := range rows {
				if v.ID == "r_sib_b" {
					base = v
				}
			}
			rev, membership, _ := h.messageQueue.HumanMembership(b)
			boot, now := h.messageQueue.HumanClock()
			edit, e := h.messageQueue.HumanApply(b, messagequeue.HumanOperation{ID: "begin_sib_b", Kind: "begin_edit", Request: base.ID, ExpectedRevision: base.Revision, ContentRevision: base.ContentRevision, ExpectedHash: base.EffectiveHash, QueueRevision: &rev, MembershipHash: membership}, boot, now)
			if e != nil || edit.Status != "committed" {
				t.Fatal(edit, e)
			}
			data := mediaPNG(t)
			sum := sha256.Sum256(data)
			item := protocol.MediaBatchItem{ItemID: "stage_image", ItemRevision: 1, UploadRequestID: "vu_stage_b", UploadKind: "image", Name: "edit.png", MediaType: "image/png", SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
			lease := edit.Input.Lease
			raw, _ := json.Marshal(map[string]any{"binding": b, "stage": map[string]any{"request_id": base.ID, "content_revision": base.ContentRevision, "expected_hash": base.EffectiveHash, "lease_id": lease.ID, "lease_generation": lease.Generation}})
			cfg := b.Config
			prepare := clientproto.Command{Kind: "media_batch_prepare", RequestID: "prepare_staged_sib_b", SessionID: s.ID, MediaBatchID: "batch_edit_sib_b", MediaBatchRevision: 1, ExpectedConfigRevision: &cfg, Content: "edited B", MediaItems: []protocol.MediaBatchItem{item}, QueueInput: raw}
			h.handleMediaBatch(c, prepare)
			prepared := waitForType(t, c, "media_batch_receipt")
			if prepared["state"] != "prepared" {
				t.Fatal(prepared)
			}
			body := prepared["body_hash"].(string)
			calls := make(chan string, 3)
			h.SetExecutor(&qaR4Executor{fakeExec: &fakeExec{sink: h}, h: h, calls: calls, firstNative: transition != "config_activation"})
			release()
			select {
			case id := <-calls:
				if id != "r_sib_a" {
					t.Fatal(id)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("preceding A not consumed")
			}
			boot, now = h.messageQueue.HumanClock()
			if !h.messageQueue.HumanStageAllowed(b, base.ID, base.ContentRevision, base.EffectiveHash, lease.ID, lease.Generation, boot, now) {
				t.Fatal("staged base/lease no longer valid")
			}
			if transition != "config_activation" && !h.messageQueue.HumanThreadCompatible(b, s.ResumeID()) {
				t.Fatal("exact initial native acceptance unavailable")
			}
			c.uploads.initCommand(clientproto.Command{SessionID: s.ID, UploadRequestID: item.UploadRequestID, UploadKind: item.UploadKind, Name: item.Name, MediaType: item.MediaType, SizeBytes: item.SizeBytes, MediaBatchID: prepare.MediaBatchID, MediaBatchRevision: 1, MediaBodyHash: body, ItemID: item.ItemID, ItemRevision: 1, SHA256: item.SHA256})
			deadline := time.After(3 * time.Second)
			for {
				select {
				case raw := <-c.send:
					var event map[string]any
					json.Unmarshal(raw, &event)
					if event["type"] == "attachment_upload_ready" && event["upload_request_id"] == item.UploadRequestID {
						return
					}
					if event["type"] == "attachment_upload_error" && event["upload_request_id"] == item.UploadRequestID {
						t.Fatalf("valid staged edit of ready B lost %s original continuation: %v", transition, event)
					}
				case <-deadline:
					t.Fatal("no staged upload receipt")
				}
			}
		})
	}
}

func TestQARecheckTextFirstBatchToPreparedMediaOriginalSend(t *testing.T) {
	for _, origin := range []string{"ordinary", "formal_intake"} {
		t.Run(origin, func(t *testing.T) {
			h, s, c, _ := mediaFixture(t)
			release := s.HoldQueue()
			defer func() { s.Close(); release() }()
			cfg := session.ConfigurationFrom(s.SettingsSnapshot())
			cfg.Model = "next"
			if _, e := s.SetFutureConfiguration(cfg, nil); e != nil {
				t.Fatal(e)
			}
			actor, _ := h.mediaActor(c)
			b := messagequeue.HumanBinding{Device: "phone-a", Authority: h.cfg.InstanceID, Session: s.ID, Config: s.SettingsSnapshot().ConfigRevision, Epoch: h.SessionRoleEpoch(s), Generation: actor.NamespaceGeneration}
			first := messagequeue.HumanOperation{ID: "reserve_text_first", Kind: "reserve", Request: "r_text_first", Content: "text A", IntentHash: strings.Repeat("8", 64), Assets: json.RawMessage(`[]`)}
			h.handleHumanInput(c, humanCoreCommand(b, first))
			waitForType(t, c, "queue_input_result")
			firstSeal := messagequeue.HumanOperation{ID: "seal_text_first", Kind: "seal", Request: first.Request, ExpectedRevision: 1, ContentRevision: 1, IntentHash: first.IntentHash, Content: first.Content, ReadyProof: first.IntentHash}
			h.handleHumanInput(c, humanCoreCommand(b, firstSeal))
			receipt, e := h.messageQueue.ReadHumanOperation(b, firstSeal.ID, "")
			if e != nil || receipt.Status != "committed" {
				t.Fatal(receipt, e)
			}
			data := mediaPNG(t)
			sum := sha256.Sum256(data)
			assets, _ := json.Marshal([]map[string]any{{"kind": "image", "media_type": "image/png", "sha256": hex.EncodeToString(sum[:]), "name": "圖片.png", "size": len(data)}})
			reserve := messagequeue.HumanOperation{ID: "reserve_media_after_text", Kind: "reserve", Request: "r_" + strings.Repeat("5", 32), IntentHash: strings.Repeat("7", 64), Assets: assets, Predecessor: first.Request}
			if origin == "formal_intake" {
				reserve.Origin = origin
			}
			h.handleHumanInput(c, humanCoreCommand(b, reserve))
			waitForType(t, c, "queue_input_result")
			prepare, item, body := mediaPrepareItem(t, h, s, c, reserve.Request, data)
			calls := make(chan string, 3)
			h.SetExecutor(&qaR4Executor{fakeExec: &fakeExec{sink: h}, h: h, calls: calls, firstNative: true})
			release()
			select {
			case id := <-calls:
				if id != first.Request {
					t.Fatal(id)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("text first effect not consumed")
			}
			upload := mediaUploadItem(t, h, s, c, prepare, item, body, data)
			seal := messagequeue.HumanOperation{ID: "seal_media_after_text", Kind: "seal", Request: reserve.Request, ExpectedRevision: 1, ContentRevision: 1, IntentHash: reserve.IntentHash, ReadyProof: body}
			images := []backend.ImageAttachment{{AttachmentID: upload, MediaType: item.MediaType, SHA256: item.SHA256, SizeBytes: item.SizeBytes}}
			cmd := humanCoreCommand(b, seal)
			cmd.Images = images
			cmd.MediaBatchID = prepare.MediaBatchID
			if origin == "formal_intake" {
				cmd = humanTypedIntakeCommand(b, seal, images, nil, prepare.MediaBatchID)
			}
			h.handleHumanInput(c, cmd)
			receipt, e = h.messageQueue.ReadHumanOperation(b, seal.ID, "")
			if e != nil || receipt.Status != "committed" {
				t.Fatal("prepared successor seal lost original", receipt, e)
			}
			select {
			case id := <-calls:
				if id != reserve.Request {
					t.Fatal(id)
				}
			case <-time.After(3 * time.Second):
				entry, _, _ := h.messageQueue.GetEffective(s.ID, reserve.Request)
				t.Fatal("original media successor not consumed", entry.State, entry.Message)
			}
			entry, _, _ := h.messageQueue.GetEffective(s.ID, reserve.Request)
			var payload queuedPayload
			json.Unmarshal(entry.Payload, &payload)
			if payload.FormalIntake != (origin == "formal_intake") || payload.OwnerDevice != b.Device {
				t.Fatal("original input origin/owner changed", payload)
			}
			if origin == "formal_intake" {
				if _, e = h.messageQueue.TaskJournal().Lookup(context.Background(), qaR6Namespace(b), reserve.Request); e != nil {
					t.Fatal("formal receipt absent", e)
				}
			}
			done, e := h.media.begin()
			if e != nil {
				t.Fatal(e)
			}
			batch := h.media.data.Batches[mediaKey(b.Device, b.Generation, prepare.MediaBatchID)]
			if batch.Thread != "" || batch.SourceRevision != 0 || batch.BodyHash != body || batch.ExecutionThread != "native_first_exact" || batch.ExecutionSource != s.SettingsSnapshot().SourceRevision {
				done()
				t.Fatal("original prepare descriptor mutated or continuation unavailable", batch)
			}
			done()
		})
	}
}
